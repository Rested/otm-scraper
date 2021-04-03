import json
import os
import shutil
from pathlib import Path
from typing import Tuple, List

import aiohttp
import aiofiles
import asyncio
from aiohttp import ClientSession, ContentTypeError

from settings import Settings
from geopy.distance import distance
from pydantic import BaseModel
import polyline
from datetime import datetime

# london bridge
LONDON_LAT_LON = (51.508238335659364, -0.08742724405779281)
# on the market urls
BASE_LOCATIONS_URL = "https://www.onthemarket.com/map/show-pins/"
BASE_DETAILS_URL = "https://www.onthemarket.com/details/{property_id}"
# max locations returned by the show-pins api
MAX_PINS = 1000

PointLike = Tuple[float, float]
BoundingBox = Tuple[PointLike, PointLike, PointLike, PointLike]


class PropertyLink(BaseModel):
    property_id: str
    lat: float
    lon: float
    miles_from_london: float


def _get_initial_bounding_box(radius_miles: int) -> BoundingBox:
    """
    Simple brute force approach
    :return: tuple of points describing bounding box
    """
    lat_offset = 0
    while (
        distance(
            LONDON_LAT_LON, (LONDON_LAT_LON[0] - lat_offset, LONDON_LAT_LON[1])
        ).miles
        < radius_miles
    ):
        lat_offset += 0.01
    lon_offset = 0
    while (
        distance(LONDON_LAT_LON, (LONDON_LAT_LON[0], LONDON_LAT_LON[1] - lon_offset))
        < radius_miles
    ):
        lon_offset += 0.01

    return (
        (LONDON_LAT_LON[0] - lat_offset, LONDON_LAT_LON[1] - lon_offset),
        (LONDON_LAT_LON[0] + lat_offset, LONDON_LAT_LON[1] - lon_offset),
        (LONDON_LAT_LON[0] - lat_offset, LONDON_LAT_LON[1] + lon_offset),
        (LONDON_LAT_LON[0] - lat_offset, LONDON_LAT_LON[1] + lon_offset),
    )


def split_box(bounding_box: BoundingBox) -> Tuple[BoundingBox, BoundingBox]:
    """Split box into two along longest axis"""
    if distance(bounding_box[0], bounding_box[1]) > distance(
        bounding_box[0], bounding_box[2]
    ):
        halfway_latitude = bounding_box[0][0] + (
            (bounding_box[1][0] - bounding_box[0][0]) / 2
        )
        return (
            (
                (
                    bounding_box[0],
                    (halfway_latitude, bounding_box[1][1]),
                    bounding_box[2],
                    (halfway_latitude, bounding_box[3][1]),
                )
            ),
            (
                (
                    (halfway_latitude, bounding_box[0][1]),
                    bounding_box[1],
                    (halfway_latitude, bounding_box[2][1]),
                    bounding_box[3],
                )
            ),
        )
    halfway_longitude = bounding_box[0][1] + (
        (bounding_box[2][1] - bounding_box[0][1]) / 2
    )
    return (
        (
            bounding_box[0],
            bounding_box[1],
            (bounding_box[2][0], halfway_longitude),
            (bounding_box[3][0], halfway_longitude),
        ),
        (
            (bounding_box[0][0], halfway_longitude),
            (bounding_box[1][0], halfway_longitude),
            bounding_box[2],
            bounding_box[3],
        ),
    )


async def get_properties_within_area(
    session: aiohttp.ClientSession, bounding_box: BoundingBox
) -> List[PropertyLink]:
    attempts = 0
    # gentle backoff if we start receiving html instead of json (hit a rate limit)
    while True:
        await asyncio.sleep(Settings().sleep_time * (attempts + 1))
        response = await session.request(
            method="GET",
            url=BASE_LOCATIONS_URL,
            params={
                "search-type": "for-sale",
                "polygons0": polyline.encode(bounding_box),
                "prop-types": Settings().property_types,
            },
        )
        print(response.url)
        try:
            response_json = await response.json()
            break
        except ContentTypeError as e:
            if attempts > 10:
                raise e

    if len(response_json["properties"]) == MAX_PINS:
        print(
            "got back more than",
            MAX_PINS,
            "properties, splitting box of width",
            distance(bounding_box[0], bounding_box[1]).miles,
            "and height",
            distance(bounding_box[0], bounding_box[2]).miles,
            "into two",
        )
        # split the box
        return sum(
            await asyncio.gather(
                *[
                    get_properties_within_area(session, box)
                    for box in split_box(bounding_box)
                ]
            ),
            [],
        )
    return [
        PropertyLink(
            miles_from_london=float(dist),
            property_id=prop_location["id"],
            **prop_location["location"],
        )
        for prop_location in response_json["properties"]
        if (
            dist := distance(
                LONDON_LAT_LON,
                (prop_location["location"]["lat"], prop_location["location"]["lon"]),
            ).miles
        )
        < Settings().miles_from_london  # ignore if not within radius
    ]


async def get_all_property_locations() -> List[PropertyLink]:
    radius = Settings().miles_from_london
    initial_box = _get_initial_bounding_box(radius_miles=radius)
    connector = aiohttp.TCPConnector(limit=Settings().max_connections)
    async with ClientSession(connector=connector) as session:
        all_properties = await get_properties_within_area(session, initial_box)
        print("got back", len(all_properties), "properties")
        path_to_locations_file = Path(
            f"outputs/locations/{datetime.now().strftime('%Y-%m-%d')}.json"
        )
        with open(path_to_locations_file, "w") as f:
            json.dump(
                {
                    "property_types": Settings().property_types,
                    "properties": [p.dict() for p in all_properties],
                },
                f,
            )
    return all_properties


async def download_property_page(
    session: aiohttp.ClientSession, property_id: str, parent_dir: Path
) -> None:
    response = await session.request(
        method="GET", url=BASE_DETAILS_URL.format(property_id=property_id)
    )
    async with aiofiles.open(Path(parent_dir, f"{property_id}.html"), "w") as f:
        await f.write(await response.text())


async def scrape_otm():
    """scrape on the market"""
    # if an existing locations json exists use that
    file_date = Settings().file_date or datetime.now().strftime("%Y-%m-%d")

    if Settings().file_date:
        async with aiofiles.open(
            Path(Settings().outputs_directory, "locations", f"{file_date}.json")
        ) as f:
            locations = [
                PropertyLink(**p) for p in json.loads(await f.read())["properties"]
            ]
    else:
        # otherwise get one for today
        locations = await get_all_property_locations()
    pages_dir = Path(Settings().outputs_directory, "property_pages", file_date)
    os.makedirs(pages_dir, exist_ok=True)

    connector = aiohttp.TCPConnector(limit=Settings().max_connections)
    print("downloading raw html for all property pages")
    async with ClientSession(connector=connector) as session:
        await asyncio.gather(
            *[
                download_property_page(session, property_link.property_id, pages_dir)
                for property_link in locations
            ]
        )


if __name__ == "__main__":
    asyncio.run(scrape_otm())
