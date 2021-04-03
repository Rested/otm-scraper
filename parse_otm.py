import json
import re
from datetime import datetime
from pathlib import Path
from typing import Optional, Union, Dict

import aiofiles
import asyncio
import pandas as pd
import bs4
from tqdm.asyncio import tqdm

from scrape_otm import PropertyLink
from settings import Settings


def _get_acres(soup: bs4.BeautifulSoup) -> Optional[float]:
    description = soup.find("section", class_="property-description")
    if not description:
        return None
    acres_matches = re.findall(
        r"([.\d]+)\s*acre", description.getText(), re.MULTILINE | re.IGNORECASE
    )
    try:
        return float(next(m for m in acres_matches if m != ".").strip())
    except StopIteration:
        pass

    hectares_abbreviated_matches = re.findall(
        r"([.\d]+)\s*Ha", description.getText(), re.MULTILINE
    )
    hectares_matches = re.findall(
        r"([.\d]+)\s*hectares", description.getText(), re.MULTILINE | re.IGNORECASE
    )
    try:
        return (
            float(
                next(
                    m
                    for m in hectares_matches + hectares_abbreviated_matches
                    if m != "."
                ).strip()
            )
            * 2.471052
        )
    except StopIteration:
        return None


def _get_price(soup: bs4.BeautifulSoup) -> Optional[int]:
    price_data = soup.find("span", class_="price-data")
    if not price_data:
        return None
    cost_txt = price_data.getText().strip("£ ").replace(",", "")
    try:
        return int(cost_txt)
    except ValueError:
        return None


async def extract_additional_property_details(
    property_link: PropertyLink, file_date: str
) -> Dict[str, Union[float, str, int]]:
    property_details = property_link.dict()

    async with aiofiles.open(
        Path(
            Settings().outputs_directory,
            "property_pages",
            file_date,
            f"{property_link.property_id}.html",
        ),
        "r",
    ) as f:
        html = await f.read()
    soup = bs4.BeautifulSoup(html, "html.parser")
    property_details["cost"] = _get_price(soup)
    property_details["acres"] = _get_acres(soup)
    return property_details


async def parse_otm():
    file_date = Settings().file_date or datetime.now().strftime("%Y-%m-%d")
    locations_file_path = Path(
        Settings().outputs_directory, "locations", f"{file_date}.json"
    )
    async with aiofiles.open(locations_file_path) as f:
        locations = [
            PropertyLink(**p) for p in json.loads(await f.read())["properties"]
        ]
    # parse the html pages and parse out some data and create a pandas dataframe from all the

    df = pd.DataFrame.from_records(
        await tqdm.gather(
            [
                extract_additional_property_details(property_link, file_date)
                for property_link in locations
            ]
        )
    )
    # here we could instead store this as sql with df.to_sql
    df.to_parquet(Path(Settings().outputs_directory, "parquet", f"{file_date}.parquet"))


if __name__ == "__main__":
    asyncio.run(parse_otm())
