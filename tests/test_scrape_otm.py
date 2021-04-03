from scrape_otm import _get_initial_bounding_box, LONDON_LAT_LON, split_box
from geopy.distance import distance


def test_get_initial_bounding_box():
    one_hundred_mile_box = _get_initial_bounding_box(radius_miles=100)
    assert len(one_hundred_mile_box) == 4 and all(len(x) == 2 for x in one_hundred_mile_box)  # it is a bounding box

    assert distance((one_hundred_mile_box[0][0], LONDON_LAT_LON[1]),
                    LONDON_LAT_LON).miles - 100 < 0.5  # box accurate to at least the nearest half mile


def test_split_box():
    assert split_box(((10, 10), (20, 10), (10, 20), (20, 20))) == (
        ((10, 10), (15.0, 10), (10, 20), (15.0, 20)),
        ((15.0, 10), (20, 10), (15.0, 20), (20, 20)))

    assert split_box(((15.0, 10), (20, 10), (15.0, 20), (20, 20))) == (
        ((15, 10), (20, 10), (15.0, 15.0), (20, 15.0)),
        ((15.0, 15.0), (20, 15.0), (15.0, 20), (20, 20)))