# Houses with land

This simple package scrapes [onthemarket.com](https://onthemarket.com) for houses which have land attached to them.

The results of this are dumped into the outputs folder and parsed to extract cost and acreage information, this is 
then stored as a parquet file although it could equally as easily be stored in a relational db at this point with
the pandas `to_sql` method.

The code makes heavy use of pythons `asyncio` since the task is highly IO bound. Python >= 3.8 is required to run this.

Some unit tests are written in pytest to ensure sanity of the parsing and map polygon division logic.

## Run the code

```bash
pip install -r requirements.txt # install requirments
# optionally configure environment variables to change settings see settings.py .e.g.
# export MILES_FROM_LONDON=250
python scrape_otm.py # run the scraper
python parse_otm.py # run the parser 
jupyter notebook visualize.ipynb # run the visualizations
# to run the tests
export PYTHONPATH=.
pytest
```
