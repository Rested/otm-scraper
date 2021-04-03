from parse_otm import _get_acres, _get_price
import bs4


def get_soup(description="", price=""):
    return bs4.BeautifulSoup("""
<html>
    <body>
        <span class="price-data">{price}</span>
        <section class="property-description">
            <div>{description}</div>
        </section>
    </body>
</html>
""".format(description=description, price=price))


def test_get_acres():
    assert _get_acres(get_soup("the property sits on 100 acres")) == 100
    assert _get_acres(get_soup("the property sits on a 20 Acre plot")) == 20
    assert _get_acres(get_soup("the property comes with 4Ha of land")) == 9.884208
    assert _get_acres(get_soup("the property comes with 4 Ha of land")) == 9.884208
    assert _get_acres(get_soup("the property comes with 4 hectares of land")) == 9.884208


def test_get_cost():
    assert _get_price(get_soup(price="£3000000")) == 3_000_000
    assert _get_price(get_soup(price="£3,000,000")) == 3_000_000
    assert _get_price(get_soup(price="3,000,000")) == 3_000_000
    assert _get_price(get_soup(price="3000000")) == 3_000_000
    assert _get_price(get_soup(price="potato")) is None
