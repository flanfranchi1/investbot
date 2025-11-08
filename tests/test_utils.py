import config
import sys
import json
from bs4 import BeautifulSoup
import datetime
from pytest import MonkeyPatch

sys.path.append(str(config.BASE_DIR))
import utils

sys.path.append(str(config.BASE_DIR))


def test_date_range_lengths_and_format():
    class mocked_date(datetime.date):
        @classmethod
        def today(cls):
            return cls(2024, 12, 31)

    MonkeyPatch.setattr(utils.date, "utils.date", mocked_date)
    start, end = utils.date_range(12)
    assert isinstance(start, str) and isinstance(end, str)
    assert ("2024-12-31", "2019-12-31") == (start, end)


def test_snake_case_variants():
    assert utils.snake_case("Hello World") == "hello_world"
    assert utils.snake_case("a-b c") == "a_b_c"


def test_list_to_dict_and_pivoting():
    headers = ["a", "b"]
    data = [["1", "2"], ["3", "4"], ["bad"]]
    res = utils.list_to_dict(data, headers)
    assert isinstance(res, list)
    assert res == [{"a": "1", "b": "2"}, {"a": "3", "b": "4"}]

    pivot = {"a": [1, 3], "b": [2, 4]}
    pv = utils.pivoting_dict(pivot)
    assert pv == [{"a": 1, "b": 2}, {"a": 3, "b": 4}]


def test_group_tickers_by_dates_range():
    input_data = [
        {
            "ticker": "A",
            "first_missing_date": "2020-01-01",
            "last_missing_date": "2020-01-10",
        },
        {
            "ticker": "B",
            "first_missing_date": "2020-01-01",
            "last_missing_date": "2020-01-10",
        },
        {
            "ticker": "C",
            "first_missing_date": "2020-02-01",
            "last_missing_date": "2020-02-05",
        },
    ]
    grouped = utils.group_tickers_by_dates_range(input_data)
    assert ("2020-01-01", "2020-01-10") in grouped
    assert set(grouped[("2020-01-01", "2020-01-10")]) == {"A", "B"}


def test_save_dates_range_dict_as_json(tmp_path):
    data = {
        ("2020-01-01", "2020-01-10"): ["A", "B"],
        ("2020-02-01", "2020-02-05"): ["C"],
    }
    out = tmp_path / "out.json"
    utils.save_dates_range_dict_as_json(data, out)
    content = json.loads(out.read_text())
    assert content.get("A") == [["2020-01-01", "2020-01-10"]] or content.get("A") == [
        ("2020-01-01", "2020-01-10")
    ]


def test_parse_wikipedia_table_simple():
    html = """
    <table id="t1">
      <tr><th>Col A</th><th>Col B</th></tr>
      <tr><td>1</td><td>2</td></tr>
      <tr><td>3</td><td>4</td></tr>
    </table>
    """
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table")
    parsed = utils.parse_wikipedia_table(table)
    assert isinstance(parsed, list)
    assert parsed == [{"col_a": "1", "col_b": "2"}, {"col_a": "3", "col_b": "4"}]
