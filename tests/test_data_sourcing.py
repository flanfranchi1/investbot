import pandas as pd

from data_sourcing import converting_list_of_dicts_to_dataframe, fetch_historical_data


def test_converting_list_of_dicts_to_dataframe_empty():
    df = converting_list_of_dicts_to_dataframe([])
    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_converting_list_of_dicts_to_dataframe_values():
    data = [{"first_date": "2020-01-01", "last_date": "2020-01-10", "ticker": "A"}]
    df = converting_list_of_dicts_to_dataframe(data)
    assert not df.empty
    assert pd.api.types.is_datetime64_any_dtype(df["first_date"])


def test_fetch_historical_data_no_tickers(monkeypatch):
    res = fetch_historical_data([], "2020-01-01", "2020-01-10")
    assert res is None


def test_fetch_historical_data_mock(monkeypatch):
    # Mock yfinance download to return an empty DataFrame
    class Dummy:
        empty = True

        def stack(self, level):
            return self

        def rename_axis(self, **kwargs):
            return self

        def reset_index(self):
            return self

    def fake_download(tickers, start, end):
        return pd.DataFrame()

    monkeypatch.setattr("data_sourcing.yf.download", fake_download)
    res = fetch_historical_data(["AAPL"], "2020-01-01", "2020-01-02")
    assert res is None
