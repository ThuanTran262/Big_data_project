import yfinance as yf
import datetime
from dateutil.relativedelta import relativedelta
from params import tables_params as tp

# START_DATE = (datetime.datetime.now() + relativedelta(hours=-48)).strftime("%Y-%m-%d")
START_DATE = '2024-01-01'
START_YEAR = (datetime.datetime.now()).strftime("%Y")


def crawl_data_every_minute():
    data = yf.download(
        tickers=tp.GOLD_TICKER, interval=tp.INTERVAL_1_MINUTE, start=START_DATE
    )
    data.reset_index(inplace=True)
    data.insert(0, tp.SYMBOL_COLUMN_ID, tp.GOLD_TICKER)
    data.columns = tp.COLUMNS_NAMES_LIST

    return data


def crawl_data_every_day():
    data = yf.download(tickers=tp.GOLD_TICKER, start=START_DATE)
    data.reset_index(inplace=True)
    data.insert(0, tp.SYMBOL_COLUMN_ID, tp.GOLD_TICKER)
    data.columns = tp.COLUMNS_NAMES_LIST

    return data


def crawl_data_in_current_year():
    data = yf.download(tickers=tp.GOLD_TICKER, start=START_YEAR + "-01-01")
    data.reset_index(inplace=True)
    data.insert(0, tp.SYMBOL_COLUMN_ID, tp.GOLD_TICKER)
    data.columns = tp.COLUMNS_NAMES_LIST

    return data
