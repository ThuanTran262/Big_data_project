import yfinance as yf
import datetime
from dateutil.relativedelta import relativedelta
from params import tables_params as tp

START_DATE = (datetime.datetime.now() + relativedelta(hours=-24)).strftime("%Y-%m-%d")
START_YEAR = (datetime.datetime.now()).strftime("%Y")
END_DATE = (datetime.datetime.now()+ relativedelta(hours=-24)).strftime("%Y-%m-%d")

def crawl_data_every_minute():
    data = yf.download(
        tickers=tp.GOLD_TICKER, interval=tp.INTERVAL_1_MINUTE, start=START_DATE
    )
    data.reset_index(inplace=True)
    data.insert(0, tp.SYMBOL_COLUMN_NAME, tp.GOLD_TICKER)
    data.columns = tp.COLUMNS_NAMES_LIST

    return


def crawl_data_every_day():
    current_date = datetime.datetime.now()
    current_time = current_date.hour

    if current_time >= 7:
        current_date = (current_date + relativedelta(hours=-24)).strftime("%Y-%m-%d")

    data = yf.download(tickers=tp.GOLD_TICKER, start=current_date)
    
    data.reset_index(inplace=True)
    data.insert(0, tp.SYMBOL_COLUMN_NAME, tp.GOLD_TICKER)
    data.columns = tp.COLUMNS_NAMES_LIST

    return data


def crawl_data_in_current_year():
    data = yf.download(tickers=tp.GOLD_TICKER, start=START_YEAR + "-01-01", end = END_DATE)
    data.reset_index(inplace=True)
    data.insert(0, tp.SYMBOL_COLUMN_NAME, tp.GOLD_TICKER)
    data.columns = tp.COLUMNS_NAMES_LIST

    return data
