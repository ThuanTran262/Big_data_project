import yfinance as yf
import datetime
from dateutil.relativedelta import relativedelta
from params import gold_params as gp



START_DATE = (datetime.datetime.now() + relativedelta(hours=-7)).strftime("%Y-%m-%d")


START_YEAR = (
    datetime.datetime.now()+ relativedelta(hours=-7)
).strftime("%Y")


def crawl_data_every_minute():
    return yf.download(
        tickers=gp.GOLD_TICKER,
        interval=gp.INTERVAL_1_MINUTE,
        start=START_DATE
    )


def crawl_data_every_day():
    return yf.download(tickers=gp.GOLD_TICKER, start=START_DATE)


def crawl_data_in_current_year():
    return yf.download(tickers=gp.GOLD_TICKER, start=START_YEAR + "-01-01")

gold_data = crawl_data_every_day()
gold_data.to_csv("D:\\Git\\BigData\\DoAn\\Big_data_project\\data\\EveryDayGoldPrice.csv")
