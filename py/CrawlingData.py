import yfinance as yf
import pendulum

gold_ticker = 'GC=F'
# [1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo]
interval_value = "5y"

start = pendulum.parse('2020-10-25 00:00').add(hours=11) #original timezone UTC-04:00
end = pendulum.parse('2024-10-29 15:30').add(hours=11)
gold_data = yf.download(tickers=gold_ticker, start=start, end=end)

gold_data.to_csv("D:\\Git\\BigData\\DoAn\\data\\GoldPrice.csv")