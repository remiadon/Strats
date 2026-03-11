import polars as pl
import yfinance_pl as yf
import dvc.api
import polars.selectors as cs
from functools import reduce

from . import kw, dump

params = dvc.api.params_show()
tickers = params['tickers']

tickers = {ticker: yf.Ticker(ticker) for ticker in tickers}
dfs = {ticker: obj.history(period='max', interval='1d') for ticker, obj in tickers.items()}
dfs = {ticker: df.select(pl.col.date.dt.date(), cs.numeric().name.prefix(ticker + '.')) for (ticker, df) in dfs.items()}

df = reduce(lambda left, right: left.join(right, on='date', how='outer', coalesce=True), dfs.values())
dump(df, kw.output)
