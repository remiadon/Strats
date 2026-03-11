import polars as pl
import yfinance_pl as yf
import dvc.api
import polars.selectors as cs
from functools import reduce
from common.io import dump

params = dvc.api.params_show()
tickers = params['tickers']

tickers = {ticker: yf.Ticker(ticker) for ticker in tickers}
dfs = {ticker: obj.history(period='10y', interval='1d').select('date', 'close.amount', 'volume') for ticker, obj in tickers.items()}
dfs = {ticker: _df for ticker, _df in dfs.items() if not _df.is_empty()}
dfs = {ticker: df.select(pl.col.date.dt.date(), cs.numeric().name.prefix(ticker + '.')) for (ticker, df) in dfs.items()}

df = reduce(lambda left, right: left.join(right, on='date', how='full', coalesce=True), dfs.values())
dump(df)
