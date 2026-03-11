import polars as pl
import itertools
from . import kw, dump, download_zip

fxrates = pl.read_csv(
    download_zip('https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.zip?6b751cd0b6f02158a105213db574b6b0'),
    decimal_comma=False,
    encoding='utf-8',
    infer_schema_length=10_000,
)

valid_curr = itertools.compress( # filter out currency columns with more than 20% nulls
    *fxrates.select(pl.col('^[A-Z]{3}$').is_null().sum().truediv(pl.len()) < .2).transpose(include_header=True)
)
dump(fxrates.select('Date', *valid_curr), kw.output, key='Date')
