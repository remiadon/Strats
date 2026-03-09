import pandas as pd
import polars as pl
# FIXME cannot write using polars because scaleway says permission error.
# FIXME pl.scan_csv cannot be done due to HEAD operations not allowed by scaleway

from . import kw, dump

fxrates = pd.read_csv(
    'https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.zip?6b751cd0b6f02158a105213db574b6b0', 
    decimal='.', 
    encoding='utf-8',
    compression='zip'
    )
valid_cols = [col for col in fxrates.columns if len(col) == 3]

nulls = fxrates[valid_cols].isnull().sum() / len(fxrates)
valid_cols = nulls[nulls < .2].index
fxrates = fxrates[['Date'] + valid_cols.to_list()]
dump(pl.from_pandas(fxrates), kw.output)