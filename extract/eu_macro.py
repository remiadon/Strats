import polars as pl
import pandas as pd

from . import args, storage_options, write_df

exchange_rates = pd.read_csv(
    'https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.zip?6b751cd0b6f02158a105213db574b6b0', 
    decimal='.', 
    encoding='utf-8',
    compression='zip'
    )
valid_cols = [col for col in exchange_rates.columns if len(col) == 3]

nulls = exchange_rates[valid_cols].isnull().sum() / len(exchange_rates)
valid_cols = nulls[nulls < .2].index
exchange_rates = exchange_rates[['Date'] + valid_cols.to_list()]

write_df(exchange_rates, args.output)
#pl.from_pandas(exchange_rates).write_csv(args.output, storage_options=storage_options)
#exchange_rates.to_csv(args.output)

print(exchange_rates)