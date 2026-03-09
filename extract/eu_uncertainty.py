import polars as pl
from functools import reduce
import time
## monthly data

from . import kw, dump

#countries = ['France', 'UK', 'Germany', 'Italy', 'Spain', 'Russia', 'Netherlands', 'Sweden', 'Poland', 'Belgium']

eupu = pl.read_excel(
    'https://policyuncertainty.com/media/Europe_Policy_Uncertainty_Data.xlsx',
    infer_schema_length=10_000,
)
eupu = eupu.filter(pl.concat_list(pl.exclude('Year', 'Month')).list.drop_nulls().list.len() > 4)
eupu = eupu.select(
    pl.date('Year', 'Month', 1).alias('Date'),
    pl.exclude('Year', 'Month').name.suffix('_newspaper_policy_uncertainty'),
)
esg = pl.read_excel(
    'https://policyuncertainty.com/media/ESGUI_Indexes.xlsx', read_options=dict(header_row=2),
    infer_schema_length=10_000,
).select(
    pl.col.Date.str.to_date('%Y %b', strict=False),
    pl.exclude('^Global.*$').name.suffix('_esg_policy_uncertainty'),
).drop_nulls(subset='Date')

df = reduce(lambda x, y: x.join(y, on='Date', how='full', validate='1:1', coalesce=True), [eupu, esg]).sort('Date')
df = df.filter(pl.col('Date').dt.to_string() >= '2008-01-01')
dump(df, kw.output)
