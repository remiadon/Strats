import polars as pl
from functools import reduce

from . import kw, dump

#countries = ['France', 'UK', 'Germany', 'Italy', 'Spain', 'Russia', 'Netherlands', 'Sweden', 'Poland', 'Belgium']
g7 = ['US', 'UK', 'France', 'Germany', 'Italy', 'Japan', 'Canada']

eupu = pl.read_excel(
    'https://policyuncertainty.com/media/Europe_Policy_Uncertainty_Data.xlsx',
    infer_schema_length=10_000,
)
eupu = eupu.filter(pl.concat_list(pl.exclude('Year', 'Month')).list.drop_nulls().list.len() > 4)
eupu = eupu.select(
    pl.date('Year', 'Month', 1).alias('date'),
    pl.exclude('Year', 'Month').name.suffix('_policy_uncertainty'),
)
esg = pl.read_excel(
    'https://policyuncertainty.com/media/ESGUI_Indexes.xlsx', read_options=dict(header_row=2),
    infer_schema_length=10_000,
    columns=['Date', 'Global_GDP_Weighted', 'Global_Equal_Weighted'] + g7,
).select(
    pl.col.Date.str.to_date('%Y %b', strict=False).alias('date'),
    pl.exclude('Date').name.suffix('_esg_policy_uncertainty'),
).drop_nulls(subset='date')

# TODO :  add https://www.matteoiacoviello.com/gpr_files/data_gpr_export.xls
# eg. `df.select(a=pl.struct(pl.col('var_name', 'var_label').drop_nulls())).get_column('a')` to get column name mappings
# updated every month, but march is not even here and we are on the 9th

df = reduce(lambda x, y: x.join(y, on='date', how='full', validate='1:1', coalesce=True), [eupu, esg]).sort('date')
df = df.filter(pl.col('date').dt.to_string() >= '2008-01-01')
dump(df, kw.output)
