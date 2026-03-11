from common import kw, import_from_gist
from common.io import dump
import polars as pl
import polars.selectors as cs
import functools

signature = import_from_gist("f26817064e1f769a32b53e55e00a20b7", "signature")

dfs = {source: pl.scan_parquet(source).select('date', cs.numeric()) for source in kw.input}
col_to_source = {col: source for source, df in dfs.items() for col in df.columns}

df = functools.reduce(lambda left, right: left.join(right, on='date', how="full", coalesce=True), dfs.values())
exprs = signature.signature(*map(pl.col, set(df.columns) - {'date'}), level=2)
print(f"extracted {len(exprs)} signatures out from {len(df.columns) - 1} columns")
exprs = {
    names: expr for names, expr in list(exprs.items())
    if len(names) == 1 or (col_to_source[names[0]] != col_to_source[names[1]])
}
print(f"narrowed down to {len(exprs)} signatures after discarding those from the same source")

feats = {",".join(k): v for k, v in exprs.items()}

print(f"final features: {list(feats.keys())[:50]}")

result, profile = df.sort('date').rolling(index_column="date", period=kw.period).agg(**feats)\
    .select('date', cs.list().list.get(0))\
        .profile(engine='streaming')

print("SIGNATURES\n", profile.select('node', total_time_seconds=(pl.col.end - pl.col.start) * 1e6))
print(result.shape)
dump(result)
