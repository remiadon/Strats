import polars as pl
import polars.selectors as cs
import functools

from itertools import product
from math import factorial
from typing import Mapping, Tuple

import polars as pl
import polars.selectors as cs


# see https://gist.github.com/remiadon/f26817064e1f769a32b53e55e00a20b7 for testing
def signature(*exprs: pl.Expr, level: int = 2, index_column: str = None) -> Mapping[Tuple[str], pl.Expr]:
    """
    Computes path signature components up to any arbitrary level.
    Uses only Polars Expressions and aligns with the piecewise linear assumption.
    """
    assert all((expr.meta.is_column() for expr in exprs)), "all elements of `exprs` must be columns, not selectors of any kind"
    #assert not (logsig and level > 3), "logsig only works up to level 3 for now"

    get_name = lambda e: e.meta.output_name()
    interpolate = lambda e: e.interpolate(method='linear') if index_column is None else e.interpolate_by(by=index_column)
    
    # 1. Prepare Base Increments (dX)
    incs = {get_name(e): interpolate(e).forward_fill().diff().fill_null(0) for e in exprs}
    
    # 2. Key Mapping (sigs): 
    # Keys are tuples of indices: (0, 1) 
    # Values are Polars Expressions
    sigs = {}

    # --- Level 1 ---
    for expr in exprs:
        sigs[(get_name(expr),)] = incs[get_name(expr)].cum_sum()

    # --- Higher Levels (2 to level) ---
    for d in range(2, level + 1):
        for word in product(map(get_name, exprs), repeat=d):
            # The "Local Integral" of the segment itself: (1/d!) * dX_1 * dX_2 * ... * dX_d
            local_integral = pl.lit(1 / factorial(d))
            for idx in word:
                local_integral = local_integral * incs[idx]

            # The "Recursive Update": S_new = S_old + (Iterative Prefix Sums) + local_integral
            # We follow the formula: Delta S_w = Sum_{split} [ S_prefix_old * Delta S_suffix ]
            term = local_integral
            
            for split in range(1, d):
                prefix_word = word[:split]
                suffix_word = word[split:]
                
                # S_old of the prefix (value before this current jump)
                s_prefix_prev = sigs[prefix_word].shift(1).fill_null(0)
                
                # Delta S of the suffix (the local integral of the remaining indices)
                delta_s_suffix = pl.lit(1 / factorial(len(suffix_word)))
                for idx in suffix_word:
                    delta_s_suffix = delta_s_suffix * incs[idx]
                
                term = term + (s_prefix_prev * delta_s_suffix)
            
            sigs[word] = term.cum_sum()

    return {k: v.tail(1) for k, v in sigs.items()}


if __name__ == '__main__':
    from common.io import dump, get_sources
    import polars as pl
    import dvc.api
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--sources", help="see params.yaml", type=str, nargs='+', required=True)
    parser.add_argument("-o", "--output", help="output file", type=str, required=True)
    parser.add_argument("-g", "--group-by", help="group key", type=str, default=None)
    parser.add_argument("-p", "--period", type=str, default='1q')
    kw = parser.parse_args()
    params = dvc.api.params_show()

    index = {'date', kw.group_by or []}

    dfs = get_sources(kw.sources)
    col_to_source = {col: source for source, df in dfs.items() for col in df.columns}
    df = functools.reduce(
        lambda left, right: left.join(right, on='date', how="full", coalesce=True), 
        dfs.values(),
    ).lazy()
    df = df.drop_nulls(subset=list(index)).select(*index, cs.numeric().cast(pl.Float64))
    exprs = signature(*map(pl.col, set(df.collect_schema().names()) - index), level=2, index_column='date')  # test with level=3
    print(f"extracted {len(exprs)} signatures out from {len(df.columns) - 1} columns")
    exprs = {
        names: expr for names, expr in list(exprs.items())
        if len(names) == 1 or (col_to_source[names[0]] != col_to_source[names[1]])
    }
    print(f"narrowed down to {len(exprs)} signatures after discarding those from the same source")

    feats = {f'S({",".join(k)})': v for k, v in exprs.items()}

    print(f"final features: {list(feats.keys())[:50]}")

    result, profile = df.sort('date').rolling(group_by=kw.group_by, index_column="date", period=kw.period).agg(**feats)\
        .with_columns(cs.list().list.get(0))\
            .profile(engine='streaming')
    print("SIGNATURES\n", profile.select('node', total_time_seconds=(pl.col.end - pl.col.start) * 1e6))
    print(result.shape)
    dump(result, output_path=kw.output, key=index)
