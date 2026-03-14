import polars as pl
import polars.selectors as cs
import yfinance_pl as yf
import urllib.request
import zipfile
import io
import itertools
import json

#rich_df = import_from_gist('b0d32072f234fba73650eb4b1e9c0017', name='rich_df', files_head_member='rich_display_pandas_dataframe.py')

def download_zip(url: str, csv_filename_in_zip: str = None) -> io.StringIO:
    # 1. Download ZIP
    with urllib.request.urlopen(url) as response:
        zip_data = response.read()

    # 2. Extract CSV from ZIP
    with zipfile.ZipFile(io.BytesIO(zip_data)) as zf:
        with zf.open(csv_filename_in_zip or zf.namelist()[0]) as csv_file:
            content = csv_file.read().decode('utf-8')

    # 3. Return StringIO
    return io.StringIO(content)


def dump(df: pl.DataFrame, output_path: str, key=['date'], **kwargs):  # TODO : DQ checks should go there, with patito or dataframely
    import warnings
    if output_path is None:
        return
    if set(df.schema.values()) != {pl.Date, pl.Float64} and not key:
        warnings.warn("DataFrame schema does not match expected values. Expected all columns to be either Date or Float64.")
    if key:
        df = df.unique(subset=key).sort(by=key, **kwargs).set_sorted('date')
    print("\n\n", output_path, "\n\n")
    print(df.head(5))
    with open(output_path.replace('data/', 'metrics/').replace(".parquet", ".json"), 'w') as f:  # TODO write to stderr and redict
        json.dump(df.select( # METRICS FOR DVC
            min=pl.struct(pl.col.date.dt.to_string().min()),
            std=pl.struct(cs.numeric().std()), 
            null_ratios=pl.struct(pl.all().null_count().truediv(pl.len())),
            zero_ratios=pl.struct(cs.numeric().sub(0.0).abs().lt(0.001).sum().truediv(pl.len())),
            n_points=pl.len(),
        ).to_dicts()[0], f, indent=2)
    df.write_parquet(output_path)

def get_stocks(tickers: list[str]):
    return pl.concat([
        yf.Ticker(ticker).history(period='10y', interval='1d').select(pl.col.date.dt.date(), pl.lit(ticker).alias('ticker'), 'close.amount', 'volume') 
        for ticker in tickers
    ]) # TODO mcp server 

def execute_polars(source: str, **kwargs):
    import dvc.api
    from functools import reduce
    _locals = {'cs': cs, 'pl': pl, 'yf': yf, 'get_stocks': get_stocks, 'reduce': reduce, **kwargs, **dvc.api.params_show()}
    return eval(source, _locals)


def get_sources(names: tuple[str] = None) -> dict[str, pl.DataFrame]:
    import dvc.api
    params = dvc.api.params_show()
    if names is None:
        cands = params['bronze']
    else:
        cands = {k: v for k, v in params['bronze'].items() if k in names}
    sources = dict()
    for name, _map in cands.items():
        url = _map.get('url', '')
        if url.endswith('.xlsx'):
            source = pl.read_excel(url, infer_schema_length=10_000)
        elif '.zip' in url:
            source = pl.read_csv(
                download_zip(url),
                decimal_comma=False,
                encoding='utf-8',
                infer_schema_length=10_000,
            )
        sources[name] = execute_polars(_map['polars'], self=source)
    return sources

if __name__ == '__main__':
    import polars as pl
    import dvc.api
    import argparse
    import itertools

    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--dataset", help="input file", type=str)
    parser.add_argument("-o", "--output", help="output file", type=str)
    parser.add_argument("--corr-threshold", type=float, default=0.7)
    kw = parser.parse_args()
    params = dvc.api.params_show()

    # TODO
    #get_tokens = lambda s: set(re.split(r'\[|\]|,\s', s))
    #dataset_tokens = get_tokens(params['silver'][dataset]['polars'])
    #cands = {name: cand for name, cands in params['bronze'].items() if get_token(cand).intersection(dataset_tokens)}
    sources = get_sources() # FIXME this gets all sources for now
    df = execute_polars(params['silver'][kw.dataset]['polars'], **sources)
    if kw.corr_threshold is not None:
        corrs = df.select(
            pl.corr(a, b, method='spearman').gt(kw.corr_threshold).alias(f"{a}<->{b}") 
            for a, b in itertools.combinations(cs.expand_selector(df, cs.numeric()), 2)
        ).transpose(include_header=True)
        print(f"HIGHLY CORRELATED\n", list(itertools.compress(*corrs)), "\n\n")
    dump(df, output_path='data/'+ kw.dataset + '.parquet')
