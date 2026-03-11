import polars as pl
import polars.selectors as cs
import urllib.request
import zipfile
import io
import itertools
import os

from . import kw, import_from_gist

rich_df = import_from_gist('b0d32072f234fba73650eb4b1e9c0017', name='rich_df', files_head_member='rich_display_pandas_dataframe.py')

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


def dump(df: pl.DataFrame, output_path = kw.output, key=['date'], **kwargs):  # TODO : DQ checks should go there, with patito or dataframely
    import warnings
    if output_path is None:
        return
    if set(df.schema.values()) != {pl.Date, pl.Float64} and not key:
        warnings.warn("DataFrame schema does not match expected values. Expected all columns to be either Date or Float64.")
    if key:
        df = df.unique(subset=key).sort(by=key, **kwargs)
    print("\n\n", output_path, "\n\n")
    print(df.head(5))
    corrs = df.select(
        pl.corr(a, b, method='spearman').gt(kw.corr_threshold).alias(f"{a}<->{b}") 
        for a, b in itertools.combinations(cs.expand_selector(df, cs.numeric()), 2)
    ).transpose(include_header=True)
    #df.select(
    #    pl.all().null_count().truediv(pl.len()).name.suffix('.null_ratio'),
    #    pl.all().std().name.suffix('.std'),
    #).to_pandas().write_json(f'metrics/{os.path.basename(output_path).replace(".parquet", ".json")}', orient='index', indent=2)
    df.to_pandas().describe(percentiles=[]).to_json(f'metrics/{os.path.basename(output_path).replace(".parquet", ".json")}', orient='index', indent=2)
    print(f"HIGHLY CORRELATED\n", list(itertools.compress(*corrs)), "\n\n")
    df.write_parquet(output_path)
