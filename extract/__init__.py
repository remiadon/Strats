import argparse
import urllib.request
import zipfile
import io
import polars as pl

def select_no_nulls(df: pl.DataFrame, max_null_ratio=0.2) -> pl.DataFrame:
    """
    eg. ```df.pipe(select_no_nulls)```
    """
    valid = df.select(pl.all().is_null().sum() < max_null_ratio).to_dict()
    
    selection = []
    for (name, valid) in valid.items():
        if valid.item():
            selection.append(name)

    return df.select(selection)

def download_zip(url: str, csv_filename_in_zip: str) -> io.StringIO:
    # 1. Download ZIP
    with urllib.request.urlopen(url) as response:
        zip_data = response.read()

    # 2. Extract CSV from ZIP
    with zipfile.ZipFile(io.BytesIO(zip_data)) as zf:
        with zf.open(csv_filename_in_zip) as csv_file:
            content = csv_file.read().decode('utf-8')

    # 3. Return StringIO
    return io.StringIO(content)

def dump(df: pl.DataFrame, output_path: str):  # TODO : DQ checks should go there, with patito or dataframely
    import warnings
    if output_path is None:
        return
    if set(df.schema.values()) != {pl.Date, pl.Float64}:
        warnings.warn("DataFrame schema does not match expected values. Expected all columns to be either Date or Float64.")
    df = df.select(pl.col(pl.Date, pl.Float64))
    df.write_csv(output_path)


parser = argparse.ArgumentParser()
parser.add_argument("-i", "--input", help="input file", type=str, nargs='+')
parser.add_argument("-o", "--output", help="output file", type=str)
kw = parser.parse_args()
