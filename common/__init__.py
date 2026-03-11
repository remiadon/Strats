import argparse
import types
from rich.console import Console
from rich.table import Table

console = Console()

parser = argparse.ArgumentParser()
parser.add_argument("-i", "--input", help="input file", type=str, nargs='+')
parser.add_argument("-o", "--output", help="output file", type=str)
parser.add_argument("-p", "--period", help="rolling period", type=str, default='1mo')
parser.add_argument("--corr-threshold", help="correlation threshold for reporting highly correlated features", type=float, default=0.6)
kw = parser.parse_args()

# -*- coding: utf-8 -*-
def load_gist(gist_id, files_head_member=None):
    """translate Gist ID to URL"""
    from json import load
    from urllib.request import urlopen

    gist_api = urlopen("https://api.github.com/gists/" + gist_id)

    files = load(gist_api)["files"]
    files_head_member = files_head_member or list(files.keys())[0]
    raw_url = files[files_head_member]["raw_url"]

    gist_src = urlopen(raw_url).read()
    return gist_src


def import_from_gist(gist_id: str, name: str, files_head_member=None):
    """import from Gist"""
    mod = types.ModuleType(name)
    code = load_gist(gist_id, files_head_member)
    exec(code, mod.__dict__)
    return mod

def print_df(df, title):
    table = Table(title=title)
    for col in df.columns:
        table.add_column(col, style="cyan", justify="left")

    for row in df.iter_rows():
        table.add_row(*[str(val) for val in row])

    console.print(table)
