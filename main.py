# -*- coding: utf-8 -*-
import types

def load_gist(gist_id):
    """translate Gist ID to URL"""
    from json import load
    from urllib.request import urlopen

    gist_api = urlopen("https://api.github.com/gists/" + gist_id)

    files = load(gist_api)["files"]
    files_head_member = list(files.keys())[0]
    raw_url = files[files_head_member]["raw_url"]

    gist_src = urlopen(raw_url).read()
    return gist_src


def import_from_gist(gist_id: str, name: str):
    """import from Gist"""
    mod = types.ModuleType(name)
    code = load_gist(gist_id)
    exec(code, mod.__dict__)
    return mod


if __name__ == "__main__":
    gist_id = "f26817064e1f769a32b53e55e00a20b7"
    signature = import_from_gist(gist_id, "signature")
    print(signature.pl.Expr)
