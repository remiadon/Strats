import argparse
import urllib.request
import zipfile
import io
import polars as pl
import boto3

session = boto3.Session(profile_name="strats")
s3 = session.client("s3")
#creds = pl.CredentialProviderAWS(profile_name='strats', _storage_options_has_endpoint_url=True)
#pl.Config.set_default_credential_provider(creds)

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

parser = argparse.ArgumentParser()
parser.add_argument("-i", "--input", help="input file", type=str, nargs='+')
parser.add_argument("-o", "--output", help="output file", type=str)
args = parser.parse_args()

def write_df(df, key: str, bucket: str = 'default'):
    with io.StringIO() as csv_buffer:
        if isinstance(df, pl.DataFrame):
            df.write_csv(csv_buffer)
        else:
            df.to_csv(csv_buffer, index=False)

        response = s3.put_object(
            Bucket=bucket, Key=key, Body=csv_buffer.getvalue()
        )

        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

        if status == 200:
            print(f"Successful S3 put_object response. Status - {status}")
        else:
            print(f"Unsuccessful S3 put_object response. Status - {status}")