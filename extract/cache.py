"""
https://github.com/aabadie/joblib-s3
https://maxhalford.github.io/blog/python-daily-cache/
"""
import datetime as dt
from joblib import Memory
from joblibs3 import register_s3fs_store_backend
from aiobotocore.session import AioSession

register_s3fs_store_backend()

backend_options = dict(
    bucket='joblib',
    session=AioSession(profile='strats'),  # must have a strats profile for this project
)

memory = Memory('joblib_cache', backend='s3', verbose=1, compress=True, backend_options=backend_options)

def daily_cache_validation_callback(metadata):
    last_call_at = dt.datetime.fromtimestamp(metadata['time'])
    return last_call_at.date() == dt.date.today()

daily_cache = memory.cache(cache_validation_callback=daily_cache_validation_callback)
