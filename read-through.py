from redisgears import log
from google.cloud import storage
import json

NAME = 'READ_BEHIND'
BUCKET_NAME = "screenshots-raz-unbiased"
EXP_TIME_SEC = 60*60*24*3 # 3 days

# move to common python
def ReadBehindLog(msg, prefix='%s - ' % NAME, logLevel='notice'):
    msg = prefix + msg
    log(msg, level=logLevel)

def ReadBehindDebug(msg):
    ReadBehindLog(msg, logLevel='debug')

def download_blob(bucket_name, blob_name):
    storage_client = storage.Client()

    # throughs an exception if bucket not found
    bucket = storage_client.get_bucket(bucket_name)

    blob = bucket.blob(blob_name)
    # throughs an exception if object not found
    contents = blob.download_as_string()

    ReadBehindLog(f"Downloaded storage object {blob_name} from bucket {bucket_name} as the following string: {contents}.")

    return contents


async def read_through_handler(r: dict):
        ReadBehindLog(f"read_through_handler: entered r={r}")

        # this is how r looks like for keymiss on get for 'asdf' key:
        # {'event': 'keymiss', 'key': 'asdf', 'type': 'empty', 'value': None}
        key = r["key"]

        # download_blob will through an exception if object not found
        contents = download_blob(BUCKET_NAME, key)

        # storing the result, only if there was no exception (if object wasn't found for example)
        # and setting expiration to EXP_TIME_SEC
        exec_ret = execute("SET", key, contents, "ex", EXP_TIME_SEC)

        # if all well, return string "OK"
        ReadBehindLog(f"exec_ret is: {exec_ret}")

        ret2 = override_reply(contents)

        return r

GB('KeysReader').map(read_through_handler).register(commands=['get'], eventTypes=['keymiss'])


# TODO:
# for write-behind:
#  batch handling
#  handle only last entry with the same key (multiple set operations on the same key) - merge them or take the last one?
# prefix to match redisgears?
# exceptions handling if needed, maybe for debugging
# errors handling in execute and in override_reply
# unit tests
# read from configuration
# metrics
# liveness?