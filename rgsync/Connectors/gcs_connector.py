# TODO: is the line below needed?
from rgsync.common import *
import json
from google.cloud import storage
from redisgears import executeCommand as execute

class GcsConnection:
    def __init__(self, bucketName):
        WriteBehindLog("GcsConnection:__init__: entered")
        self._bucketName = bucketName

    @property
    def bucketName(self):
        return self._bucketName() if callable(self._bucketName) else self._bucketName

    def Connect(self):
        from google.cloud import storage

        #WriteBehindLog('Connect: connecting db=%s keyspace=%s' % (self.db, self.keyspace))
        WriteBehindLog('Connect: connecting to GCS')
        storageClient = storage.Client()
        bucket = None

        try:
            bucket = storageClient.get_bucket(self.bucketName)
            WriteBehindLog('Connect: Connected')
        except Exception as e:
            msg = 'Connect: Failed connecting to GCS, bucket="%s", error="%s"' % self._bucketName, str(e)
            WriteBehindLog(msg)
            raise Exception(msg) from None

        # TODO: remove
        print(bucket)

        return bucket

class GcsConnector:
    def __init__(self, connection):
        WriteBehindLog("GcsConnector:__init__: entered")
        self.connection = connection
        self.bucket = None
        self.supportedOperations = [OPERATION_DEL_REPLICATE, OPERATION_UPDATE_REPLICATE]

    def PrepereQueries(self, mappings):
        # TODO: remove print
        WriteBehindLog("PrepereQueries: entered - doing nothing")
        return

    def TableName(self):
        # TODO: remove print
        WriteBehindLog("TableName: entered - returning empty string")
        return ""

    def PrimaryKey(self):
        # TODO: remove print
        WriteBehindLog("PrimaryKey: entered - returning empty string")
        return ""

    def WriteData(self, data):
        # TODO: remove print
        WriteBehindLog("WriteData: entered")

        if len(data) == 0:
            WriteBehindLog('WriteData: Warning, got an empty batch')
            return

        if not self.bucket:
            self.bucket = self.connection.Connect()

        try:
            # TODO: handle batch (need to separate between write and delete) - see here: https://stackoverflow.com/questions/45100483/batch-request-with-google-cloud-storage-python-client
            # TODO: do I need to keep only the last record for a key?
            print(data)
            for d in data:
                x = d['value']

                op = x.pop(OP_KEY, None)

                if op == OPERATION_DEL_REPLICATE:
                    # TODO: implement
                    WriteBehindLog("WriteData: delete not implemented yet")

                elif op == OPERATION_UPDATE_REPLICATE:
                    EXP_TIME_SEC = 60*60*24*3 # 3 days

                    key = x['key']
                    value = x['value']
                    WriteBehindLog(f"WriteData: writing... key={key}, value={value}")

                    # writing to gcs
                    blob = self.bucket.blob(key)
                    blob.upload_from_string(value,content_type='application/json')
                    
                    # setting expiration time
                    execute("EXPIRE", key, EXP_TIME_SEC)

                else:
                    msg = 'WriteData: Got unknown operation: "%s"' % op
                    WriteBehindLog(msg)
                    raise Exception(msg) from None

        except Exception as e:
            self.bucket = None # next time we will reconnect to the dtabase
            # TODO: fix print below
            msg = f'WriteData: Got exception when writing to GCS, error="{str(e)}".'
            WriteBehindLog(msg)
            raise Exception(msg) from None

    def DownloadBlob(self, key):
        if not self.bucket:
            self.bucket = self.connection.Connect()

        blob = self.bucket.blob(key)

        # throughs an exception if object not found
        contents = blob.download_as_string()

        WriteBehindLog(f"Downloaded storage object {key} from bucket {self.connection.bucketName} as the following string: {contents}.")

        return contents

