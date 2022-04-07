from xmlrpc.client import Boolean, boolean
from redisgears import executeCommand as execute
from rgsync.common import *
import json
import uuid
import traceback

BUCKET_NAME = "screenshots-raz-unbiased"
EXP_TIME_SEC = 60*60*24*3 # 3 days

skipNotification = False

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
			print(f"data type is {type(data)}")
			for d in data:
				print(f"d type is {type(data)}")
				x = d['value']
				print(f"x type is {type(data)}")

				op = x.pop(OP_KEY, None)

				if op == OPERATION_DEL_REPLICATE:
					# TODO: implement
					WriteBehindLog("WriteData: delete not implemented yet")

				elif op == OPERATION_UPDATE_REPLICATE:
					EXP_TIME_SEC = 60*60*24*3 # 3 days

					key = x['key']
					value = x['value']
					print(f"HERE: value type is: {type(value)}")

					WriteBehindLog(f"WriteData: writing... key={key}, value={value}")

					print("HERE: NOT SKIPPING BUT RETURNING!!!")
					return

					# writing to gcs
					blob = self.bucket.blob(key)
					blob.upload_from_string(value,content_type='application/json')
					WriteBehindLog(f"WriteData: Done... key={key}, value={value}")
					
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

		WriteBehindLog(f"Downloaded storage object {key} from bucket {self.connection.bucketName} as the following string: {str(contents)}.")

		return contents

connection = GcsConnection(BUCKET_NAME)

connector = GcsConnector(connection)

def CreateAddToStreamFunction(self):
	def func(r):
		print("CreateAddToStreamFunction:func:")
		traceback.print_stack()
		print("DONE TRACE")
		print(r)
		print(type(r))

		if skipNotification:
			print("CreateAddToStreamFunction: skipping...")
		
		data = []
		data.append(['key', r['key']])
		data.append(['value', r['value']])
		data.append([OP_KEY, OPERATION_UPDATE_REPLICATE])
		print(type(data))
		print(data)
		execute('xadd', self.GetStreamName(self.connector.TableName()), '*', *sum(data, []))
	return func

def CreateWriteDataFunction(connector, dataKey=None):
	def func(data):
		print("CreateWriteDataFunction:func:")
		print(data)

		connector.WriteData(data)

	return func

def RegistrationArrToDict(registration, depth):
	if depth >= 2:
		return registration
	if type(registration) is not list:
		return registration
	d = {}
	for i in range(0, len(registration), 2):
		d[registration[i]] = RegistrationArrToDict(registration[i + 1], depth + 1)
	return d

def UnregisterOldVersions(name, version):
	WriteBehindLog('Unregistering old versions of %s' % name)
	registrations = execute('rg.dumpregistrations')
	for registration in registrations:
		registrationDict = RegistrationArrToDict(registration, 0)
		# descStr = registrationDict['desc']
		# try:
		# 	desc = json.loads(descStr)
		# except Exception as e:
		# 	continue
		# if 'name' in desc.keys() and name in desc['name']:
		# 	WriteBehindLog('Version auto upgrade is not atomic, make sure to use it when there is not traffic to the database (otherwise you might lose events).', logLevel='warning')
		# 	if 'version' not in desc.keys():
		# 		execute('rg.unregister', registrationDict['id'])
		# 		WriteBehindLog('Unregistered %s' % registrationDict['id'])
		# 		continue
		# 	v = desc['version']
		# 	execute('rg.unregister', registrationDict['id'])
		# 	WriteBehindLog('Unregistered %s' % registrationDict['id'])
		print(f"registrationDict id: {registrationDict['id']}")

		execute('rg.unregister', registrationDict['id'])
		WriteBehindLog('Unregistered %s' % registrationDict['id'])

	WriteBehindLog('Unregistered old versions')

class RGWriteBase():
	def __init__(self, mappings, connector, name, version=None):
		UnregisterOldVersions(name, version)

		self.connector = connector
		self.mappings = mappings

		try:
			self.connector.PrepereQueries(self.mappings)
		except Exception as e:
			# cases like mongo, that don't implement this, silence the warning
			if "object has no attribute 'PrepereQueries'" in str(e):
				return
			WriteBehindLog('Skip calling PrepereQueries of connector, err="%s"' % str(e))

class RGWriteBehind(RGWriteBase):
	def __init__(self, GB, keysPrefix, mappings, connector, name, version=None,
				 onFailedRetryInterval=5, batch=100, duration=100, transform=lambda r: r, eventTypes=['set']):
		UUID = str(uuid.uuid4())
		self.GetStreamName = CreateGetStreamNameCallback(UUID)

		RGWriteBase.__init__(self, mappings, connector, name, version)

		## create the execution to write each changed key to stream
		descJson = {
			'name':'%s.KeysReader' % name,
			'version':version,
			'desc':'add each changed key with prefix %s:* to Stream' % keysPrefix,
		}

		GB('KeysReader', desc=json.dumps(descJson)).\
		map(transform).\
		foreach(CreateAddToStreamFunction(self)).\
		register(mode='sync', prefix='*', eventTypes=eventTypes, convertToStr=False)

		## create the execution to write each key from stream to DB
		descJson = {
			'name':'%s.StreamReader' % name,
			'version':version,
			'desc':'read from stream and write to DB table %s' % self.connector.TableName(),
		}
		GB('StreamReader', desc=json.dumps(descJson)).\
		aggregate([], lambda a, r: a + [r], lambda a, r: a + r).\
		foreach(CreateWriteDataFunction(self.connector)).\
		count().\
		register(prefix='_%s-stream-%s-*' % (self.connector.TableName(), UUID),
				 mode="async_local",
				 batch=batch,
				 duration=duration,
				 onFailedPolicy="retry",
				 onFailedRetryInterval=onFailedRetryInterval,
				 convertToStr=False)


objMappings = {}

RGWriteBehind(GB, keysPrefix='', mappings=objMappings, connector=connector, name='GcsWriteBehind',  version='99.99.99')

async def ReadThroughHandler(data: dict):
	WriteBehindLog(f"ReadThroughHandler: entered data={data}")

	# this is how data looks like for keymiss on get for 'asdf' key:
	# {'event': 'keymiss', 'key': 'asdf', 'type': 'empty', 'value': None}
	key = data["key"]

	# download_blob will through an exception if object not found
	contents = connector.DownloadBlob(key)

	# storing the result, only if there was no exception (if object wasn't found for example)
	# and setting expiration to EXP_TIME_SEC

	print(f"HERE: type={type(contents)}")
	
	# setting skipNotification to True so that the CreateAddToStreamFunction will know to ignore it.
	# TODO: this wasn't tested yet...
	# according to answer I got on discord this code is under redis lock and CreateAddToStreamFunction is called
	# under it
	global skipNotification
	with atomic():
		skipNotification = True
		exec_ret = execute("SET", key, contents, "ex", EXP_TIME_SEC)
		skipNotification = False

	# if all well, this call returns the string "OK"
	WriteBehindLog(f"exec_ret is: {exec_ret}. contents is: {contents}")

	print("HERE 31")
	override_reply(contents)
	print("HERE 32")

	# TODO: remove!!!
	data["value"] = contents

	return data

GB('KeysReader').map(ReadThroughHandler).register(commands=['get'], eventTypes=['keymiss'], mode="async_local")


# TODO:
# JS plugin in 6 months in 6 moths. write behind scenario will be available too, not sure about the time frame
# BUT!!! it will not support code changes - which means, that it will be availalbe only for the provided connectors
# for write-behind:
#  move to json type? or to rewrite rgsync code with the unneeded mapping and the unneeded hash, validations and prefix
#  retry policy
#  batch handling
#  handle only last entry with the same key (multiple set operations on the same key) - merge them or take the last one?
#  call from read-through to write-behind due to "set expiration" need to be skipped - implemented with the skipNotificatio. Not teste yet...
# exceptions handling if needed, maybe for debugging
# errors handling in execute and in override_reply
# read from configuration
# unit tests?
# metrics
# liveness?

# Here is what I got back on discord regarding loading pythin without deleting first - Not tested yet
# Yes, you can just not specify the requirement when you send the RedisGears function and install all the requirements and manage them
# yourself (instead of letting RedisGears manage it for you), you just need to see the path of the python interpreter RedisGears works with and
# manually install everything you need, you can also upgrade the code manually whenever you want. Notice that a restart will
# still be require but there will be no need to delete anything
# (see this reply on how to find the interpreter path: https://discord.com/channels/697882427875393627/732335407043444797/958980866640584734)

# loading redis server:
# ➜  /home/ramir/dev/junk/RedisGears git:(master) ✗ redis-server --loadmodule /home/ramir/dev/junk/RedisGears/bin/linux-x64-release/redisgears.so Plugin /home/ramir/dev/junk/RedisGears/bin/linux-x64-release/gears_python/gears_python.so
# deleting old rgsync and cloud storage before starting:
# ➜  /home/ramir/dev/junk/RedisGears git:(master) ✗ find . -name \*rgsync\*|xargs rm -rf;rm dump.rdb;rm -rf /home/ramir/dev/junk/RedisGears/bin/linux-x64-release/python3_99.99.99/google-cloud-storage;								   

# building rgsync:
# ➜  /home/ramir/dev/junk/rgsync git:(master) ✗ python3 -m build																																									   
# loading rgsync and code into redis server:
# ➜  /home/ramir/dev/junk/rgsync git:(master) ✗ gears-cli run examples/gcs/example.py --requirements examples/gcs/requirements.txt																	   

# hooks registration status and last error output:
# ➜  /home/ramir/dev/redisgears redis-cli RG.DUMPREGISTRATIONS																													  

# unregistering hooks:
# ➜  /home/ramir/dev/redisgears redis-cli RG.UNREGISTER 0000000000000000000000000000000000000000-XXXX