from kazoo.client import KazooClient
import json, os, random, string
import MySQLdb
import sys

def retrieveDbSettings(data):
	dbs = {}
	for db in data["servers"]:
		dbs[db["name"]] = {"host":db['host'], "port":db['port'], "user":db['user'], "password":db['password']}
	return dbs

def dbSetup(zk,data,zkNode):
	dbsProps = retrieveDbSettings(data)
	dbs = []
	for dbName in dbsProps:
		print "Setting up DB \'"+ dbName+"\'"
		db = dbsProps[dbName]
		addApiDb(dbName, db)
		jdbcString = "jdbc:mysql:replication://HOST:PORT,HOST:PORT/?characterEncoding=utf8&useServerPrepStmts=true&logger=com.mysql.jdbc.log.StandardLogger&roundRobinLoadBalance=true&transformedBitIsBoolean=true&rewriteBatchedStatements=true"
		jdbcString = jdbcString.replace("HOST", db["host"]).replace("PORT",str(db["port"]));
		del(db["host"])
		del(db["port"])
		db['jdbc'] = jdbcString
		db['name'] = dbName
		dbs.append(db)
	dbcpObj = {"dbs": dbs}
	zk.ensure_path(zkNode)
	zk.set(zkNode,json.dumps(dbcpObj))


def memcachedSetup(zk, data, zkNode):
	servers = []
	print "Setting up memcache servers"
	for server in data["servers"]:
		host = server['host']
		port = server['port']
		serverStr = host+":"+str(port)
		servers.append(serverStr)
        server_list=str(",".join(servers))
        zkNodeValueBuilder = {}
        zkNodeValueBuilder["servers"] = server_list
        zkNodeValueBuilder["numClients"] = 1
        zkNodeValue = json.dumps(zkNodeValueBuilder)
	zk.ensure_path(zkNode)
	zk.set(zkNode,zkNodeValue)

def addClientDb(clientName, dbSettings, consumer_details=None):

	js_consumer_key     = consumer_details['js_consumer_key']       if consumer_details != None and consumer_details.has_key('js_consumer_key')     else None
	all_consumer_key    = consumer_details['all_consumer_key']      if consumer_details != None and consumer_details.has_key('all_consumer_key')    else None
	all_consumer_secret = consumer_details['all_consumer_secret']   if consumer_details != None and consumer_details.has_key('all_consumer_secret') else None

	db = MySQLdb.connect(host=dbSettings["host"],
                     	user=dbSettings["user"],
                      passwd=dbSettings["password"])
	cur = db.cursor()
	dir = os.path.dirname(os.path.abspath(__file__))
	filename = os.path.join(dir, "../db-schema/mysql/client.sql")
	f = open(filename, 'r')
	query = " ".join(f.readlines())
	numrows = cur.execute("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = \'"+clientName+"\'")
	if numrows < 1:
		cur.execute("CREATE DATABASE "+clientName)
		cur.execute("USE "+clientName)
		cur.execute(query)
		more = True
		while more:
			more = cur.nextset()
	else:
		print("Client \'"+clientName+"\' has already been added to the DB")
	cur.execute("USE API")
	numrows = cur.execute("SELECT * FROM CONSUMER WHERE SHORT_NAME=\'"+clientName+"\' and SCOPE=\'js\'")
	if numrows < 1:
		consumer_key = js_consumer_key if js_consumer_key != None else generateRandomString()
		print "Adding JS consumer key for client \'"+clientName +"\' : \'"+consumer_key+"\'"
		cur.execute("INSERT INTO `CONSUMER` (`consumer_key`, `consumer_secret`, `name`, `short_name`, `time`, `active`, `secure`, `scope`) VALUES (\'"+consumer_key+"\', '',\'"+clientName+"\',\'"+ clientName+"\',CURRENT_TIMESTAMP(), 1, 0, 'js')")
	else:
		print "JS Consumer key already added for client \'"+clientName+"\'"
	numrows = cur.execute("SELECT * FROM CONSUMER WHERE SHORT_NAME=\'"+clientName+"\' and SCOPE=\'all\'")
	if numrows < 1:
		consumer_key    = all_consumer_key      if all_consumer_key != None     else generateRandomString()
		consumer_secret = all_consumer_secret   if all_consumer_secret != None  else generateRandomString()
		print "Adding REST API key for client \'"+clientName +"\' : consumer_key=\'"+consumer_key+"\' consumer_secret=\'"+consumer_secret+"\'"
		cur.execute("INSERT INTO `CONSUMER` (`consumer_key`, `consumer_secret`, `name`, `short_name`, `time`, `active`, `secure`, `scope`) VALUES (\'"+consumer_key+"\',\'"+consumer_secret+"\',\'"+clientName+"\',\'"+ clientName+"\',CURRENT_TIMESTAMP(), 1, 0, 'all')")
	else:
		print "REST API key already added for client \'"+clientName+"\'"

def addApiDb(dbName, dbSettings):

	db = MySQLdb.connect(host=dbSettings["host"],
                     	user=dbSettings["user"],
                      passwd=dbSettings["password"])
	cur = db.cursor()
	dir = os.path.dirname(os.path.abspath(__file__))
	filename = os.path.join(dir, "../db-schema/mysql/api.sql")
	f = open(filename, 'r')
	query = " ".join(f.readlines())
	numrows = cur.execute("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = \'api\'")
	if numrows < 1:
		print "Adding api DB to MySQL DB \'"+dbName+"\'"
		cur.execute(query)
	else:
		print "API DB has already been added to the MySQL DB \'"+dbName+"\'"

def clientSetup(zk, client_data, db_data, zkNode, consumer_details=None):
	dbs= retrieveDbSettings(db_data)
	for client in client_data:

		print "Adding client \'"+client['name']+"\'"
		dbname = client['db']
		if client['db'] is None:
			dbname = dbs.keys()[0]
		addClientDb(client['name'],dbs[dbname], consumer_details)
		clientNode = zkNode + "/" + client['name']
		zk.ensure_path(clientNode)
		clientNodeValue = {"DB_JNDI_NAME":dbname}
		zk.set(clientNode,json.dumps(clientNodeValue))
		for setting in client:
			if setting != "name" and setting != "db":
				zk.ensure_path(clientNode + "/" + setting)
				zk.set(clientNode + "/" + setting, str(client[setting]))

def generateRandomString():
	return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(20))
