#!/usr/bin/python
import ConfigParser
import sys

config = ConfigParser.ConfigParser()
try: 
	config.read('/etc/metahive/metahive.conf')
except Exception as e:
	print "Could not read config file - waaaaaagh"
	print repr(e)
	sys.exit(0)

def db_credentials():
	if not 'database' in config.sections():
		return False
	creds = {}
	for field in [ 'db_name', 'db_username', 'db_password' ]:
		try:
			foo = config.get('database', field)
			creds[field] = foo
		except:
			print "Could not find %s in database section of config file" %(field)
			creds[field] = False

	return creds

