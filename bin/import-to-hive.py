#!/usr/bin/python
import os, sys
import hashlib
import MySQLdb
import MySQLdb.cursors
from datetime import datetime
import shutil
import magic

sys.path.append('%s/../lib' %(os.path.dirname(__file__)))
import metahivesettings.settings

#from metahive.scanners mport *
import metahive.scanners

registeredScanners = []
regScan = {}
scannersByMimetype = {}
for name in metahive.scanners.__all__:
    plugin = getattr(metahive.scanners, name)
    try:
        register_plugin = plugin.register
    except AttributeError:
        print "Plugin %s does not have a register() function" %(name)
        pass
    else:
        supported_mimetypes = register_plugin()
        for mimetype in supported_mimetypes:
            if mimetype not in scannersByMimetype:
                scannersByMimetype[mimetype] = []
            scannersByMimetype[mimetype].append(name)
    registeredScanners.append(plugin)
    regScan[name] = plugin

db_credentials = metahivesettings.settings.db_credentials()

#print registeredScanners
#print scannersByMimetype

m=magic.open(magic.MAGIC_MIME_TYPE)
m.load()


def getMimeType(filename):
	try:
		result = m.file(filename)
	except Exception as e:
		result = False
		print repr(e)
	return result


if not db_credentials:
	print "No database credentials, cannot run."
	sys.exit(1)

try:
	db = MySQLdb.connect(user=db_credentials['db_username'], passwd=db_credentials['db_password'], db=db_credentials['db_name'], cursorclass=MySQLdb.cursors.DictCursor)
except Exception as e:
	print "Could not connect to SQL Server"
	print repr(e)
	sys.exit(2)

try:
	c = db.cursor()
except Exception as e:
	print "Could not acquire a DB cursor"
	print repr(e)
	sys.exit(3)

mimetype='image/gif'

if mimetype in scannersByMimetype:
    for plugin in scannersByMimetype[mimetype]:
        regScan[plugin].scan('foobar')

