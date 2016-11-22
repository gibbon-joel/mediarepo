#!/usr/bin/python
import os, sys
import hashlib
import MySQLdb
import MySQLdb.cursors
from datetime import datetime
import time
import shutil
import magic
import argparse

sys.path.append('%s/../lib' %(os.path.dirname(__file__)))
import metahivesettings.settings

#from metahive.scanners mport *
import metahive.scanners

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
    regScan[name] = plugin

db_credentials = metahivesettings.settings.db_credentials()

#print registeredScanners
#print scannersByMimetype


parser = argparse.ArgumentParser()
parser.add_argument("-s", "--sourcedir", help="Top level directory to work on (e.g. /path/to/upload/folder", required=True)
parser.add_argument("-v", "--verbose", help="Be verbose (more debug output)", required=False, default=False)
args = parser.parse_args()


m=magic.open(magic.MAGIC_MIME_TYPE)
m.load()


def hash_file(filename, hashtype='sha1'):
    BUF_SIZE=1024*1024 # to read files (and compute incremental hash) in 1MB blocks, not having to read in 2TB file at once...
    if hashtype == 'sha1':
        sha1 = hashlib.sha1()
        with open(filename, 'rb') as f:
            while True:
                data = f.read(BUF_SIZE)
                if not data:
                    break
                sha1.update(data)
        hexhash = sha1.hexdigest()

    return hexhash


def getMimeType(filename):
    try:
        result = m.file(filename)
    except Exception as e:
        result = False
        print repr(e)
    return result

def hash_filenames_array(filenames_array):
    begin = time.time()
    fileHashes = {}
    for filename in filenames_array:
        fileHashes[filename] = hash_file(filename)
    finish = time.time()
    time_taken = finish - begin
    files_per_second = len(filenames_array) / float(time_taken)
    print "It took %0.2f seconds to hash %i files (%0.1f files per second)" %(time_taken, len(filenames_array), files_per_second)
    return fileHashes


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


filesByMimetype = {}
debugcount = 0
for (dirpath, dirnames, filenames) in os.walk(args.sourcedir, topdown=True, onerror=None, followlinks=False):
    if filenames:
        print "Working on directory %s" %(dirpath)
        for filename in filenames:
            fullfilename = '%s/%s' %(dirpath, filename)
            try:
                mimetype = getMimeType(fullfilename)
            except Exception as e:
                print "Could not detect MIME type for %s" %(fullfilename)
                mimetype = None
                continue
            if mimetype not in filesByMimetype:
                filesByMimetype[mimetype] = []
            filesByMimetype[mimetype].append(fullfilename)

            debugcount += 1
            if debugcount > 20:
                print "*** DEBUG: breaking after 20 files ***"
                break

for mimetype in filesByMimetype:
    if mimetype in scannersByMimetype:

        # supported file (we have at least one scanner that can give us metadata), so hash it...
        fileHashes = hash_filenames_array(filesByMimetype[mimetype])

        # check whether we have data already in SQL; figure out whether we need to import & delete... etc.
        # TODO

        # iterate over registered metadata scanners for the current mimetype
        for plugin in scannersByMimetype[mimetype]:
            begin = time.time()
            metadata = regScan[plugin].scanBulk(filesByMimetype[mimetype])
            finish = time.time()
            time_taken = finish - begin
            if time_taken <= 0:
                files_per_second = -1  # avoid division by zero
            else:
                files_per_second = len(filesByMimetype[mimetype]) / float(time_taken)
            print "plugin %s took %0.2f seconds to parse %i files (%0.1f files per second)" %(plugin, time_taken, len(filesByMimetype[mimetype]), files_per_second)
            for filename, metaDict in metadata.iteritems():
                print "%s: %s: %s" %(filename, fileHashes[filename], metaDict)
    else:
        if args.verbose:
            print "There is no plugin to handle mimetype %s." %(mimetype)
            print filesByMimetype[mimetype]
            print "--"

