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
parser.add_argument("-v", "--verbose", help="Be verbose (more debug output)", required=False, default=False, action='store_true')
parser.add_argument("-c", "--copy-to-repo", help="copy scanned supported files into the media repository", required=False, default=False, action='store_true', dest='copy_to_repo')
parser.add_argument("-d", "--delete-original", help="delete original/duplicate files if we have a copy in the media repository", required=False, default=False, action='store_true', dest='delete_original')
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

def gatherBasicInfo(filenames_array):
    """
    This will collect "basic" OS-level information like ctime, mtime, size etc.
    It expects an array of filenames (with full path information) and will return a dict with the
    full filename as key and the basic info as a dict.

    input: [ '/path/to/file/1.jpg', '/path/to/file/2.jpg' ]
    output: { '/path/to/file/1.jpg' : { 'hash.sha1': '...', 'ctime': '...' }, ... }
    """
    begin = time.time()
    fileInfo = {}
    for filename in filenames_array:
        try:
            info = os.stat(filename)
        except:
            print "Could not stat file '%s'" %(filename)
        else:
            file_mtime = datetime.fromtimestamp(info.st_mtime)
            file_ctime = datetime.fromtimestamp(info.st_ctime)
            fileInfo[filename] = {
                    'ctime': file_ctime,
                    'mtime': file_mtime,
                    'size': info.st_size,
                    }

    finish = time.time()
    time_taken = finish - begin
    files_per_second = len(filenames_array) / float(time_taken)
    print "It took %0.2f seconds to gather basic info for %i files (%0.1f files per second)" %(time_taken, len(filenames_array), files_per_second)
    return fileInfo


def getRepoStateForFiles(filenames_dict):
    """
    Expects a dict of dicts (essentially, the output of "gatherBasicInfo"). Constructs SQL to check
    which of the files (if any) we have already in the database.
    """



    """
    As we do not want to hash everything again if it's known but not stored in the repo, we will
    rely on os.stat + filename as a rough initial check, only hashing if we do not find an exact match...
    """
    for filename, filedata in filenames_dict.iteritems():
        sql = "SELECT id, sha1, file_size, original_ctime as ctime, original_mtime as mtime, is_in_repo FROM files WHERE file_size=%s and original_ctime=%s and original_mtime=%s and original_filename=%s"

        numHits = c.execute ( sql, [ filedata['size'], filedata['ctime'], filedata['mtime'], filename ] )
        if numHits > 0:
            if numHits > 1:
                #print "AAAARGH - file %s found more than once in the database - this should never happen" %(filename)
                print "<5> More than one hit for %s found in DB, cannot use hash from db, hashing live..."
                filenames_dict[filename]['hash.sha1'] = hash_file(filename)
            else:
                row = c.fetchone()
                print "<6> Exactly one match for stat-params for %s found in DB, using hash %s from DB" %(filename, row['sha1'])
                filenames_dict[filename]['hash.sha1'] = row['sha1']
        else:
            print "<6> File %s not known yet - hash it" %(filename)
            filenames_dict[filename]['hash.sha1'] = hash_file(filename)



    hash_lookup = {}
    #hash_lookup['463699b9bc849c94e0f45ff2f21b171d2d128bec'] = {'size': 0, 'name': 'undefined name'}
    for filename, filedata in filenames_dict.iteritems():
        #print filedata
        hash_lookup[filedata['hash.sha1']] = { 'size': filedata['size'], 'name': filename }

    # I want to create SQL of the form 'SELECT id, filesize FROM files WHERE hash IN ( hash1, hash2, hash3, ... )'
    # then compare hash & filesizes
    placeholders = ', '.join(['%s'] * len(hash_lookup))
    sql = 'SELECT * FROM files WHERE sha1 IN (%s)' %(placeholders)
    #print sql
    #print hash_lookup.keys()
    c.execute( sql, hash_lookup.keys() )
    rows = c.fetchall()
    # ({'sha1': '463699b9bc849c94e0f45ff2f21b171d2d128bec', 'id': 284L, 'file_size': None},)
    known = {}
    for row in rows:
        if row['sha1'] in hash_lookup  and  'name' in hash_lookup[row['sha1']]:
            #print hash_lookup[row['sha1']]
            filename = hash_lookup[row['sha1']]['name']
        else:
            filename = 'unknown filename'
        known[row['sha1']] = {
                'size': row['file_size'],
                'name': filename,
                'ctime': filenames_dict[filename]['ctime'],
                'mtime': filenames_dict[filename]['mtime'],
                'id': row['id'],
                'is_in_repo': row['is_in_repo']
                }
    notKnown = {}
    for hashvalue, value in hash_lookup.iteritems():
        if hashvalue not in known:
            notKnown[hashvalue] = {
                    'size': filenames_dict[value['name']]['size'],
                    'name': value['name'],
                    'ctime': filenames_dict[value['name']]['ctime'],
                    'mtime': filenames_dict[value['name']]['mtime'],
                    'id': None,
                    'is_in_repo': False
                    }

    #diffkeys = set(hash_lookup) - set(known)
    #print hash_lookup
    #print known
    #print diffkeys
    #print notKnown
    #print rows
    return [ notKnown, known ]


def addFileIntoDB ( filehash, mimetype, extraInfo ):
    """
    takes a hash and the "extraInfo" dict with ctime, mtime, size, name and is_in_repo values, then tries to add it into the db.
    """
    # f7bef5ce2781d8667f2ed85eac4627d532d32222, {'is_in_repo': False, 'ctime': datetime.datetime(2015, 10, 14, 19, 1, 52, 418553), 'mtime': datetime.datetime(2015, 4, 26, 14, 24, 26), 'size': 2628630, 'id': None, 'name': '/treasure/media-throwaway/temp/upload/foobar/IMG_6344.JPG'}
    sql = """INSERT INTO files SET
    is_in_repo = %s,
    original_filename = %s,
    type = %s,
    sha1 = %s,
    file_size = %s,
    original_mtime = %s,
    original_ctime = %s
    """
    try:
        affected = c.execute(sql, [ extraInfo['is_in_repo'], extraInfo['name'], mimetype, filehash, extraInfo['size'], extraInfo['mtime'], extraInfo['ctime'] ] )
    except Exception as e:
        print "Cannot insert file %s into DB" %(filehash)
        print repr(e)
        return False
    print "Successfully INSERTed. Affected: %i" %(affected)
    return True


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
        filesBasicInfo = gatherBasicInfo(filesByMimetype[mimetype])

        # check whether we have data already in SQL; figure out whether we need to import & delete... etc.
        notKnown, known = getRepoStateForFiles ( filesBasicInfo )

        for filehash, extraInfo in known.iteritems():
            # extraInfo is hash, ctime, db_id and the "lives_in_repo" field.
            print "known file: %s, info: %s" %(filehash, extraInfo)
        for filehash, extraInfo in notKnown.iteritems():
            # extraInfo is hash + ctime etc
            print "unknown %s file: %s, info: %s" %(mimetype, filehash, extraInfo)
            if args.copy_to_repo:
                try:
                    safelyImportFileIntoRepo(filehash, extraInfo)
                except Exception as e:
                    print "Could not import file %s(%s) into repo" %(filehash, extraInfo['filename'])
                else:
                    extraInfo['is_in_repo'] = True
            addFileIntoDB(filehash, mimetype, extraInfo)
            # hmmm. When to commit the DB? After every file, or at some other point?
            db.commit()

        #print "not found in Repo: %s" %("\n".join(notKnown))
        #print "already in Repo: %s" %("\n".join(known))



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
                print "%s: %s: %s" %(filename, filesBasicInfo[filename], metaDict)
    else:
        if args.verbose:
            print "There is no plugin to handle mimetype %s." %(mimetype)
            print filesByMimetype[mimetype]
            print "--"

