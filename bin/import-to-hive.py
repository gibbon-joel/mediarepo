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
import re

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
repoDir = metahivesettings.settings.repo_dir()

#print registeredScanners
#print scannersByMimetype


parser = argparse.ArgumentParser()
parser.add_argument("-s", "--sourcedir", help="Top level directory to work on (e.g. /path/to/upload/folder", required=True)
parser.add_argument("-v", "--verbose", help="Be verbose (more debug output)", required=False, default=False, action='store_true')
parser.add_argument("-c", "--copy-to-repo", help="copy scanned supported files into the media repository", required=False, default=False, action='store_true', dest='copy_to_repo')
parser.add_argument("-d", "--delete-original", help="delete original/duplicate files if we have a copy in the media repository", required=False, default=False, action='store_true', dest='delete_original')
args = parser.parse_args()


if args.copy_to_repo and not repoDir:
    print "repository directory is not set in config's [repository] section - cannot copy to repo'"
    sys.exit(2)

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

def makePathFromHash(hash):
    # INPUT: 2ef94a0e9a4ef32fda6e10b83b1e698036b726f1
    # Should create a usable full path for this file
    # OUTPUT: $repoDir/2/e/f
    output = '%s/%s/%s' %(hash[0], hash[1], hash[2])
    return output


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
    Returns False on failure or the insert_id on success.
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
    return c.lastrowid


def getExtension(filename):
    extensionPos = filename.rfind('.')
    return filename[extensionPos+1:].lower()

def getMetadataForFiles(files, scannersOnly = False):
    """
    retrieve metadata for a dict of files
    if scannersOnly = True, we're only interested in the actual scanners (to know which plugins to run again)
    """
    placeholders = ', '.join(['%s'] * len(files))
    filesById = []
    #print files
    for filename, extraInfo in files.iteritems():
        if 'id' in extraInfo:
            filesById.append(extraInfo['id'])
    if scannersOnly:
        sql = 'SELECT DISTINCT file_id, scanner FROM metadata WHERE file_id IN (%s) GROUP BY file_id, scanner' %(placeholders)
    else:
        sql = 'SELECT * FROM metadata WHERE file_id IN (%s)' %(placeholders)
    #print sql
    #print hash_lookup.keys()
    c.execute( sql, filesById )
    rows = c.fetchall()
    metadata = {}
    for row in rows:
        fileId = row['file_id']
        if fileId not in metadata:
            metadata[fileId] = {}
        metadata[fileId][row['scanner']] = {}
        for k, v in row.iteritems():
            metadata[fileId][row['scanner']][k] = v
    return metadata

def getFileIDByHash(filehash):
    if filehash in known:
        if 'id' in known[filehash]:
            return known[filehash]['id']
    numrows = c.execute('SELECT id FROM files WHERE sha1=%s', [filehash])
    if numrows == 1:
        return c.fetchone()[0]
    return False

def getMetadataFromDB(file_id, scanner = 'all' ):
    #print "%s has id %s" %(filehash, file_id)
    if not file_id:
        return False
    #+----------+--------------+------+-----+---------+----------------+
    #| Field    | Type         | Null | Key | Default | Extra          |
    #+----------+--------------+------+-----+---------+----------------+
    #| id       | bigint(20)   | NO   | PRI | NULL    | auto_increment |
    #| file_id  | bigint(20)   | NO   |     | NULL    |                |
    #| scanner  | varchar(255) | YES  |     | NULL    |                |
    #| tagname  | varchar(255) | YES  |     | NULL    |                |
    #| tagvalue | varchar(255) | YES  |     | NULL    |                |
    #+----------+--------------+------+-----+---------+----------------+

    if scanner is 'all':
        numrows = c.execute("SELECT * FROM metadata WHERE file_id=%s", [file_id])
    else:
        numrows = c.execute("SELECT * FROM metadata WHERE file_id=%s AND scanner=%s", [file_id, scanner])
    #print "getMeta fetched %i rows" %(numrows)
    result = c.fetchall()
    metadata = {}
    for row in result:
        if row['scanner'] not in metadata:
            metadata[row['scanner']] = {}
        metadata[row['scanner']][row['tagname']] = row['tagvalue']
    return metadata


def compareMetadata(old, new):
    deleted = {}
    added = {}
    #print repr(old)
    #print repr(new)
    for scanner in old:
        if scanner not in new:
            deleted[scanner] = old[scanner]
        else:
            for tagname in old[scanner]:
                if tagname not in new[scanner]:
                    if scanner not in deleted:
                        deleted[scanner] = {}
                    deleted[scanner][tagname] = old[scanner][tagname]
                else:
                    if str(old[scanner][tagname]) != str(new[scanner][tagname]):
                        if scanner not in deleted:
                            deleted[scanner] = {}
                        if scanner not in added:
                            added[scanner] = {}
                        print "value of tag %s differs: %s vs %s" %(tagname, repr(old[scanner][tagname]), repr(new[scanner][tagname]))
                        deleted[scanner][tagname] = old[scanner][tagname]
                        added[scanner][tagname] = new[scanner][tagname]
    for scanner in new:
        if scanner not in old:
            added[scanner] = new[scanner]
        else:
            for tagname in new[scanner]:
                if tagname not in old[scanner]:
                    if scanner not in added:
                        added[scanner] = {}
                    added[scanner][tagname] = new[scanner][tagname]
    return [ deleted, added ]

def makeString(indict):
    for k, v in indict.iteritems():
        indict[k] = str(v)
    return indict

def putMetadataIntoDB(scanner, filehash, metaDict):
    print "Put metadata from scanner %s for filehash %s into DB" %(scanner, filehash)
    file_id = getFileIDByHash(filehash)
    oldData = getMetadataFromDB(file_id, scanner=scanner)
    #print oldData
    if not oldData: oldData = { scanner: {} }
    newData = { scanner: makeString(metaDict) }
    deleted, added = compareMetadata(oldData, newData)
    #print "diff:"
    #print deleted
    #print "--"
    #print added
    #print "++"
    #print "***"


    deletedRows = c.execute('DELETE FROM metadata WHERE file_id=%s and scanner=%s', [file_id, scanner])

    placeholders = ', '.join(["(%s, '%s', %%s, %%s, %%s, %%s)" %(file_id, scanner)] * len(newData[scanner]))
    sql = 'INSERT INTO metadata (file_id, scanner, tagname, tagvalue, tagvalue_float, tagvalue_date) VALUES %s' %(placeholders)
    #print sql
    #print hash_lookup.keys()
    sqlarray = []
    for tagname, tagvalue in newData[scanner].iteritems():
        sqlarray.append(tagname)
        sqlarray.append(tagvalue)

        try:
            valFloat = float(tagvalue)
        except ValueError:
            valFloat = None
        sqlarray.append(valFloat)

        valDate = None
        if 'date' in tagname.lower() or 'time' in tagname.lower():
            try:
                # 2015:08:22 19:09:58.241
                # 2015:09:14
                # 2015:08:22 19:09:58.241
                # 2015:08:22 19:09:58+02:00
                # 2015:08:22 19:09:58
                # 2015:08:22 19:09:58.241
                # 17:09:56.52
                # 2015:08:22 17:09:56.52Z
                # 2015:08:22
                m = re.search('^((19|20|21)[0-9][0-9])[-:._]((0[1-9]|1[0-2]))[-:._]([0-3][0-9])(.*)', tagvalue )
                if m:
                    valDate = "%s-%s-%s %s" %(m.group(1), m.group(3), m.group(5), m.group(6))
                    print "Matched %s in %s => %s" %(tagvalue, tagname, valDate)
                else:
                    m = re.search('^([01][0-9]|2[0-3])[-:._]([0-5][0-9])[-:._]([0-5][0-9])(\.[0-9]+)?', tagvalue )
                    if m:
                        valDate = "1970-01-01 %s:%s:%s" %(m.group(1), m.group(2), m.group(3))
                        if m.group(4):
                            valDate = "%s%s" %(valDate, m.group(4))
                        print "Matched %s in %s => %s" %(tagvalue, tagname, valDate)
                    #else:
                        #print "Could not match %s in %s" %(tagvalue, tagname)


            except ValueError:
                valDate = None
        sqlarray.append(valDate)

    try:
        numrows = c.execute( sql, sqlarray )
    except Exception as e:
        print "error on INSERT metadata"
        print repr(e)
    else:
        print "<7> %i rows INSERTed for scanner %s on file %s" %(numrows, scanner, file_id)
        db.commit()

def getExtension(filename):
    extensionPos = filename.rfind('.')
    return filename[extensionPos+1:].lower()


def safelyImportFileIntoRepo ( filehash, extraInfo ):
    extension = getExtension(extraInfo['name'])
    targetFilename = '%s/%s/%s.%s' %(repoDir, makePathFromHash(filehash), filehash, extension)
    print "<7> safely import %s to %s" %(extraInfo['name'], targetFilename)
    try:
        dirExists = os.stat(os.path.dirname(targetFilename))
    except Exception as e:
        if e.errno == 2:
            # No such file or directory
            try:
                os.makedirs(os.path.dirname(targetFilename))
            except Exception as e:
                print "<4> Could not create repo directory: %s" %(os.path.dirname(targetFilename))
                print repr(e)
                return False
        else:
            print repr(e)
            return False

    if os.path.exists(targetFilename):
        # file already exists in repo
        destHash = hash_file(targetFilename)
        if destHash != filehash:
            print "<4> Hash collision - a file with the same hash %s already exists in the repo - this should never happen" %(destHash)
            return False
        else:
            # file in repo is the same we want to import so don't do anything
            print "<7> %s already exists in the repo, doing nothing" %(filehash)
            return True

    # only if target does not exist yet:
    try:
        shutil.copy2(extraInfo['name'], targetFilename) # copy2 preserves mtime/atime
    except Exception as e:
        print "<5> Could not copy '%s' to '%s'" %(filename, targetFilename)
        print repr(e)
        return False

    destHash = hash_file(targetFilename)
    if destHash != filehash:
        print "<5> Newly copied file has non-matching hash: original = '%s', copy = '%s'" %(filehash, destHash)
        return False
    else:
        print "<7> Successfully imported %s into the repo" %(filehash)
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
            if debugcount > 32:
                print "*** DEBUG: breaking after %i files ***" %(debugcount)
                break

for mimetype in filesByMimetype:
    if mimetype in scannersByMimetype:

        # supported file (we have at least one scanner that can give us metadata), so hash it...
        filesBasicInfo = gatherBasicInfo(filesByMimetype[mimetype])

        # check whether we have data already in SQL; figure out whether we need to import & delete... etc.
        notKnown, known = getRepoStateForFiles ( filesBasicInfo )

        hashByFilename = {}
        for filehash, extraInfo in notKnown.iteritems():
            # extraInfo is hash + ctime etc
            print "unknown %s file: %s, info: %s" %(mimetype, filehash, extraInfo)
            fileId = addFileIntoDB(filehash, mimetype, extraInfo)
            if fileId:
            # hmmm. When to commit the DB? After every file, or at some other point?
                try:
                    db.commit()
                except Exception as e:
                    print "Could not commit DB changes."
                    print repr(e)
                else:
                    extraInfo['id'] = fileId
                    known[filehash] = extraInfo
                    hashByFilename[extraInfo['name']] = filehash

        for filehash, extraInfo in known.iteritems():
            # extraInfo is hash, ctime, db_id and the "lives_in_repo" field.
            print "known file: %s, info: %s" %(filehash, extraInfo)
            if args.copy_to_repo  and  not extraInfo['is_in_repo']:
                try:
                    importedIntoRepo = safelyImportFileIntoRepo(filehash, extraInfo)
                except Exception as e:
                    print repr(e)
                    print "Could not import file %s(%s) into repo" %(filehash, extraInfo['name'])
                else:
                    if not importedIntoRepo:
                        print "Could not import file %s(%s) into repo" %(filehash, extraInfo['name'])
                    else:
                        try:
                            affected_rows = c.execute('UPDATE files SET is_in_repo=True WHERE id=%s', [extraInfo['id']])
                        except:
                            print "Could not update DB status for file %s (id %s)" %(filehash, extraInfo['id'])
                        else:
                            print "%i rows updated for file %i" %(affected_rows, extraInfo['id'])
                            extraInfo['is_in_repo'] = True
                            known[filehash]['is_in_repo'] = True
                db.commit()
            if args.delete_original  and  extraInfo['is_in_repo']:
                extension = getExtension(extraInfo['name'])
                targetFilename = '%s/%s/%s.%s' %(repoDir, makePathFromHash(filehash), filehash, extension)
                if os.path.exists(targetFilename)  and  hash_file(targetFilename) == filehash:
                    print "<6> We have a valid copy of %s in the repo, going to delete %s" %(filehash, extraInfo['name'])
                    try:
                        os.unlink(extraInfo['name'])
                    except Exception as e:
                        print "Could not delete original %s" %(extraInfo['name'])
                        print repr(e)
                    else:
                        print "<6> Successfully deleted original of %s (%s)" %(filehash, extraInfo['name'])
                else:
                    print "<4> A file that we think is in the repo does not exist - NOT deleting original: %s" %(filehash)
                    try:
                        importedIntoRepo = safelyImportFileIntoRepo(filehash, extraInfo)
                    except Exception as e:
                        print repr(e)
                        print "Could not import file %s(%s) into repo" %(filehash, extraInfo['name'])
                    else:
                        if not importedIntoRepo:
                            print "Could not import file %s(%s) into repo" %(filehash, extraInfo['name'])
                        else:
                            print "<5> Re-imported file %s into the repo" %(filehash)


        #print "not found in Repo: %s" %("\n".join(notKnown))
        #print "already in Repo: %s" %("\n".join(known))

        knownMetaData = getMetadataForFiles(files = known, scannersOnly = True)
        print "=================="
        print "knownMetaData:"
        print knownMetaData
        print "=================="

        hashById = {}
        for k, v in known.iteritems():
            #print "hbF: %s = %s" %(k, v)
            if v['name'] not in hashByFilename:
                hashByFilename[v['name']] = k
            if v['id'] not in hashById:
                hashById[v['id']] = k
            else:
                print "Duplicate filename %s?! This should not happen" %(v['name'])
        #print "hbF:"
        #print hashByFilename
        #print "**"

        # iterate over registered metadata scanners for the current mimetype
        for plugin in scannersByMimetype[mimetype]:
            begin = time.time()
            list_of_files_to_scan = []
            for filename in filesByMimetype[mimetype]:
                if filename in hashByFilename:
                    filehash = hashByFilename[filename]
                    if filehash in known:
                        if 'id' in known[filehash]:
                            fileId = known[filehash]['id']
                            if fileId in knownMetaData:
                                fmd = knownMetaData[fileId]
                                if plugin in fmd:
                                    print "Not scanning file %s with scanner %s, already have data in DB" %(filename, plugin)
                                    continue
                list_of_files_to_scan.append(filename)
            print "list of files to scan with %s: %s" %(plugin, list_of_files_to_scan)
            if list_of_files_to_scan:
                metadata = regScan[plugin].scanBulk(list_of_files_to_scan)
                finish = time.time()
                time_taken = finish - begin
                if time_taken <= 0:
                    files_per_second = -1  # avoid division by zero
                else:
                    files_per_second = len(filesByMimetype[mimetype]) / float(time_taken)
                print "plugin %s took %0.2f seconds to parse %i files (%0.1f files per second)" %(plugin, time_taken, len(filesByMimetype[mimetype]), files_per_second)
            else:
                metadata = False
            if metadata:
                for filename, metaDict in metadata.iteritems():
                    if filename in hashByFilename:
                        filehash = hashByFilename[filename]
                    else:
                        print "file %s - no hash found, skip" %(filename)
                        continue

                    try:
                        putMetadataIntoDB(plugin, filehash, metaDict)
                    except Exception as e:
                        print "Could not put metadata into DB"
                        print repr(e)
                    else:
                        print "<7> successfully updated metadata for %s" %(filename)
                    print "%s: %s: %s" %(filename, filesBasicInfo[filename], metaDict)

    else:
        if args.verbose:
            print "There is no plugin to handle mimetype %s." %(mimetype)
            print filesByMimetype[mimetype]
            print "--"

