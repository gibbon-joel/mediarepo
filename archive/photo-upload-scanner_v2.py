#!/usr/bin/python
# scan a folder structure for images/movies (as it would come from a digital camera)
# create a checksum, read meta information from media files
# move the file into the media repository, renamed to it's checksum.extension
# e.g.
# upload/IMG_7911.JPG
# has a sha1sum of ee609e6057a48b6a4c81d9600e6f582492d1b71d
# so it gets analysed (exif, ctime, mtime, orig. filename, orig. folder)
# we then check our repository whether the same file exists / same meta information exists
# if not, insert into DB && mv upload/IMG_7911 $repo/$hash_subfolder/ee609e6057a48b6a4c81d9600e6f582492d1b71d.jpg
# set the ctime/mtime of the moved file to the original ctime/mtime
import exifread
import exiftool
import os, sys
import hashlib
import MySQLdb
import MySQLdb.cursors
from datetime import datetime
import shutil

supportedExtensions = [ 'jpg', 'jpeg', 'png' ]
sourceDir = '/source/dir/'
destDir = '/dest/dir/repository'

def makePathFromHash(hash):
  # INPUT: 2ef94a0e9a4ef32fda6e10b83b1e698036b726f1
  # Should create a usable full path for this file
  # OUTPUT: $destDir/
  output = '%s/%s/%s' %(hash[0], hash[1], hash[2])
  return output

def getExtension(filename):
  extensionPos = filename.rfind('.')
  return filename[extensionPos+1:].lower()

def convertMetadataIntoSQL(fileID, metadata):
  query = "INSERT INTO exifdata (file_id, exif_tag_name, exif_tag_value) VALUES "
  stuff = []
  for key, value in metadata.iteritems():
    stuff.append ("(%s, '%s', '%s')" %(fileID, key, value))
  query = "%s %s" %(query, ','.join(stuff))
  return query  

def safelyImportIntoRepo(filename, hashhex, metadata, dbcursor):
  extension = getExtension(filename)
  shortfilename = filename[(len(sourceDir)+1):]
  info = os.stat(filename)
  file_mtime = datetime.fromtimestamp(info.st_mtime)
  file_ctime = datetime.fromtimestamp(info.st_ctime)
  targetFilename = '%s/%s/%s.%s' %(destDir, makePathFromHash(hashhex), hashhex, extension)
  print "safely import %s" %(filename)
  try:
    dirExists = os.stat(os.path.dirname(targetFilename))
  except Exception as e:
    if e.errno == 2:
      # No such file or directory
      try:
        os.makedirs(os.path.dirname(targetFilename))
      except Exception as e:
        print "Could not create repo directory: %s" %(os.path.dirname(targetFilename))
        print repr(e)
        return False
    else:
      print repr(e)
 
  try:
    shutil.copy2(filename, targetFilename) # copy2 preserves mtime/atime
  except Exception as e:
    print "Could not copy '%s' to '%s'" %(filename, targetFilename)
    print repr(e)
    return False

  destHash = createSHA(targetFilename)
  if destHash != hashhex:
    print "Newly copied file has non-matching hash: original = '%s', copy = '%s'" %(hashhex, destHash)
    return False

  try:
    query = """INSERT INTO files (original_filename, type, sha1, original_mtime, original_ctime) VALUES('%s', '%s', '%s', '%s', '%s')""" %(shortfilename, extension, sha_hex, file_mtime, file_ctime)
    dbcursor.execute(query)
  except Exception as e:
    print "Could not insert file data into DB"
    print repr(e)
    # TODO: should we remove the previously copied file again?
    os.unlink(targetFilename)
    return False
  
  fileID = dbcursor.lastrowid

  #print "mv '%s' to '%s'" %(filename, targetFilename)
  #print query
  metaquery = convertMetadataIntoSQL(fileID, metadata)
  # this needs to be rewritten using some python magic to count tuples and then let dbcursor.execute escape it. 
  try:
    dbcursor.execute(metaquery)
  except Exception as e:
    print "Could not insert file EXIF metadata into DB"
    print metaquery
    print repr(e)

  return True
  #print metaquery
  #print "--"


BUF_SIZE=1024*1024 # to read files for hashing, not having to read in 2TB file at once...

def createSHA(filename):
  sha1 = hashlib.sha1()
  with open(filename, 'rb') as f:
    while True:
      data = f.read(BUF_SIZE)
      if not data:
	break
      sha1.update(data)
  sha_hex = sha1.hexdigest()
  return sha_hex

def readExif ( dirpath, filename ):
  try:
    f = open('%s/%s'%(dirpath,filename), 'rb')
    tags = exifread.process_file(f, details=False, strict=False)
    f.close()
  except:
    return False
  return tags
def indexMe ( dirpath, filename ):
  #print "indexing %s/%s" %(dirpath, filename)
  
  exifData = readExif ( dirpath, filename )
  if not exifData:
    print "no valid exifData found in %s" %(filename)
  else:
    print filename
    for tag in sorted(exifData.keys()):
      if tag not in ('JPEGThumbnail', 'TIFFThumbnail', 'EXIF MakerNote'):
        print "Key: %s, value %s" % (tag, exifData[tag])
    sys.exit(1)



db = MySQLdb.connect(user="user", passwd="passwd", db="dbname", cursorclass=MySQLdb.cursors.DictCursor)
c = db.cursor()

for (dirpath, dirnames, filenames) in os.walk(sourceDir, topdown=True, onerror=None, followlinks=False):
  if len(filenames) > 0:
    print "Working on directory %s" %(dirpath)
    with exiftool.ExifTool() as et:
      lookupFilenames = []
      count = 0
      for filename in filenames:
        if getExtension(filename) in supportedExtensions:
          lookupFilenames.append('%s/%s' %(dirpath, filename))
	  count += 1
 	  if count > 30:
	    break
        else:
          print "Unsupported file: %s, move to unsupported folder" %(filename)
      #print filenames
      if len(lookupFilenames) > 0:
	resultDict = {}
	print "fetch metadata for %i files" %(len(lookupFilenames))
        metadata = et.get_metadata_batch(lookupFilenames)
        for m in metadata:
	  #print m['File:FileName']
	  filename = '%s/%s'%(m['File:Directory'], m['File:FileName'])
	  sha_hex = createSHA(filename)
	  query = """SELECT id, original_filename, UNIX_TIMESTAMP(original_mtime) AS original_mtime, original_ctime, type, inserted_at FROM files WHERE sha1='%s'""" %(sha_hex)
	  rows_found = c.execute(query)
          if rows_found > 0:
	    # duplicate file
	    row = c.fetchone()
	    info = os.stat(filename)
            file_mtime = info.st_mtime
            shortfilename = filename[(len(sourceDir)+1):]

	    print """Found a duplicate file
DB  : %s\t%s\t%s
Disk: %s\t%s\t%s""" %(row['original_mtime'], row['inserted_at'], row['original_filename'], file_mtime, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), shortfilename)
            print "Unlinking duplicate..."
            try:
              os.unlink(filename)
            except Exception as e:
              print repr(e)
	  else:
	    # new file
	    if safelyImportIntoRepo(filename, sha_hex, m, c):
              try:
		db.commit()
              except Exception as e:
                print "Could not COMMIT SQL"
                print repr(e)
                break
              os.unlink(filename)

	  #print "%s;%s;%s" %(filename, sha_hex, shortfilename)
	  resultDict[filename] = m
          #indexMe ( dirpath, filename )
        

        #print resultDict
