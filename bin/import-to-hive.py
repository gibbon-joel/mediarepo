#!/usr/bin/python
import os, sys
import hashlib
import MySQLdb
import MySQLdb.cursors
from datetime import datetime
import shutil
import magic

m=magic.open(magic.MAGIC_MIME_TYPE)
m.load()


def getMimeType(filename)
	try:
		result = m.file(filename)
	except Exception as e:
		result = False
		print repr(e)
	return result



