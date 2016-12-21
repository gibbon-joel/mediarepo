#!/usr/bin/python
from flask import Flask, request, jsonify
import sys, os
import MySQLdb
import MySQLdb.cursors
from htsql import HTSQL
from datetime import datetime
import time
sys.path.append('%s/../lib' %(os.path.dirname(__file__)))
import metahivesettings.settings

db_credentials = metahivesettings.settings.db_credentials()
repoDir = metahivesettings.settings.repo_dir()

api = Flask(__name__)

try:
    db = MySQLdb.connect(user=db_credentials['db_username'], passwd=db_credentials['db_password'], db=db_credentials['db_name'],           cursorclass=MySQLdb.cursors.DictCursor)
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

try:
    htdb = HTSQL('mysql://%s:%s@localhost/%s' %(db_credentials['db_username'], db_credentials['db_password'], db_credentials['db_name']) )
except Exception as e:
    print "Could not connect to SQL Server using HTSQL"
    print repr(e)
    sys.exit(2)


@api.route("/")
def hello():
    return "Hello WOrld!1!!"

@api.route("/GetFilesByMetadataViaHTSQL/<string:htquery>")
def GetFilesByMetadataViaHTSQL(htquery):
    #return htquery
    rows=htdb.produce('/%s' %(htquery))
    from htsql.core.fmt.emit import emit
    with htdb:
        text = ''.join(emit('x-htsql/json', rows))
    return text
    return jsonify(results=rows)
    return htquery


@api.route("/CreateCollectionTable/")
def CreateCollectionTable():
    """
    we want: a list of scanner:tagname that we're interested in, to create a table containing each scanner:tagname tuple as a column + file_id
    | file_id | scanner1:tagname1 | scanner1:tagname2 | scanner2:tagname1 |
    we then fill that table with individual SELECTs into the metadata table
    ( and then? create a SQL table and use HTSQL? Hmm.. )
    """
    m = request.args.getlist('m') # "metadata", as key-value dict
    try:
        # use "ServerSide DictCursor" as result sets could potentially be very large?!
        db = MySQLdb.connect(user=db_credentials['db_username'], passwd=db_credentials['db_password'], db=db_credentials['db_name'],           cursorclass=MySQLdb.cursors.SSDictCursor)
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
    #query = 'SELECT * FROM metadata WHERE 1 AND '
    query = """SELECT file_id, tagname, tagvalue, tagvalue_float, tagvalue_date FROM metadata WHERE """
    query_where=[]
    sql_values=[]
    scanner_tagname_dict = {}
    data = None
    try:
        for kv in m:
            scanner, tagname = kv.split('|')
            if not scanner in scanner_tagname_dict:
                scanner_tagname_dict[scanner] = {}
            scanner_tagname_dict[scanner][tagname] = True
            query_where.append('(tagname=%s AND scanner=%s)')
            sql_values.append(tagname)
            sql_values.append(scanner)
        query = '%s %s ' %(query, ' OR '.join(query_where))
        #return "\n".join(sql_values)
        #return  query + "\n".join(sql_values)
    except Exception as e:
        raise
        data = None
    else:
        temp_table = {}
        begin = time.time()
        c.execute(query, sql_values)
        rows = 0
        while True:
            row = c.fetchone()
            rows += 1
            if row is None:
                break
            if row['file_id'] not in temp_table:
                temp_table[row['file_id']] = {}
            if row['tagvalue_date']:
                val = row['tagvalue_date']
                val_type = 'date'
            elif row['tagvalue_float']:
                val = row['tagvalue_float']
                val_type = 'float'
            else:
                val = row['tagvalue']
                val_type = 'string'
            temp_table[row['file_id']][row['tagname']] = {val_type: val}
        #return "\n".join(temp_table)
        time_taken = time.time() - begin
        stats = {}
        stats['total_time_ms'] = '%4.2f' %(time_taken * 1000)
        stats['total_rows'] = rows
        stats['total_files'] = len(temp_table)
        result = jsonify({'results' : temp_table, 'stats': stats} )
        return result



    if data is None:
        return "Error"
    else:
        return result




@api.route("/GetFilesByMetadata/")
def GetFilesByMetadata():
    """
    supported args to limit selection (default: everything, with a limit of 100)
    mimetype
    limit
    m[key] = value
    """
    max_lines_to_return = request.args.get('limit')
    pagination_start = request.args.get('start')
    m = request.args.getlist('m') # "metadata", as key-value dict
    #return repr(m)
    if max_lines_to_return  and  int(max_lines_to_return) > 0:
        max_lines_to_return = int(max_lines_to_return)
    else:
        max_lines_to_return = 100
    if pagination_start  and  int(pagination_start) > 0:
        pagination_start = int(pagination_start)
    else:
        pagination_start = 0

    try:
        limit_mimetypes_to = request.args.get('mimetypes')
    except:
        limit_mimetypes_to = None

    try:
        query_where = []
        sql_values  = []
        query = 'SELECT * FROM metadata WHERE 1 AND '
        # EXIF:ExifImageHeight > 500  AND  EXIF:ExifImageWidth > 500;
        # order by Composite:GPSDateTime DESC

        # create a temporary table with each queried k as column, including all k/v pairs.
        # for this, we need a distinct list of tagnames (which will then form the column names together with the scanner name,
        # i.e. exifscanner:Composite:GPSDateTime)

        for kv in m:
            k, v = kv.split('|')
            query_where.append('(tagname=%s AND tagvalue=%s)')
            sql_values.append(k)
            sql_values.append(v)

        query = '%s %s LIMIT %%s,%%s' %(query, ' AND '.join(query_where))
        sql_values.append(pagination_start)
        sql_values.append(max_lines_to_return)
        print sql_values
        print query
        c.execute(query, sql_values)
        data = c.fetchall()
        result = jsonify(results=data)
    except Exception as e:
        raise
        data = None
    if data is None:
        return "Error"
    else:
        return result
        #return "Logged in successfully"

@api.route("/GetFiles")
def GetFiles():
    """
    supported args to limit selection (default: everything, with a limit of 100)
    mimetype
    limit
    """
    max_files_to_return = request.args.get('limit')
    pagination_start = request.args.get('start')
    if max_files_to_return  and  int(max_files_to_return) > 0:
        max_files_to_return = int(max_files_to_return)
    else:
        max_files_to_return = 100
    if pagination_start  and  int(pagination_start) > 0:
        pagination_start = int(pagination_start)
    else:
        pagination_start = 0

    try:
        limit_mimetypes_to = request.args.get('mimetypes')
    except:
        limit_mimetypes_to = None

    try:
        c.execute("SELECT * from files LIMIT %s,%s", [pagination_start, max_files_to_return])
        data = c.fetchall()
        result = jsonify(results=data)
    except Exception as e:
        raise
        data = None
    if data is None:
        return "Error"
    else:
        return result
        #return "Logged in successfully"


if __name__ == "__main__":
    #api.run(host='0.0.0.0')
    api.run(host='127.0.0.1', debug=True, port=5000)

