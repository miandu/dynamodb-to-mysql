#!/usr/bin/python3

import argparse,json,decimal,sys,os,re
sys.path.append(os.path.join(os.path.dirname(__file__), "libs"))
from progress.bar import Bar
import pymysql.cursors
from logger import logger

def create_connection(cf):
    # Connect to the database
    return pymysql.connect(host='twitter-test.cnjspbidnvrv.eu-west-1.rds.amazonaws.com',
                           user='twitter',
                           password='twitter2018-test',
                           db='twitter',
                           charset='utf8mb4',
                           cursorclass=pymysql.cursors.DictCursor)



def execute_query(connection,query):
    try:
        with connection.cursor() as cursor:
            # Create a new record
            cursor.execute(query)
	
        # connection is not autocommit by default. So you must commit to save
        # your changes.
        connection.commit()
    except Exception as e:
        logger.error(e)
    finally:
        connection.close()

def escape_name(s):
    #return '{}'.format(s.replace('"',"'").replace("\\",""))
    #return "{}".format(s.replace('"',"\\\""))
    #print(s)
    #return str(s)
    return '{}'.format(s.replace('"','""').replace("\\",""))

def json_value_to_string(value):
    if value:
        if isinstance(value, bool):
            return str(value).lower()
        elif isinstance(value, int):
            return value
        elif isinstance(value, float):
            return value
        elif isinstance(value, str):
            return u'"%s"' %(escape_name(value))
        else:
            return u'"%s"' %(value)
    else:
        return '""'

def simple_json_to_mysql_query(object):
    ## convert simple json object (object with only one level of attributes, the value could be string, integer, date, or float) to two strings for mysql query, one is the attribute string and another one is the value string
    attributes=""
    values=""
    first_entry=True
    for key in object:
        if first_entry:
            sep=""
            first_entry=False
        else:
            sep=","
        attributes="%s%s%s" %(attributes,sep, key)
        values="%s%s%s" %(values,sep,json_value_to_string(object[key]))
    return attributes,values

def get_all_columns(cf,table):
    connection = create_connection(cf)
    print(table,connection)
    attributes = execute_query(connection,"SHOW COLUMNS FROM %s" %(table))
    if attributes:
        return[x['Field'] for x in attributes]
    else:
        return["id"]

def add_columns_if_non_exists(cf,table,item):
    attributes,values = simple_json_to_mysql_query(item)
    non_seen_attributes=[]
    db_columns=get_all_columns(cf,table)
    for attr in attributes.split(","):
        if not attr in db_columns:
            non_seen_attributes.append(attr)
    if len(non_seen_attributes)>0:
        columns=make_columns_from_attributes(non_seen_attributes)    
        add_column_to_mysql(cf,table,columns,non_seen_attributes)

def add_column_to_mysql(cf,table,columns,keys):
    connection = create_connection(cf)
    ##query_str ="alter table %s add column %s,add key %s" %(table,
    ##                                                       ", add column ".join(columns),
    ##                                                       ", add key ".join(["%s (%s)" %(x,x) for x in keys]))
    query_str ="alter table %s add column %s" %(table,
                                                ", add column ".join(columns))
    execute_query(connection,query_str)

def create_table_if_non_exists(cf,table,like_table=None):
    connection = create_connection(cf)
    if like_table:
        query_str= "CREATE TABLE IF NOT EXISTS "+table+" like "+like_table
    else:
        query_str = """CREATE TABLE IF NOT EXISTS """+table+ " ( `id` int(11) NOT NULL AUTO_INCREMENT,PRIMARY KEY (`id`)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
    execute_query(connection,query_str)

def make_columns_from_attributes(attributes):
    columns=[]
    for attr in attributes:
        if re.search("negative|positive|total",attr):
            columns.append("`%s` int(10) default 0" %(attr))
        else:
            columns.append("`%s` varchar(255)" %(attr))
    return columns


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='General storage to RDS mysql for twitter.') 
    parser.add_argument('config', type=str, help='an config file for general storage')
    
    args = parser.parse_args()
    config = __import__(args.config)
    cf =config.Config() 

    
    connection = create_connection(cf)
    query="insert into %s(page_id) values('x233')" %('twit_posts_'+cf.short_name)
    execute_query(connection,query)
    print("Done")    
