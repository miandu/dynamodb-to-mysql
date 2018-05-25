#!/usr/bin/python3

import argparse,json,decimal,sys,os
sys.path.append(os.path.join(os.path.dirname(__file__), "libs"))
from progress.bar import Bar
import pymysql.cursors

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
    finally:
        connection.close()

def escape_name(s):
    return '{}'.format(s.replace('"',"'").replace("\\",""))

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
