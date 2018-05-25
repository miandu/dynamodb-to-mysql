#!/usr/bin/python3
import json,time,urllib.parse,csv,sys,os
sys.path.append(os.path.join(os.path.dirname(__file__), "libs"))
import dateutil.parser
from datetime import datetime
from progress.bar import Bar
import general_storage,sqs

def parse_time(s):
    try:
        ret = dateutil.parser.parse(s)
    except ValueError:
        ret = datetime.utcfromtimestamp(s)
    return ret

def url_encode(url_string):
    return urllib.parse.quote_plus(url_string)

def get_current_posix_number():
    return int(str(time.time())[:10])

def clean_empty(item):
    if not isinstance(item, (dict, list)):
        return item
    if isinstance(item, list):
        return [v for v in (clean_empty(v) for v in item) if v]
    return {k: v for k, v in ((k, clean_empty(v)) for k, v in item.items()) if v}

def clean_empty_str(item):
    if not isinstance(item, (dict, list)):
        return item
    if isinstance(item, list):
        return [v for v in (clean_empty_str(v) for v in item) if v!=""]
    return {k: v for k, v in ((k, clean_empty_str(v)) for k, v in item.items())if v!=""}

def str_number(item):
    # convert all float number to string
    json_str = json.dumps(item,cls=general_storage.DecimalEncoder)
    return json.loads(json_str, parse_float=str)
    #return json.loads(json_str)

def fix_data(item):
    ## fix json object for database use
    return str_number(clean_empty_str(item))    

def fix_data_to_string(item):
    ## convert json object to string suitable for database use
    return json.dumps(fix_data(item))

def date_str_to_posix(date):
    return int(str(dateutil.parser.parse(date).timestamp())[:10])


def run_until_finish(function):
    '''Function to run function until all items are processed'''
    counter=1000
    total=0
    while counter>0:
        counter,error=function()
        total+=counter
        print("%s item are updated" %(str(total)))
    return total

def send_rerun(cf,task_name,queue_name,process,start,distributed,batch_size=1,limit=1000):
    '''Main function for reunning a large number of items.
    1. distributed rerun mode: We use sqs queue to manage the rerun; this mode gets items of limited size, sends items in a message to sqs.
    2. serial rerun model: this mode gets items of limited size, calls rerun process on these items, updates items in DB'''
    table=general_storage.dynamodb.Table(cf.table_name)
    #items=general_storage.scan_items(table, Attr("normalized").ne(2), limit)
    items=general_storage.get_item_by_task(table,task_name,start,limit)
    if not items:
        print("Finished rerun")
        return 0

    if distributed:
        return sqs.send_to_queue(cf,queue_name,items)
    else:
        return process_rerun(cf,items,process,batch_size)

def process_rerun(cf,items,process,batch_size):
    #table=general_storage.dynamodb.Table(cf.table_name)
    table=general_storage.dynamodb.Table(cf.table_name)
    error=0
    processed_items=[]
    counter=0
    data=[]
    bar = Bar('Processing items', max=len(items))
    for item in items: 
        counter+=1
        data.append(item)
        if counter==batch_size:
            try:                 
                data=process(cf,data)
                processed_items+=data
                data=[]
                counter=0
                bar.next(len(data))  
            except Exception as e:
                print(e)
                error+=1

    if (len(data)>0):
        data=process(cf,data)
        processed_items+=data
        
    bar.finish()
    print("%s item are processed, error %s" %(str(len(processed_items)),str(error)))

    #counter,error=general_storage.batch_update_item(table,processed_items)
    counter,error=general_storage.update_items(table,processed_items)
    print("%s item are updated, error %s" %(str(counter),str(error)))
    return counter,error

def process_sqs_rerun(cf,queue_name,process,batch_size=100):
    queue_url=sqs.get_url_by_name(queue_name)
    table=general_storage.dynamodb.Table(cf.table_name)
    message,handler=sqs.read_message(queue_url)
    if len(message)>0:
        processed_items=[]
        print("Processing sqs items")
        print(len(message))
        items = general_storage.get_items_by_ids(cf, [x['id'] for x in message])
        print(len(items))
        counter,error=process_rerun(cf,items,process,batch_size)
        print(counter,error)
        if handler and counter > 0:
            sqs.delete_message(queue_url,handler)
        return counter,error
    else:
        print("No message was found")
        return 0,0
        
def write_to_csv(csvfile,str_list):
    spamwriter = csv.writer(csvfile, delimiter='|', quoting=csv.QUOTE_MINIMAL)
    spamwriter.writerow(str_list)

def output_tweets_for_ids(tweets,ids):    
    ## For each id in ids, find all replies to it from tweets, and output all replies to a csv file with the id as the name
    ## Output file contains lines of replies, each line is a reply in the format of id|created_time|message
    replies_by_id={}
    for tweet in tweets:
        for id in ids:
            if tweet.get('post_id')==id:
                print(tweet['id'], '|', tweet['created_time_pretty'],"|",tweet['message'])
                if id in replies_by_id:
                    replies_by_id[id].append(tweet)
                else:
                    replies_by_id[id]=[tweet]
    for id in ids:
        with open(id+".csv", 'w') as csvfile:
            for tweet in replies_by_id.get(id,[]):
                write_to_csv(csvfile,[tweet['object_id'],tweet['created_time_pretty'],tweet['message']])        
