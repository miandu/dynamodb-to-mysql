#!/usr/bin/python3

import argparse,boto3,json,decimal,sys,os,random
sys.path.append("libs")
from progress.bar import Bar
from boto3.dynamodb.conditions import Attr
from elasticsearch import Elasticsearch
import general_storage,utils
#host = 'https://search-client-u-dbyh4kgiin3hynbwdk557cp4ue.eu-west-1.es.amazonaws.com/'
#host = 'https://search-client-sc-om2a3opn6w4htnju5yxpkkjfou.eu-west-1.es.amazonaws.com'

#awsauth = AWS4Auth(your_access_key, your_secret_key, region, 'es')

# Get the service resource.
dynamodb = boto3.resource('dynamodb')


def query_items(cf, query,limit=100000,size=10000):
    ## query ES to find relevant items, currently returns total count of items, id of items (platform_id+"_"+object_id) and object_id of items  
    items=[]
    from_=0
    total=0
    if size>limit:
        size=limit
    this_total=size
    es = Elasticsearch(cf.es_host,use_ssl=True,verify_certs=False)
    while this_total>=size and total < limit:
        hits=es.search(index=cf.table_name, 
                       filter_path=cf.es_filter_path,
                       size=size,
                       sort='created_time',
                       q=query
                   ).get("hits")
        this_total=hits['total']
        total+=this_total
        if this_total>0:
            new_items = [x['_source'] for x in hits.get('hits')]
            items = items + new_items
            if this_total>=size:
                ## go to next round of search by giving the biggest object_id in query
                query = query + " AND object_id>" + new_items[len(new_items)-1]['object_id']
                print(query)
    return total,items

def output_replies_to_ids(cf,path,ids):
    ## For each id, find all replies and output them into a csv file in the path folder with the name id.csv
    os.makedirs(path, exist_ok=True)
    for id in args.ids.split(','):
        comments=general_storage.get_item_comments(cf,{"id":id,"object_type":"post"})
        with open(path+id+".csv", 'w') as csvfile:
            for tweet in comments:
                utils.write_to_csv(csvfile,[tweet['post_id'],tweet['object_id'],tweet['original_data']['created_at'],tweet['original_data'].get('user').get('name'),tweet['message']])       

def es_outputs_to_ids(items):
    #convert a list of items returned by es into ids
    return [x['id'] for x in items]
 
def output_items_random(cf,path,query,random_no):
    ## Randomly select give number of tweets from results of query, limiting the query to the size 10*random_no
    os.makedirs(path, exist_ok=True)
    total,items=query_items(cf,query,random_no*10)
    if total>0:
        selected_items_ids=random.sample(es_outputs_to_ids(items),min(random_no, len(items)))
        with open(path+query+".csv", 'w') as csvfile:
            for item in general_storage.get_items_by_ids(cf,selected_items_ids):
                utils.write_to_csv(csvfile,[item['post_id'],item['object_id'],item['original_data']['created_at'],item['original_data'].get('user').get('name'),item['message']])           

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='General query for twitter.') 
    parser.add_argument('config', type=str, help='an config file for general storage')
    parser.add_argument('--type',  type=str, default="ids", help='type of output, ids or random')
    parser.add_argument('--path', type=str, default='tmp/', help='output path for the csv files')
    parser.add_argument('--ids', type=str, default='', help='if provided, output replies of these comma separated post ids to their separate reply csv file')
    parser.add_argument('--query', type=str, default='', help='if provided, query and filter replies of the results to a csv file')
    parser.add_argument('--random_no', type=int, default=1000, help='if provided with the query argument, randomly select the given number of replies and output them to a csv file')

    args = parser.parse_args()
    config = __import__(args.config)
    cf =config.Config() 

    ## Example queries
    #total,items = query_items(cf,"messsage:'7 year anniversary'")
    #total,items = query_items(cf,"user_id:857888651895820288 AND object_type:post")
    #total,items = query_items(cf,"message:never AND object_type:comment")
    
    if args.type=="ids":
        output_replies_to_ids(cf,args.path,args.ids)
    if args.type=="random":
        output_items_random(cf,args.path,args.query,args.random_no)

