#!/usr/bin/python3
import json,datetime,time,argparse,logging,sys,os
sys.path.append(os.path.join(os.path.dirname(__file__), "libs"))
from boto3.dynamodb.conditions import Attr
import general_storage,sqs,utils,query,general_storage_mysql
from progress.bar import Bar
from pprint import pprint
class Normalizer():
    ## Normalizer class hold data and configurations for normalizing source/target pairs
    ## source(input) of normalization
    source={}
    ## target(output) of normalization
    target={}
    ## mapping from target key to source key or lambda function
    target_source_rule={}
    
    def set_source(self,source):
        self.source=source

    def set_target(self,target):
        self.target=target

    def get_source_value(self,s):
        ## get value from source with key s or lambda function s
        mapping=self.target_source_rule[s]
        if isinstance(mapping,str):
            ## if mapping is a string key
            return self.source.get(mapping)
        else:            
            ## if mapping is lambda function
            return mapping(self)

    def get_info(self,item):
        ## get info field 
        author = self.get_author(item) 
        return utils.fix_data_to_string({
            "created_time" : item["created_time"],
            "message":item['message'],
            "from" : json.loads(author)
        })
        
    def get_author(self,item):
        ## get author field
        return utils.fix_data_to_string({"id":item["user_id"],
                                         "name":item.get("user_name","unknown"),
                                         "profile_picture_url":item['original_data'].get("user",{}).get("profile_image_url_https","")})

    def normalize_source_to_target(self,cf,source):
    ## Normalizing from source obect to target object
        self.set_source(source)
        if self.source:
            for s in self.target_source_rule:
                self.target[s] = self.get_source_value(s)
        else:
            print("No source specified")


class Normalizer_post_dynomodb_mysql(Normalizer):
    ## Normalizer class for post from dynamodb to mysql
    name="posts"
    ## source(input) of normalization
    source={}
    ## target(output) of normalization
    target={}
    target_source_rule={'page_id':'asset_id',
                        'sub_page_id':'asset_id',
                        'post_id':'object_id',
                        'updated_time':'updated_time',
                        'created_time':'created_time',
                        'info':lambda x: x.get_info(x.source),
                        'json_search':'',
                        'author':lambda x:x.get_author(x.source),
                        'tags':'',
                        'task_ids':''
             }


class Normalizer_comment_dynomodb_mysql(Normalizer):
    ## Normalizer class for comment from dynamodb to mysql
    name="comments"
    ## source(input) of normalization
    source={}
    ## target(output) of normalization
    target={}
    target_source_rule={'page_id':'asset_id',
                        'sub_page_id':'asset_id',
                        'message':'message',
                        'post_id':'post_id',
                        'comment_id':'object_id',
                        #'parent_id':'post_id',
                        #'updated_time':'updated_time',
                        'created_time':'created_time',
                        'info':lambda x: x.get_info(x.source),
                        'json_search':'',
                        'author':lambda x:x.get_author(x.source),
                        'tags':'',
                        'task_ids':''
             }

def insert_dynamodb_item_into_mysql(cf,i):
    ## Main function to call normalizer to normalize object from dynamodb object to mysql object, and then insert normalized item to mysql database
    if i['object_type']=='post':
        nl = Normalizer_post_dynomodb_mysql()
    else:
        nl = Normalizer_comment_dynomodb_mysql()

    nl.normalize_source_to_target(cf,i)
    connection = general_storage_mysql.create_connection(cf)
    attributes,values = general_storage_mysql.simple_json_to_mysql_query(nl.target)
    query="insert into twit_%s_%s(%s) values(%s)" %(nl.name,cf.client_short_name,attributes,values)
    #print(query)
    general_storage_mysql.execute_query(connection,query)

def delete_mysql_item(cf,i):
    ## Main function to call deleteitem to mysql database
    if i['object_type']=='post':
        query="delete from twit_posts_%s(%s) where post_id=%s" %(cf.client_short_name,i['object_id'])
    else:
        query="delete from twit_comments_%s(%s) where comment_id=%s" %(cf.client_short_name,i['object_id'])

    connection = general_storage_mysql.create_connection(cf)
    general_storage_mysql.execute_query(connection,query)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Normalizer for twitter between DynamoDB and mysql') 
    parser.add_argument('config', type=str, help='an config file for normalizer')
    parser.add_argument('--query', type=str, default=None, help='query to get data for normalizer')
    parser.add_argument('--type', type=str, default="own", help='general or own. general:get everything using query; own:get own post and all replies')
    args = parser.parse_args()
    config = __import__(args.config)
    cf =config.Config() 

    if args.type=="own":     
        query_str = args.query
        if query_str:
            query_str = query_str + " AND user_id:%s AND object_type:post" %(cf.twitter_user_id)
        else:
            query_str="user_id:%s AND object_type:post" %(cf.twitter_user_id)
        print(query_str)
        total,posts = query.query_items(cf,query_str)
        if total>0:
            for post_id in [x["id"] for x in posts]:
                post_with_comments=general_storage.get_item_and_comments(cf,post_id)
                #print("%s comments" %(len(post_with_comments["comments"])))
                insert_dynamodb_item_into_mysql(cf,post_with_comments["item"])
                for comment in post_with_comments["comments"]:
                    insert_dynamodb_item_into_mysql(cf,comment)
            
    elif args.type=="general":
        #utils.run_until_finish(lambda: utils.process_sqs_rerun(cf,queue_name,process_clara,cf.clara_batch_size))
        db_items=general_storage.get_items_by_ids(cf,query.es_outputs_to_ids(items))
        for i in db_items:
            insert_dynamodb_item_into_mysql(cf,i)
