#!/usr/bin/python3
import json,datetime,time,argparse,logging,sys,os
sys.path.append(os.path.join(os.path.dirname(__file__), "libs"))
from boto3.dynamodb.conditions import Attr
import general_storage,sqs,utils,lang_detect
import dateutil.parser
from progress.bar import Bar


def get_offset(keyword, string):
    string.lower().find(keyword)

def get_text(tweet):
    if "extended_tweet" in tweet:
        return tweet['extended_tweet'].get('full_text') or tweet.get('text','')
    else:
        return tweet.get('full_text') or tweet.get('text','')
        
def get_entities_from_keword(keyword,tweet):
    entities=[]
    text=get_text(tweet)
    for k in keyword.split(" OR "):
        k.replace("%22","")
        entities.append({'id':k,'name':k,'length':len(k),'type':'k','offset':get_offset(k,text)})
    return entities

def normalize_hashtags(hashtags):
    return [{'id':h['text'],
             'name':h['text'],
             'type':'hashtag',
             'offset':h['indices'][0],
             'length':len(h['text'])
         } for h in hashtags]        
    
def normalize_tweets(cf,tweets):
    return [normalize_tweet(cf,x) for x in tweets]

def change_to_comment(tweet):
    ori_tweet=tweet['original_data']
    if 'in_reply_to_status_id_str' in ori_tweet and ori_tweet['in_reply_to_status_id_str']:
        tweet['post_id'] = ori_tweet['in_reply_to_status_id_str']
        if 'in_reply_to_user_id_str' in ori_tweet:
            tweet['asset_id'] = ori_tweet['in_reply_to_user_id_str']
        if 'in_reply_to_screen_name' in ori_tweet:
            tweet['asset_name'] = ori_tweet['in_reply_to_screen_name']
        tweet['object_type'] = 'comment'    
    return tweet


def normalize_tweet(cf,tweet):
    text=get_text(tweet)
    
    keyword=cf.keyword
    platform_id=50
    created_at=utils.date_str_to_posix(tweet['created_at'])
    normalized = {'id':"%s_%s" %(str(platform_id),tweet['id_str']),
                  'platform_id':platform_id,
                  'message':text,
                  'created_time_pretty':tweet['created_at'],
                  'created_time':created_at,
                  'updated_time':created_at,
                  'object_id':tweet['id_str'],
                  'object_type':'post',
                  'user_id':tweet.get('user').get('id'),
                  'user_name':tweet.get('user').get('name'),
                  'lang':tweet.get('lang','uncertain'),
                  'normalized':utils.get_current_posix_number(),
                  'entities':get_entities_from_keword(keyword,tweet),
                  'original_data':tweet}
    ## if we have set entities in configuration, update the entities attribute
    #if hasattr(cf,'entities'):
    #    normalized['entities']=[{'id':e,
    #                             'name':e,
    #                             'length':len(e),
    #                             'type':'e',
    #                             'offset':get_offset(e,text)} 
    #                            for e in cf.entities]

    ## create entity objects from tweets' hashtags
    if 'entities' in tweet and 'hashtags' in tweet['entities']:
        try:
            hashtags = normalize_hashtags(tweet['entities']['hashtags'])
            for h in hashtags:
                normalized['entities'].append(h)
        except Exception as e:
            print("Error when creating hashtags")
            
    ## create post_id and change type to comment if the object is a reply
    normalized = change_to_comment(normalized)

    ## create lang if twitter provide it, otherwise use language detector to assign the most probable language above threshold 
    try:
        normalized['lang_detect']=lang_detect.get_most_probable_lang(cf,list(lang_detect.detect_languages(cf,normalized['message']))),
    except Exception as e:
        logging.error('Language detection failed for %s:%s' %(normalized['id'], normalized['message']))
        normalized['lang_detect']='uncertain'
    if normalized['lang']=='uncertain':
        logging.info("Uncertain language for %s:%s" %(normalized['id'], normalized['message']))
        normalized['lang']=normalized['lang_detect']
        logging.info("Detected language for %s:%s" %(normalized['id'], normalized['lang_detect']))
    return utils.fix_data(normalized)
    
def rerun_normalizer(cf,tweets):
    return [combine_normalized_data(cf,tweet) for tweet in tweets]

def combine_normalized_data(cf,tweet):
    normalized_tweet = normalize_tweet(cf,tweet['original_data'])
    for attr in normalized_tweet:
        tweet[attr]=normalized_tweet[attr]
        #tweet['updated_time']=date_str_to_posix(datetime.datetime.now())
    return tweet

#def send_rerun(cf,start,distributed):
#    table=general_storage.dynamodb.Table(cf.table_name)
#    limit=1000
#    #items=general_storage.scan_items(table, Attr("normalized").ne(2), limit)
#    items=general_storage.get_item_by_task(table,"normalized",start,limit)
#    if not items:
#        print("Finished: %s data are updated." %(str(counter)))
#        return 0
#
#    if distributed:
#        return sqs.send_to_queue(queue_name,items)
#    else:
#        return process_rerun(cf,items)
#
#def process_rerun(cf,items):
#    #table=general_storage.dynamodb.Table(cf.table_name)
#    table=general_storage.dynamodb.Table(cf.table_name)
#    error=0
#    normalized_items=[]
#    print("Normalizing items")
#    for item in items: 
#        item=rerun_normalizer(cf,item)
#        try:
#            item=rerun_normalizer(cf,item)
#            #print(item['entities'])
#            normalized_items.append(item)
#        except Exception as e:
#            print(e)
#            error+=1
#
#    print("%s data are normalized, error %s" %(str(len(normalized_items)),str(error)))
#
#    counter,error=general_storage.batch_update_item(table,normalized_items)
#    print("%s data are updated, error %s" %(str(counter),str(error)))
#    return counter,error
#    #print([x['id'] for x in normalized_items])
#
#def process_sqs_rerun(cf,queue_url):
#    table=general_storage.dynamodb.Table(cf.table_name)
#    message,handler=sqs.read_message(queue_url)
#    normalized_items=[]
#    print("Normalizing items")
#    bar = Bar('rerun items', max=len(message))
#    #for m in message:
#    #    bar.next()
#    #    item = general_storage.get_item_by_id(table,m['id'])
#    #    try:
#    #        item=rerun_normalizer(cf.keyword,item)
#    #        #print(item['entities'])
#    #        normalized_items.append(item)
#    #    except Exception as e:
#    #        print(e)
#    #        error+=1
#    items = general_storage.get_items_by_ids(cf, [x['id'] for x in message])
#    counter,error=process_rerun(cf,items)
#    if handler and counter > 0:
#        sqs.delete_message(queue_url,handler)
#    return counter

def process_online(cf,start):
    table=general_storage.dynamodb.Table(cf.table_name)
    counter=0
    error=0
    limit=1000
    #items=general_storage.scan_items(table, Attr("normalized").ne(2), limit)
    items=general_storage.get_item_by_task(table,"normalized",start,limit)
    normalized_items=[]
    print("Normalizing items")
    for item in items: 
        try:
            item=rerun_normalizer(cf,item)
            normalized_items.append(item)
        except Exception as e:
            print(e)
            error+=1

    counter,error=general_storage.batch_update_item(table,normalized_items)
    ##for item in normalized_items: 
    ##    try:
    ##        if counter % 10 == 0:
    ##            print(counter)
    ##        general_storage.updateItem(table,item['original_data']['id'],item)
    ##        counter+=1
    ##    except Exception as e:
    ##        print(e)
    ##        error+=1

    print("%s data are normalized, error %s" %(str(counter),str(error)))
    #print([x['id'] for x in normalized_items])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Normalizer for twitter.') 
    parser.add_argument('config', type=str, help='an config file for normalizer')
    parser.add_argument('type', type=str, help='type of normalization process, i.e., online or rerun')
    parser.add_argument('-main',default='t', help='t or f: main process (True) started the rerun or distribute the rerun; child(False) process process the normalization by reading from sqs')
    parser.add_argument('-distributed',default=False, help='use sqs to distribute process or not')
    parser.add_argument('-start',default=False, help='set the start-time for re-normalization, any tweet with "normalized" time older than this will be normalized')

    args = parser.parse_args()
    config = __import__(args.config)
    start=int(str(time.time())[:10])
    #cf =config.DevelopmentConfig()    
    cf =config.Config() 
    logging.basicConfig(filename=cf.log_file,level=logging.INFO)
  
    queue_name="normalizer_"+cf.client_short_name
    if args.type=='rerun':
        if args.main=='t':
            utils.run_until_finish(lambda: utils.send_rerun(cf,"normalized",queue_name,rerun_normalizer,start,args.distributed))
        else:
            utils.run_until_finish(lambda: utils.process_sqs_rerun(cf,queue_name,rerun_normalizer,cf.normalizer_batch_size))
    if args.type=='online':
        process_online(cf,args.start)
