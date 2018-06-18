#!/usr/bin/python3

import sys,os,json,argparse
sys.path.append(os.path.join(os.path.dirname(__file__), "libs"))
import boto3
from progress.bar import Bar
import general_storage,query
client = boto3.client('sqs')

def send_message(queue_name,message):
    '''Send a message to sqs: specify name of queue and provide a message'''
    try:
        queue_url = get_url_by_name(queue_name)    
    except Exception as e:
        print("No queue was found, creating one...")
        response = client.create_queue(
            QueueName=queue_name
        )
        queue_url = get_url_by_name(queue_name)    
    try:
        response = client.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(message,cls=general_storage.DecimalEncoder))
        return True
    except Exception as e:
        print("Writing to SQS faild:" +str(e))
    

def send_messages(queue_name,message_list):
    '''Send a list of messages in a seqential mode, which is used when the size of the
    message reaches the limit of sqs(so batch model won't help).
    Return: number of success 
    Input: specify name of queue and provide a list of messages'''
    counter=0
    error=0
    for message in message_list:        
        send_status=send_message(queue_name,message)
        if send_status:
            counter+=1
        else:
            error+=1
    return counter,error
    #return 0

def send_message_batch(queue_name,message_list):
    '''Send a list of messages in a batch mode
    Return: number of success 
    Input: specify name of queue and provide a list of messages'''
    queue_url = get_url_by_name(queue_name)
    try:
        response = client.send_message_batch(
            QueueUrl=queue_url,
            Entries=message_list)
        return len(message_list)
    except Exception as e:
        print("Batch writing to SQS faild:" +str(e))

def read_message(queue_url):
    response = client.receive_message(
        QueueUrl=queue_url,
    )
    if "Messages" in response and "Body" in response['Messages'][0]:
        # Let the queue know that the message is processed
        #message.delete()
        message=json.loads(response['Messages'][0]['Body'],parse_float=str)
        
        return message['data'],response['Messages'][0]['ReceiptHandle']
    else:
        return [],False

def delete_message(queue_url,handler):
    response = client.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=handler
    )    

def get_url_by_name(name):
    return client.get_queue_url(QueueName=name).get('QueueUrl')

def create_message(this_message,extra):
    ## create a sqs message
    message={'data':this_message}
    for key in extra:
        message[key]=extra[key]                
    return message

def create_messages(batch_size,task,items,extra):
    ## create a list of sqs messages
    messages=[]
    this_message=[]
    bar = Bar('Creating messages', max=len(items))
    counter=0
    for i in items:
        #bar.next()
        this_message.append({'id':i['id'],
                             'object_id':i['object_id']})
        counter+=1
        if counter==batch_size or sys.getsizeof(this_message) > 10000:
            counter=0
            messages.append(create_message(this_message,extra))            
            #messages.append({
            #    'Id':str(i['id']),
            #    'MessageBody': json.dumps({'data':this_message})
            #})
            this_message=[]
    bar.finish()
    if (len(this_message) > 0):
        messages.append(create_message(this_message,extra))            
            #{
            #    'Id':str(items[len(items)-1]['id']),
            #    'MessageBody': json.dumps({'data':this_message})
            #}
            #json.dumps({'data':this_message})
    return messages

def send_to_queue(cf,task,items,batch,extra):
    queue_name=task+"_"+cf.client_short_name
    messages = create_messages(batch,task,items,extra)
    return send_messages(queue_name,messages)    

def send_task(cf,query_str,task,batch=100,extra={}):
    ## find items using "query" in lucene format to query ES and send the items to SQS task queue  
    total,items = query.query_items(cf,query_str)
    if extra.get('type')=='post':
        ## if process type is post, get all replies of the post and send replies to queue        
        process_post_comments(cf,create_messages(batch,task,items,extra),task,batch)
        send_to_queue(cf,task,items,batch,extra)
    else:
        ## if process type is general, send the data to queue directly
        #print("Sending to queue:%s" %(len(items)))
        send_to_queue(cf,task,items,batch,extra)
    return total
    #print("%s items have been added to SQS" %(total))

def process_post_comments(cf,data,task,batch):
    print("Processing post comments")
    total=0
    for item in data: 
        for d in item['data']:
            total+=send_task(cf, "post_id:%s" %(d['object_id']), task, batch, {"post_id":d['object_id']})    
    return total


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='SQS queue management') 
    parser.add_argument('config', type=str, help='an config file for queue')
    parser.add_argument('--query', type=str, default=None, help='lucene format query string for obtaining objects to send to queue')
    parser.add_argument('--type',type=str, default='general', help='general or post: general process send input data to sqs; post process get all comments of the post and send them to sqs')
    parser.add_argument('--batch',type=int,default=100, help='batch size of the sqs message, default to 100')
    parser.add_argument('--task', type=str, default='normalizer', help='Task for the queue, e.g. normalizer, clara, etc.')

    args = parser.parse_args()
    config = __import__(args.config)
    cf =config.Config() 

    ## example test and query
    #send_task(cf, "user_id:857888651895820288 AND object_type:post", "normalizer")
    if args.type=='general':
        print("SQS: in general process")
        send_task(cf, args.query, args.task, args.batch)
    elif args.type=='post':
        print("SQS: in post process")
        send_task(cf, args.query, args.task, args.batch, {'type':'post'})
    else:
        print("SQS: wrong type specified.")

