#!/usr/bin/python3

import argparse,json,decimal,sys,os
sys.path.append(os.path.join(os.path.dirname(__file__), "libs"))
import boto3
from progress.bar import Bar
from boto3.dynamodb.conditions import Attr
from elasticsearch import Elasticsearch
import query

#awsauth = AWS4Auth(your_access_key, your_secret_key, region, 'es')

# Get the service resource.
dynamodb = boto3.resource('dynamodb')

# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

def get_dynamodb_table(table_name):
    return dynamodb.Table(table_name)

def create_table(name, key):
    # Create the DynamoDB table.
    table = dynamodb.create_table(
        TableName=name,
        KeySchema=[
            {
                'AttributeName': key,
                'KeyType': 'S'
            },
        ],
        AttributeDefinitions=[
            {
                'AttributeName': key,
                'AttributeType': 'S'
            },

        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )

    # Wait until the table exists.
    table.meta.client.get_waiter('table_exists').wait(TableName=name)

    # Print out some data about the table.
    print("Table "+name+" is created.")
    return table

def lock_items_by_task(table,items,task):
    ## Depreciated (too slow): locking items for task in DynamoDB so other works won't take the same object to perform the same task
    for item in items:
        #general_storage.updateItem(table,item['id'],{task:1})
        item[task]=1
    #general_storage.batch_update_item(table,items)
    return items

def get_item_by_task(table,task,start,limit):
    ## get items for performing tasks, such as normalization, clara, etc. 
    ## set batch_limit smaller to lock items faster
    batch_limit=min(500,limit)
    counter=0
    locked_items=[]
    items=[0]
    LastEvaluatedKey=False
    bar = Bar('Retrieving data from DB', max=limit)
    while counter < limit and (len(items)>0 or LastEvaluatedKey):
        ## get items for task
        if start:
            items,LastEvaluatedKey=scan_items(table, Attr(task).lt(start), batch_limit,LastEvaluatedKey)
        else:
            items,LastEvaluatedKey=scan_items(table, False, batch_limit,LastEvaluatedKey)
        counter+=len(items)
        bar.next(batch_limit)
        ## lock items so other worker won't get them
        locked_items=locked_items+lock_items_by_task(table,items,task)
    bar.finish()
    return locked_items
    

def get_item_by_id(table,id):
    ## get the item by id
    return table.get_item(Key={'id':id}).get('Item')

def get_items_by_ids(cf,ids):
    ## get a list of items in batch mode by a list of ids
    items=[]
    keys=[{'id':id} for id in ids]
    index=0
    ## dynamodb batch read supports reading at most 100 items a time
    max_items=100
    keys_count = len(keys)
    while index < keys_count:
        response=dynamodb.meta.client.batch_get_item(
            RequestItems={
                cf.table_name: {
                    'Keys': keys[index:index+100]            
                }        
            },
            ReturnConsumedCapacity='TOTAL'
        )
        index+=100
        items=items+response['Responses'][cf.table_name]
    return items

def insert_item(table, item):
    ## insert one item into DB
    table.put_item(Item=item)


def insert_items(table,items):
    ## insert a list of items into DB one by one 
    counter=0
    error=0
    true=True
    for i in items:
        try:
           table.put_item(Item=i)
           counter+=1
        except Exception as e:
            print(i, e)
            error+=1
    #print("Writing to DB: %s done, %s error" %(str(counter),str(error)))

def insert_batch(table, batchItems):
    ## insert a list of items into DB in batch mode
    true=True
    c=0
    with table.batch_writer() as batch:
        for i in batchItems:
            try:
                batch.put_item(
                    Item=i
                )
            except Exception as e:
                c+=1
                continue
    print("Errors: %s" %(str(c)))

                 
def scan_items(table,query=False,limit=10000,LastEvaluatedKey=False):
    #Retrieve up-to limit of data from DynamoDb directly, query is optional 
    if query:
        if LastEvaluatedKey:
            response = table.scan(
                FilterExpression=query,
                ExclusiveStartKey=LastEvaluatedKey
            )
        else:
            response = table.scan(
                FilterExpression=query,
            )
    else:
        if LastEvaluatedKey:
            response = table.scan(ExclusiveStartKey=LastEvaluatedKey)
        else:
            response = table.scan()

    items = response['Items']
    LastEvaluatedKey=response.get('LastEvaluatedKey')

    ## needs to use pagination to get all data, each request to dynamodb can get maximum 1MB data
    while LastEvaluatedKey and len(items) < limit:
        if query:
            response = table.scan(
                FilterExpression=query,
                ExclusiveStartKey=LastEvaluatedKey
            )
        else:
            response = table.scan(ExclusiveStartKey=LastEvaluatedKey)
        items = items + response['Items']
        LastEvaluatedKey=response.get('LastEvaluatedKey',False)

    return items, LastEvaluatedKey
    

def get_item_and_comments(cf,item_id):
    ## get tweet object. If item is an post, get all its comments from DB
    item=get_item_by_id(dynamodb.Table(cf.table_name),item_id)
    comments=get_item_comments(cf,item)
    return {"item":item,
            "type":item['object_type'],
            "comments":comments}
    
def get_item_comments_ids(cf,item):
    if item['object_type']=='post':
        total, items = query.query_items(cf,"post_id:%s" %(item['object_id']))
        if total > 0:
            return [x["id"] for x in items]
def get_item_comments(cf,item):
    ## get all comments of an item from DB
    comments=[]
    comments_ids=get_item_comments_ids(cf,item)
    if comments_ids:
        comments = get_items_by_ids(cf,comments_ids)
    return comments

def update_items(table,items):
    ## update a list of items in DB one by one
    counter=0
    error=0
    bar=Bar('Updating data', max=len(items))
    for i in items:
        try:
            update_item(table,i['id'],i)
            bar.next()
            counter+=1
        except Exception as e:
            print("Updating failded for %s" %(i['id']))
            print(e)
            error+=1
    bar.finish()
    return counter,error


def update_item(table,id,attributes):
    ## update an item in DB
    update_expression="SET "
    attribute_values={}
    attributes=attributes
    for a in attributes:
        if a=="id":
            continue
        update_expression=update_expression+"%s = :attr_%s, " %(a,a)
        attribute_values[":attr_%s" %(a)]=attributes[a]
    
    update_expression=update_expression[:len(update_expression)-2]

    table.update_item(
        Key={
            'id': id,
        },
        UpdateExpression=update_expression,
        ExpressionAttributeValues=attribute_values
    )

def batch_update_item(table,items):
    ## update a list of items in DB in a batch mode 
    counter=0
    error=0
    bar = Bar('Updating data', max=len(items))
    with table.batch_writer(overwrite_by_pkeys=['id']) as batch:
        for item in items:
            bar.next()
            try:
                batch.delete_item(
                    Key={
                        'id': item['id'],
                        #'id': item['original_data']['id']
                        #'created_time': str(item['created_time'])+".0"
                    }
                )
                batch.put_item(
                    Item=item
                )
                counter+=1

            except Exception as e:
                print(e)
                error+=1
    bar.finish()
    #bar = Bar('Writing to new table', max=len(items))
    #with dynamodb.Table('client_n').batch_writer(overwrite_by_pkeys=['id']) as batch:
    #    for item in items:
    #        bar.next()
    #        try:
    #            batch.put_item(
    #                Item=item
    #            )
    #            counter+=1
    #
    #        except Exception as e:
    #            print(e)
    #            error+=1
    #bar.finish()
    return counter,error

def batch_delete_item(table,items):
    ## delete a list of items in DB in a batch mode 
    counter=0
    error=0
    bar = Bar('deleting data', max=len(items))
    with table.batch_writer(overwrite_by_pkeys=['id']) as batch:
        for item in items:
            bar.next()
            try:
                batch.delete_item(
                    Key={
                        'id': item['id']
                    }
                )
                counter+=1

            except Exception as e:
                print(e)
                error+=1
    bar.finish()
    return counter,error

def delete_post_and_comments_by_id(cf,post_id):
    items_to_delete = [{"id":"50_"+post_id}]
    #items_to_delete = []
    total, items = query.query_items(cf,"post_id:%s" %(post_id))
    for i in items:
        items_to_delete.append(i)
    print(batch_delete_item(dynamodb.Table(cf.table_name),items_to_delete))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='General storage for twitter.') 
    parser.add_argument('config', type=str, help='an config file for general storage')
    parser.add_argument('--type', default='query',type=str, help='query or delete a post, default operation is query')
    parser.add_argument('--post_id', type=str, help='post_id to query or delete')

    args = parser.parse_args()
    config = __import__(args.config)
    cf =config.Config() 

    if args.type=='delete':
        delete_post_and_comments_by_id(cf,args.post_id)
    
