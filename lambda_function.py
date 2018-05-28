#!/usr/bin/python3
import argparse

import base64
import datetime
import json
import logging
import os
import time
import traceback
import urllib
import urllib.parse 

from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import get_credentials
from botocore.endpoint import BotocoreHTTPSession
from botocore.session import Session
from boto3.dynamodb.types import TypeDeserializer
import normalizer_mysql


# The following parameters can be optionally customized
DOC_TABLE_FORMAT = '{}'         # Python formatter to generate index name from the DynamoDB table name
DOC_TYPE_FORMAT = '{}_type'     # Python formatter to generate type name from the DynamoDB table name, default is to add '_type' suffix
DEBUG = True                    # Set verbose debugging information
config_file = "config_sc"

print("Streaming to RDS")
logger = logging.getLogger()
logger.setLevel(logging.DEBUG if DEBUG else logging.INFO)


# Subclass of boto's TypeDeserializer for DynamoDB to adjust for DynamoDB Stream format.
class StreamTypeDeserializer(TypeDeserializer):
   def _deserialize_n(self, value):
      return float(value)

   def _deserialize_b(self, value):
      return value  # Already in Base64


# Extracts the DynamoDB table from an ARN
# ex: arn:aws:dynamodb:eu-west-1:123456789012:table/table-name/stream/2015-11-13T09:23:17.104 should return 'table-name'
def get_table_name_from_arn(arn):
   return arn.split(':')[5].split('/')[1]


# Compute a compound doc index from the key(s) of the object in lexicographic order: "k1=key_val1|k2=key_val2"
def compute_doc_index(keys_raw, deserializer):
   index = []
   for key in sorted(keys_raw):
      index.append('{}={}'.format(key, deserializer.deserialize(keys_raw[key])))
      return '|'.join(index)


def _lambda_handler(event, context):
   logger.info('Event:'+json.dumps(event))
   logger.info('Context:'+json.dumps(context))
   records = event['Records']
   now = datetime.datetime.utcnow()
   
   config = __import__(config_file)
   cf =config.Config() 
   
   ddb_deserializer = StreamTypeDeserializer()
   cnt_insert = cnt_modify = cnt_remove = 0
   for record in records:
      # Handle both native DynamoDB Streams or Streams data from Kinesis (for manual replay)
      if record.get('eventSource') == 'aws:dynamodb':
         ddb = record['dynamodb']
         ddb_table_name = get_table_name_from_arn(record['eventSourceARN'])
         doc_seq = ddb['SequenceNumber']
      elif record.get('eventSource') == 'aws:kinesis':
         ddb = json.loads(base64.b64decode(record['kinesis']['data']))
         ddb_table_name = ddb['SourceTable']
         doc_seq = record['kinesis']['sequenceNumber']
      else:
         logger.error('Ignoring non-DynamoDB event sources: %s', record.get('eventSource'))
         continue

      # Compute DynamoDB table, type and index for item
      doc_table = DOC_TABLE_FORMAT.format(ddb_table_name.lower())  # Use formatter
      doc_type = DOC_TYPE_FORMAT.format(ddb_table_name.lower())    # Use formatter
      doc_index = compute_doc_index(ddb['Keys'], ddb_deserializer)
      
      # Dispatch according to event TYPE
      event_name = record['eventName'].upper()  # INSERT, MODIFY, REMOVE

      # Treat events from a Kinesis stream as INSERTs
      if event_name == 'AWS:KINESIS:RECORD':
         event_name = 'INSERT'
          
      # Update counters
      if event_name == 'INSERT':
         cnt_insert += 1
      elif event_name == 'MODIFY':
         cnt_modify += 1
      elif event_name == 'REMOVE':
         cnt_remove += 1
      else:
         logger.warning('Unsupported event_name: %s', event_name)         

      # If DynamoDB INSERT or MODIFY, send 'item' to RDS
      if event_name == 'INSERT' or event_name == 'MODIFY':
         if 'NewImage' not in ddb:
            logger.warning('Cannot process stream if it does not contain NewImage')
            continue
         # Deserialize DynamoDB type to Python types
         doc_fields = ddb_deserializer.deserialize({'M': ddb['NewImage']})

         # Now only store own post and replies
         if doc_fields['object_type']=='post' and str(doc_fields['user_id'])!=cf.twitter_user_id:
            continue
            
         # Now only store own post and replies
         if doc_fields['object_type']=='comment' and doc_fields['asset_id']!=str(cf.twitter_user_id):
            continue
                     
         # Normalize DynamoDB object to Mysql object and write to RDS
         normalizer_mysql.insert_dynamodb_item_into_mysql(cf,doc_fields)           
         
  #    # If DynamoDB REMOVE, send 'delete' to ES
  # elif event_name == 'REMOVE':
  #    normalizer_mysql.delete_mysql_item(cf,doc_fields)           

# Global lambda handler - catches all exceptions to avoid dead letter in the DynamoDB Stream
def lambda_handler(event, context):
   try:
      return _lambda_handler(event, context)
   except Exception:
      logger.error(traceback.format_exc())

if __name__ == "__main__":
   parser = argparse.ArgumentParser(description='Copying DynamoDB stream to mysql') 
   parser.add_argument('config', type=str, help='an config file for normalizer')
   args = parser.parse_args()
   config_file =args.config
   config = __import__(config_file)
   cf =config.Config() 
   with open('event-test.json', 'r') as myfile:
      event = json.loads(myfile.read())
      lambda_handler(event, "")
      
