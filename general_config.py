#!/usr/bin/python3
# config.py
import os
dir_path = os.path.dirname(os.path.realpath(__file__))
#dir_path = "/tmp/"

class Config:
    type='general'
    ## Crawler
    consumer_key=''
    consumer_secret=''
    access_token_key=''
    access_token_secret=''    
    max_return_count=100
    result_type='recent'
    clients_table='clients'
    crawler_timout=30

    ## Normalizer 
    normalizer_batch_size=1000

    ## Language detector
    minimum_lang_probability=0.6
    
    ## Clara
    #clara_params={"classifiers" : "utag-extract,utag-remove,spam,langdet,translate,positive"}
    clara_params={"classifiers" : "utag-extract,utag-remove,langdet,translate,positive,negative,spam,sexual,other, piracy, statsent_neutral, non_harmful_zero, non_harmful_zero_ext, nnmalware, non_harmful, nh_positive, statsent_simplified_neutral, nnpii, nnaddme, pii, random, profanity_mild, nnhcm, hcm, statsent_positive, statsent_highpositive, discrimination, statsent_simplified_positive, nnprofanity, unauth_selling, statsent_simplified_negative, profanity_extreme, drugs, statsent_highnegative, statsent_undefined, protest, nnspam, vpn, statsent_negative", "allow_meta":"true"}
    #clara_params={"classifiers" : "utag-extract,utag-remove,langdet,translate,positive,negative,spam,sexual", "allow_meta":"true"}
    clara_api_url_base='http://clara.staging.mod-app.com/clara/test/'
    clara_api_headers = {'Content-Type': 'application/json'}
    clara_batch_size=50

    ## ES
    es_filter_path=['hits.total','hits.hits._source.id','hits.hits._source.object_id']

    ## RDS MySQL
    host='twitter-test.cnjspbidnvrv.eu-west-1.rds.amazonaws.com'
    user='twitter'
    password='twitter2018-test'
    db='twitter'
    charset='utf8mb4'


    def get_clara_max(item):
        return item.clara_batch_size

class DevelopmentConfig(Config):
    type='dev'
    consumer_key='5Gh2OUk7fSzsJNfUCcLJg3g4p'
    consumer_secret='raUbjg26nJ5bVhNLbZYA8fqzhLdjSAPxe13AnQ6BiMCok9iDza'
    access_token_key='964103953037307904-8MTFSnIR3pkCKlVeYZQjeaQ2KMA1yrg'
    access_token_secret='RttX99tzu2YzhL0hYnuHDkTrfZFMUuwhZzsgPZRt7BYzg'   
    
    
class TestConfig(Config):
    type='test'
    
class ProductionConfig(Config):
    type='prod'
