#!/usr/bin/python3

import general_config
class Config(general_config.Config):
    client_id=9
    client_short_name='sc'
    twitter_user_id=171271070
    consumer_key='c9C1Vav59hV93y2cJjwBmtFz4'
    consumer_secret='xOuoEHHpzBSJjlnu09PZoMKLmYk56gtgOmfZFjBD3b9MUhZWq2'
    access_token_key='964103953037307904-LCJXkC1GzbbyU6EfexWA1IOeWcnhebI'
    access_token_secret='pTx3iGPHfQBUePJxONllkNnhwWvMMC9VD3NY28iGbv0gw'   
    bearer_token='Bearer AAAAAAAAAAAAAAAAAAAAAHxZ5QAAAAAAo%2B%2FJmvpfJbeSDvnjdoKa3a3SkHw%3DFk0am5cQyD2n9nkRQLlekg3zwdbkaoVXSI3Co6bWFcpeF7xJmH'
    table_name='client_sc'
    keyword='supercell'
    last_since_id_file=general_config.dir_path+"/"+keyword+'_last_id'
    cronjob_file=general_config.dir_path+"/"+keyword+'_cronjob'

    ## Logging
    log_file='%s/logs/%s.log' %(general_config.dir_path,table_name)

    ## Clara
    clara_params={"classifiers" : "utag-extract,utag-remove,langdet,translate,nh_positive,positive,negative,spam,sexual,pii,random,vpn,other,discrimination,drugs,hcm,profanity_extreme,protest,non_harmful", "allow_meta":"true"}
    #clara_params={"classifiers" : "utag-extract,utag-remove,positive,negative,spam,sexual", "allow_meta":"true"}
    #clara_params={"classifiers" : "utag-extract,utag-remove"}

    ## ES
    es_host='https://search-client-sc-om2a3opn6w4htnju5yxpkkjfou.eu-west-1.es.amazonaws.com'

