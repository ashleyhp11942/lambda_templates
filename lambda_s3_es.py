"""
This lambda function updates Elasticsearch when a new file is uploaded in the S3 bucket. It converts a csv to json then uploads it to Elasticsearch. 
"""

import boto3
import json
import os
import sys

sys.path.append('/opt/')

import requests
from requests_aws4auth import AWS4Auth
from elasticsearch import Elasticsearch, RequestsHttpConnection
import urllib
import random
import string
import csv

s3_res = boto3.resource('s3')

## ElasticSearch Cluster credentials
es_domain = os.getenv("")
host = 'https://' + es_domain

## Index details
es_index = os.getenv("")  # realtime-iot-index

headers = {"Content-Type": "application/json"}
## Connecting to ES cluster

region = ''
service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

def randomStringDigits(stringLength=10):
    """Generate a random string of letters and digits """
    lettersAndDigits = string.ascii_letters + string.digits
    return 'DOC_'+''.join(random.choice(lettersAndDigits) for i in range(stringLength))
    
    
print('Loading function')

def make_json(csvFilePath):
    data = []
      
    with open(csvFilePath) as csvf: 
        csvReader = csv.DictReader(csvf) 
         
        for rows in csvReader: 
            data.append(rows) 
            
    return json.loads(json.dumps(data))


def lambda_handler(event, context):
    es_url = host + '/' + es_index + '/_doc' 
    print(event)
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    tmp_csv = "/tmp/file.csv"
    return_log = "/tmp/data_upload_log.csv"
    
    s3_object = s3_res.Object(bucket, key)
    
    try:
        try:
            data = s3_object.get()['Body'].read().decode('utf-8-sig')
        except:
            data = s3_object.get()['Body'].read().decode('utf-8')
    
        with open(tmp_csv, 'a') as csv_data:
            csv_data.write(data)
            
        json_content = make_json(tmp_csv)
        print(type(json_content))
        
        # delete ES index and recreate 
        delete_response = requests.delete(host + '/' + es_index , auth = awsauth)
        print(delete_response)
        create_response = requests.put(host + '/' + es_index , auth=awsauth)
        print(create_response)
        
        i = 0 
        for each_row in json_content:
            data_dict = each_row
        ## Putting data into Elasticsearch 
            es_url = es_url
            
            req = None
            req = requests.post(es_url, auth=awsauth, json=data_dict, headers=headers)
           
            ## Faliure
            if req.status_code != 200 and req.status_code != 201:
                print(req.text)
                raise Exception('Received non 200 response from ES.')
    
            #print(str(r._content(), encoding))
            print("ES Status: {} and content {}".format(req.status_code, req.text))
            i = i+1 
        print("Number of records uploaded: {}".format(i)) 
        
        # Add return note to S3 bucket of successful data upload. 
        log_info = ""
        log_info = log_info + "Data upload Successful!"
        log_info = log_info + "\n {} records uploaded.".format(i)
    
    except: 
        log_info = "Data upload Unsuccessful. \n File format error. Please confirm column names and upload in csv format."
    
    # Upload data upload status to S3 
    
    s3_res.Bucket(bucket).put_object(Key='data_upload_status.txt', Body = log_info)  #, ACL = 'public-read')