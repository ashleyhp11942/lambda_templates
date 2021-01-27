"""
This lambda takes the input from Lex and returns search results from Elastic search. 
The results are returned in a random order. 
"""
import sys

sys.path.append('/opt')
import boto3
import json
import os
import requests
from requests_aws4auth import AWS4Auth
import time
import datetime
import collections 
from datetime import datetime, timedelta
from datetime import datetime
from dateutil import tz
from elasticsearch import Elasticsearch, RequestsHttpConnection
from nltk.stem import PorterStemmer 
from nltk.tokenize import word_tokenize 
import logging
import random


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# Global Setup
## ElasticSearch Cluster credentials
es_domain = os.getenv("ES_DOMAIN")
host = 'https://' + es_domain

## Index details
es_index = 'chatbot-data' 
headers = {"Content-Type": "application/json"}

## Connecting to ES cluster

region = ''
service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

# --- Helpers that build all of the responses ---

def try_ex(func):
    """
    Call passed in function in try block. If KeyError is encountered return None.
    This function is intended to be used to safely access dictionary.

    Note that this function would have negative impact on performance.
    """

    try:
        return func()
    except KeyError:
        return None

def query_execute(url, query_to_execute):
	"""
	Queries Elasticsearch and returns the response. 
	"""
    query = query_to_execute
  
    # ES 6.x requires an explicit Content-Type header
    headers = { "Content-Type": "application/json" }
    start_time = datetime.now()
    # Make the signed HTTP request
    r = requests.get(url, auth=awsauth, headers=headers, data=json.dumps(query))

    # Create the response and add some extra content to support CORS
    response = {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin": '*'
        },
        "isBase64Encoded": False
    }

    # Add the search results to the response
    response['body'] = r.text
    end_time = datetime.now()
    print("Query Execution Completed. Time taken: {} seconds".format(str(end_time-start_time)))
    return response

def close(session_attributes, fulfillment_state, message):
	"""
	Creates the response to send back to Lex. This JSON is the input to the web interface. 
	"""
    response = {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }

    return response

def main_search_function(intent_request):
    """
 	Parses the user input to create the list of search terms. Calls query_execute() to query Elasticsearch and close() 
	to return the results.
	
	The query uses terms that were identified from the slots - along with other words that were greater than 4 letters. 
	 
    """
	
	# Create variables from input json 
    session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}
    intent_1 = try_ex(lambda: intent_request['currentIntent']['slots']['intent_1'])
    intent_2 = try_ex(lambda: intent_request['currentIntent']['slots']['intent_2'])
    intent_3 = try_ex(lambda: intent_request['currentIntent']['slots']['intent_3'])
   
    slot_resolutions = [intent_1 if intent_1 != None else ""] + [intent_2 if intent_2 != None else ""]
    actual_question = try_ex(lambda: intent_request['inputTranscript'])
    question_breakdown = actual_question.split()
	
	# this uses all words input that are greater than 4 or in the slot_resolutions 
    ques_list = [word for word in question_breakdown if len(word)>4 or word in slot_resolutions ]
    question_str = (' ').join(ques_list)
 
    print("Search terms : {}".format(question_str))
    
    ps = PorterStemmer() 
      
    # choose some words to be stemmed 
    words = question_str
    query_word = ""
    query_word_fmt = ""
    for w in words.split(): 
        w = w.replace(',', '')
        query_word = query_word + ps.stem(w) +'* '		
        # formatting the key_words for error handling 
        if len(words.split()) == 1 or w == words.split()[-2]:
            query_word_fmt = query_word_fmt + w
        elif w == words.split()[0]: 
            query_word_fmt = query_word_fmt + w + " "
        elif w == words.split()[-1]:
            query_word_fmt = query_word_fmt + ' or ' + w
        else:
            query_word_fmt = query_word_fmt + " " + w +', ' 
        
    print(query_word)
    
     url = host + '/' + es_index + '/_search'
    
    if intent_3 != None:
        Query_to_exec = {
            "query" : {
                "query_string": {
                    "query": query_word, 
                    "fields" : [""]
                   }
            }
            
        }
        results = json.loads(query_execute(url, Query_to_exec)['body'])
        results_list = (results['hits']['hits'])
        
        if len(ques_list) > 1: 
            query_word_fmt = query_word_fmt.replace("or", "")
        
    elif len(ques_list) < 2:
        Query_to_exec = {
        "query": {
            "query_string" : {
                "query" : query_word
            }
        }
        }
        results = json.loads(query_execute(url, Query_to_exec)['body'])
        results_list = (results['hits']['hits'])
        # randomize results 
        results_list = random.sample(results_list, len(results_list))
        
    else:
        Query_to_exec = {
            "query": {
                "multi_match" : {
                    "query": query_word,
                    "type": "most_fields",
                    "fields": [ ]
                }
            }
        }
        
        results = json.loads(query_execute(url, Query_to_exec)['body'])
        results_list = (results['hits']['hits'])
    
        
    print(Query_to_exec)
    
    
    results = json.loads(query_execute(url, Query_to_exec)['body'])
    results_list = (results['hits']['hits'])
    
    # Return error message if no results were found for the search terms. 
    if len(results_list) == 0:
        Ret_Str = 'Sorry, no results were found for {}. Please try a different phrase.'.format(query_word_fmt)
    else:              
        Ret_Str = 'Here are the results for {}: \n'.format(query_word_fmt)
        Ret_Str = Ret_Str + '\n'
        
		# Return the details of the response 
        
        
    session_attributes['OutputStr'] = Ret_Str
    
    return close(
        session_attributes,
        'Fulfilled',
        {
            'contentType': 'SSML',
            'content': Ret_Str
        }
    )
# --- Intents ---


def dispatch(intent_request):
    """
    Called when the user specifies an intent for the lex bot.
    """
    print(intent_request)
    logger.debug('dispatch userId={}, intentName={}'.format(intent_request['userId'], intent_request['currentIntent']['name']))

    intent_name = intent_request['currentIntent']['name']

    if intent_name == '':
        return main_search_function(intent_request)
    else:
        raise Exception('Intent with name ' + intent_name + ' not supported')


# --- Main handler ---


def lambda_handler(event, context):
    """
    Route the incoming request based on intent.
    The JSON body of the request is provided in the event slot.
    """
    # By default, treat the user request as coming from the America/New_York time zone.
    

    return dispatch(event)
    
