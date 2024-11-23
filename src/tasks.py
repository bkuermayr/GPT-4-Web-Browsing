import re
import logging
from celery import Celery
from dotenv import load_dotenv
import os
import time
import ssl
from DatabaseUtil import findValueCustomFields, client
from celery.signals import task_postrun

import grequests
from fetch_web_content import WebContentFetcher
from llm_answer import GPTAnswer
from locate_reference import ReferenceLocator
from retrieval import EmbeddingRetriever
from csv_postprocessor import process_data


# Load environment variables from .env file
load_dotenv()

# Configure Celery using environment variables
broker_url = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
backend_url = os.getenv('REDIS_URL', 'redis://localhost:6379/0')

# Define SSL options if using rediss
if broker_url.startswith('rediss://'):
    ssl_options = {
        'ssl_cert_reqs': ssl.CERT_NONE  # Set to CERT_NONE if you don't have SSL certificates
    }
else:
    ssl_options = None

celery = Celery('tasks', broker=broker_url, backend=backend_url)

if ssl_options:
    celery.conf.update(
        broker_use_ssl=ssl_options,
        redis_backend_use_ssl=ssl_options
    )


@celery.task
def process_query_task(productData,automationData):
    query = productData.get('title',"")
    prompt = automationData.get('prompt', '')
    output_format = automationData.get('output_format', "json_object")
    profile = automationData.get('profile', "")
    search_location = automationData.get('search_location', "")
    search_language = automationData.get('search_language', "")
    output_language = automationData.get('output_language', "")
    job_id = automationData.get('automation_job_id', "")
    product_id = productData.get('id',"")
    use_web_search = automationData.get('use_web_search', True)
    searchAttr = automationData.get('searchAttributes',[])
    aiAttr = automationData.get('aiAttributes',[])
    customFields = productData.get('custom_fields',[])

    searchVal = '' 
    aiVal = ''
    for i in searchAttr:
        temp = findValueCustomFields(customFields,i)
        searchVal = f'{searchVal} {temp}'
    for j in aiAttr:
        temp = findValueCustomFields(customFields,j)
        aiVal = f'{aiVal} {temp}'
    query = query + searchVal
    #url = data.get('whitelist',"")
    #temp = ""
    #if len(url) >= 1:
    #    temp = f"site:{url[0]}"
    #for i in range(1,len(url)):
    #    temp = f"{temp} OR site:{url[i]}"
    # query = temp + query
    # Query für Serper: site:https://www.nike.com OR site:adidas.com Schuhe

    logging.info(f'Received query: {query}, search_location: {search_location}, search_language: {search_language}, output_language: {output_language}')
    logging.info(f'Received prompt: {prompt}')
    content_processor = GPTAnswer()

    if use_web_search:
        web_contents_fetcher = WebContentFetcher(query=query, search_location=search_location, search_language=search_language, output_language=output_language)
        web_contents, serper_response = web_contents_fetcher.fetch()
        retriever = EmbeddingRetriever()
        relevant_docs_list = retriever.retrieve_embeddings(web_contents, serper_response['links'], query)
        formatted_relevant_docs = content_processor._format_reference(relevant_docs_list, serper_response['links'])

        # if no urls are found, return json response with empty answer
        if not formatted_relevant_docs:
            response = {
                'query': query,
                'job_id': job_id,
                'product_id': product_id,
                'answer': {},
                'gpt_answer_time': 0,
                'output_language': output_language,
                'reference_cards': [],
                'failure' : True
            }
            return response
    else:
        formatted_relevant_docs = None
        serper_response = None

    start = time.time()
    ai_message_obj = content_processor.get_answer(prompt, formatted_relevant_docs, output_language, output_format, profile)
    answer = ai_message_obj.content
    answer = clean_json_string(answer)
    end = time.time()

    logging.info(f'Generated answer in {end - start} seconds')

    locator = ReferenceLocator(answer, serper_response)
    reference_cards = locator.locate_source()

    response = {
        'query': query,
        'job_id': job_id,
        'product_id': product_id,
        'answer': answer,
        'gpt_answer_time': end - start,
        'output_language': output_language,
        'reference_cards': reference_cards,
        'failure' : False
    }


    return response


@task_postrun.connect(sender=process_query_task)
def task_postrun_notifier(state=None, retval=None, task_id=None, args=None,**kwargs):
    aID = args[1].get('automation_job_id')
    product_id = args[0].get('id',"")
    if state=='SUCCESS':
        success = not retval.get('failure',True)
        client.table('automation_job_data').insert({'product_id':product_id,'automation_job_id':aID,'success':success,'data':retval}).execute()
    else:
        client.table('automation_job_data').insert({'product_id':product_id,'automation_job_id':aID,'success':False,'data':None,'error':retval}).execute()




@celery.task
def process_csv_feed(url,remote_directory, filename):
    process_data(url, remote_directory, filename)

    return {
        'status': 'success',
        'message': f'Processed CSV feed from {url} with filename {filename} in directory {remote_directory}'
    }

def clean_json_string(json_string):
    pattern = r'^```json\s*(.*?)\s*```$'
    cleaned_string = re.sub(pattern, r'\1', json_string, flags=re.DOTALL)
    return cleaned_string.strip()

@celery.task
def test(i):
    return {'query':' i ü ä ö test'}
