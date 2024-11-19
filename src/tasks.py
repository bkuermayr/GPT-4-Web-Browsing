import json
import logging
from celery import Celery
from dotenv import load_dotenv
import os
import time
import ssl
from DatabaseUtil import findValueCustomFields

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
    output_format = automationData.get('output_format', "json")
    profile = automationData.get('profile', "")
    search_location = automationData.get('search_location', "")
    search_language = automationData.get('search_language', "")
    output_language = automationData.get('output_language', "")
    job_id = automationData.get('job_id', "")
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
            }
            return response
    else:
        formatted_relevant_docs = None
        serper_response = None

    start = time.time()
    ai_message_obj = content_processor.get_answer(prompt, formatted_relevant_docs, output_language, output_format, profile)
    answer = ai_message_obj.content + '\n'
    end = time.time()

    logging.info(f'Generated answer in {end - start} seconds')

    locator = ReferenceLocator(answer, serper_response)
    reference_cards = locator.locate_source()
    
    # Parse the JSON string to a Python dictionary (JSON object)
    prefix = '```json'
    suffix = '```'
    
    if answer.startswith(prefix) and answer.endswith(suffix):
        # Remove the prefix and suffix
        answer_clean = answer[len(prefix):-len(suffix)]
    else:
        answer_clean = answer
    
    try:
        answer_json_object = json.loads(answer_clean)
    except json.JSONDecodeError as e:
        logging.error(f'JSONDecodeError: {e} for answer: {answer_clean}')
        answer_json_object = {}

    response = {
        'query': query,
        'job_id': job_id,
        'product_id': product_id,
        'answer': answer_json_object,
        'gpt_answer_time': end - start,
        'output_language': output_language,
        'reference_cards': reference_cards,
    }

    try:
        base_url = os.getenv('JOB_SERVER_URL', 'http://localhost:5000')
        req = grequests.post(f'{base_url}/save-automation-response', json=response)  
        post_response = grequests.map([req])[0]  
        if post_response and post_response.status_code == 200:
            logging.info(f'Posted response to save-automation-response endpoint with status code: {post_response.status_code}')
        else:
            logging.error(f'Failed to post response with status code: {post_response.status_code}')
    except Exception as e:
        logging.error(f'Failed to post response to save-automation-response endpoint: {e}')

    return response


@celery.task
def process_csv_feed(url,remote_directory, filename):
    process_data(url, remote_directory, filename)

    return {
        'status': 'success',
        'message': f'Processed CSV feed from {url} with filename {filename} in directory {remote_directory}'
    }