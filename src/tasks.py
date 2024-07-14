# src/tasks.py

import os
import time
import json
import logging

import requests
from fetch_web_content import WebContentFetcher
from retrieval import EmbeddingRetriever
from llm_answer import GPTAnswer
from locate_reference import ReferenceLocator
from celery import Celery
import os
import time

# Configure Celery using environment variables
celery = Celery('tasks', 
                broker=os.getenv('CELERY_BROKER_URL', 'redis://localhost:6379/0'), 
                backend=os.getenv('CELERY_RESULT_BACKEND', 'redis://localhost:6379/0'))

@celery.task
def process_query_task(data):
    query = data.get('search_query', '')
    prompt = data.get('prompt', '')
    output_format = data.get('output_format', "")
    profile = data.get('profile', "")
    search_location = data.get('search_location', "")
    search_language = data.get('search_language', "")
    output_language = data.get('output_language', "")
    job_id = data.get('job_id', "")
    product_id = data.get('product_id', "")
    use_web_search = data.get('use_web_search', True)

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
    answer_json_object = json.loads(answer)

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
        post_response = requests.post(f'{base_url}/save-automation-response', json=response)    
        logging.info(f'Posted response to save-automation-response endpoint with status code: {post_response.status_code}')
    except Exception as e:
        logging.error(f'Failed to post response to save-automation-response endpoint: {e}')

    return response
