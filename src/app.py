from flask import Flask, request, jsonify
import time
import json
import logging
from fetch_web_content import WebContentFetcher
from retrieval import EmbeddingRetriever
from llm_answer import GPTAnswer
from locate_reference import ReferenceLocator
from celery import Celery
from celery.result import AsyncResult
import redis
import os

# Initialize the Flask application
app = Flask(__name__)

# Configure Redis URL
redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
print(f'Using Redis URL: {redis_url}')

# Initialize Redis connection pool
redis_pool = redis.ConnectionPool.from_url(redis_url, max_connections=10)
redis_client = redis.StrictRedis(connection_pool=redis_pool, decode_responses=True)

# Configure Celery
app.config['broker_url'] = redis_url
app.config['result_backend'] = redis_url

def make_celery(app):
    celery = Celery(
        app.import_name,
        broker=app.config['broker_url'],
        backend=app.config['result_backend'],
        result_backend_thread_safe=True
    )
    celery.conf.update(app.config)
    celery.conf.broker_connection_retry_on_startup = True

    # Configure Celery to use the same Redis connection pool
    celery.conf.broker_transport_options = {
        'max_connections': 10,
    }
    celery.conf.redis_backend_use_ssl = {
        'ssl_cert_reqs': None
    }

    return celery

celery = make_celery(app)

@celery.task(name='app.process_query_task')
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

    response = {
        'answer': answer,
        'gpt_answer_time': end - start,
        'output_language': output_language,
        'reference_cards': reference_cards,
    }

    return response

@app.route('/api/query', methods=['POST'])
def process_query():
    data = request.get_json()
    task = process_query_task.apply_async(args=[data])
    return jsonify({"task_id": task.id})

@app.route('/api/status/<task_id>', methods=['GET'])
def task_status(task_id):
    task_result = AsyncResult(task_id, app=celery)
    if task_result.state == 'PENDING':
        response = {
            'state': task_result.state,
            'status': 'Pending...'
        }
    elif task_result.state != 'FAILURE':
        response = {
            'state': task_result.state,
            'result': task_result.result,
        }
    else:
        response = {
            'state': task_result.state,
            'status': str(task_result.info),
        }
    return jsonify(response)

@app.route('/')
def hello():
    return 'Hello, World!'

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0')
