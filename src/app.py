from flask import Flask, request, jsonify
from celery import Celery
from dotenv import load_dotenv
import os
import ssl
from gevent import monkey
monkey.patch_all()  # Apply gevent monkey patches

from tasks import process_query_task, process_csv_feed

# Load environment variables from .env file
load_dotenv()

# Initialize the Flask application
app = Flask(__name__)

# Configure Celery using environment variables
app.config['broker_url'] = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
app.config['result_backend'] = os.getenv('REDIS_URL', 'redis://localhost:6379/0')

# Define SSL options if using rediss
if app.config['broker_url'].startswith('rediss://'):
    ssl_options = {
        'ssl_cert_reqs': ssl.CERT_NONE  # Set to CERT_NONE if you don't have SSL certificates
    }
else:
    ssl_options = None

def make_celery(app):
    celery = Celery(
        app.import_name,
        broker=app.config['broker_url'],
        backend=app.config['result_backend']
    )
    if ssl_options:
        celery.conf.update(
            broker_use_ssl=ssl_options,
            redis_backend_use_ssl=ssl_options,
            broker_connection_retry_on_startup=True
        )
    celery.conf.update(app.config)
    return celery

celery = make_celery(app)

@app.route('/api/query', methods=['POST'])
def process_query():
    data = request.get_json()
    task = process_query_task.apply_async(args=[data])
    return jsonify({"task_id": task.id})

@app.route('/api/status/<task_id>', methods=['GET'])
def task_status(task_id):
    task = process_query_task.AsyncResult(task_id)
    if task.state == 'PENDING':
        response = {
            'state': 'PENDING',
            'status': 'Pending...'
        }
    elif task.state != 'FAILURE':
        response = {
            'state': task.state,
            'result': task.result,
        }
    else:
        response = {
            'state': 'FAILURE',
            'status': str(task.info),  # this is the exception raised
        }
    return jsonify(response)

@app.route('/api/process_csv_feed', methods=['GET'])
def trigger_process_csv_feed():
    url = request.args.get('url')
    filename = request.args.get('filename')
    task = process_csv_feed.apply_async(args=[url, filename])
    return jsonify({"task_id": task.id})

@app.route('/')
def hello():
    return 'Hello, World!'

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', threaded=False)
