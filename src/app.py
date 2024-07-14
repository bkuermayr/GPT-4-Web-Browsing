from flask import Flask, request, jsonify
from celery import Celery
import os

from tasks import process_query_task

# Initialize the Flask application
app = Flask(__name__)

# Configure Celery using environment variables
app.config['CELERY_BROKER_URL'] = os.getenv('CELERY_BROKER_URL', 'redis://localhost:6379/0')
app.config['CELERY_RESULT_BACKEND'] = os.getenv('CELERY_RESULT_BACKEND', 'redis://localhost:6379/0')

def make_celery(app):
    celery = Celery(
        app.import_name,
        backend=app.config['CELERY_RESULT_BACKEND'],
        broker=app.config['CELERY_BROKER_URL']
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

@app.route('/')
def hello():
    return 'Hello, World!'

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', threaded=False)
