from flask import Flask, request, jsonify
from rq import Queue
from rq.job import Job
from worker import conn
from tasks import process_query_task

# Initialize the Flask application
app = Flask(__name__)

# Initialize Redis Queue
q = Queue(connection=conn)

@app.route('/api/query', methods=['POST'])
def process_query():
    data = request.get_json()
    job = q.enqueue_call(
        func='tasks.process_query_task', args=(data,), result_ttl=5000
    )
    return jsonify({"task_id": job.get_id()})

@app.route('/api/status/<task_id>', methods=['GET'])
def task_status(task_id):
    job = Job.fetch(task_id, connection=conn)
    if job.is_finished:
        response = {
            'state': 'FINISHED',
            'result': job.result,
        }
    elif job.is_failed:
        response = {
            'state': 'FAILED',
            'status': str(job.exc_info),
        }
    else:
        response = {
            'state': job.get_status(),
            'status': 'Pending...'
        }
    return jsonify(response)

@app.route('/')
def hello():
    return 'Hello, World!'

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', threaded=False)
