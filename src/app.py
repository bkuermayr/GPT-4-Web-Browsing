from flask import Flask, request, jsonify
from celery import group
from tasks import celery
from dotenv import load_dotenv
import os
import ssl
from gevent import monkey
monkey.patch_all()  # Apply gevent monkey patches

from tasks import process_query_task, process_csv_feed, process_category_task
from DatabaseUtil import client, getCategoryStructure
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
    remote_directory = request.args.get('remote_directory')
    task = process_csv_feed.apply_async(args=[url, remote_directory, filename])
    return jsonify({"task_id": task.id})

@app.route('/api/assignCategory', methods=['POST'])
def assignCategory():
    data = request.get_json()
    job_id = data.get('job_id','')
    products_ids = data.get('product_ids', [])
    try:
        automationID = client.table('automation_job').select('automation_id').eq('id',job_id).single().execute().data['automation_id']
        autoData = client.table('automation').select('*').eq("id",automationID).single().execute().data
        inputAttributes = client.table('automation_field_attributes_view').select('special_field, attribute_name').eq("automation_id",automationID).eq('is_input_field',True).execute().data
        products = client.table('products').select('id,title,custom_fields').in_("id",products_ids).filter("parent_id","is","null").execute().data
        categories = client.table('categories').select('id, title, parent_id').eq('org_id',autoData.get('org_id')).execute().data
        cs = getCategoryStructure(categories)
        attributeName = []
        for x in inputAttributes:
            attribute = x.get('special_field')
            if not attribute: 
                attribute = x.get('attribute_name')
            if not attribute: continue
            attributeName.append(attribute)
    except Exception as e:
        print(f'Error: {e}')
        return jsonify({'Error': True})
    subtasks = []
    for x in products:
        subtasks.append(process_category_task.s(cs,attributeName,x,job_id, autoData.get('use_first_product_image', False)))
    job = group(subtasks)
    task = job.apply_async()
    task.save()
    print(f"Task ID: {task.id}")
    return jsonify({"task_id": task.id, "count":len(subtasks)})

@app.route('/api/createDescription',methods=['POST'])
def createDescription():
    data = request.get_json() 
    job_id = data.get('job_id','') 
    products_ids = data.get('product_ids',[])
    try:
        automationID = client.table('automation_job').select('automation_id').eq('id',job_id).single().execute().data['automation_id']
        response = client.table('automation').select('*').eq("id",automationID).single().execute()
        automationFields = client.table('automation_field_attributes_view').select('special_field, is_search_term, attribute_name').eq("automation_id",automationID).execute().data
        autoData = response.data
        searchAttributes = []
        aiAttributes = []
        for x in automationFields:
            attribute = x.get('special_field')
            if not attribute: 
                attribute = x.get('attribute_name')
            if not attribute: continue
            if x['is_search_term'] == False:
                aiAttributes.append(attribute)
            else:
                searchAttributes.append(attribute)
        products = client.table('products').select('id,title,custom_fields').in_("id",products_ids).filter("parent_id","is","null").execute().data
        autoData['searchAttributes'] = searchAttributes
        autoData['aiAttributes'] = aiAttributes 
        autoData['automation_job_id']=job_id
        autoData['automation_id'] = automationID
    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"Error":True})
    subtasks = []
    for x in products:
        subtasks.append(process_query_task.s(x,autoData))
    job = group(subtasks)
    task = job.apply_async()
    task.save()
    print(f"Task ID: {task.id}")
    return jsonify({"task_id": task.id, "count":len(subtasks)})


@app.route('/api/createDescription/<task_id>', methods=['GET'])
def taskGroupStatus(task_id):
    try:
        task = celery.GroupResult.restore(task_id)
    except Exception as e:
        return jsonify({"Error":"Group already deleted, therefore done"}),400
    response = {
            'comp_num':task.completed_count(),
            'done':task.ready()
    }
    return jsonify(response),200


@app.route('/')
def hello():
    return 'Hello, World!'

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', threaded=False)
