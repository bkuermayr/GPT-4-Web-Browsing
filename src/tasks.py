import re
import logging
from celery import Celery, group
from dotenv import load_dotenv
import os
import time
import ssl
from DatabaseUtil import findValueCustomFields, client, getURLLink
from celery.signals import task_postrun

import json
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
        redis_backend_use_ssl=ssl_options,
        broker_connection_retry_on_startup=True
    )


@celery.task
def process_query_task(productData,automationData):
    query = productData.get('title',"")
    prompt = automationData.get('prompt', '')
    profile = automationData.get('profile', "")
    search_location = automationData.get('search_location', "")
    search_language = automationData.get('search_language', "")
    output_language = automationData.get('output_language', "")
    job_id = automationData.get('automation_job_id', "")
    product_id = productData.get('id',"")
    use_web_search = automationData.get('use_web_search', True)
    aiAttr = automationData.get('aiAttributes',[])
    customFields = productData.get('custom_fields',[])
    useFirstImage = automationData.get('use_first_product_image',False)

    aiVal = ''
    for j in aiAttr:
        temp = findValueCustomFields(customFields,j)
        if j == 'title' or j == 'Title': continue
        if(temp == ''): continue
        aiVal =f'{aiVal} {j}: {temp} \n'
    urls = automationData.get('search_domains','').split(",")
    serperQuery = ""
    for i in urls:
        if i.strip() == '': continue
        if serperQuery == '':
            serperQuery = f'site:{i.strip()}'
        else:
            serperQuery = f"{serperQuery} OR site:{i.strip()}"
    serperQuery = f'{serperQuery} {query}'
    logging.info(f'Received searchQuery: {serperQuery}')
    # Query für Serper: site:https://www.nike.com OR site:adidas.com Schuhe

    logging.info(f'Received query: {query}, search_location: {search_location}, search_language: {search_language}, output_language: {output_language}')
    logging.info(f'Received prompt: {prompt}')
    logging.info(f'Received following attributes: {aiVal}')
    content_processor = GPTAnswer()

    if use_web_search:
        web_contents_fetcher = WebContentFetcher(query=serperQuery, search_location=search_location, search_language=search_language, output_language=output_language)
        web_contents, serper_response = web_contents_fetcher.fetch()
        retriever = EmbeddingRetriever()
        if serper_response is None:
            response = {
                'query': query,
                'job_id': job_id,
                'product_id': product_id,
                'answer': {},
                'gpt_answer_time': 0,
                'output_language': output_language,
                'reference_cards': [],
                'failure' : True,
                'reason': 'Not enough quality sources'
            }
            return response
        relevant_docs_list = retriever.retrieve_embeddings(web_contents, serper_response['links'], query, product_id, job_id)
        '''f = open(f"demofile{product_id}.txt", "w")
        f.write(f'{relevant_docs_list.__str__()}')
        f.close()'''
        
        formatted_relevant_docs = content_processor._format_reference(relevant_docs_list, serper_response['links'])
        if not formatted_relevant_docs:
            response = {
                'query': query,
                'job_id': job_id,   
                'product_id': product_id,
                'answer': {},
                'gpt_answer_time': 0,
                'output_language': output_language,
                'reference_cards': [],
                'failure' : True,
                'reason': 'Not enough quality sources'
            }
            return response
    else:
        formatted_relevant_docs = None
        serper_response = None
    start = time.time()
    assetUrl = None
    if useFirstImage == True:
        assetUrl = getURLLink(product_id)
    try:
        ai_message_obj = content_processor.get_answer(prompt, formatted_relevant_docs, output_language, profile,assetUrl,aiVal, query)
        answer = ai_message_obj.content
        answer = clean_json_string(answer.strip())
        end = time.time()
        logging.info(f'Generated answer in {end - start} seconds')
        response = {
        'query': query,
        'job_id': job_id,
        'product_id': product_id,
        'answer': json.loads(answer),
        'gpt_answer_time': end - start,
        'output_language': output_language,
        'failure' : False
        }
        return response
    except Exception as e:
         response = {
                'query': query,
                'job_id': job_id,   
                'product_id': product_id,
                'answer': {},
                'gpt_answer_time': 0,
                'output_language': output_language,
                'reference_cards': [],
                'failure' : True,
                'reason': f'OpenAI did not provide Answer/parsable Answer because: {e}'
            }
         return response


@task_postrun.connect(sender=process_query_task)
def task_postrun_notifier(state=None, retval=None, task_id=None, args=None,**kwargs):
    aID = args[1].get('automation_job_id')
    product_id = args[0].get('id',"")
    print(f'Postrun reached by {product_id}')
    if state=='SUCCESS':
        success = not retval.get('failure',True)
        data = {
            'answer':"",
            'references':"",
            'failureReason': ""
            }
        if success == False:
            data['failureReason'] = retval['reason']
        else:
            flags = retval['answer'].get("flags",{})
            data['answer'] = retval['answer']
            data['emptyWebResults'] = flags.get('emptyWebResults',True)
            data['references'] = retval['answer'].get('references',[])
            data['answer'].pop('references', None)
            data['answer'].pop('flags', None)
            realAnswer = data['answer'].get('answer',"")
            data['answer'] = realAnswer
            data.pop('failureReason', None)
            if success:
                success = not data.get('emptyWebResults', True)
                success = success and (not flags.get('different', False))
            data['answer']=flatten_answer(data)
        client.table('automation_job_data').insert({'product_id':product_id,'automation_job_id':aID,'success':success,'data':data}).execute()
    else:
        client.table('automation_job_data').insert({'product_id':product_id,'automation_job_id':aID,'success':False,'data':{'error':retval.__str__()},'error':retval.__str__()}).execute()
        #print('test')
    client.rpc("increment_processed_products", {'job_id': aID}).execute()



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
    cleaned_json = re.sub(r',\s*([\]}])', r'\1', cleaned_string)
    return cleaned_json.strip()

def flatten_nested(data, parent_key=''):
    """ Recursively flattens the dictionary while keeping arrays intact. """
    items = []
    
    for key, value in data.items():
        # Create the new key by combining the parent_key and the current key
        new_key = f"{parent_key}.{key}" if parent_key else key
        
        if isinstance(value, dict):
            # Recursively flatten the dictionary
            items.extend(flatten_nested(value, new_key).items())
        else:
            # Add the key-value pair to the list of items
            items.append((new_key, value))
    
    return dict(items)


def flatten_answer(data):
    if "answer" in data and isinstance(data["answer"], dict):
        return flatten_nested(data["answer"])
    return {}

if __name__ == "__main__":
    test = '''{
  "answer": {
    "Attribute": "",
    "Merkmale": [
      "Ultraleichtes Design für müheloses Spiel",
      "Optimierte Schlagfläche für verbesserte Kontrolle",
      "Ergonomischer Griff für maximalen Komfort",
      "Hochwertige Materialien für Langlebigkeit",
      "Vielseitige Anwendung auf verschiedenen Platzbedingungen"
    ],
    "Test": {
        "Test1": "Test2",
        "Test2": "Test3"
    },
    "Fliesstext": "Der #48 RH Ultralight Sandwedge ist der perfekte Begleiter für Golfer, die Wert auf Leichtigkeit und Präzision legen. Mit seinem ultraleichten Design ermöglicht dieser Schläger müheloses Spiel, selbst bei langen Runden. Die optimierte Schlagfläche sorgt für verbesserte Kontrolle und Genauigkeit, während der ergonomische Griff maximalen Komfort bietet. Hergestellt aus hochwertigen Materialien, ist dieser Sandwedge nicht nur langlebig, sondern auch vielseitig einsetzbar auf verschiedenen Platzbedingungen. Egal, ob Sie ein Anfänger oder ein erfahrener Spieler sind, dieser Schläger wird Ihre Leistung auf dem Golfplatz erheblich steigern. Entdecken Sie jetzt die Vorteile des #48 RH Ultralight Sandwedge und verbessern Sie Ihr Spiel!",
  },
  "flags": {
    "different": false,
    "usedImage": false,
    "emptyWebResults": false
  },
  "references": []
}
'''
    test2 = json.loads(clean_json_string(test.strip()))
    test2['answer']=flatten_answer(test2)
    print(test2)
    '''
    subtasks = []
    products = ['1/4 Zip Fleece Pulover','505U Premium HE RH #3 S (GDI IZ 95)','2-Ball Ten Triple-Track Putter','Adicross Beyond 18 Slim 5-Pocket Pant Carbon','2024 Adidas Season Opener Kappe','2021 ANSER 4 Putter','1/2-Sleeve Mesh Blocked Polo']
    x = """{
    "Name": "Jennifer Smith",
    "Contact Number": 7867567898,
    "Email": "jen123@gmail.com",
    "Hobbies":["Reading", "Sketching", "Horse Riding"]
    }"""
    #print(json.loads(x))
    test = json.loads(x)
    test.pop('Name', None)
    #print(test)'''