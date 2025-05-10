import re
import logging
from anyio import sleep
from celery import Celery, group
from dotenv import load_dotenv
import os
import time
import ssl
from DatabaseUtil import doesDocumentExist, findValueCustomFields, client, getURLLink
from celery.signals import task_postrun

import json
from fetch_web_content import WebContentFetcher
from llm_answer import GPTAnswer, clean_data
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
def process_translation_task(inputFields, parent_data, children_data, automation, job_id):
    output_lang = automation.get('output_language')
    context = automation.get('prompt',"")
    if not output_lang:
        return {
            'job_id': job_id,
            'answer': {},
            'failure' : True,
            'reason' : "No Output Language provided"
        }
    parentContext = getInputContext(inputFields, parent_data.get('custom_fields'))
    inputContext = {f"{parent_data['id']}": parentContext}
    for x in children_data:
        customFields = x.get('custom_fields',[])
        product_id = x.get('id', "")
        contextInput = getInputContext(inputFields, customFields)
        inputContext[f'{product_id}'] = contextInput
    content_processor = GPTAnswer()
    template, scheme = content_processor.get_template_translation(inputContext,context, output_lang)
    try:
        ai_message_obj = content_processor.get_answer(template, scheme, None, False)
        answer = ai_message_obj
        logging.info(answer)
        response = {
            'job_id': job_id,
            'answer': answer,
            'failure' : False
        }
        return response
    except Exception as e:
         logging.error(e)
         response = {
                'job_id': job_id,   
                'answer': {},
                'failure' : True,
                'reason': f'OpenAI did not provide Answer/parsable Answer because: {e}'
            }
         return response


@task_postrun.connect(sender=process_translation_task)
def task_postrun_notifier_translator(state=None, retval=None, task_id=None, args=None,**kwargs):
    aID = args[4]
    inserts = []
    parent = args[1]
    children = args[2]
    ids = []
    ids.append(parent['id'])
    for x in children:
        ids.append(x['id'])
    print(f'Postrun reached')
    if state=='SUCCESS':
        success = not retval.get('failure',True)
        if success == False:
            data = {
                'failureReason': retval['reason']
            }
            for i in ids:
                inserts.append({'product_id':i,'automation_job_id':aID,'success':False,'data':data, 'error': data})
            client.table('automation_job_data').insert(inserts).execute()
        else:
            data = retval['answer']
            for key in data:
                help = {
                    'answer': {}
                }
                help['answer'] = data[f'{key}']
                inserts.append({'product_id':key,'automation_job_id':aID,'success':success,'data':help})
            client.table('automation_job_data').insert(inserts).execute()
    else:
        for i in ids:
                inserts.append({'product_id':i,'automation_job_id':aID,'success':False,'data':{'error':retval.__str__()},'error':retval.__str__()})
        client.table('automation_job_data').insert(inserts).execute()
    client.rpc("increment_processed_products", {'job_id': aID}).execute()   

@celery.task
def process_extraction_variants_task(inputFields, outputFields, parent_id, variant_ids, useImage, automation_job_id, use_filled_output_attributes):
    categoryId = client.table('products_with_categories').select('category_ids').eq("id", parent_id).single().execute().data['category_ids']
    products = client.table('products').select('id,title,custom_fields').in_("id",variant_ids).execute().data
    assetUrl = None
    categoryOutputFields = removeNonCategoryFields(outputFields,categoryId)
    output_attributes = createOutputStructure(categoryOutputFields)
    input_context = {}
    if use_filled_output_attributes == True:
        inputFields = inputFields
        for x in categoryOutputFields:
            inputFields.append(x.get('attributename', ''))
    if useImage == True:
        assetUrl = getURLLink(parent_id)
    for x in products:
        customFields = x.get('custom_fields',[])
        productName = x.get('title',"")
        product_id = x.get('id', "")
        contextInput = getInputAttributesContext(inputFields, customFields)
        input_context[f'{productName} ({product_id})'] = contextInput
    content_processor = GPTAnswer()
    template, scheme = content_processor.get_template_attribute_variants(input_context,output_attributes)
    try:
        ai_message_obj = content_processor.get_answer(template, scheme, assetUrl, False)
        answer = ai_message_obj
        logging.info(answer)
        response = {
            'job_id': automation_job_id,
            'answer': answer,
            'failure' : False
        }
        return response
    except Exception as e:
         logging.error(e)
         response = {
                'job_id': automation_job_id,   
                'answer': {},
                'failure' : True,
                'reason': f'OpenAI did not provide Answer/parsable Answer because: {e}'
            }
         return response
    
@task_postrun.connect(sender=process_extraction_variants_task)
def task_postrun_notifier_extraction_variant(state=None, retval=None, task_id=None, args=None,**kwargs):
    aID = args[5] 
    product_id = args[2]
    variants_ids = args[3]  
    inserts = []
    print(f'Postrun reached by variations of {product_id}')
    if state=='SUCCESS':
        success = not retval.get('failure',True)
        if success == False:
            data = {
                'failureReason': retval['reason']
            }
            for i in variants_ids:
                inserts.append({'product_id':i,'automation_job_id':aID,'success':False,'data':data, 'error': data})
            client.table('automation_job_data').insert(inserts).execute()
        else:
            data = retval['answer']
            print(data)
            for i in variants_ids:
                help = {
                    'answer': {}
                }
                help['answer'] = data[f'{i}']
                inserts.append({'product_id':i,'automation_job_id':aID,'success':success,'data':help})
            client.table('automation_job_data').insert(inserts).execute()
    else:
        for i in variants_ids:
                inserts.append({'product_id':i,'automation_job_id':aID,'success':False,'data':{'error':retval.__str__()},'error':retval.__str__()})
        client.table('automation_job_data').insert(inserts).execute()
    client.rpc("increment_processed_products", {'job_id': aID}).execute()


@celery.task
def process_extraction_parent_task(inputFields, outputFields, productData, automationJobId, useImage):
    customFields = productData.get('custom_fields',[])
    productName = productData.get('title',"")
    product_id = productData.get('id', "")
    product_categories = productData.get('category_ids', [])
    contextInput = getInputAttributesContext(inputFields, customFields)
    categoryOutputFields = removeNonCategoryFields(outputFields,product_categories)
    output_attributes = createOutputStructure(categoryOutputFields)
    content_processor = GPTAnswer()
    template, scheme = content_processor.get_template_attribute_parent(productName,contextInput,output_attributes)
    assetUrl = None
    if useImage == True:
        assetUrl = getURLLink(product_id)
    try:
        ai_message_obj = content_processor.get_answer(template, scheme, assetUrl, False)
        answer = ai_message_obj
        logging.info(answer)
        response = {
            'job_id': automationJobId,
            'product_id': product_id,
            'answer': answer,
            'failure' : False
        }
        return response
    except Exception as e:
         response = {
                'job_id': automationJobId,   
                'product_id': product_id,
                'answer': {},
                'failure' : True,
                'reason': f'OpenAI did not provide Answer/parsable Answer because: {e}'
            }
         return response


@task_postrun.connect(sender=process_extraction_parent_task)
def task_postrun_notifier_extraction_parent(state=None, retval=None, task_id=None, args=None,**kwargs):
    aID = args[3] 
    product_id = args[2].get('id',"")
    print(f'Postrun reached by {product_id}')
    if state=='SUCCESS':
        success = not retval.get('failure',True)
        if success == False:
            data = {
                'failureReason': retval['reason']
            }
            client.table('automation_job_data').insert({'product_id':product_id,'automation_job_id':aID,'success':False,'data':data, 'error': data}).execute()
        else:
            data = retval['answer']
            client.table('automation_job_data').insert({'product_id':product_id,'automation_job_id':aID,'success':success,'data':data}).execute()
    else:
        client.table('automation_job_data').insert({'product_id':product_id,'automation_job_id':aID,'success':False,'data':{'error':retval.__str__()},'error':retval.__str__()}).execute()
    client.rpc("increment_processed_products", {'job_id': aID}).execute()


@celery.task
def process_category_task(categories, attributes, productData, automationJobId, useImage):
    customFields = productData.get('custom_fields',[])
    productName = productData.get('title',"")
    product_id = productData.get('id', "")
    attributeList = ''
    for j in attributes:
        if j == 'title' or j == 'Title': continue
        temp = findValueCustomFields(customFields,j)
        if(temp == ''): continue
        attributeList =f'{attributeList} {j}: {temp} \n'
    content_processor = GPTAnswer()
    template, scheme = content_processor.get_template_category(productName,categories,attributeList)
    assetUrl = None
    if useImage == True:
        assetUrl = getURLLink(product_id)
    try:
        ai_message_obj = content_processor.get_answer(template, scheme, assetUrl, False)
        answer = ai_message_obj
        logging.info(answer)
        response = {
            'job_id': automationJobId,
            'product_id': product_id,
            'answer': answer,
            'failure' : False
        }
        return response
    except Exception as e:
         logging.error(e)
         response = {
                'job_id': automationJobId,   
                'product_id': product_id,
                'answer': {},
                'failure' : True,
                'reason': f'OpenAI did not provide Answer/parsable Answer because: {e}'
            }
         return response

@task_postrun.connect(sender=process_category_task)
def task_postrun_notifier_category(state=None, retval=None, task_id=None, args=None,**kwargs):
    aID = args[3] 
    product_id = args[2].get('id',"")
    print(f'Postrun reached by {product_id}')
    if state=='SUCCESS':
        success = not retval.get('failure',True)
        if success == False:
            data = {
                'failureReason': retval['reason']
            }
            client.table('automation_job_data').insert({'product_id':product_id,'is_prepared_step':True,'automation_job_id':aID,'success':False,'data':data, 'error': data}).execute()
        else:
            data = {
                'answer':{'category':""},
                'debugInfo': ""
            }
            data['answer']['category'] = retval['answer']['name']
            data['debugInfo'] = retval['answer']
            
            if data['debugInfo'].get('answer_id',-1) == -1:
                success = False
            client.table('automation_job_data').insert({'product_id':product_id,'is_prepared_step':True,'automation_job_id':aID,'success':success,'data':data}).execute()
    else:
        client.table('automation_job_data').insert({'product_id':product_id,'is_prepared_step':True,'automation_job_id':aID,'success':False,'data':{'error':retval.__str__()},'error':retval.__str__()}).execute()
    client.rpc("increment_processed_products", {'job_id': aID, "updatedstatus": "completed categorization"}).execute()

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
    searchRule = 0
    for i in urls:
        if i.strip() == '': continue
        if serperQuery == '':
            serperQuery = f'site:{i.strip()}'
        else:
            serperQuery = f"{serperQuery} OR site:{i.strip()}"
        searchRule = 1
    serperQuery = f'{serperQuery} {query}'
    logging.info(f'Received searchQuery: {serperQuery}')
    # Query für Serper: site:https://www.nike.com OR site:adidas.com Schuhe

    logging.info(f'Received query: {query}, search_location: {search_location}, search_language: {search_language}, output_language: {output_language}')
    logging.info(f'Received prompt: {prompt}')
    logging.info(f'Received following attributes: {aiVal}')
    content_processor = GPTAnswer()

    if use_web_search:
        web_contents_fetcher = WebContentFetcher(query=serperQuery, search_location=search_location, search_language=search_language, output_language=output_language)
        web_contents, serper_response = web_contents_fetcher.fetch(searchRule)
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
        relevant_docs_list = retriever.retrieve_embeddings(web_contents, serper_response['links'], query, product_id, job_id, rule=searchRule)
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
        summary_template, scheme = content_processor.get_template_generativeText(prompt, formatted_relevant_docs, output_language, profile, aiVal, query)
        ai_message_obj = content_processor.get_answer(summary_template, scheme, assetUrl, use_web_search)
        answer = ai_message_obj
        end = time.time()
        logging.info(f'Generated answer in {end - start} seconds')
        response = {
        'query': query,
        'job_id': job_id,
        'product_id': product_id,
        'answer': answer,
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
        if success == False:
            data = {
                'failureReason': retval['reason']
            }
            client.table('automation_job_data').insert({'product_id':product_id,'automation_job_id':aID,'success':False,'data':data, 'error': data}).execute()
        else:
            data = {
                'answer':"",
                'references':"",
            }
            flags = retval['answer'].get("flags",{})
            data['answer'] = retval['answer']
            data['emptyWebResults'] = flags.get('emptyWebResults',True)
            data['references'] = retval['answer'].get('references',[])
            data['answer'].pop('references', None)
            data['answer'].pop('flags', None)
            realAnswer = data['answer'].get('answer',"")
            data['answer'] = realAnswer
            if success:
                success = not data.get('emptyWebResults', True)
                success = success and (not flags.get('different', False))
            data['answer']=flatten_answer(data)
            client.table('automation_job_data').insert({'product_id':product_id,'automation_job_id':aID,'success':success,'data':data}).execute()
    else:
        client.table('automation_job_data').insert({'product_id':product_id,'automation_job_id':aID,'success':False,'data':{'error':retval.__str__()},'error':retval.__str__()}).execute()
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

def getInputAttributesContext(inputFields, customFields):
    contextInput = ''
    for j in inputFields:
        if j == 'title' or j == 'Title': continue
        temp = findValueCustomFields(customFields,j)
        if(temp == ''): continue
        contextInput =f'{contextInput} {j}: {temp} \n'
    return contextInput

def getInputContext(inputFields, customFields):
    contextInput = {}
    for j in inputFields:
        if j.lower() == 'title':
            continue
        temp = findValueCustomFields(customFields, j)
        if temp == '':
            continue
        contextInput[j] = temp
    return contextInput

def getOutputReq(outputFields):
    return outputFields

def removeNonCategoryFields(outputFields, categoriesIds):
    if(not categoriesIds): return outputFields
    categoriesIds = set(map(int, categoriesIds))
    
    # Filter out fields that don't match category IDs
    return [
        field for field in outputFields 
        if not field["categories"] or any(int(cat) in categoriesIds for cat in field["categories"])
    ]


def createOutputStructure(outputFields):
    output_structure = []
    
    for field in outputFields:
        entry = {"name": field["attributename"], "type": field["attributetype"]}
        
        if field["allowedvalues"]:
            entry["domain"] = field["allowedvalues"]
        
        output_structure.append(entry)
    
    return output_structure

def flatten_answer(data):
    if "answer" in data:
        if isinstance(data["answer"], dict):
            return flatten_nested(data["answer"])
        else:
            return {"answer": data["answer"]}
    return {}


@celery.task
def process_single_task(productData,automationData):
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
    searchRule = 0
    for i in urls:
        if i.strip() == '': continue
        if serperQuery == '':
            serperQuery = f'site:{i.strip()}'
        else:
            serperQuery = f"{serperQuery} OR site:{i.strip()}"
        searchRule = 1
    serperQuery = f'{serperQuery} {query}'
    logging.info(f'Received searchQuery: {serperQuery}')
    # Query für Serper: site:https://www.nike.com OR site:adidas.com Schuhe

    logging.info(f'Received query: {query}, search_location: {search_location}, search_language: {search_language}, output_language: {output_language}')
    logging.info(f'Received prompt: {prompt}')
    logging.info(f'Received following attributes: {aiVal}')
    content_processor = GPTAnswer()

    if use_web_search and not doesDocumentExist(product_id):
        web_contents_fetcher = WebContentFetcher(query=serperQuery, search_location=search_location, search_language=search_language, output_language=output_language, setTimeout=True)
        web_contents, serper_response = web_contents_fetcher.fetch(searchRule)
        retriever = EmbeddingRetriever()
        if serper_response is None:
            response = {
                'query': query,
                'product_id': product_id,
                'answer': {},
                'output_language': output_language,
                'failure' : True,
                'reason': 'Not enough quality sources'
            }
            return response
        relevant_docs_list = retriever.retrieve_embeddings(web_contents, serper_response['links'], query, product_id, job_id, rule=searchRule)
        
        formatted_relevant_docs = content_processor._format_reference(relevant_docs_list, serper_response['links'])
        if not formatted_relevant_docs:
            response = {
                'query': query,
                'product_id': product_id,
                'answer': {},
                'output_language': output_language,
                'failure' : True,
                'reason': 'Not enough quality sources'
            }
            return response
    elif not use_web_search:
        formatted_relevant_docs = None
        serper_response = None
    else:
        retriever = EmbeddingRetriever()
        relevant_docs_list = retriever.retrieveExisting(product_id)
        formatted_relevant_docs = content_processor._format_reference(relevant_docs_list, [])
    assetUrl = None
    if useFirstImage == True:
        assetUrl = getURLLink(product_id)
    try:
        summary_template, scheme = content_processor.get_template_generativeText_single(prompt, formatted_relevant_docs, output_language, profile, aiVal, query)
        ai_message_obj = content_processor.get_answer(summary_template, scheme, assetUrl, use_web_search)
        answer = ai_message_obj
        response = {
            'query': query,
            'product_id': product_id,
            'answer': answer,
            'output_language': output_language,
            'failure' : False
        }
        return response
    except Exception as e:
         response = {
                'query': query,
                'product_id': product_id,
                'answer': {},
                'output_language': output_language,
                'failure' : True,
                'reason': f'OpenAI did not provide Answer/parsable Answer because: {e}'
            }
         return response

@task_postrun.connect(sender=process_single_task)
def task_postrun_notifier_single(state=None, retval=None, task_id=None, args=None,**kwargs):
    aID = -1
    product_id = args[0].get('id',"")
    print(f'Postrun reached by {product_id}')

    if state=='SUCCESS':
        success = not retval.get('failure',True)
        if success == False:
            data = {
                'failureReason': retval['reason']
            }
            client.table('automation_job_data').insert({'product_id':product_id,'success':False,'data':data, 'error': data}).execute()
        else:
            data = {
                'answer':"",
                'references':"",
            }
            flags = retval['answer'].get("flags",{})
            data['answer'] = retval['answer']
            data['emptyWebResults'] = flags.get('emptyWebResults',True)
            data['references'] = retval['answer'].get('references',[])
            data['answer'].pop('references', None)
            data['answer'].pop('flags', None)
            realAnswer = data['answer'].get('answer',"")
            data['answer'] = realAnswer
            if success:
                success = not data.get('emptyWebResults', True)
                success = success and (not flags.get('different', False))
            data['answer']=flatten_answer(data)
            client.table('automation_job_data').insert({'product_id':product_id,'success':success,'data':data}).execute()
    else:
        client.table('automation_job_data').insert({'product_id':product_id,'success':False,'data':{'error':retval.__str__()},'error':retval.__str__()}).execute()



if __name__ == "__main__":
    output = [{'attributename': 'Schaft Material ()', 'attributeid': 2482, 'attributetype': 'text', 'allowedvalues': [], 'categories': [1]}]
    ids = ['28979']
    answer = removeNonCategoryFields(output,ids)
    print(answer)