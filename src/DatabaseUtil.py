import os
from supabase import create_client

url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")
client = create_client(url,key)

def findValueCustomFields(customFields, name):
    output = ''
    for x in customFields:
        if(x['name'] == name):
            for y in x['values']:
                output = f'{output} {y}'
    return output

if __name__ == '__main__':
    try:
        products_ids = ['4248240']
        response = client.table('automation').select('*').eq("id",6).single().execute()
        automationFields = client.table('automation_field').select('special_field, is_search_term').eq("automation_id",6).execute().data
        products = client.table('products').select('id,title').in_("id",products_ids).execute().data
        for x in automationFields:
            print(x['special_field'])
        autoData = response.data
        searchAttributes = []
        aiAttributes = []
        for x in automationFields:
            if 'category' in x['special_field'].lower() or 'categories' in x['special_field'].lower():
                continue
            if x['is_search_term']:
                searchAttributes.append(x['special_field'])
            else:
                aiAttributes.append(x['special_field'])
        autoData['searchAttributes'] = searchAttributes
        autoData['aiAttributes'] = aiAttributes 
        products = client.table('products').select('id,title,custom_fields').in_("id",products_ids).filter("parent_id","is","null").execute().data
        searchAttributes.append('Website Größe')
        searchAttributes.append('Farbe DE')
        searchVal = ''
        #for i in searchAttributes:
            #searchVal = f'{searchVal} {findValueCustomFields(products.get("custom_fields"),i)}'
        print(products)
    except Exception as e:
        print(e)