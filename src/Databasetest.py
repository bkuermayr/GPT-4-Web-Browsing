import os
from supabase import create_client

url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")
client = create_client(url,key)

if __name__ == '__main__':
    try:
        products_ids = ['3044187','2784511']
        response = client.table('automation').select('*').eq("id",6).single().execute()
        products = client.table('products').select('id,title').in_("id",products_ids).execute().data
        automationFields = client.table('automation_field').select('special_field, is_search_term').eq("automation_id",6).execute().data
        autoData = response.data
    except Exception as e:
        print(e)

    print(products)
    print(autoData)
    print(automationFields)