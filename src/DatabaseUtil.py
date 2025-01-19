import os
from supabase import create_client

url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")
client = create_client(url,key)
assetUrl = 'https://oarreivvqvbvowbekecs.supabase.co/storage/v1/object/public/assets/'


def findValueCustomFields(customFields, name):
    output = ''
    for x in customFields:
        #print(x.__str__())
        if(x.get('name','') == name):
            for y in x.get('values'):
                output = f'{output} {y},'
    return output

def getURLLink(product_id):
    assets = client.table('product_assets_with_assets').select('*').eq('product_id',product_id).order('order',desc=False).execute().data
    for x in assets:
        if 'image' in x['type']:
            temp = f'{assetUrl}{x["path"]}'
            if temp[-1] == '.': 
                temp = temp[:-1]
            return temp
    return None

if __name__ == '__main__':
    try:
        products_ids = ['4248240']
        response = client.table('automation').select('*').eq("id",6).single().execute()
        automationFields = client.table('automation_field').select('special_field, is_search_term').eq("automation_id",6).execute().data
        autoData = response.data
        products = client.table('products').select('id,title,custom_fields').in_("id",products_ids).filter("parent_id","is","null").execute().data
            #searchVal = f'{searchVal} {findValueCustomFields(products.get("custom_fields"),i)}'
    except Exception as e:
        print(e)