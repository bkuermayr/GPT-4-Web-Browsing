import os
from supabase import create_client

url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")
client = create_client(url,key)
assetUrl = 'https://oarreivvqvbvowbekecs.supabase.co/storage/v1/object/public/assets/'

from collections import defaultdict
import json


def getCategoryStructure(categories):
    tree = {}
    lookup = {}

    # Create a lookup dictionary for quick access
    for item in categories:
        key = f"{item['title']} ({item['id']})"
        lookup[item['id']] = {"key": key, "children": {}}

    # Build the hierarchical structure
    for item in categories:
        key = lookup[item['id']]["key"]
        parent_id = item['parent_id']

        if parent_id is None:
            tree[key] = lookup[item['id']]["children"] 
        else:
            parent_key = lookup.get(parent_id)
            if parent_key:
                parent_key["children"][key] = lookup[item['id']]["children"] 
    return clean_tree(tree)

def clean_tree(node):
    if isinstance(node, dict):
        return {k: clean_tree(v) if v else "" for k, v in node.items()}
    return node

def findValueCustomFields(customFields, name):
    output = ''
    for x in customFields:
        #print(x.__str__())
        if(x.get('name','') == name):
            for y in x.get('values'):
                if(y == None or y == ''): continue
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
        categories = client.table('categories').select('id, title, parent_id').eq('org_id',16).execute().data
        cs = getCategoryStructure(categories)
        print(cs)
    except Exception as e:
        print(e)