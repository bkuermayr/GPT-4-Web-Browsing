import os
from supabase import create_client

url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")
client = create_client(url,key)
assetUrl = 'https://oarreivvqvbvowbekecs.supabase.co/storage/v1/object/public/assets/'

def getAttributesWithCategoriesAndValues(attribute_ids):
    attributes = client.table('attributes_categories_view_automation').select('*').in_("attributeid",attribute_ids).execute().data
    return attributes

def seperateInputField(combinedData, criteria):
    criteria_map = {row["attribute_id"]: row["is_input_field"] for row in criteria}
    
    # Initialize input and output lists
    input_data = []
    output_data = []
    
    # Iterate through combinedData and split based on criteria
    for row in combinedData:
        attribute_id = row["attributeid"]
        
        if criteria_map.get(attribute_id, False):  # Default to False if not found
            input_data.append(row.get("attributename", ""))
        else:
            output_data.append(row)
    
    return input_data, output_data

def convertFromFieldKeyToArray(key, map):
    results = []
    
    if map == None:
        return results
    for x in map:
        value = x.get(key,None)
        if(value == None): continue
        results.append(value)
    return results

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
    if customFields == None: return output
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
    getAttributesWithCategoriesAndValues([1241,83])