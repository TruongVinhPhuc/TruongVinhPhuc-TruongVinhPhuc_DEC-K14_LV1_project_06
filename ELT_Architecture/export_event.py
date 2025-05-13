import pymongo
import json
from io import BytesIO
from bson import ObjectId
import datetime
from google.cloud import storage
import time

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client['test']
summary = db['summary']
bucket_name = 'raw_k14_phuc'
destination = 'raw_event/raw_event.jsonl'
log = db['log_raw']
storage_client = storage.Client()

def export_to_gcs():
    
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination)
    list_event = get_data_from_mongo()
    if list_event:
        buffer = BytesIO()
        for doc in list_event:
            buffer.write((json.dumps(doc, ensure_ascii=False) + '\n').encode("utf-8"))

        buffer.seek(0)  # Đưa con trỏ về đầu để upload

        # Upload lên GCS
        blob.upload_from_file(buffer, content_type="application/json")
        print(f"Exported {len(list_event)} records to gs://{bucket_name}/{destination}")



def get_data_from_mongo():
    last_time_extract = log.find_one()
    list_event = None
    
    if not last_time_extract:
        # last_time_extract = {"LTE": None, "_id": None}
        last_time_extract = summary.find_one({},sort=[("_id", pymongo.ASCENDING)])
        last_time_extract_objectID = last_time_extract["_id"]
        list_event = list(summary.find({"_id": {"$gt": last_time_extract["_id"]}}).sort("_id", 1).limit(1000000))
        
        if not list_event:
            print("No new events to export.")
            return
        
        for event in list_event:
            event["_id"] = str(event["_id"])    

        process_data(list_event)
                
        last_time_extract = list_event[-1]["_id"]
        log.update_one({"_id": last_time_extract_objectID}, {"$set": {"LTE": last_time_extract}}, upsert=True)
    
    else:
        list_event = list(summary.find({"_id": {"$gt": ObjectId(last_time_extract["LTE"])}}).sort("_id", 1).limit(1000000))
        last_time_extract_objectID = last_time_extract["_id"]
        if not list_event:
            print("No new events to export.")
            return
        
        for event in list_event:
            event["_id"] = str(event["_id"])    

        process_data(list_event)
                
        last_time_extract = list_event[-1]["_id"]
        log.update_one({"_id": last_time_extract_objectID}, {"$set": {"LTE": last_time_extract}}, upsert=True)
    
    return list_event
    
        
        
def process_data(list_event):
    option_quality = ['select_product_option_quality']
    option_recommend = ['view_all_recommend']
    cart_product = ['checkout_success', 'view_shopping_cart', 'checkout']
    option = ['add_to_cart_action', 'select_product_option', 'view_product_detail']
    for doc in list_event:
        if 'price' in doc.keys():
            doc['price'] = convert_to_price_float(doc.get('price')) if doc.get('price') not in (None, '') else None
        if 'order_id' in doc.keys():
            if doc['order_id'] == '':
                doc['order_id'] = None        
            else:
                doc['order_id'] = int(doc['order_id'])
                
        if 'utm_source' in doc.keys():
            if isinstance(doc.get('utm_source'), bool):
                doc['utm_source'] = str(doc.get('utm_source')).lower()
        
        if 'utm_medium' in doc.keys():
            if isinstance(doc.get('utm_medium'), bool):
                doc['utm_source'] = str(doc.get('utm_medium')).lower()     
                
        if doc['collection'] in option:
            for i in range(len(doc['option'])):
                try:
                    option_id = doc['option'][i].get('option_id')
                    value_id = doc['option'][i].get('value_id')

                    if isinstance(option_id, str) and option_id.strip().startswith('['):
                        try:
                            option_id = json.loads(option_id)
                        except json.JSONDecodeError:
                            try:
                                option_id = json.loads(option_id.strip() + ']')
                            except Exception:
                                option_id = None
                    if isinstance(option_id, list):
                        option_id = option_id[0] if option_id else None

                    if isinstance(value_id, str) and value_id.strip().startswith('['):
                        try:
                            value_id = json.loads(value_id)
                        except json.JSONDecodeError:
                            try:
                                value_id = json.loads(value_id.strip() + ']')
                            except Exception:
                                value_id = None
                    if isinstance(value_id, list):
                        value_id = value_id[0] if value_id else None
                    doc['option'][i]['option_id'] = int(option_id) if option_id not in (None, '', '""', '"') else None
                    doc['option'][i]['value_id'] = int(value_id) if value_id not in (None, '', '""', '"') else None

                except Exception as e:
                    print("There's an error during transformation:", e)

        elif doc['collection'] in option_quality:
            doc['option'][0]['option_id'] = int(doc['option'][0].get('option_id')) if doc['option'][0].get('option_id') not in (None, '') else None
            doc['option'][0]['value_id'] = int(doc['option'][0].get('value_id')) if doc['option'][0].get('value_id') not in (None, '') else None
                                  
            
        elif doc['collection'] in option_recommend:
            recommend_option = doc['option']
            if 'category id' in recommend_option:
                value = doc['option'].pop('category id')
                doc['option']['category_id'] = int(value) if value not in (None, '') else None

            doc['option']['price'] = convert_to_price_float(recommend_option.get('price', 0))
            
            kollektion_value = doc['option'].get('kollektion_id')
            doc['option']['kollektion_id'] = int(kollektion_value) if kollektion_value not in (None, '') else None
            
        elif doc['collection'] in cart_product:
            for i in range(len(doc['cart_products'])):
                doc['cart_products'][i]['price'] = convert_to_price_float(doc['cart_products'][i].get('price')) if doc['cart_products'][i].get('price') else None
                if isinstance(doc['cart_products'][i]['option'], dict):
                    doc['cart_products'][i]['option'] = [doc['cart_products'][i]['option']]
                
                if doc['cart_products'][i].get('option') in (None, "", ''): # Vì sẽ có một số option dạng '' sẽ bị lỗi không phải REPEATED nên cần đưa về []
                    doc['cart_products'][i]['option'] = []
                    continue
                                    
                for j in range(len(doc['cart_products'][i]['option'])):
                    doc['cart_products'][i]['option'][j]['value_id'] = int(doc['cart_products'][i]['option'][j].get('value_id')) if doc['cart_products'][i]['option'][j].get('value_id') not in (None, '', ) else None
                    doc['cart_products'][i]['option'][j]['option_id'] = int(doc['cart_products'][i]['option'][j].get('option_id')) if doc['cart_products'][i]['option'][j].get('option_id') not in (None, '') else None
                
def convert_to_price_float(str_price):
    if ',' in str_price and '.' in str_price:
        if str_price.find('.') < str_price.find(','):
            str_price = str_price.replace('.', '').replace(',', '.')
        else:
            str_price = str_price.replace(',', '')
    elif ',' in str_price:
        str_price = str_price.replace('.', '').replace(',', '.')
    else:
        str_price = str_price.replace(',', '')

    try:
        return float(str_price)
    except ValueError:
        return None

def main():
    export_to_gcs()
    
if __name__ == "__main__":
    start = time.time()
    main()
    end = time.time()
    print(end - start)