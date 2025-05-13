from google.cloud import storage
import pymongo
import json
# from io import BytesIO
from bson import ObjectId
import datetime
from google.cloud import storage, bigquery
import time

bq_client = bigquery.Client()
storage_client = storage.Client()
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client['test']
summary = db['summary']
bucket_name = 'raw_k14_phuc'
destination = 'raw_event/raw_event.json'
log = db['log']

Product_Option = []
Product_Quality = []
Recommend_Option = []
Event = []
Event_Option = []
Event_Cart = []
Event_Filter = []
Cart_Product_Option = []
Filter_Option = []
Event_Recommend = []

def export_to_gcs():
    bucket = storage_client.bucket(bucket_name)

    data_to_export = {
        'Product_Option': Product_Option,
        'Product_Quality': Product_Quality,
        'Recommend_Option': Recommend_Option,
        'Event': Event,
        'Event_Option': Event_Option,
        'Event_Cart': Event_Cart,
        'Event_Filter': Event_Filter,
        'Cart_Product_Option': Cart_Product_Option,
        'Filter_Option': Filter_Option,
        'Event_Recommend': Event_Recommend,
    }

    for name, data in data_to_export.items():
        destination = f"{name}.jsonl"
        blob = bucket.blob(destination)

        jsonl_data = "\n".join(json.dumps(record, ensure_ascii=False) for record in data)

        blob.upload_from_string(
            data=jsonl_data,
            content_type='application/json'
        )
        print(f"Exported {name} to {destination} on GCS")

def get_data_from_mongo():
    last_time_extract = log.find_one()
    list_event = None
    
    if not last_time_extract:
        # last_time_extract = {"LTE": None, "_id": None}
        last_time_extract = summary.find_one({},sort=[("_id", pymongo.ASCENDING)])
        last_time_extract_objectID = last_time_extract["_id"]
        list_event = list(summary.find({"_id": {"$gt": last_time_extract["_id"]}}).limit(10000))
        
        if not list_event:
            print("No new events to export.")
            return
        
        for event in list_event:
            event["_id"] = str(event["_id"])    

        process_data(list_event)
                
        last_time_extract = list_event[-1]["_id"]
        log.update_one({"_id": last_time_extract_objectID}, {"$set": {"LTE": last_time_extract}}, upsert=True)
    
    else:
        list_event = list(summary.find({"_id": {"$gt": ObjectId(last_time_extract["LTE"])}}).limit(10000))
        last_time_extract_objectID = last_time_extract["_id"]
        if not list_event:
            print("No new events to export.")
            return
        
        for event in list_event:
            event["_id"] = str(event["_id"])    

        process_data(list_event)
                
        last_time_extract = list_event[-1]["_id"]
        log.update_one({"_id": last_time_extract_objectID}, {"$set": {"LTE": last_time_extract}}, upsert=True)

def process_data(list_event):
    filter_option = ['listing_page_recommendation_clicked', 'listing_page_recommendation_noticed', 'listing_page_recommendation_visible', 'view_listing_page', 'view_sorting_relevance']
    option_quality = ['select_product_option_quality']
    option_recommend = ['view_all_recommend']
    cart_product = ['checkout_success', 'view_shopping_cart', 'checkout']
    option = ['add_to_cart_action', 'select_product_option', 'view_product_detail']
    exclude_field = ['option', 'cart_products']
    for doc in list_event:
        event = {k: v for k, v in doc.items() if k not in exclude_field}
        event['created_date'] = datetime.datetime.now().strftime("%Y-%m-%d")
        if 'price' in event.keys():
            event['price'] = convert_to_price_float(event.get('price')) if event.get('price') not in (None, '') else None
        if 'order_id' in event.keys():
            if event['order_id'] == '':
                event['order_id'] = None
            else:
                event['order_id'] = int(event['order_id'])
                
        if 'utm_source' in event.keys():
            if isinstance(event.get('utm_source'), bool):
                event['utm_source'] = str(event.get('utm_source')).lower()
        
        if 'utm_medium' in event.keys():
            if isinstance(event.get('utm_medium'), bool):
                event['utm_source'] = str(event.get('utm_medium')).lower()        
        
        try:
            Event.append(event)
        except Exception as e:
            print(f"Error inserting event: {e}")
            
        if doc['collection'] in filter_option:
            try:
                table_name = 'Filter_Option'
                column_dict = {
                    'alloy': doc['option']['alloy'],
                    'diamond': doc['option']['diamond'],
                    'shapediamond': doc['option']['shapediamond'],
                }                                
                is_duplicate, _id = check_duplicate(table_name, column_dict, 'filter')
                
                if not is_duplicate:
                    filter_option_largest_id = get_largest_incremental_id(table_name)
                    _id = filter_option_largest_id + 1
                    column_dict['_id'] = _id
                    column_dict['created_date'] = datetime.datetime.now().strftime("%Y-%m-%d")                    
                    Filter_Option.append(column_dict)
                    
                event_id = doc['_id']
                Event_Filter.append({'event_id': event_id, 'filter_option_id': _id, 'created_date': datetime.datetime.now().strftime("%Y-%m-%d")})
                
            except Exception as e:
                print(f"Error processing filter option: {e}")
            
        elif doc['collection'] in option:
            table_name = 'Product_Option'
            for option_doc in doc['option']:
                column_dict = {
                    'option_label': option_doc.get('option_label'),
                    'option_id': int(option_doc.get('option_id')) if option_doc.get('option_id') not in (None, '') else None,
                    'value_id': int(option_doc.get('value_id')) if option_doc.get('value_id') not in (None, '') else None,
                    'value_label': option_doc.get('value_label'),
                }
                            
                is_duplicate, _id = check_duplicate(table_name, column_dict, 'option')
                if not is_duplicate:
                    option_largest_id = get_largest_incremental_id(table_name)
                    _id = option_largest_id + 1
                    column_dict['_id'] = _id
                    column_dict['created_date'] = datetime.datetime.now().strftime("%Y-%m-%d")
                    Product_Option.append(column_dict)                    
                
                Event_Option.append({'event_id': doc['_id'], 'product_option_id': _id, 'created_date': datetime.datetime.now().strftime("%Y-%m-%d")})
        
        elif doc['collection'] in option_quality:
            option_doc = doc['option'][0]
            
            option_dict = {
                'option_label': option_doc.get('option_label'),
                'option_id': int(option_doc.get('option_id')) if option_doc.get('option_id') not in (None, '') else None,
                'value_id': int(option_doc.get('value_id')) if option_doc.get('value_id') not in (None, '') else None,
                'value_label': option_doc.get('value_label'),
            }
                        
            quality_dict = {
                'quality': option_doc.get('quality'),
                'quality_label': option_doc.get('quality_label', option_doc.get('quality'))
            }
                        
            option_is_duplicate, option_id = check_duplicate('Product_Option', option_dict, 'option')
            if not option_is_duplicate:
                option_largest_id = get_largest_incremental_id('Product_Option')
                option_id = option_largest_id + 1
                option_dict['_id'] = option_id
                option_dict['created_date'] = datetime.datetime.now().strftime("%Y-%m-%d")
                Product_Option.append(option_dict)
            
            quality_is_duplicate, quality_id = check_duplicate('Product_Quality', quality_dict, 'quality')
            if not quality_is_duplicate:
                quality_largest_id = get_largest_incremental_id('Product_Quality')
                quality_id = quality_largest_id + 1
                quality_dict['_id'] = quality_id
                quality_dict['created_date'] = datetime.datetime.now().strftime("%Y-%m-%d")      
                Product_Quality.append(quality_dict)          
            
            Event_Option.append({'event_id': doc['_id'], 'product_option_id': option_id, 'product_quality_id': quality_id , 'created_date': datetime.datetime.now().strftime("%Y-%m-%d")})
            
            
        elif doc['collection'] in option_recommend:
            table_name = 'Recommend_Option'
            recommend_option = doc['option']
            if 'category id' in recommend_option:
                value = recommend_option.pop('category id')
                recommend_option['category_id'] = int(value) if value not in (None, '') else None

            recommend_option['price'] = convert_to_price_float(recommend_option.get('price', 0))
            
            kollektion_value = recommend_option.get('kollektion_id')
            recommend_option['kollektion_id'] = int(kollektion_value) if kollektion_value not in (None, '') else None
            
            is_duplicate, _id = check_duplicate(table_name, recommend_option, 'recommend')
            
            if not is_duplicate:
                recommend_largest_id = get_largest_incremental_id(table_name)
                _id = recommend_largest_id + 1
                recommend_option['_id'] = _id
                recommend_option['created_date'] = datetime.datetime.now().strftime("%Y-%m-%d")
                Recommend_Option.append(recommend_option)                
            
            Event_Recommend.append({'event_id': doc['_id'], 'recommend_option_id': _id, 'created_date': datetime.datetime.now().strftime("%Y-%m-%d")})
            
            
        elif doc['collection'] in cart_product:
            for product in doc['cart_products']:
                event_cart_dict = {
                    'event_id': doc['_id'],
                    'amount': product.get('amount'),
                    'price': convert_to_price_float(product.get('price')) if product.get('price') else None,
                    'product_id': product.get('product_id'),
                    'currency': product.get('currency'),
                    'created_date': datetime.datetime.now().strftime("%Y-%m-%d")
                }
                Event_Cart.append(event_cart_dict)
                if product['option'] == '':
                    cart_product_option_dict = {
                        'event_id': doc['_id'],
                        'product_id': product['product_id'],
                        'product_option_id': None,
                        'created_date': datetime.datetime.now().strftime("%Y-%m-%d")
                    }
                    
                    Cart_Product_Option.append(cart_product_option_dict)     
                                   
                for options in product['option']:
                    column_dict = {
                        'option_label': options.get('option_label'),
                        'option_id': int(options.get('option_id')) if options.get('option_id') not in (None, '') else None,
                        'value_id': int(options.get('value_id')) if options.get('value_id') not in (None, '') else None,
                        'value_label': options.get('value_label'),
                    }

                    
                    is_duplicate, _id = check_duplicate('Product_Option', column_dict, 'option')
                    if not is_duplicate:
                        option_largest_id = get_largest_incremental_id('Product_Option')
                        _id = option_largest_id + 1
                        column_dict['_id'] = option_id
                        column_dict['created_date'] = datetime.datetime.now().strftime("%Y-%m-%d")                        
                        Product_Option.append(column_dict)
                    
                    cart_product_option_dict = {
                        'event_id': doc['_id'],
                        'product_id': product['product_id'],
                        'product_option_id': option_id,
                        'created_date': datetime.datetime.now().strftime("%Y-%m-%d")
                    }
                    
                    Cart_Product_Option.append(cart_product_option_dict)


def get_largest_incremental_id(table_name):
    cache = None

    if table_name == 'Product_Quality':
        cache = Product_Quality
    elif table_name == 'Product_Option':
        cache = Product_Option
    elif table_name == 'Filter_Option':
        cache = Filter_Option
    elif table_name == 'Recommend_Option':
        cache = Recommend_Option
    else:
        cache = None    
    
    if cache and len(cache) > 0:
        return cache[-1]['_id']
    
    
    query = f"""
        SELECT MAX(_id) as max_incremental_id
        FROM `bionic-truck-452607-i7.raw.{table_name}`
    """
    query_job = bq_client.query(query)
    results = query_job.result()
    for row in results:
        return row.max_incremental_id if row.max_incremental_id else 0
    
def check_duplicate(table_name, column_dict, collection):
    cache = None

    if collection == 'quality':
        cache = Product_Quality
    elif collection == 'option':
        cache = Product_Option
    elif collection == 'filter':
        cache = Filter_Option
    elif collection == 'recommend':
        cache = Recommend_Option
    else:
        cache = None

    if cache is not None:
        for item in cache:
            match = True
            for key, value in column_dict.items():
                if key not in item:
                    match = False
                    break
                if item[key] != value:
                    match = False
                    break
            if match:
                return True, item.get("_id")

    int_column = ['option_id', 'value_id', 'kollektion_id', 'category_id', 'price']

    conditions = []
    for key, value in column_dict.items():
        if value is None:
            conditions.append(f"{key} IS NULL")
        elif key in int_column:
            conditions.append(f"{key} = {value}")
        else:
            conditions.append(f"{key} = '{value}'")
    
    condition_str = " AND ".join(conditions)
    
    query = f"""
        SELECT _id
        FROM `bionic-truck-452607-i7.raw.{table_name}`
        WHERE 1 = 1 AND {condition_str}
    """

    query_job = bq_client.query(query)
    results = query_job.result()

    for row in results:
        return True, row._id

    return False, None
    
    
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
    get_data_from_mongo()
    export_to_gcs()
    
if __name__ == "__main__":
    start = time.time()
    main()
    end = time.time()
    print(end - start)