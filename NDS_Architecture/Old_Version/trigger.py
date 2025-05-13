import functions_framework
import json
from google.cloud import storage, bigquery
import datetime

bq_client = bigquery.Client()
storage_client = storage.Client()

product_option_data = []
product_quality_data = []
recommend_option_data = []
filter_option_data = []

# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def hello_gcs(cloud_event):
    data = cloud_event.data

    name = data["name"]
    bucket = data["bucket"]
    print(f'Processing file: {name} in bucket: {bucket}')
    filter_option = ['listing_page_recommendation_clicked', 'listing_page_recommendation_noticed', 'listing_page_recommendation_visible', 'view_listing_page', 'view_sorting_relevance']
    option_quality = ['select_product_option_quality']
    option_recommend = ['view_all_recommend']
    cart_product = ['checkout_success', ' view_shopping_cart', 'checkout']
    option = ['add_to_cart_action', 'select_product_option', 'view_product_detail']
    exclude_field = ['option', 'cart_products']
    
    bucket = storage_client.bucket(bucket)
    blob = bucket.blob(name)
    content = blob.download_as_text()
    json_data = json.loads(content)
    for doc in json_data:
        event = {k: v for k, v in doc.items() if k not in exclude_field}
        if 'price' in event.keys():
            event['price'] = convert_to_price_float(event.get('price')) if event.get('price') not in (None, '') else None
        event['created_date'] = datetime.datetime.now().strftime("%Y-%m-%d")
        if 'order_id' in event.keys():
            if event['order_id'] == '':
                event['order_id'] = None
        try:
            bq_client.insert_rows_json('bionic-truck-452607-i7.raw.Event', [event])
            # insert_query('Event', event)
            print("Event inserted successfully")
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
                query_dict = column_dict.copy()
                
                filter_option_id = None
                
                if not check_duplicate(table_name, column_dict):
                    print(f"No entry found in {table_name} for {column_dict}, inserting...")
                    filter_option_largest_id = get_largest_incremental_id(table_name)
                    filter_option_id = filter_option_largest_id + 1
                    column_dict['_id'] = filter_option_id
                    column_dict['created_date'] = datetime.datetime.now().strftime("%Y-%m-%d")
                                        
                    try:
                        bq_client.insert_rows_json(f'bionic-truck-452607-i7.raw.{table_name}', [column_dict])
                        # insert_query(table_name, column_dict)
                    except Exception as e:
                        print(f"Error inserting filter option: {e}")
                
                if filter_option_id is None:
                    filter_option_id = get_id_from_table(table_name, query_dict)
                    
                event_id = doc['_id']
                try:
                    bq_client.insert_rows_json('bionic-truck-452607-i7.raw.Event_Filter', [{'event_id': event_id, 'filter_option_id': filter_option_id, 'created_date': datetime.datetime.now().strftime("%Y-%m-%d")}]) 
                    # insert_query('Event_Filter', {'event_id': event_id, 'filter_option_id': filter_option_id, 'created_date': datetime.datetime.now().strftime("%Y-%m-%d")})
                except Exception as e:
                    print(f"Error inserting event filter: {e}")
                
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
            
                query_dict = column_dict.copy()
                option_id = None
                
                if not check_duplicate(table_name, column_dict):
                    print(f"No entry found in {table_name} for {column_dict}, inserting...")
                    option_largest_id = get_largest_incremental_id(table_name)
                    option_id = option_largest_id + 1
                    column_dict['_id'] = option_id
                    column_dict['created_date'] = datetime.datetime.now().strftime("%Y-%m-%d")
                    
                    try:
                        bq_client.insert_rows_json(f'bionic-truck-452607-i7.raw.{table_name}', [column_dict])
                        # insert_query(table_name, column_dict)
                    except Exception as e:
                        print(f"Error inserting product option at collection product_option: {e}")

                if option_id is None:
                    option_id = get_id_from_table(table_name, query_dict)
                try:
                    bq_client.insert_rows_json('bionic-truck-452607-i7.raw.Event_Option', [{'event_id': doc['_id'], 'product_option_id': option_id, 'created_date': datetime.datetime.now().strftime("%Y-%m-%d")}])
                    # insert_query('Event_Option', {'event_id': doc['_id'], 'product_option_id': option_id, 'created_date': datetime.datetime.now().strftime("%Y-%m-%d")})
                except Exception as e:
                    print(f"Error inserting event option: {e}")
        
        elif doc['collection'] in option_quality:
            option_doc = doc['option'][0]
            
            option_dict = {
                'option_label': option_doc.get('option_label'),
                'option_id': int(option_doc.get('option_id')) if option_doc.get('option_id') not in (None, '') else None,
                'value_id': int(option_doc.get('value_id')) if option_doc.get('value_id') not in (None, '') else None,
                'value_label': option_doc.get('value_label'),
            }
            
            query_option_dict = option_dict.copy()
            
            quality_dict = {
                'quality': option_doc.get('quality'),
                'quality_label': option_doc.get('quality_label', option_doc.get('quality'))
            }
            
            query_quality_dict = quality_dict.copy()
            
            option_id = None
            quality_id = None
            
            if not check_duplicate('Product_Option', option_dict):
                print(f"No entry found in Product_Option for {option_dict}, inserting...")
                option_largest_id = get_largest_incremental_id('Product_Option')
                option_id = option_largest_id + 1
                option_dict['_id'] = option_id
                option_dict['created_date'] = datetime.datetime.now().strftime("%Y-%m-%d")
                try:
                    bq_client.insert_rows_json('bionic-truck-452607-i7.raw.Product_Option', [option_dict])
                    # insert_query('Product_Option', option_dict)
                except Exception as e:
                    print(f"Error inserting product option at collection option_quality: {e}")
                
            if not check_duplicate('Product_Quality', quality_dict):
                print(f"No entry found in Product_Quality for {quality_dict}, inserting...")
                quality_largest_id = get_largest_incremental_id('Product_Quality')
                quality_id = quality_largest_id + 1
                quality_dict['_id'] = quality_id
                quality_dict['created_date'] = datetime.datetime.now().strftime("%Y-%m-%d")
                
                try:
                    bq_client.insert_rows_json('bionic-truck-452607-i7.raw.Product_Quality', [quality_dict])
                    # insert_query('Product_Quality', quality_dict)
                except Exception as e:
                    print(f"Error inserting product quality: {e}")
            
            if quality_id is None:
                quality_id = get_id_from_table('Product_Quality', query_quality_dict)
                
            if option_id is None:
                option_id = get_id_from_table('Product_Option', query_option_dict)
            
            try:
                bq_client.insert_rows_json('bionic-truck-452607-i7.raw.Event_Option', [{'event_id': doc['_id'], 'product_option_id': option_id, 'product_quality_id': quality_id , 'created_date': datetime.datetime.now().strftime("%Y-%m-%d")}])
                # insert_query('Event_Option', {'event_id': doc['_id'], 'product_option_id': option_id, 'created_date': datetime.datetime.now().strftime("%Y-%m-%d")})
            except Exception as e:
                print(f"Error inserting event option quality: {e}")
            
        elif doc['collection'] in option_recommend:
            table_name = 'Recommend_Option'
            recommend_option = doc['option']
            query_recommend_option = recommend_option.copy()
            if 'category id' in recommend_option:
                recommend_option['category_id'] = recommend_option.pop('category id')
            
            recommend_id = None
            
            if not check_duplicate(table_name, recommend_option):
                print(f"No entry found in {table_name} for {recommend_option}, inserting...")
                recommend_largest_id = get_largest_incremental_id(table_name)
                recommend_id = recommend_largest_id + 1
                recommend_option['_id'] = recommend_id
                recommend_option['created_date'] = datetime.datetime.now().strftime("%Y-%m-%d")
                try:
                    bq_client.insert_rows_json(f'bionic-truck-452607-i7.raw.{table_name}', [recommend_option])
                    # insert_query(table_name, recommend_option)
                except Exception as e:
                    print(f"Error inserting recommend option: {e}")
            
            if recommend_id is None:
                recommend_option_id = get_id_from_table(table_name, query_recommend_option)
            
            try:
                bq_client.insert_rows_json('bionic-truck-452607-i7.raw.Event_Recommend', [{'event_id': doc['_id'], 'recommend_option_id': recommend_option_id, 'created_date': datetime.datetime.now().strftime("%Y-%m-%d")}])
                # insert_query('Event_Recommend', {'event_id': doc['_id'], 'recommend_option_id': recommend_option_id, 'created_date': datetime.datetime.now().strftime("%Y-%m-%d")})
            except Exception as e:
                print(f"Error inserting event recommend: {e}")    
            
        elif doc['collection'] in cart_product:
            for product in doc['cart_products']:
                event_cart_dict = {
                    'event_id': doc['_id'],
                    'amount': product['amount'],
                    'price': convert_to_price_float(product['price']),
                    'product_id': product['product_id'],
                    'currency': product['currency'],
                    'created_date': datetime.datetime.now().strftime("%Y-%m-%d")
                }
                try:
                    bq_client.insert_rows_json('bionic-truck-452607-i7.raw.Event_Cart', [event_cart_dict])
                    # insert_query('Event_Cart', event_cart_dict)
                except Exception as e:
                    print(f"Error inserting event cart: {e}")
                    
                for options in product['option']:
                    column_dict = {
                        'option_label': options.get('option_label'),
                        'option_id': int(options.get('option_id')) if options.get('option_id') not in (None, '') else None,
                        'value_id': int(options.get('value_id')) if options.get('value_id') not in (None, '') else None,
                        'value_label': options.get('value_label'),
                    }
                    option_id = None
                    query_dict = column_dict.copy()
                    
                    if not check_duplicate('Product_Option', column_dict):
                        print(f"No entry found in Product_Option for {column_dict}, inserting...")
                        option_largest_id = get_largest_incremental_id('Product_Option')
                        option_id = option_largest_id + 1
                        column_dict['_id'] = option_id
                        column_dict['created_date'] = datetime.datetime.now().strftime("%Y-%m-%d")
                        try:
                            bq_client.insert_rows_json('bionic-truck-452607-i7.raw.Product_Option', [column_dict])
                            # insert_query('Product_Option', column_dict)
                        except Exception as e:
                            print(f"Error inserting product option at cart_products : {e}")
                    
                    if option_id is None:
                        option_id = get_id_from_table('Product_Option', query_dict)
                    cart_product_option_dict = {
                        'event_id': doc['_id'],
                        'product_id': product['product_id'],
                        'product_option_id': option_id,
                        'created_date': datetime.datetime.now().strftime("%Y-%m-%d")
                    }
                    try:
                        bq_client.insert_rows_json('bionic-truck-452607-i7.raw.Cart_Product_Option', [cart_product_option_dict])
                        # insert_query('Cart_Product_Option', cart_product_option_dict)
                    except Exception as e:
                        print(f"Error inserting cart product option: {e}")

        else:
            print(f"Other collections don't need to process further: {doc['collection']}")
    

def get_largest_incremental_id(table_name):
    query = f"""
        SELECT MAX(_id) as max_incremental_id
        FROM `bionic-truck-452607-i7.raw.{table_name}`
    """
    query_job = bq_client.query(query)
    results = query_job.result()
    for row in results:
        return row.max_incremental_id if row.max_incremental_id else 0
    
def check_duplicate(table_name, column_dict):
    int_column = ['option_id', 'value_id']
    
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
        SELECT COUNT(*) as count
        FROM `bionic-truck-452607-i7.raw.{table_name}`
        WHERE 1 = 1 AND {condition_str}
    """
    
    query_job = bq_client.query(query)
    results = query_job.result()
    
    for row in results:
        return row.count > 0
    
def get_id_from_table(table_name, column_dict):
    int_column = ['option_id', 'value_id']
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
        return row._id if row._id else 0
    
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
    
# def insert_query(table_name, column_dict):
#     project_id = 'bionic-truck-452607-i7'
#     table = f'{project_id}.raw.{table_name}'
#     filtered_items = {k: v for k, v in column_dict.items() if v is not None}
    
#     columns = ", ".join(f"`{col}`" for col in filtered_items.keys())
    
#     values = []
#     for v in filtered_items.values():
#         if isinstance(v, str):
#             values.append(f"'{v}'")
#         else:
#             values.append(str(v))
    
#     values_str = ", ".join(values)
    
#     query = f"INSERT INTO `{table}` ({columns}) VALUES ({values_str})"
#     bq_client.query(query)
#     print(f"Query executed: {query}")

