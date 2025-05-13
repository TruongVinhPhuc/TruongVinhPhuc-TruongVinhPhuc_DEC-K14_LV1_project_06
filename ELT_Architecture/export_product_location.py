import pymongo
import json
from io import BytesIO
import datetime
from google.cloud import storage
import time

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client['test']
summary = db['summary']
bucket_name = 'raw_k14_phuc'
product_destination = 'raw_event/product.jsonl'
location_destination = 'raw_event/location.jsonl'
storage_client = storage.Client()

def export_to_gcs():

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(product_destination)
    list_product = get_product_data()
    if list_product:
        buffer = BytesIO()
        for doc in list_product:
            buffer.write((json.dumps(doc, ensure_ascii=False) + '\n').encode("utf-8"))

        buffer.seek(0)  # Đưa con trỏ về đầu để upload

        # Upload lên GCS
        blob.upload_from_file(buffer, content_type="application/json")
        print(f"Exported {len(list_product)} records to gs://{bucket_name}/{product_destination}")

    list_location = get_location_data()
    blob = bucket.blob(location_destination)
    if list_location:
        buffer = BytesIO()
        for doc in list_location:
            buffer.write((json.dumps(doc, ensure_ascii=False) + '\n').encode("utf-8"))

        buffer.seek(0)  # Đưa con trỏ về đầu để upload

        # Upload lên GCS
        blob = bucket.blob(location_destination)
        blob.upload_from_file(buffer, content_type="application/json")
        print(f"Exported {len(list_location)} records to gs://{bucket_name}/{location_destination}")


def get_product_data():
    product_data = db['product'].find()
    product_dict = []
    for product in product_data:
        product.pop('_id', None)
        product_dict.append(product)
    return product_dict

def get_location_data():
    location_data = db['ip_location'].find()
    location_dict = []
    for location in location_data:
        location.pop('_id', None)
        location_dict.append({
            "ip": location.get("ip"),
            "country_name": location.get("country_long"),
            "city": location.get("city"),
            "region": location.get("region"),
        })
    return location_dict

export_to_gcs()