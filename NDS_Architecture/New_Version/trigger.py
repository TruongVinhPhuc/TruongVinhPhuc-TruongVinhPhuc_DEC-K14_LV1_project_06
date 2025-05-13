import functions_framework
from google.cloud import bigquery
import os
import traceback



# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def hello_gcs(cloud_event):
    bq_client = bigquery.Client()
    schema = [
        bigquery.SchemaField("_id", "STRING"),
        bigquery.SchemaField("time_stamp", "INTEGER"),
        bigquery.SchemaField("ip", "STRING"),
        bigquery.SchemaField("user_agent", "STRING"),
        bigquery.SchemaField("resolution", "STRING"),
        bigquery.SchemaField("user_id_db", "STRING"),
        bigquery.SchemaField("device_id", "STRING"),
        bigquery.SchemaField("api_version", "STRING"),
        bigquery.SchemaField("store_id", "STRING"),
        bigquery.SchemaField("local_time", "DATETIME"),
        bigquery.SchemaField("show_recommendation", "STRING"),
        bigquery.SchemaField("current_url", "STRING"),
        bigquery.SchemaField("referrer_url", "STRING"),
        bigquery.SchemaField("email_address", "STRING"),
        bigquery.SchemaField("collection", "STRING"),
        bigquery.SchemaField("cat_id", "INTEGER"),
        bigquery.SchemaField("is_paypal", "BOOLEAN"),
        bigquery.SchemaField("recommendation_product_id", "STRING"),
        bigquery.SchemaField("recommendation_product_position", "STRING"),
        bigquery.SchemaField("recommendation_clicked_position", "STRING"),
        bigquery.SchemaField("key_search", "STRING"),
        bigquery.SchemaField("order_id", "INTEGER"),
        bigquery.SchemaField("viewing_product_id", "STRING"),
        bigquery.SchemaField("recommendation", "BOOLEAN"),
        bigquery.SchemaField("utm_source", "STRING"),
        bigquery.SchemaField("utm_medium", "STRING"),
        bigquery.SchemaField("product_id", "STRING"),
        bigquery.SchemaField("collect_id", "STRING"),
        bigquery.SchemaField("option", "JSON"),
        bigquery.SchemaField("cart_products", "RECORD", mode="REPEATED",
            fields=[
                bigquery.SchemaField("product_id", "INTEGER"),
                bigquery.SchemaField("amount", "INTEGER"),
                bigquery.SchemaField("price", "FLOAT"),
                bigquery.SchemaField("currency", "STRING"),
                bigquery.SchemaField(
                    "option",
                    "RECORD",
                    mode="REPEATED",
                    fields=[
                        bigquery.SchemaField("option_label", "STRING"),
                        bigquery.SchemaField("option_id", "INTEGER"),
                        bigquery.SchemaField("value_label", "STRING"),
                        bigquery.SchemaField("value_id", "INTEGER"),
                    ]
                ),
            ]
        ),
        bigquery.SchemaField("price", "FLOAT"),
        bigquery.SchemaField("currency", "STRING")
    ]
    try:
        data = cloud_event.data
        bucket_name = data["bucket"]
        blob_name = data["name"]

        table_name = os.path.splitext(os.path.basename(blob_name))[0]
        if 'raw_event' in table_name:
            dataset_id = "raw_event"
            table_id = f"bionic-truck-452607-i7.{dataset_id}.event"
            uri = f"gs://{bucket_name}/{blob_name}"

            job_config = bigquery.LoadJobConfig(
                schema=schema,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND
            )

            load_job = bq_client.load_table_from_uri(uri, table_id, job_config=job_config)
        else:
            dataset_id = "raw"
            table_id = f"bionic-truck-452607-i7.{dataset_id}.{table_name}"
            uri = f"gs://{bucket_name}/{blob_name}"

            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND
            )
            load_job = bq_client.load_table_from_uri(uri, table_id, job_config=job_config)

            if 'product' in table_name:
                dataset_id = 'raw_event'
                table_id = f"bionic-truck-452607-i7.{dataset_id}.{table_name}"

                job_config = bigquery.LoadJobConfig(
                    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND
                )
                load_job = bq_client.load_table_from_uri(uri, table_id, job_config=job_config)


            if 'location' in table_name:
                dataset_id = 'raw_event'
                table_id = f"bionic-truck-452607-i7.{dataset_id}.{table_name}"

                job_config = bigquery.LoadJobConfig(
                    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND
                )
                load_job = bq_client.load_table_from_uri(uri, table_id, job_config=job_config)            


    except Exception as e:
        print("Error processing file:", e)
