import functions_framework
from google.cloud import bigquery


PROJECT_ID = "project-6978f7f5-636f-40bd-83c"
DATASET_ID = "glamira_dataset_raw"


# =====================
# SCHEMAS
# =====================

OPTION_SCHEMA = [
    bigquery.SchemaField("option_label", "STRING"),
    bigquery.SchemaField("option_id", "STRING"),
    bigquery.SchemaField("value_label", "STRING"),
    bigquery.SchemaField("value_id", "STRING"),
]


EVENT_SCHEMA = [
    bigquery.SchemaField("event_id", "STRING"),
    bigquery.SchemaField("collection", "STRING"),

    bigquery.SchemaField("time_stamp", "INT64"),
    bigquery.SchemaField("local_time", "STRING"),

    bigquery.SchemaField("ip", "STRING"),
    bigquery.SchemaField("user_agent", "STRING"),
    bigquery.SchemaField("resolution", "STRING"),

    bigquery.SchemaField("user_id_db", "STRING"),
    bigquery.SchemaField("device_id", "STRING"),
    bigquery.SchemaField("api_version", "STRING"),
    bigquery.SchemaField("store_id", "STRING"),

    bigquery.SchemaField("show_recommendation", "STRING"),
    bigquery.SchemaField("current_url", "STRING"),
    bigquery.SchemaField("referrer_url", "STRING"),
    bigquery.SchemaField("email_address", "STRING"),

    bigquery.SchemaField("recommendation", "STRING"),
    bigquery.SchemaField("utm_source", "STRING"),
    bigquery.SchemaField("utm_medium", "STRING"),

    bigquery.SchemaField("product_id", "STRING"),
    bigquery.SchemaField("viewing_product_id", "STRING"),
    bigquery.SchemaField("order_id", "STRING"),

    bigquery.SchemaField(
        "option",
        "RECORD",
        mode="REPEATED",
        fields=OPTION_SCHEMA,
    ),

    bigquery.SchemaField(
        "cart_products",
        "RECORD",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField("product_id", "STRING"),
            bigquery.SchemaField("amount", "INT64"),
            bigquery.SchemaField("price", "STRING"),
            bigquery.SchemaField("currency", "STRING"),
            bigquery.SchemaField(
                "option",
                "RECORD",
                mode="REPEATED",
                fields=OPTION_SCHEMA,
            ),
        ],
    ),

    bigquery.SchemaField("payload_json", "STRING"),
]


IP_LOCATION_SCHEMA = [
    bigquery.SchemaField("ip", "STRING"),
    bigquery.SchemaField("country_code", "STRING"),
    bigquery.SchemaField("country_name", "STRING"),
    bigquery.SchemaField("region", "STRING"),
    bigquery.SchemaField("city", "STRING"),
]


PRODUCT_SCHEMA = [
    bigquery.SchemaField("product_id", "STRING"),
    bigquery.SchemaField("product_name", "STRING"),
    bigquery.SchemaField("url", "STRING"),
    bigquery.SchemaField("final_url", "STRING"),
    bigquery.SchemaField("status_code", "INT64"),
    bigquery.SchemaField("status", "STRING"),
    bigquery.SchemaField("method", "STRING"),
    bigquery.SchemaField("reason", "STRING"),
]


def get_table_config(file_name: str):
    """
    Map GCS file path to BigQuery raw table and schema.
    """

    if file_name.startswith("raw/event/") and file_name.endswith(".jsonl.gz"):
        return "raw_event", EVENT_SCHEMA

    if file_name.startswith("raw/ip_location/") and file_name.endswith(".jsonl.gz"):
        return "raw_ip_location", IP_LOCATION_SCHEMA

    if file_name.startswith("raw/product/") and file_name.endswith(".jsonl.gz"):
        return "raw_product", PRODUCT_SCHEMA

    return None, None


@functions_framework.cloud_event
def trigger_bigquery_load(cloud_event):
    """
    Triggered when a new file is uploaded to GCS.

    1. Detect new file in GCS
    2. Start BigQuery load job
    3. Log results
    """

    data = cloud_event.data

    bucket = data["bucket"]
    file_name = data["name"]

    print(f"Detected new file: gs://{bucket}/{file_name}")

    table_name, schema = get_table_config(file_name)

    if table_name is None:
        print(f"Skip unsupported file: {file_name}")
        return

    gcs_uri = f"gs://{bucket}/{file_name}"
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

    client = bigquery.Client(project=PROJECT_ID)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        ignore_unknown_values=True,
        max_bad_records=1000,
    )

    print(f"Starting BigQuery load job: {gcs_uri} -> {table_id}")

    load_job = client.load_table_from_uri(
        gcs_uri,
        table_id,
        job_config=job_config,
    )

    load_job.result()

    print(f"BigQuery load completed.")
    print(f"Destination table: {table_id}")
    print(f"Rows loaded: {load_job.output_rows}")
