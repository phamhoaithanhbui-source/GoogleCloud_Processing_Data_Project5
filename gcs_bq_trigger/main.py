import functions_framework
from google.cloud import bigquery

PROJECT_ID = "project-6978f7f5-636f-40bd-83c"
DATASET_ID = "glamira_dataset"

PRODUCT_SCHEMA = [
    bigquery.SchemaField("product_id", "STRING"),
    bigquery.SchemaField("product_name", "STRING"),
    bigquery.SchemaField("url", "STRING"),
    bigquery.SchemaField("final_url", "STRING"),
    bigquery.SchemaField("status_code", "INTEGER"),
    bigquery.SchemaField("status", "STRING"),
    bigquery.SchemaField("method", "STRING"),
    bigquery.SchemaField("reason", "STRING"),
]

IP_SCHEMA = [
    bigquery.SchemaField("ip", "STRING"),
    bigquery.SchemaField("country_code", "STRING"),
    bigquery.SchemaField("country_name", "STRING"),
    bigquery.SchemaField("region", "STRING"),
    bigquery.SchemaField("city", "STRING"),
]


def get_table_config(file_name: str):
    if file_name == "product_id_success.jsonl":
        return "raw_product", PRODUCT_SCHEMA

    if file_name == "ip_location.jsonl":
        return "raw_ip_location", IP_SCHEMA

    return None, None


@functions_framework.cloud_event
def trigger_bigquery_load(cloud_event):
    data = cloud_event.data

    bucket = data["bucket"]
    file_name = data["name"]

    print(f"Detected file: gs://{bucket}/{file_name}")

    if not file_name.endswith(".jsonl"):
        print(f"Skip non-jsonl file: {file_name}")
        return

    table_name, schema = get_table_config(file_name)

    if not table_name:
        print(f"Skip unknown file: {file_name}")
        return

    gcs_uri = f"gs://{bucket}/{file_name}"
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

    client = bigquery.Client(project=PROJECT_ID)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
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

    table = client.get_table(table_id)

    print(f"Load completed: {table_id}")
    print(f"Rows loaded: {table.num_rows}")