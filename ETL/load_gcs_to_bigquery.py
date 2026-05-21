import logging
from google.cloud import bigquery
from google.api_core.exceptions import Conflict


PROJECT_ID = "project-6978f7f5-636f-40bd-83c"
DATASET_ID = "glamira_dataset_raw"
LOCATION = "US"

BUCKET = "glamira-project-dataset"

LOG_FILE = "load_gcs_to_bigquery.log"


logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def log(message: str):
    print(message, flush=True)
    logging.info(message)


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


TABLE_CONFIGS = [
    {
        "table_name": "raw_event",
        "gcs_uri": f"gs://{BUCKET}/raw/event/*.jsonl.gz",
        "schema": EVENT_SCHEMA,
    },
    {
        "table_name": "raw_ip_location",
        "gcs_uri": f"gs://{BUCKET}/raw/ip_location/*.jsonl.gz",
        "schema": IP_LOCATION_SCHEMA,
    },
    {
        "table_name": "raw_product",
        "gcs_uri": f"gs://{BUCKET}/raw/product/*.jsonl.gz",
        "schema": PRODUCT_SCHEMA,
    },
]


# =====================
# BIGQUERY FUNCTIONS
# =====================

def create_dataset_if_not_exists(client: bigquery.Client):
    dataset_id = f"{PROJECT_ID}.{DATASET_ID}"

    dataset = bigquery.Dataset(dataset_id)
    dataset.location = LOCATION

    try:
        client.create_dataset(dataset)
        log(f"Created dataset: {dataset_id}")
    except Conflict:
        log(f"Dataset already exists: {dataset_id}")


def load_table_from_gcs(
    client: bigquery.Client,
    table_name: str,
    gcs_uri: str,
    schema: list,
):
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        ignore_unknown_values=True,
        max_bad_records=1000,
    )

    log(f"Loading {gcs_uri} -> {table_id}")

    load_job = client.load_table_from_uri(
        gcs_uri,
        table_id,
        job_config=job_config,
    )

    try:
        load_job.result()
    except Exception as e:
        log(f"ERROR loading table {table_name}: {e}")

        if load_job.errors:
            for error in load_job.errors:
                log(f"BigQuery error: {error}")

        raise

    table = client.get_table(table_id)

    log(f"Loaded table: {table_id}")
    log(f"Rows loaded: {table.num_rows:,}")


def main():
    client = bigquery.Client(project=PROJECT_ID)

    create_dataset_if_not_exists(client)

    for table_config in TABLE_CONFIGS:
        load_table_from_gcs(
            client=client,
            table_name=table_config["table_name"],
            gcs_uri=table_config["gcs_uri"],
            schema=table_config["schema"],
        )

    log("All raw tables loaded successfully.")


if __name__ == "__main__":
    main()
