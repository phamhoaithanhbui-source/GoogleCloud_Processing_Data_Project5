import logging
from google.cloud import bigquery

PROJECT_ID = "project-6978f7f5-636f-40bd-83c"
DATASET_ID = "glamira_dataset"
BUCKET = "glamira-project-dataset"

logging.basicConfig(
    filename="load_bigquery.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

TABLES = [
    {
        "table_name": "raw_product",
        "gcs_uri": f"gs://{BUCKET}/product_id_success.jsonl",
        "schema": [
            bigquery.SchemaField("product_id", "STRING"),
            bigquery.SchemaField("product_name", "STRING"),
            bigquery.SchemaField("url", "STRING"),
            bigquery.SchemaField("final_url", "STRING"),
            bigquery.SchemaField("status_code", "INTEGER"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("method", "STRING"),
            bigquery.SchemaField("reason", "STRING"),
        ],
    },
    {
        "table_name": "raw_ip_location",
        "gcs_uri": f"gs://{BUCKET}/ip_location.jsonl",
        "schema": [
            bigquery.SchemaField("ip", "STRING"),
            bigquery.SchemaField("country_code", "STRING"),
            bigquery.SchemaField("country_name", "STRING"),
            bigquery.SchemaField("region", "STRING"),
            bigquery.SchemaField("city", "STRING"),
        ],
    },
]


def load_table(client, table_name, gcs_uri, schema):
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        ignore_unknown_values=True,
        max_bad_records=1000,
    )

    logging.info(f"Loading {gcs_uri} into {table_id}")
    print(f"Loading {gcs_uri} -> {table_id}")

    load_job = client.load_table_from_uri(
        gcs_uri,
        table_id,
        job_config=job_config,
    )

    load_job.result()

    table = client.get_table(table_id)

    logging.info(f"Loaded {table.num_rows} rows into {table_id}")
    print(f"Done: {table_id} | rows={table.num_rows}")


def main():
    client = bigquery.Client(project=PROJECT_ID)

    for table in TABLES:
        try:
            load_table(
                client=client,
                table_name=table["table_name"],
                gcs_uri=table["gcs_uri"],
                schema=table["schema"],
            )
        except Exception as e:
            logging.error(f"Failed loading {table['table_name']}: {e}")
            print(f"ERROR loading {table['table_name']}: {e}")


if __name__ == "__main__":
    main()