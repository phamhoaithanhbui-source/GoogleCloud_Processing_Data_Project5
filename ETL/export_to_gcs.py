import argparse
import gzip
import json
import logging
import subprocess
from pathlib import Path

from bson import json_util
from pymongo import MongoClient


# =====================
# CONFIG
# =====================
MONGO_URI = "mongodb://localhost:27017/"
DATABASE_NAME = "countly"
COLLECTION_NAME = "summary"

BUCKET_NAME = "glamira-project-dataset"

BASE_DIR = Path.home() / "ETL"
DATA_DIR = BASE_DIR / "data"
LOG_DIR = BASE_DIR / "logs"

RAW_OUTPUT_DIR = DATA_DIR / "event"
IP_OUTPUT_DIR = DATA_DIR / "ip_location"
PRODUCT_OUTPUT_DIR = DATA_DIR / "products"

LOG_FILE = LOG_DIR / "export_to_gcs.log"

# Existing local files on VM
IP_LOCATION_FILE = Path.home() / "ip_pipeline" / "ip_location.jsonl"
PRODUCT_FILE = Path.home() / "crawl_output" / "product_id_success.jsonl"

# GCS paths
GCS_RAW_PREFIX = "raw/event"
GCS_IP_PREFIX = "raw/ip_location"
GCS_PRODUCT_PREFIX = "raw/product"

BATCH_SIZE = 10_000
CHUNK_SIZE = 500_000


# =====================
# SETUP
# =====================
for directory in [
    DATA_DIR,
    LOG_DIR,
    RAW_OUTPUT_DIR,
    IP_OUTPUT_DIR,
    PRODUCT_OUTPUT_DIR,
]:
    directory.mkdir(parents=True, exist_ok=True)


logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def log(message: str):
    print(message, flush=True)
    logging.info(message)


# =====================
# COMMON HELPERS
# =====================
def run_command(command):
    log(f"Running command: {' '.join(command)}")

    result = subprocess.run(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    if result.stdout:
        log(f"STDOUT: {result.stdout.strip()}")

    if result.stderr:
        log(f"STDERR: {result.stderr.strip()}")

    if result.returncode != 0:
        raise subprocess.CalledProcessError(
            result.returncode,
            command,
            output=result.stdout,
            stderr=result.stderr,
        )


def upload_to_gcs(local_file: Path, gcs_prefix: str):
    gcs_uri = f"gs://{BUCKET_NAME}/{gcs_prefix}/{local_file.name}"

    command = [
        "gcloud",
        "storage",
        "cp",
        str(local_file),
        gcs_uri,
    ]

    log(f"Uploading {local_file} -> {gcs_uri}")
    run_command(command)
    log(f"Uploaded successfully: {gcs_uri}")

    try:
        local_file.unlink()
        log(f"Deleted local file after upload: {local_file}")
    except Exception as e:
        log(f"WARNING could not delete local file: {local_file}, error={e}")


def to_string(value):
    if value is None:
        return None
    return str(value)


def to_json_string(value):
    if value is None:
        return None
    return json_util.dumps(value, ensure_ascii=False)

def to_int(value, default=0):
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except (ValueError, TypeError):
        return default


def write_jsonl_gz(input_file: Path, output_file: Path, sample_limit=None):
    """
    Compress existing JSONL file into JSONL.GZ.
    Also validates that each line is valid JSON.
    """

    total = 0
    bad = 0

    log(f"Compressing JSONL: {input_file} -> {output_file}")

    with open(input_file, "r", encoding="utf-8") as infile, \
            gzip.open(output_file, "wt", encoding="utf-8") as outfile:

        for line_no, line in enumerate(infile, start=1):
            if sample_limit is not None and total >= sample_limit:
                break

            line = line.strip()

            if not line:
                continue

            try:
                row = json.loads(line)
                outfile.write(json.dumps(row, ensure_ascii=False))
                outfile.write("\n")
                total += 1

            except Exception as e:
                bad += 1
                log(f"BAD JSON line={line_no} file={input_file} error={e}")

    log(f"Compressed file completed: {output_file}")
    log(f"Valid rows: {total:,}, bad rows: {bad:,}")

    if bad > 0:
        raise ValueError(f"Found {bad} bad JSON rows in {input_file}")

    return total


# =====================
# RAW DATA EXPORT
# =====================

def clean_option_list(options):
    """
    Convert option array into BigQuery REPEATED RECORD format.
    Handles both top-level option and cart_products.option.
    """

    if not isinstance(options, list):
        return []

    cleaned_options = []

    for opt in options:
        if not isinstance(opt, dict):
            continue

        cleaned_options.append({
            "option_label": to_string(opt.get("option_label")),
            "option_id": to_string(opt.get("option_id")),
            "value_label": to_string(opt.get("value_label")),
            "value_id": to_string(opt.get("value_id")),
        })

    return cleaned_options


def clean_cart_products(cart_products):
    """
    Convert cart_products array into BigQuery REPEATED RECORD format.
    Includes nested option array inside each cart product.
    """

    if not isinstance(cart_products, list):
        return []

    cleaned_products = []

    for product in cart_products:
        if not isinstance(product, dict):
            continue

        cleaned_products.append({
            "product_id": to_string(product.get("product_id")),
            "amount": to_int(product.get("amount")),
            "price": to_string(product.get("price")),
            "currency": to_string(product.get("currency")),
            "option": clean_option_list(product.get("option")),
        })

    return cleaned_products


def normalize_raw_data_doc(doc):
    """
    Convert MongoDB document into BigQuery-friendly JSONL row.

    This version keeps nested arrays as BigQuery REPEATED RECORD:
    - option[]
    - cart_products[]
    - cart_products[].option[]

    payloy keeps the full original MongoDB document for audit/debug.
    """

    return {
        "event_id": to_string(doc.get("_id")),
        "collection": doc.get("collection"),

        "time_stamp": doc.get("time_stamp"),
        "local_time": doc.get("local_time"),

        "ip": doc.get("ip"),
        "user_agent": doc.get("user_agent"),
        "resolution": doc.get("resolution"),

        "user_id_db": to_string(doc.get("user_id_db")),
        "device_id": doc.get("device_id"),
        "api_version": doc.get("api_version"),
        "store_id": to_string(doc.get("store_id")),

        "show_recommendation": to_string(doc.get("show_recommendation")),
        "current_url": doc.get("current_url"),
        "referrer_url": doc.get("referrer_url"),
        "email_address": doc.get("email_address"),

        "recommendation": to_string(doc.get("recommendation")),
        "utm_source": to_string(doc.get("utm_source")),
        "utm_medium": to_string(doc.get("utm_medium")),

        "product_id": to_string(doc.get("product_id")),
        "viewing_product_id": to_string(doc.get("viewing_product_id")),
        "order_id": to_string(doc.get("order_id")),

        # Top-level nested array, used in view_product_detail events
        "option": clean_option_list(doc.get("option")),

        # Nested array with nested option[], used in checkout_success events
        "cart_products": clean_cart_products(doc.get("cart_products")),

        # Full original MongoDB document backup
        "payload_json": json_util.dumps(doc, ensure_ascii=False),
    }

def export_raw_data_to_gcs(sample_limit=None, upload=True):
    log("========== EXPORT RAW DATA START ==========")
    log(f"MongoDB URI: {MONGO_URI}")
    log(f"Database: {DATABASE_NAME}")
    log(f"Collection: {COLLECTION_NAME}")
    log(f"Batch size: {BATCH_SIZE:,}")
    log(f"Chunk size: {CHUNK_SIZE:,}")
    log(f"Sample limit: {sample_limit}")

    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]

    cursor = collection.find({}, no_cursor_timeout=True).batch_size(BATCH_SIZE)

    total_count = 0
    chunk_count = 0
    current_chunk_rows = 0
    current_file = None
    writer = None

    try:
        for doc in cursor:
            if sample_limit is not None and total_count >= sample_limit:
                break

            if writer is None or current_chunk_rows >= CHUNK_SIZE:
                if writer is not None:
                    writer.close()
                    log(f"Finished chunk: {current_file}")

                    if upload:
                        upload_to_gcs(current_file, GCS_RAW_PREFIX)

                chunk_count += 1
                current_chunk_rows = 0

                current_file = RAW_OUTPUT_DIR / f"event_data_part{chunk_count:06d}.jsonl.gz"
                writer = gzip.open(current_file, "wt", encoding="utf-8")

                log(f"Started new chunk: {current_file}")

            try:
                row = normalize_raw_data_doc(doc)

                writer.write(json.dumps(row, ensure_ascii=False))
                writer.write("\n")

                total_count += 1
                current_chunk_rows += 1

                if total_count % 100_000 == 0:
                    log(f"Exported raw data rows: {total_count:,}")

            except Exception as row_error:
                log(
                    f"ERROR normalizing Mongo document "
                    f"_id={doc.get('_id')} error={row_error}"
                )

        if writer is not None:
            writer.close()
            log(f"Finished final chunk: {current_file}")

            if upload:
                upload_to_gcs(current_file, GCS_RAW_PREFIX)

    except Exception as e:
        log(f"ERROR exporting raw data: {e}")
        raise

    finally:
        cursor.close()
        client.close()

    log("========== EXPORT RAW DATA DONE ==========")
    log(f"Total raw data rows exported: {total_count:,}")
    log(f"Total chunks created: {chunk_count:,}")


# =====================
# IP2LOCATION EXPORT
# =====================
def export_ip_location_to_gcs(sample_limit=None, upload=True):
    log("========== EXPORT IP2LOCATION START ==========")

    if not IP_LOCATION_FILE.exists():
        raise FileNotFoundError(f"IP location file not found: {IP_LOCATION_FILE}")

    output_file = IP_OUTPUT_DIR / "ip_location.jsonl.gz"

    total = write_jsonl_gz(
        input_file=IP_LOCATION_FILE,
        output_file=output_file,
        sample_limit=sample_limit,
    )

    if upload:
        upload_to_gcs(output_file, GCS_IP_PREFIX)

    log("========== EXPORT IP2LOCATION DONE ==========")
    log(f"Total IP rows exported: {total:,}")


# =====================
# PRODUCT EXPORT
# =====================
def export_product_to_gcs(sample_limit=None, upload=True):
    log("========== EXPORT PRODUCT START ==========")

    if not PRODUCT_FILE.exists():
        raise FileNotFoundError(f"Product file not found: {PRODUCT_FILE}")

    output_file = PRODUCT_OUTPUT_DIR / "product_id_success.jsonl.gz"

    total = write_jsonl_gz(
        input_file=PRODUCT_FILE,
        output_file=output_file,
        sample_limit=sample_limit,
    )

    if upload:
        upload_to_gcs(output_file, GCS_PRODUCT_PREFIX)

    log("========== EXPORT PRODUCT DONE ==========")
    log(f"Total product rows exported: {total:,}")


# =====================
# ORCHESTRATOR
# =====================
def export_to_gcs(sample=False, upload=True):
    """
    Main ETL function required by assignment.

    1. Connect to MongoDB on VM
    2. Extract data in batches
    3. Convert data to JSONL.GZ
    4. Upload all data from VM to GCS
    5. Log operations
    """

    sample_limit = 10_000 if sample else None

    log("############################################")
    log("ETL EXPORT TO GCS START")
    log(f"Sample mode: {sample}")
    log(f"Upload enabled: {upload}")
    log("############################################")

    try:
        export_raw_data_to_gcs(sample_limit=sample_limit, upload=upload)
        export_ip_location_to_gcs(sample_limit=sample_limit, upload=upload)
        export_product_to_gcs(sample_limit=sample_limit, upload=upload)

    except Exception as e:
        log(f"ETL FAILED: {e}")
        raise

    log("############################################")
    log("ETL EXPORT TO GCS COMPLETED SUCCESSFULLY")
    log("############################################")


def main():
    parser = argparse.ArgumentParser(
        description="Export MongoDB raw data, IP2Location, and Product data to GCS."
    )

    parser.add_argument(
        "--sample",
        action="store_true",
        help="Export only sample data for testing.",
    )

    parser.add_argument(
        "--no-upload",
        action="store_true",
        help="Only create local JSONL.GZ files, do not upload to GCS.",
    )

    args = parser.parse_args()

    export_to_gcs(
        sample=args.sample,
        upload=not args.no_upload,
    )


if __name__ == "__main__":
    main()
