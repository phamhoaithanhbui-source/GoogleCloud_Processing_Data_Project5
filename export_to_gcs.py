import json
from pathlib import Path
from datetime import datetime
import subprocess

# CONFIG
BASE_DIR = Path.home() / "data_export"
BASE_DIR.mkdir(exist_ok=True)

FILES = [
    Path.home() / "crawl_output" / "product_id_success.jsonl",
    Path.home() / "ip_pipeline" / "ip_location.jsonl",
]

BUCKET = "gs://glamira-project-dataset"

def log(msg):
    print(f"[{datetime.now()}] {msg}")

def upload_to_gcs(local_file):
    try:
        cmd = [
            "gcloud", "storage", "cp",
            str(local_file),
            f"{BUCKET}/{local_file.name}"
        ]
        subprocess.run(cmd, check=True)
        log(f"Uploaded: {local_file}")
    except Exception as e:
        log(f"ERROR upload {local_file}: {e}")

def export_to_gcs():
    log("START EXPORT")

    for file in FILES:
        if not file.exists():
            log(f"File not found: {file}")
            continue

        log(f"Processing: {file}")
        upload_to_gcs(file)

    log("DONE EXPORT")

if __name__ == "__main__":
    export_to_gcs()