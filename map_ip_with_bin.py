import csv
import json
from pathlib import Path
import IP2Location

BASE_DIR = Path.home() / "ip_pipeline"
INPUT_FILE = BASE_DIR / "unique_ips.jsonl"
OUTPUT_CSV = BASE_DIR / "ip_location.csv"
OUTPUT_JSONL = BASE_DIR / "ip_location.jsonl"
BIN_FILE = Path.home() / "IP-COUNTRY-REGION-CITY.BIN"

PROGRESS_EVERY = 10000


def stream_json_lines(file_path: Path):
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def main():
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Input file not found: {INPUT_FILE}")

    if not BIN_FILE.exists():
        raise FileNotFoundError(f"BIN file not found: {BIN_FILE}")

    ip_db = IP2Location.IP2Location()
    ip_db.open(str(BIN_FILE))

    total = 0
    errors = 0

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as csv_file, \
         open(OUTPUT_JSONL, "w", encoding="utf-8") as jsonl_file:

        writer = csv.DictWriter(
            csv_file,
            fieldnames=["ip", "country_code", "country_name", "region", "city"]
        )
        writer.writeheader()

        for doc in stream_json_lines(INPUT_FILE):
            ip = doc.get("ip")
            if not ip:
                continue

            try:
                rec = ip_db.get_all(ip)

                row = {
                    "ip": ip,
                    "country_code": rec.country_short,
                    "country_name": rec.country_long,
                    "region": rec.region,
                    "city": rec.city,
                }

                writer.writerow(row)
                json.dump(row, jsonl_file, ensure_ascii=False)
                jsonl_file.write("\n")

                total += 1
                if total % PROGRESS_EVERY == 0:
                    print(f"Processed {total:,} IPs... errors={errors:,}", flush=True)

            except Exception as e:
                errors += 1
                err_row = {
                    "ip": ip,
                    "error": str(e)
                }
                json.dump(err_row, jsonl_file, ensure_ascii=False)
                jsonl_file.write("\n")

    print("Done.")
    print(f"Total processed: {total:,}")
    print(f"Errors: {errors:,}")
    print(f"CSV saved to: {OUTPUT_CSV}")
    print(f"JSONL saved to: {OUTPUT_JSONL}")


if __name__ == "__main__":
    main()