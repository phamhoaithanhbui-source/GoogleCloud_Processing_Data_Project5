import json
from pathlib import Path
from pymongo import MongoClient

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "countly"
COLLECTION_NAME = "summary"

OUTPUT_DIR = Path.home() / "ip_pipeline"
OUTPUT_FILE = OUTPUT_DIR / "unique_ips.jsonl"

WRITE_BUFFER = 10000
PROGRESS_EVERY = 100000

def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
    client.admin.command("ping")
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    cursor = collection.find(
        {"ip": {"$exists": True, "$ne": None}},
        {"_id": 0, "ip": 1}
    ).sort("ip", 1).hint("ip_1")

    count = 0
    scanned = 0
    buffer = []
    last_ip = None

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for doc in cursor:
            scanned += 1
            ip = doc.get("ip")

            if not ip:
                continue

            if ip == last_ip:
                continue

            last_ip = ip
            buffer.append(json.dumps({"ip": ip}, ensure_ascii=False))
            count += 1

            if len(buffer) >= WRITE_BUFFER:
                f.write("\n".join(buffer) + "\n")
                buffer.clear()

            if scanned % PROGRESS_EVERY == 0:
                print(
                    f"Scanned {scanned:,} rows | Exported {count:,} unique IPs...",
                    flush=True
                )

        if buffer:
            f.write("\n".join(buffer) + "\n")

    client.close()
    print(f"Done. Scanned {scanned:,} rows.")
    print(f"Exported {count:,} unique IPs.")
    print(f"Saved to: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()