import json
from pathlib import Path

INPUT_DIR = Path.home() / "filtered_data"
OUTPUT_DIR = Path.home() / "processed_data"
OUTPUT_FILE = OUTPUT_DIR / "distinct_products.json"

FILES = [
    "view_product_detail.json",
    "select_product_option.json",
    "select_product_option_quality.json",
    "add_to_cart_action.json",
    "product_detail_recommendation_visible.json",
    "product_detail_recommendation_noticed.json",
    "product_view_all_recommend_clicked.json",
]


def stream_json_lines(file_path: Path):
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def merge_and_deduplicate():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    products = {}
    total_read = 0
    total_valid = 0

    for filename in FILES:
        file_path = INPUT_DIR / filename

        if not file_path.exists():
            print(f"Skip missing file: {file_path}")
            continue

        print(f"Reading: {file_path}")

        for doc in stream_json_lines(file_path):
            total_read += 1

            product_id = doc.get("product_id")
            url = doc.get("url")

            if not product_id or not url:
                continue

            product_id = str(product_id)

            if product_id not in products:
                products[product_id] = {
                    "product_id": product_id,
                    "url": url
                }
                total_valid += 1

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for item in products.values():
            json.dump(item, f, ensure_ascii=False)
            f.write("\n")

    print(f"\nTotal rows read: {total_read:,}")
    print(f"Distinct valid products: {total_valid:,}")
    print(f"Saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    merge_and_deduplicate()