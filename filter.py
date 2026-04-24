from pymongo import MongoClient
import json
from tqdm import tqdm
import os


def init_mongodb(uri):
    try:
        client = MongoClient(uri)
        print(f"Status: {client.admin.command('ping')}")
        return client
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return None


def normalize_doc(doc, event_type):
    main_events = {
        "view_product_detail",
        "select_product_option",
        "select_product_option_quality",
        "add_to_cart_action",
        "product_detail_recommendation_visible",
        "product_detail_recommendation_noticed",
    }

    if event_type in main_events:
        return {
            "source_event": event_type,
            "product_id": doc.get("product_id") or doc.get("viewing_product_id"),
            "url": doc.get("current_url"),
        }

    elif event_type == "product_view_all_recommend_clicked":
        return {
            "source_event": event_type,
            "product_id": doc.get("viewing_product_id"),
            "url": doc.get("referrer_url"),
        }

    return None


def query_documents(collection, query, projection, event_type):
    try:
        data_cursor = collection.find(query, projection)
        for doc in data_cursor:
            yield normalize_doc(doc, event_type)
    except Exception as e:
        print(f"Error during query from MongoDB: {e}")


def save_to_json(streaming_data, name, total_docs):
    output_dir = "filtered_data"
    os.makedirs(output_dir, exist_ok=True)

    filename = os.path.join(output_dir, f"{name}.json")
    count = 0

    try:
        with open(filename, "w", encoding="utf-8") as file:
            for doc in tqdm(streaming_data, total=total_docs, desc=f"Saving {filename}"):
                json.dump(doc, file, ensure_ascii=False)
                file.write("\n")
                count += 1
        print(f"Saved {count} documents to {filename}.")
    except Exception as e:
        print(f"Error saving {filename}: {e}")


def main():
    mongodb_port = 27017
    mongo_uri = f"mongodb://localhost:{mongodb_port}"
    db_name = "countly"
    collection_name = "summary"

    event_types = [
        "product_view_all_recommend_clicked",
        "add_to_cart_action",
        "product_detail_recommendation_visible",
        "product_detail_recommendation_noticed",
        "view_product_detail",
        "select_product_option",
        "select_product_option_quality",
    ]

    client = init_mongodb(mongo_uri)
    if not client:
        return

    db = client[db_name]
    collection = db[collection_name]

    for event_type in event_types:
        query = {"collection": event_type}
        projection = {
            "_id": 0,
            "product_id": 1,
            "viewing_product_id": 1,
            "current_url": 1,
            "referrer_url": 1,
        }

        total_docs = collection.count_documents(query)
        print(f"Total {event_type} documents to filter: {total_docs}")

        stream_data = query_documents(collection, query, projection, event_type)
        save_to_json(stream_data, event_type, total_docs)

    client.close()
    print("Processing complete. MongoDB connection closed.")


if __name__ == "__main__":
    main()