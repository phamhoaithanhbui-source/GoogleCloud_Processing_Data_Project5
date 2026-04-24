import asyncio
import aiohttp
import json
import random
import re
import time
from pathlib import Path
from urllib.parse import urlparse
from lxml import html

# =====================
# CONFIG
# =====================
MAX_CONCURRENCY = 2
BATCH_SIZE = 5
REQUEST_TIMEOUT_TOTAL = 10
REQUEST_TIMEOUT_CONNECT = 5
REQUEST_TIMEOUT_SOCK_CONNECT = 5
REQUEST_TIMEOUT_SOCK_READ = 5

MAX_RETRIES = 0
RETRY_BACKOFF_FACTOR = 2
BASE_SLEEP = 1.0
JITTER_MIN = 0.3
JITTER_MAX = 1.0

BASE_DIR = Path.home()
INPUT_FILE = BASE_DIR / "processed_data" / "distinct_products.json"
OUTPUT_DIR = BASE_DIR / "crawl_output"
SUCCESS_FILE = OUTPUT_DIR / "product_info_success.jsonl"
ERROR_FILE = OUTPUT_DIR / "product_info_error.jsonl"
CHECKPOINT_FILE = OUTPUT_DIR / "checkpoint.txt"
LOG_FILE = OUTPUT_DIR / "crawl.log"

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0",
    "Mozilla/5.0 (X11; Linux i686; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 OPR/109.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.2420.81",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 OPR/109.0.0.0",
]

HEADERS_TEMPLATE = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "DNT": "1",
}

# =====================
# HELPERS
# =====================
def ensure_dirs():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def log(msg: str):
    line = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
    print(line, flush=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def stream_json_lines(file_path: Path):
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)

def load_checkpoint():
    if CHECKPOINT_FILE.exists():
        return CHECKPOINT_FILE.read_text(encoding="utf-8").strip()
    return None

def save_checkpoint(product_id: str):
    CHECKPOINT_FILE.write_text(str(product_id), encoding="utf-8")

def clean_text(text):
    if not text:
        return None
    text = re.sub(r"\s+", " ", text).strip()
    return text or None

def slug_fallback(url: str):
    try:
        path = urlparse(url).path
        slug = path.rstrip("/").split("/")[-1]
        slug = re.sub(r"\.html?$", "", slug, flags=re.IGNORECASE)
        slug = slug.replace("-", " ").replace("_", " ")
        slug = clean_text(slug)
        return slug.title() if slug else None
    except Exception:
        return None

def extract_title_from_html(text: str):
    try:
        tree = html.fromstring(text)

        xpaths = [
            '//meta[@property="og:title"]/@content',
            '//meta[@name="title"]/@content',
            '//*[contains(@class, "page-title")]//text()',
            '//*[contains(@class, "product-name")]//text()',
            '//h1//text()',
            '//*[@itemprop="name"]//text()',
            '//title/text()',
        ]

        for xp in xpaths:
            values = tree.xpath(xp)
            if not values:
                continue

            if isinstance(values, list):
                value = clean_text(" ".join(v for v in values if isinstance(v, str)))
            else:
                value = clean_text(str(values))

            if value:
                return value

        return None
    except Exception:
        return None

def flush_jsonl(file_path: Path, rows):
    if not rows:
        return
    with open(file_path, "a", encoding="utf-8") as f:
        for row in rows:
            json.dump(row, f, ensure_ascii=False)
            f.write("\n")

def load_records_with_resume():
    last_done_id = load_checkpoint()
    skip_mode = last_done_id is not None
    skipped = 0

    for doc in stream_json_lines(INPUT_FILE):
        product_id = str(doc.get("product_id", "")).strip()
        url = doc.get("url")

        if not product_id or not url:
            continue

        if skip_mode:
            skipped += 1
            if product_id == last_done_id:
                skip_mode = False
            continue

        yield {
            "product_id": product_id,
            "url": url,
        }

    if last_done_id:
        log(f"Resume checkpoint used: {last_done_id} | skipped approx {skipped:,} rows")

def batch_records(records, batch_size):
    batch = []
    for record in records:
        batch.append(record)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch

# =====================
# FETCH
# =====================
async def fetch(session, sem, data, retry_count=0):
    url = data.get("url")
    product_id = data.get("product_id")

    headers = {
        **HEADERS_TEMPLATE,
        "User-Agent": random.choice(USER_AGENTS),
        "Referer": url,
    }

    timeout = aiohttp.ClientTimeout(
        total=REQUEST_TIMEOUT_TOTAL,
        connect=REQUEST_TIMEOUT_CONNECT,
        sock_connect=REQUEST_TIMEOUT_SOCK_CONNECT,
        sock_read=REQUEST_TIMEOUT_SOCK_READ,
    )

    log(f"QUEUE product_id={product_id} retry={retry_count}")

    try:
        async with sem:
            log(f"ACQUIRED product_id={product_id} retry={retry_count}")

            async with session.get(
                url,
                headers=headers,
                timeout=timeout,
                allow_redirects=True,
                ssl=False,
            ) as resp:
                status = resp.status
                final_url = str(resp.url)
                text = await resp.text(errors="ignore")

                log(f"STATUS product_id={product_id} status={status}")

                if status in [200, 201]:
                    title_text = extract_title_from_html(text)
                    if not title_text:
                        title_text = slug_fallback(final_url or url)

                    if title_text:
                        return {
                            "status": "success",
                            "product_id": product_id,
                            "product_name": title_text,
                            "url": url,
                            "final_url": final_url,
                            "status_code": status,
                            "reason": None,
                        }

                    return {
                        "status": "failed",
                        "product_id": product_id,
                        "product_name": None,
                        "url": url,
                        "final_url": final_url,
                        "status_code": status,
                        "reason": "title_not_found",
                    }

                elif status in [403, 429, 500, 502, 503, 504] and retry_count < MAX_RETRIES:
                    sleep_s = random.uniform(0.5, 1.5) * (RETRY_BACKOFF_FACTOR * (retry_count + 1))
                    log(f"RETRY product_id={product_id} status={status} sleep={sleep_s:.2f}s")
                    await asyncio.sleep(sleep_s)
                    return await fetch(session, sem, data, retry_count + 1)

                else:
                    return {
                        "status": "failed",
                        "product_id": product_id,
                        "product_name": None,
                        "url": url,
                        "final_url": final_url,
                        "status_code": status,
                        "reason": f"status_{status}",
                    }

    except Exception as e:
        log(f"ERROR product_id={product_id} retry={retry_count} error={str(e)}")
        if retry_count < MAX_RETRIES:
            sleep_s = random.uniform(0.5, 1.5) * (RETRY_BACKOFF_FACTOR * (retry_count + 1))
            log(f"RETRY product_id={product_id} after exception sleep={sleep_s:.2f}s")
            await asyncio.sleep(sleep_s)
            return await fetch(session, sem, data, retry_count + 1)

        return {
            "status": "failed",
            "product_id": product_id,
            "product_name": slug_fallback(url),
            "url": url,
            "final_url": None,
            "status_code": None,
            "reason": str(e),
        }

async def batch_crawl_from_url(data_batch):
    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENCY, ssl=False)

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [fetch(session, sem, data) for data in data_batch]
        results = await asyncio.gather(*tasks)

    result = [r for r in results if r["status"] == "success"]
    faulty_package = [r for r in results if r["status"] != "success"]
    return result, faulty_package

def main():
    ensure_dirs()

    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Input file not found: {INPUT_FILE}")

    total = 0
    total_success = 0
    total_error = 0

    log("Starting async crawl...")
    log(f"Input file: {INPUT_FILE}")
    log(
        f"Concurrency={MAX_CONCURRENCY}, Batch size={BATCH_SIZE}, "
        f"Retries={MAX_RETRIES}, Timeout(total/connect/read)="
        f"{REQUEST_TIMEOUT_TOTAL}/{REQUEST_TIMEOUT_CONNECT}/{REQUEST_TIMEOUT_SOCK_READ}"
    )

    records = load_records_with_resume()

    for batch_num, batch in enumerate(batch_records(records, BATCH_SIZE), start=1):
        start_time = time.perf_counter()
        log(f"RUNNING batch={batch_num} size={len(batch)}")

        result, faulty_package = asyncio.run(batch_crawl_from_url(batch))

        if faulty_package:
            flush_jsonl(ERROR_FILE, faulty_package)
        if result:
            flush_jsonl(SUCCESS_FILE, result)

        total += len(batch)
        total_success += len(result)
        total_error += len(faulty_package)

        save_checkpoint(batch[-1]["product_id"])

        end_time = time.perf_counter()
        log(
            f"Batch {batch_num}: total={len(batch)}, "
            f"success={len(result)}, error={len(faulty_package)}, "
            f"time={end_time - start_time:.2f}s"
        )

        time.sleep(random.uniform(1.0, 2.0))

    log("Done!")
    log(f"Total processed: {total:,}")
    log(f"Total success: {total_success:,}")
    log(f"Total error: {total_error:,}")
    log(f"Success file: {SUCCESS_FILE}")
    log(f"Error file: {ERROR_FILE}")

if __name__ == "__main__":
    main()