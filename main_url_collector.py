"""
m4uhd.page — URL Collector
===========================
Collect every movie URL from /new-movies listing pages.
Collect every series URL from /new-tvseries listing pages.
Saves movies to movies.json, movies2.json, etc. (max 5000 records each).
Saves series to series.json, series2.json, etc. (max 5000 records each).
Tracks processed pages in page_already_processed.txt (movies)
                       and series_page_already_processed.txt (series).

Usage:
    python main_url_collector.py                         # movies pages 1-2 (default)
    python main_url_collector.py 1 10                    # movies pages 1-10
    python main_url_collector.py 1 10 --series 1 177     # movies 1-10 AND series 1-177
    python main_url_collector.py --series 1 177          # series only
"""

import sys, json, time, random, logging, os
from curl_cffi import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from dataclasses import dataclass, asdict

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
BASE_URL                      = "https://ww1.m4uhd.page"
DELAY_MIN                     = 1.5
DELAY_MAX                     = 3.0
MAX_RETRIES                   = 3
TIMEOUT                       = 30
MAX_RECORDS                   = 5000
MOVIES_PROCESSED_PAGES_FILE   = "page_already_processed.txt"
SERIES_PROCESSED_PAGES_FILE   = "series_page_already_processed.txt"

# ── Argument Parsing ──────────────────────────────────────────────────────────
def parse_args():
    """
    Supports:
        scraper.py                         → movies 1-2
        scraper.py 1 10                    → movies 1-10
        scraper.py 1 10 --series 1 177     → movies 1-10, series 1-177
        scraper.py --series 1 177          → series 1-177 only (movies skipped)
    """
    args = sys.argv[1:]
    movie_start = movie_end = None
    series_start = series_end = None

    if "--series" in args:
        idx = args.index("--series")
        # Everything before --series is movie range
        movie_args = args[:idx]
        series_args = args[idx + 1:]

        if len(series_args) >= 2:
            series_start = int(series_args[0])
            series_end   = int(series_args[1])

        if len(movie_args) >= 2:
            movie_start = int(movie_args[0])
            movie_end   = int(movie_args[1])
        elif len(movie_args) == 0:
            # --series only mode: skip movies
            movie_start = movie_end = None
        else:
            movie_start = movie_end = int(movie_args[0])
    else:
        # No --series flag — original behaviour
        if len(args) >= 2:
            movie_start = int(args[0])
            movie_end   = int(args[1])
        elif len(args) == 1:
            movie_start = movie_end = int(args[0])
        else:
            movie_start, movie_end = 1, 2

    return movie_start, movie_end, series_start, series_end


# ── Proxies ───────────────────────────────────────────────────────────────────
PROXY_USER = "dxicdysy"
PROXY_PASS = "yndikr9coeto"

PROXY_LIST = [
    ("31.59.20.176",    6754),
    ("31.56.127.193",   7684),
    ("45.38.107.97",    6014),
    ("198.105.121.200", 6462),
    ("64.137.96.74",    6641),
    ("198.23.243.226",  6361),
    ("38.154.185.97",   6370),
    ("84.247.60.125",   6095),
    ("142.111.67.146",  5611),
    ("191.96.254.138",  6185),
]

def get_random_proxy() -> dict:
    host, port = random.choice(PROXY_LIST)
    url = f"http://{PROXY_USER}:{PROXY_PASS}@{host}:{port}"
    return {"http": url, "https": url}


# ── Data model ────────────────────────────────────────────────────────────────
@dataclass
class Entry:
    title:     str
    url:       str
    type:      str
    serial_no: int = 0
    imdb_id:   str = ""
    tmdb_id:   str = ""
    server1:   str = ""
    server2:   str = ""
    server3:   str = ""
    server4:   str = ""

def entry_to_record(e: Entry) -> dict:
    return {
        "serial_no": e.serial_no,
        "title":     e.title,
        "url":       e.url,
        "type":      e.type,
        "imdb_id":   e.imdb_id,
        "tmdb_id":   e.tmdb_id,
        "server1":   e.server1,
        "server2":   e.server2,
        "server3":   e.server3,
        "server4":   e.server4,
    }


# ── Page Tracking ─────────────────────────────────────────────────────────────
def init_tracker_file(filepath: str):
    """Creates the tracking file if it doesn't already exist."""
    if not os.path.exists(filepath):
        open(filepath, 'a', encoding="utf-8").close()
        log.info(f"Created new tracking file: {filepath}")

def get_processed_pages(filepath: str) -> set:
    """Reads the processed pages file and returns a set of completed page numbers."""
    processed = set()
    if os.path.exists(filepath):
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line.isdigit():
                    processed.add(int(line))
    return processed

def mark_page_processed(page: int, filepath: str) -> None:
    """Appends a successfully scraped page number to the tracker file."""
    with open(filepath, "a", encoding="utf-8") as f:
        f.write(f"{page}\n")


# ── Helpers ───────────────────────────────────────────────────────────────────
def http_get(url: str) -> str | None:
    for attempt in range(1, MAX_RETRIES + 1):
        proxy = get_random_proxy()
        host  = proxy["http"].split("@")[1]
        try:
            S = requests.Session(impersonate="chrome124")
            S.proxies.update(proxy)
            r = S.get(url, timeout=TIMEOUT)
            r.raise_for_status()
            log.info(f"  ✓ via {host}")
            return r.text
        except Exception as e:
            log.warning(f"GET {url} attempt {attempt}/{MAX_RETRIES} via {host} failed: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(2 ** attempt)
    return None

def parse_listing_page(html: str, force_type: str | None = None) -> list[Entry]:
    """
    force_type: if 'series' or 'movie', override URL-based detection.
    Useful for /new-tvseries pages where all items are series.
    """
    soup    = BeautifulSoup(html, "html.parser")
    entries = []
    for item in soup.select("div.item"):
        anchor = item.select_one("div.imagecover > a")
        if not anchor:
            continue
        href = anchor.get("href", "").strip()
        if not href:
            continue
        url   = urljoin(BASE_URL, href)
        title = anchor.get("title", "").strip() or anchor.get_text(strip=True)
        if force_type:
            kind = force_type
        else:
            kind = "series" if "/watch-tvseries-" in url else "movie"
        entries.append(Entry(title=title, url=url, type=kind))
    return entries

def collect_urls(start: int, end: int, listing_path: str,
                 tracker_file: str, force_type: str | None = None) -> list[Entry]:
    """Generic page collector. listing_path e.g. '/new-movies' or '/new-tvseries'."""
    all_entries: list[Entry] = []
    processed_pages = get_processed_pages(tracker_file)

    for page in range(start, end + 1):
        if page in processed_pages:
            log.info(f"Skipping page {page}/{end} — already processed.")
            continue

        url  = f"{BASE_URL}{listing_path}?page={page}"
        log.info(f"Page {page}/{end} → {url}")

        html = http_get(url)
        if html is None:
            log.warning(f"Skipping page {page} due to fetch failure.")
            continue

        found = parse_listing_page(html, force_type=force_type)
        log.info(f"Page {page}: {len(found)} entries collected.")
        all_entries.extend(found)

        mark_page_processed(page, tracker_file)

        if page < end:
            time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

    log.info(f"Total URLs collected this run: {len(all_entries)}")
    return all_entries


# ── JSON file helpers ─────────────────────────────────────────────────────────
def get_json_filename(index: int, prefix: str) -> str:
    """
    prefix='movies' → movies.json, movies2.json, movies3.json …
    prefix='series' → series.json, series2.json, series3.json …
    """
    return f"{prefix}.json" if index == 1 else f"{prefix}{index}.json"

def load_existing(filename: str) -> list[dict]:
    if os.path.exists(filename):
        try:
            with open(filename, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return []

def load_global_state(prefix: str) -> tuple[set, int]:
    """Scan all existing JSON files for a prefix and return (seen_urls, max_serial)."""
    seen_urls  = set()
    max_serial = 0
    file_index = 1

    while True:
        fname = get_json_filename(file_index, prefix)
        if not os.path.exists(fname):
            break
        try:
            with open(fname, "r", encoding="utf-8") as f:
                records = json.load(f)
                for rec in records:
                    if "url" in rec:
                        seen_urls.add(rec["url"])
                    if "serial_no" in rec:
                        max_serial = max(max_serial, rec.get("serial_no", 0))
        except Exception as e:
            log.warning(f"Could not parse {fname} for deduplication state: {e}")
        file_index += 1

    return seen_urls, max_serial

def save_with_split(new_entries: list[Entry], prefix: str) -> None:
    """Append records to JSON files, splitting at MAX_RECORDS per file."""
    new_records = [entry_to_record(e) for e in new_entries]

    # Find the latest active file that isn't full yet
    file_index = 1
    while True:
        fname = get_json_filename(file_index, prefix)
        if not os.path.exists(fname):
            break
        current_records = load_existing(fname)
        if len(current_records) < MAX_RECORDS:
            break
        file_index += 1

    current_records = load_existing(get_json_filename(file_index, prefix))

    for record in new_records:
        if len(current_records) >= MAX_RECORDS:
            fname = get_json_filename(file_index, prefix)
            with open(fname, "w", encoding="utf-8") as f:
                json.dump(current_records, f, indent=2, ensure_ascii=False)
            log.info(f"File {fname} reached {MAX_RECORDS} limit — starting {get_json_filename(file_index + 1, prefix)}")
            file_index    += 1
            current_records = []

        current_records.append(record)

    if current_records:
        fname = get_json_filename(file_index, prefix)
        with open(fname, "w", encoding="utf-8") as f:
            json.dump(current_records, f, indent=2, ensure_ascii=False)
        log.info(f"Saved to {fname} ({len(current_records)} total records in this file)")


# ── Per-type runner ───────────────────────────────────────────────────────────
def run_collection(start: int, end: int, listing_path: str,
                   tracker_file: str, prefix: str,
                   force_type: str | None = None) -> None:
    label = prefix.capitalize()
    log.info(f"=== Collecting {label} URLs (pages {start}–{end}) ===")

    entries = collect_urls(start, end, listing_path, tracker_file, force_type)

    if not entries:
        log.info(f"No new {label} URLs collected.")
        return

    log.info(f"Checking {label} entries for duplicates…")
    seen_urls, current_max_serial = load_global_state(prefix)

    unique_entries = []
    for e in entries:
        if e.url not in seen_urls:
            current_max_serial += 1
            e.serial_no = current_max_serial
            unique_entries.append(e)
            seen_urls.add(e.url)

    if not unique_entries:
        log.info(f"All {label} URLs already exist. Nothing new to add.")
        return

    save_with_split(unique_entries, prefix)

    movies_count = sum(1 for e in unique_entries if e.type == "movie")
    series_count = sum(1 for e in unique_entries if e.type == "series")
    print(f"\n{'='*65}")
    print(f"{label} done!  {len(unique_entries)} NEW unique entries  |  "
          f"{movies_count} movies  |  {series_count} series")
    print(f"{'='*65}")
    print(f"{'S.No':<6} {'Type':<8} {'Title':<35} URL")
    print("-" * 65)
    for e in unique_entries:
        print(f"{e.serial_no:<6} {e.type:<8} {e.title[:34]:<35} {e.url}")


# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    movie_start, movie_end, series_start, series_end = parse_args()

    # Initialise tracker files
    init_tracker_file(MOVIES_PROCESSED_PAGES_FILE)
    init_tracker_file(SERIES_PROCESSED_PAGES_FILE)

    # ── Movies ────────────────────────────────────────────────────────────────
    if movie_start is not None and movie_end is not None:
        run_collection(
            start        = movie_start,
            end          = movie_end,
            listing_path = "/new-movies",
            tracker_file = MOVIES_PROCESSED_PAGES_FILE,
            prefix       = "movies",
            force_type   = "movie",
        )
    else:
        log.info("Movie collection skipped (no movie page range provided).")

    # ── Series ────────────────────────────────────────────────────────────────
    if series_start is not None and series_end is not None:
        run_collection(
            start        = series_start,
            end          = series_end,
            listing_path = "/new-tv-series",
            tracker_file = SERIES_PROCESSED_PAGES_FILE,
            prefix       = "series",
            force_type   = "series",
        )
    else:
        log.info("Series collection skipped (no --series page range provided).")
