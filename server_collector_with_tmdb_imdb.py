import re
import sys
import json
import os
import time
import random
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from itertools import cycle

# ── API Configurations ───────────────────────────────────────────
TMDB_API_KEY = "6fad3f86b8452ee232deb7977d7dcf58"

# File paths
TARGET_JSON = os.getenv("TARGET_JSON", "movies.json")
PROCESSED_FILE = "list_of_already_processed_urls.txt"
ERROR_FILE = "list_of_facing_error.txt"

# Detect if we are processing a series file
IS_SERIES = "series" in TARGET_JSON.lower()

# ── URL Limit ────────────────────────────────────────────────────
def parse_url_limit():
    raw = os.getenv("URL_LIMIT", "100").strip().lower()
    if raw == "full":
        return None
    try:
        val = int(raw)
        return val if val > 0 else 100
    except ValueError:
        print(f"[!] Invalid URL_LIMIT value '{raw}'. Defaulting to 100.")
        return 100

URL_LIMIT = parse_url_limit()

# ── Proxies ──────────────────────────────────────────────────────
PROXY_USER = "dxicdysy"
PROXY_PASS = "yndikr9coeto"

PROXY_LIST_RAW = [
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

formatted_proxies = [
    f"http://{PROXY_USER}:{PROXY_PASS}@{ip}:{port}" for ip, port in PROXY_LIST_RAW
]
proxy_pool = cycle(formatted_proxies)

# ── Single Session ───────────────────────────────────────────────
S = requests.Session()
S.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "X-Requested-With": "XMLHttpRequest"
})

def set_new_proxy():
    new_proxy = next(proxy_pool)
    S.proxies.update({"http": new_proxy, "https": new_proxy})
    safe_display = new_proxy.split('@')[-1]
    print(f"  [*] Rotating IP... Now using proxy: {safe_display}")

set_new_proxy()

# ── File I/O Helpers ─────────────────────────────────────────────
def init_files():
    if not os.path.exists(PROCESSED_FILE):
        open(PROCESSED_FILE, "w", encoding="utf-8").close()
    if not os.path.exists(ERROR_FILE):
        open(ERROR_FILE, "w", encoding="utf-8").close()
    with open(PROCESSED_FILE, "r", encoding="utf-8") as f:
        return set(line.strip() for line in f if line.strip())

def log_processed(url):
    with open(PROCESSED_FILE, "a", encoding="utf-8") as f:
        f.write(url + "\n")

def log_error(url, error_msg):
    with open(ERROR_FILE, "a", encoding="utf-8") as f:
        f.write(f"{url} | ERROR: {error_msg}\n")

# ── TMDB Lookup ──────────────────────────────────────────────────
def get_tmdb_id_from_imdb(imdb_id):
    if not TMDB_API_KEY:
        return ""
    url = f"https://api.themoviedb.org/3/find/{imdb_id}?api_key={TMDB_API_KEY}&external_source=imdb_id"
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        if data.get("movie_results"):
            return str(data["movie_results"][0]["id"])
        elif data.get("tv_results"):
            return str(data["tv_results"][0]["id"])
    except Exception as e:
        print(f"  [!] Failed to fetch TMDb ID for {imdb_id}: {e}")
    return ""

# ── HTML Helpers ─────────────────────────────────────────────────
def base(url):
    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}"

def csrf(html):
    m = re.search(r'<meta name="csrf-token" content="([^"]+)"', html)
    return m.group(1) if m else ""

def spans(html):
    soup = BeautifulSoup(html, "html.parser")
    return [
        (s.get_text(strip=True), s["data"])
        for s in soup.find_all("span", attrs={"data": True})
        if len(s.get("data", "")) > 10
    ]

def iframe(html):
    m = re.search(r'<iframe[^>]+src="([^"]+)"', html)
    return m.group(1) if m else ""

def post(url, data, ref):
    r = S.post(
        url, data=data,
        headers={"Referer": ref, "Content-Type": "application/x-www-form-urlencoded"},
        timeout=15
    )
    r.raise_for_status()
    return r.text

# ── Fetch servers for a single episode ID ────────────────────────
def fetch_servers_for_episode(root, token, ep_id, target_url, max_retries=3):
    """
    POSTs to /ajaxtv with the given ep_id and resolves all embed iframes.
    Returns a list of embed URLs (could be empty on failure).
    """
    for attempt in range(max_retries):
        try:
            time.sleep(random.uniform(1.5, 3.0))
            server_html = post(
                f"{root}/ajaxtv",
                {"idepisode": ep_id, "_token": token},
                target_url
            )
            servers = spans(server_html)
            embeds = []
            for label, data in servers:
                embed_html = post(
                    f"{root}/ajax",
                    {"m4u": data, "_token": token},
                    target_url
                )
                url = iframe(embed_html)
                if url:
                    embeds.append(url)
            return embeds

        except requests.exceptions.RequestException as e:
            print(f"    [!] Episode {ep_id} attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                set_new_proxy()
            else:
                print(f"    [!] Giving up on episode {ep_id}.")
                return []
        except Exception as e:
            print(f"    [!] Unexpected error on episode {ep_id}: {e}")
            return []

# ── Extract all episode IDs from series page ─────────────────────
def get_all_episode_ids(html):
    """
    Scrapes all episode IDs from the series page HTML.
    The site renders them as idepisode="XXXXX" attributes.
    Returns an ordered list of unique episode IDs.
    """
    # Collect all matches preserving order, deduplicate keeping first occurrence
    seen = set()
    ordered = []
    for ep_id in re.findall(r'idepisode=["\'](\w+)["\']', html):
        if ep_id not in seen:
            seen.add(ep_id)
            ordered.append(ep_id)
    return ordered

# ── Main extraction: movies ───────────────────────────────────────
def extract_movie_servers(target_url, max_retries=3):
    for attempt in range(max_retries):
        try:
            time.sleep(random.uniform(2.5, 5.0))
            html = S.get(target_url, timeout=15).text
            token = csrf(html)
            root = base(target_url)
            servers = spans(html)
            embeds = []
            for label, data in servers:
                embed_html = post(f"{root}/ajax", {"m4u": data, "_token": token}, target_url)
                url = iframe(embed_html)
                if url:
                    embeds.append(url)
            return embeds

        except requests.exceptions.RequestException as e:
            print(f"  [!] Attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                set_new_proxy()
            else:
                log_error(target_url, f"Failed after {max_retries} retries: {str(e)}")
                return []
        except Exception as e:
            log_error(target_url, f"Unexpected error: {str(e)}")
            return []

# ── Main extraction: series (all episodes) ───────────────────────
def extract_series_all_episodes(target_url, max_retries=3):
    """
    Fetches the series page, collects ALL episode IDs, then loops through
    every episode fetching its servers.

    Returns a dict:
    {
        "total_episodes": 12,
        "episodes": {
            "1": ["url1", "url2", ...],
            "2": ["url1"],
            ...
        },
        "imdb_id": "tt1234567"   # first one found across all embeds
    }
    """
    for attempt in range(max_retries):
        try:
            time.sleep(random.uniform(2.5, 5.0))
            html = S.get(target_url, timeout=15).text
            token = csrf(html)
            root = base(target_url)
            break  # page loaded fine

        except requests.exceptions.RequestException as e:
            print(f"  [!] Page load attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                set_new_proxy()
            else:
                log_error(target_url, f"Series page load failed after {max_retries} retries: {str(e)}")
                return None
        except Exception as e:
            log_error(target_url, f"Unexpected error loading series page: {str(e)}")
            return None

    ep_ids = get_all_episode_ids(html)
    if not ep_ids:
        log_error(target_url, "No episode IDs found on series page.")
        return None

    print(f"  [*] Found {len(ep_ids)} episodes to process.")

    result = {
        "total_episodes": len(ep_ids),
        "episodes": {},
        "imdb_id": ""
    }

    for ep_num, ep_id in enumerate(ep_ids, start=1):
        print(f"    -> Episode {ep_num}/{len(ep_ids)} (id={ep_id})")
        embeds = fetch_servers_for_episode(root, token, ep_id, target_url)

        if embeds:
            result["episodes"][str(ep_num)] = embeds
            # Grab first IMDb ID found across any embed URL
            if not result["imdb_id"]:
                for embed_url in embeds:
                    match = re.search(r'(tt\d{7,10})', embed_url)
                    if match:
                        result["imdb_id"] = match.group(1)
                        break
            print(f"       Got {len(embeds)} server(s).")
        else:
            result["episodes"][str(ep_num)] = []
            print(f"       No servers found for episode {ep_num}.")

        # Small pause between episodes to be polite to the server
        time.sleep(random.uniform(1.0, 2.0))

    return result

# ── Apply series result to JSON item ─────────────────────────────
def apply_series_result(item, series_data):
    """
    Writes the flat key structure onto the item dict:
      total_episodes, episode-1-server1, episode-1-server2, ...
    Cleans up old movie-style server keys if present.
    """
    # Remove old movie-style keys if they exist
    for k in ["server1", "server2", "server3", "server4"]:
        item.pop(k, None)

    item["total_episodes"] = series_data["total_episodes"]

    for ep_num_str, embeds in series_data["episodes"].items():
        for server_idx, embed_url in enumerate(embeds, start=1):
            key = f"episode-{ep_num_str}-server{server_idx}"
            item[key] = embed_url

# ── Is series item already processed? ────────────────────────────
def series_already_done(item):
    """Returns True if the item already has at least episode-1-server1 filled."""
    return bool(item.get("episode-1-server1", ""))

# ── Main ─────────────────────────────────────────────────────────
def main():
    limit_label = "full (no limit)" if URL_LIMIT is None else str(URL_LIMIT)
    print(f"[*] Starting job for file : {TARGET_JSON}")
    print(f"[*] Mode                  : {'SERIES' if IS_SERIES else 'MOVIES'}")
    print(f"[*] URL limit             : {limit_label}")

    if not os.path.exists(TARGET_JSON):
        print(f"[!] Error: {TARGET_JSON} not found in repository.")
        sys.exit(1)

    processed_urls = init_files()

    with open(TARGET_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)

    print(f"[*] Total records in {TARGET_JSON}: {len(data)}")

    # ── Build queue ───────────────────────────────────────────────
    if IS_SERIES:
        queue = [
            item for item in data
            if item.get("url")
            and not series_already_done(item)
            and item["url"] not in processed_urls
        ]
    else:
        queue = [
            item for item in data
            if item.get("url")
            and not item.get("server1")
            and item["url"] not in processed_urls
        ]

    if URL_LIMIT is not None:
        queue = queue[:URL_LIMIT]

    print(f"[*] Items queued for this run: {len(queue)}")

    try:
        for item in queue:
            target_url = item["url"]
            print(f"\n-> Processing: {item.get('title', 'Unknown Title')}")
            print(f"   URL: {target_url}")

            try:
                # ── SERIES PATH ───────────────────────────────────
                if IS_SERIES:
                    series_data = extract_series_all_episodes(target_url)

                    if not series_data:
                        log_error(target_url, "Series extraction returned nothing.")
                        continue

                    apply_series_result(item, series_data)

                    # IMDb / TMDb
                    found_imdb_id = series_data.get("imdb_id", "")
                    if found_imdb_id:
                        item["imdb_id"] = found_imdb_id
                        print(f"   Found IMDb ID : {found_imdb_id}")
                        tmdb_id = get_tmdb_id_from_imdb(found_imdb_id)
                        if tmdb_id:
                            item["tmdb_id"] = tmdb_id
                            print(f"   Fetched TMDb ID: {tmdb_id}")

                    print(f"   Done — {series_data['total_episodes']} episodes written.")

                # ── MOVIES PATH ───────────────────────────────────
                else:
                    embeds = extract_movie_servers(target_url)

                    if not embeds:
                        log_error(target_url, "No embeds found or extraction failed.")
                        continue

                    for i in range(1, 5):
                        item[f"server{i}"] = embeds[i - 1] if i <= len(embeds) else ""

                    found_imdb_id = ""
                    for url in embeds:
                        match = re.search(r'(tt\d{7,10})', url)
                        if match:
                            found_imdb_id = match.group(1)
                            break

                    if found_imdb_id:
                        item["imdb_id"] = found_imdb_id
                        print(f"   Found IMDb ID : {found_imdb_id}")
                        tmdb_id = get_tmdb_id_from_imdb(found_imdb_id)
                        if tmdb_id:
                            item["tmdb_id"] = tmdb_id
                            print(f"   Fetched TMDb ID: {tmdb_id}")

                    print(f"   Processed and mapped {len(embeds)} servers.")

                # Mark done
                processed_urls.add(target_url)
                log_processed(target_url)

            except Exception as e:
                print(f"  [!] Error processing item: {e}")
                log_error(target_url, f"Item processing crashed: {str(e)}")

    except KeyboardInterrupt:
        print("\n[!] Script manually interrupted. Saving JSON...")
    finally:
        with open(TARGET_JSON, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)
        print(f"\n[*] Saved updates to {TARGET_JSON}.")

if __name__ == "__main__":
    main()
