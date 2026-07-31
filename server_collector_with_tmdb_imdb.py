import re
import sys
import json
import os
import time
import random
import threading
import requests
import urllib3
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from collections import deque

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── API Configurations ───────────────────────────────────────────
TMDB_API_KEY = "6fad3f86b8452ee232deb7977d7dcf58"

TARGET_JSON    = os.getenv("TARGET_JSON", "movies.json")
PROCESSED_FILE = "list_of_already_processed_urls.txt"
ERROR_FILE     = "list_of_facing_error.txt"

IS_SERIES = "series" in TARGET_JSON.lower()

HTTPS_TEST_URL = "https://ww1.m4uhd.page/"

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


# ══════════════════════════════════════════════════════════════════
#  FREE PROXY SCRAPER
# ══════════════════════════════════════════════════════════════════

def scrape_free_proxies():
    proxies = []
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

    try:
        r = requests.get("https://free-proxy-list.net/", headers=headers, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")
        table = soup.find("table")
        if table:
            for row in table.find_all("tr")[1:]:
                cols = row.find_all("td")
                if len(cols) >= 7:
                    ip     = cols[0].text.strip()
                    port   = cols[1].text.strip()
                    https  = cols[6].text.strip().lower()
                    scheme = "https" if https == "yes" else "http"
                    proxies.append(f"{scheme}://{ip}:{port}")
        print(f"  [+] free-proxy-list.net  : {len(proxies)} proxies")
    except Exception as e:
        print(f"  [-] free-proxy-list.net failed : {e}")

    count_before = len(proxies)
    try:
        r = requests.get("https://www.sslproxies.org/", headers=headers, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")
        table = soup.find("table")
        if table:
            for row in table.find_all("tr")[1:]:
                cols = row.find_all("td")
                if len(cols) >= 2:
                    ip   = cols[0].text.strip()
                    port = cols[1].text.strip()
                    proxies.append(f"https://{ip}:{port}")
        print(f"  [+] sslproxies.org       : {len(proxies) - count_before} proxies")
    except Exception as e:
        print(f"  [-] sslproxies.org failed : {e}")

    count_before = len(proxies)
    try:
        url = (
            "https://api.proxyscrape.com/v2/?request=displayproxies"
            "&protocol=http&timeout=5000&country=all&ssl=all&anonymity=all"
        )
        r = requests.get(url, headers=headers, timeout=10)
        for line in r.text.strip().splitlines():
            line = line.strip()
            if ":" in line:
                proxies.append(f"http://{line}")
        print(f"  [+] proxyscrape API      : {len(proxies) - count_before} proxies")
    except Exception as e:
        print(f"  [-] proxyscrape API failed : {e}")

    count_before = len(proxies)
    try:
        url = (
            "https://proxylist.geonode.com/api/proxy-list"
            "?limit=100&page=1&sort_by=lastChecked&sort_type=desc&protocols=http,https"
        )
        r = requests.get(url, headers=headers, timeout=10)
        data = r.json()
        for entry in data.get("data", []):
            ip       = entry.get("ip", "")
            port     = entry.get("port", "")
            protocol = entry.get("protocols", ["http"])[0]
            if ip and port:
                proxies.append(f"{protocol}://{ip}:{port}")
        print(f"  [+] geonode API          : {len(proxies) - count_before} proxies")
    except Exception as e:
        print(f"  [-] geonode API failed : {e}")

    proxies = list(dict.fromkeys(proxies))
    print(f"\n  [*] Total unique proxies scraped: {len(proxies)}")
    return proxies


# ══════════════════════════════════════════════════════════════════
#  PROXY POOL
# ══════════════════════════════════════════════════════════════════

class ProxyPool:
    def __init__(self, proxies, test_url=HTTPS_TEST_URL, timeout=10):
        self._all      = proxies
        self._live     = deque()
        self._lock     = threading.Lock()
        self._test_url = test_url
        self._timeout  = timeout
        self._validate_all()

    def _validate_all(self):
        print(f"\n[*] Validating {len(self._all)} proxies against {self._test_url} ...")
        threads = []
        for p in self._all:
            t = threading.Thread(target=self._check, args=(p,))
            t.daemon = True
            threads.append(t)
            t.start()
        for t in threads:
            t.join(timeout=self._timeout + 3)
        print(f"[*] {len(self._live)} / {len(self._all)} proxies can tunnel HTTPS to target.")
        if not self._live:
            raise RuntimeError("[!] No live proxies found that support HTTPS tunneling to the target.")

    def _check(self, proxy_url):
        try:
            r = requests.get(
                self._test_url,
                proxies={"http": proxy_url, "https": proxy_url},
                timeout=self._timeout,
                verify=False,
                allow_redirects=True,
            )
            # Only accept proxies where the site returns 200
            # 403 means site blocked the proxy IP — useless for scraping
            if r.status_code == 200:
                with self._lock:
                    self._live.append(proxy_url)
                print(f"  [+] Live [200] : {proxy_url}")
        except Exception:
            pass

    def next(self):
        with self._lock:
            if not self._live:
                raise RuntimeError("[!] Proxy pool is empty.")
            proxy = self._live.popleft()
            self._live.append(proxy)
            return proxy

    def remove(self, proxy_url):
        with self._lock:
            try:
                self._live.remove(proxy_url)
                print(f"  [x] Removed dead proxy: {proxy_url} | Remaining: {len(self._live)}")
            except ValueError:
                pass

    def size(self):
        with self._lock:
            return len(self._live)

    def is_empty(self):
        with self._lock:
            return len(self._live) == 0

    def refill(self):
        print("\n[!] Pool running low — re-scraping fresh proxies...")
        new_proxies = scrape_free_proxies()
        print(f"[*] Validating {len(new_proxies)} candidates against {self._test_url} ...")
        found = []
        lock  = threading.Lock()

        def _check_new(proxy_url):
            try:
                r = requests.get(
                    self._test_url,
                    proxies={"http": proxy_url, "https": proxy_url},
                    timeout=self._timeout,
                    verify=False,
                    allow_redirects=True,
                )
                if r.status_code == 200:
                    with lock:
                        found.append(proxy_url)
            except Exception:
                pass

        threads = [threading.Thread(target=_check_new, args=(p,)) for p in new_proxies]
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join(timeout=self._timeout + 3)

        added = 0
        with self._lock:
            existing = set(self._live)
            for p in found:
                if p not in existing:
                    self._live.append(p)
                    added += 1

        print(f"[*] Refill complete. Added {added} new proxies. Pool size: {self.size()}")


# ══════════════════════════════════════════════════════════════════
#  SESSION + PROXY HELPERS
# ══════════════════════════════════════════════════════════════════

S = requests.Session()
S.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "X-Requested-With": "XMLHttpRequest"
})

_current_proxy = {"url": None}
pool = None


def set_new_proxy():
    if pool.size() < 5:
        pool.refill()
    _current_proxy["url"] = pool.next()
    S.proxies.update({
        "http":  _current_proxy["url"],
        "https": _current_proxy["url"]
    })
    print(f"  [*] Rotating IP... Now using proxy: {_current_proxy['url']}")


def report_bad_proxy():
    if _current_proxy["url"]:
        pool.remove(_current_proxy["url"])
    if pool.is_empty():
        pool.refill()
    set_new_proxy()


# ══════════════════════════════════════════════════════════════════
#  FILE I/O HELPERS
# ══════════════════════════════════════════════════════════════════

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


# ══════════════════════════════════════════════════════════════════
#  TMDB LOOKUP
# ══════════════════════════════════════════════════════════════════

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


# ══════════════════════════════════════════════════════════════════
#  HTML HELPERS — with debug prints to expose silent failures
# ══════════════════════════════════════════════════════════════════

def base(url):
    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}"

def csrf(html):
    m = re.search(r'<meta name="csrf-token" content="([^"]+)"', html)
    token = m.group(1) if m else ""
    if not token:
        print("  [DEBUG] WARNING: csrf-token NOT found in page HTML")
    else:
        print(f"  [DEBUG] csrf-token found: {token[:20]}...")
    return token

def spans(html):
    soup = BeautifulSoup(html, "html.parser")
    all_spans_with_data = soup.find_all("span", attrs={"data": True})
    valid = [
        (s.get_text(strip=True), s["data"])
        for s in all_spans_with_data
        if len(s.get("data", "")) > 10
    ]
    print(f"  [DEBUG] spans() — total <span data=...> found: {len(all_spans_with_data)}, valid (len>10): {len(valid)}")
    if not valid:
        # Show a snippet of the raw HTML to see what the page actually looks like
        snippet = html[:2000].replace("\n", " ")
        print(f"  [DEBUG] Page HTML snippet (first 2000 chars):\n{snippet}\n")
    return valid

def iframe(html):
    m = re.search(r'<iframe[^>]+src="([^"]+)"', html)
    url = m.group(1) if m else ""
    if not url:
        print(f"  [DEBUG] iframe() — no <iframe src=...> found. Response snippet: {html[:500]}")
    else:
        print(f"  [DEBUG] iframe() — found embed URL: {url[:80]}")
    return url

def post(url, data, ref):
    r = S.post(
        url, data=data,
        headers={"Referer": ref, "Content-Type": "application/x-www-form-urlencoded"},
        timeout=15,
        verify=False
    )
    r.raise_for_status()
    return r.text


# ══════════════════════════════════════════════════════════════════
#  EPISODE HELPERS
# ══════════════════════════════════════════════════════════════════

def fetch_servers_for_episode(root, token, ep_id, target_url, max_retries=3):
    for attempt in range(max_retries):
        try:
            time.sleep(random.uniform(1.5, 3.0))
            server_html = post(
                f"{root}/ajaxtv",
                {"idepisode": ep_id, "_token": token},
                target_url
            )
            servers = spans(server_html)
            embeds  = []
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
                report_bad_proxy()
            else:
                print(f"    [!] Giving up on episode {ep_id}.")
                return []
        except Exception as e:
            print(f"    [!] Unexpected error on episode {ep_id}: {e}")
            return []


def get_all_episode_ids(html):
    seen    = set()
    ordered = []
    for ep_id in re.findall(r'idepisode=["\'](\w+)["\']', html):
        if ep_id not in seen:
            seen.add(ep_id)
            ordered.append(ep_id)
    return ordered


# ══════════════════════════════════════════════════════════════════
#  MAIN EXTRACTION: MOVIES
# ══════════════════════════════════════════════════════════════════

def extract_movie_servers(target_url, max_retries=3):
    for attempt in range(max_retries):
        try:
            time.sleep(random.uniform(2.5, 5.0))
            print(f"  [DEBUG] Fetching page: {target_url}")
            r = S.get(target_url, timeout=15, verify=False)
            print(f"  [DEBUG] Page status code: {r.status_code}")
            html    = r.text
            token   = csrf(html)
            root    = base(target_url)
            servers = spans(html)

            if not servers:
                print(f"  [DEBUG] No servers found in page — skipping AJAX calls")
                log_error(target_url, "spans() returned empty — page structure may have changed")
                return []

            embeds = []
            for label, data in servers:
                print(f"  [DEBUG] POSTing to /ajax for server label='{label}' data='{data[:30]}...'")
                try:
                    embed_html = post(f"{root}/ajax", {"m4u": data, "_token": token}, target_url)
                    url = iframe(embed_html)
                    if url:
                        embeds.append(url)
                except Exception as e:
                    print(f"  [DEBUG] /ajax POST failed for label='{label}': {e}")

            return embeds

        except requests.exceptions.RequestException as e:
            print(f"  [!] Attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                report_bad_proxy()
            else:
                log_error(target_url, f"Failed after {max_retries} retries: {str(e)}")
                return []
        except Exception as e:
            print(f"  [!] Unexpected error: {e}")
            log_error(target_url, f"Unexpected error: {str(e)}")
            return []


# ══════════════════════════════════════════════════════════════════
#  MAIN EXTRACTION: SERIES
# ══════════════════════════════════════════════════════════════════

def extract_series_all_episodes(target_url, max_retries=3):
    for attempt in range(max_retries):
        try:
            time.sleep(random.uniform(2.5, 5.0))
            print(f"  [DEBUG] Fetching series page: {target_url}")
            r = S.get(target_url, timeout=15, verify=False)
            print(f"  [DEBUG] Page status code: {r.status_code}")
            html  = r.text
            token = csrf(html)
            root  = base(target_url)
            break

        except requests.exceptions.RequestException as e:
            print(f"  [!] Page load attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                report_bad_proxy()
            else:
                log_error(target_url, f"Series page load failed after {max_retries} retries: {str(e)}")
                return None
        except Exception as e:
            log_error(target_url, f"Unexpected error loading series page: {str(e)}")
            return None

    ep_ids = get_all_episode_ids(html)
    print(f"  [DEBUG] Episode IDs found: {ep_ids[:5]}{'...' if len(ep_ids) > 5 else ''}")
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

        time.sleep(random.uniform(1.0, 2.0))

    return result


# ══════════════════════════════════════════════════════════════════
#  APPLY SERIES RESULT
# ══════════════════════════════════════════════════════════════════

def apply_series_result(item, series_data):
    for k in ["server1", "server2", "server3", "server4"]:
        item.pop(k, None)
    item["total_episodes"] = series_data["total_episodes"]
    for ep_num_str, embeds in series_data["episodes"].items():
        for server_idx, embed_url in enumerate(embeds, start=1):
            key = f"episode-{ep_num_str}-server{server_idx}"
            item[key] = embed_url

def series_already_done(item):
    return bool(item.get("episode-1-server1", ""))


# ══════════════════════════════════════════════════════════════════
#  DETECT TARGET HOST FROM JSON
# ══════════════════════════════════════════════════════════════════

def detect_target_host(json_path):
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        for item in data:
            url = item.get("url", "")
            if url.startswith("https://"):
                p = urlparse(url)
                return f"{p.scheme}://{p.netloc}/"
    except Exception:
        pass
    return HTTPS_TEST_URL


# ══════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════

def main():
    global pool

    limit_label = "full (no limit)" if URL_LIMIT is None else str(URL_LIMIT)
    print(f"[*] Starting job for file : {TARGET_JSON}")
    print(f"[*] Mode                  : {'SERIES' if IS_SERIES else 'MOVIES'}")
    print(f"[*] URL limit             : {limit_label}")

    if not os.path.exists(TARGET_JSON):
        print(f"[!] Error: {TARGET_JSON} not found in repository.")
        sys.exit(1)

    test_url = detect_target_host(TARGET_JSON)
    print(f"[*] Proxy test URL        : {test_url}")

    print("\n[*] Scraping free proxies...")
    raw_proxies = scrape_free_proxies()
    pool = ProxyPool(raw_proxies, test_url=test_url)
    set_new_proxy()

    processed_urls = init_files()

    with open(TARGET_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)

    print(f"\n[*] Total records in {TARGET_JSON}: {len(data)}")

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
            if pool.size() < 5:
                pool.refill()

            target_url = item["url"]
            print(f"\n-> Processing: {item.get('title', 'Unknown Title')}")
            print(f"   URL: {target_url}")

            try:
                if IS_SERIES:
                    series_data = extract_series_all_episodes(target_url)

                    if not series_data:
                        log_error(target_url, "Series extraction returned nothing.")
                        continue

                    apply_series_result(item, series_data)

                    found_imdb_id = series_data.get("imdb_id", "")
                    if found_imdb_id:
                        item["imdb_id"] = found_imdb_id
                        print(f"   Found IMDb ID : {found_imdb_id}")
                        tmdb_id = get_tmdb_id_from_imdb(found_imdb_id)
                        if tmdb_id:
                            item["tmdb_id"] = tmdb_id
                            print(f"   Fetched TMDb ID: {tmdb_id}")

                    print(f"   Done — {series_data['total_episodes']} episodes written.")

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
