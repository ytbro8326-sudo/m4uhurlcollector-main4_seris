"""
server_collector_with_tmdb_imdb.py
====================================
Scraper — rotates across a fixed pool of 10 Webshare proxies.

Architecture
────────────
• 10 dedicated Webshare proxies in a round-robin pool (no free-proxy fallback).
• On 419/429: rotate to the next proxy and back-off before retrying.
• Sequential episodes (EPISODE_WORKERS=1) with a random delay between each
  to avoid rate-bans on /ajaxtv.
• Token-bucket rate limiter keeps total req/s gentle.
• Per-thread requests.Session — no shared-lock bottleneck.
• Thread-safe JSON write at the end.
"""

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
from concurrent.futures import ThreadPoolExecutor, as_completed

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── API / file config ────────────────────────────────────────────────────────
TMDB_API_KEY   = "6fad3f86b8452ee232deb7977d7dcf58"
TARGET_JSON    = os.getenv("TARGET_JSON", "movies.json")
PROCESSED_FILE = "list_of_already_processed_urls.txt"
ERROR_FILE     = "list_of_facing_error.txt"
IS_SERIES      = "series" in TARGET_JSON.lower()
HTTPS_TEST_URL = "https://ww1.m4uhd.page/"

# ── Concurrency tuning ───────────────────────────────────────────────────────
# Reduced to avoid 419/429 "Too Many Requests" from the target site.
# The site's /ajaxtv endpoint is sensitive; slow and steady wins.
RATE_LIMIT_RPS  = 60 / 60           # tokens per second  ≈ 1 req/s
ITEM_WORKERS    = 1                  # one title at a time (site bans bursts)
SERVER_WORKERS  = 2                  # parallel /ajax POSTs inside one episode

# Minimum pause between consecutive episode POSTs (seconds)
EPISODE_DELAY_MIN = 1.5
EPISODE_DELAY_MAX = 3.0

# ── Webshare rotating proxy pool ─────────────────────────────────────────────
# Primary: 10 Webshare proxies rotated round-robin.
# Fallback: free proxy pool — activated only when ALL 10 Webshare IPs are
#           exhausted (each has failed at least once in the current run).
_WS_USER = "mjyfvhwg"
_WS_PASS = "avgrq102gw1y"
_WS_PROXIES_RAW = [
    ("31.59.20.176",    "6754"),
    ("31.56.127.193",   "7684"),
    ("45.38.107.97",    "6014"),
    ("198.105.121.200", "6462"),
    ("64.137.96.74",    "6641"),
    ("198.23.243.226",  "6361"),
    ("38.154.185.97",   "6370"),
    ("84.247.60.125",   "6095"),
    ("142.111.67.146",  "5611"),
    ("191.96.254.138",  "6185"),
]
WEBSHARE_POOL = [
    f"http://{_WS_USER}:{_WS_PASS}@{ip}:{port}/"
    for ip, port in _WS_PROXIES_RAW
]

_proxy_lock     = threading.Lock()
_proxy_index    = 0        # round-robin cursor into WEBSHARE_POOL
_ws_failed      = set()    # URLs that have failed at least once this run
_using_webshare = True     # False once all 10 WS proxies are dead
_free_pool      = None     # ProxyPool — built only on fallback


def _next_proxy() -> str:
    """Return the current Webshare proxy URL (round-robin)."""
    global _proxy_index
    with _proxy_lock:
        if _using_webshare:
            url = WEBSHARE_POOL[_proxy_index % len(WEBSHARE_POOL)]
            _proxy_index += 1
            return url
        # On free-pool mode, let _apply_proxy_to_session handle it
        return _free_pool.next() if _free_pool else WEBSHARE_POOL[0]


def _rotate_proxy() -> str:
    """Mark current proxy as failed, advance to next.
    Triggers free-proxy fallback once ALL 10 Webshare IPs have failed."""
    global _proxy_index, _using_webshare

    with _proxy_lock:
        if not _using_webshare:
            # Already on free pool — let caller handle via _free_pool
            return None
        failed_url = WEBSHARE_POOL[(_proxy_index - 1) % len(WEBSHARE_POOL)]
        _ws_failed.add(failed_url)
        all_dead = len(_ws_failed) >= len(WEBSHARE_POOL)
        _proxy_index += 1
        next_url = WEBSHARE_POOL[_proxy_index % len(WEBSHARE_POOL)]

    if all_dead:
        with _proxy_lock:
            _using_webshare = False
        print("\n  [proxy] All 10 Webshare proxies exhausted — switching to free proxy pool.")
        _init_free_pool()
        with _proxy_lock:
            return _free_pool.next() if _free_pool else next_url

    print(f"  [proxy] Rotated Webshare → {next_url.split('@')[1]} ({len(_ws_failed)}/10 marked bad)")
    return next_url



def parse_url_limit():
    raw = os.getenv("URL_LIMIT", "100").strip().lower()
    if raw == "full":
        return None
    try:
        val = int(raw)
        return val if val > 0 else 100
    except ValueError:
        print(f"[!] Invalid URL_LIMIT '{raw}'. Defaulting to 100.")
        return 100

URL_LIMIT = parse_url_limit()


# ══════════════════════════════════════════════════════════════════════════════
#  TOKEN-BUCKET RATE LIMITER
#  Keeps the total request rate across ALL threads ≤ RATE_LIMIT_RPS.
# ══════════════════════════════════════════════════════════════════════════════

class TokenBucket:
    """Thread-safe token bucket. Each call to acquire() blocks until a token
    is available, then consumes one."""

    def __init__(self, rate, capacity=None):
        self._rate     = rate                    # tokens added per second
        self._capacity = capacity or rate * 4    # burst capacity
        self._tokens   = self._capacity
        self._lock     = threading.Lock()
        self._last     = time.monotonic()

    def acquire(self, tokens=1):
        while True:
            with self._lock:
                now   = time.monotonic()
                delta = now - self._last
                self._last   = now
                self._tokens = min(self._capacity, self._tokens + delta * self._rate)
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return
                wait = (tokens - self._tokens) / self._rate
            time.sleep(wait)


_bucket = TokenBucket(rate=RATE_LIMIT_RPS, capacity=20)   # burst = 20 tokens


def rate_limited_request(session, method, url, **kwargs):
    """Drop-in replacement for session.get / session.post that acquires a
    rate-limit token before sending the request."""
    _bucket.acquire()
    fn = session.get if method == "get" else session.post
    return fn(url, **kwargs)


# ══════════════════════════════════════════════════════════════════════════════
#  PER-THREAD SESSION FACTORY
#  Each worker gets its own session so there's no shared-lock bottleneck on
#  S.proxies or S.headers.
# ══════════════════════════════════════════════════════════════════════════════

_thread_local = threading.local()


def get_session() -> requests.Session:
    """Return a requests.Session scoped to the current thread."""
    if not hasattr(_thread_local, "session"):
        s = requests.Session()
        s.headers.update({
            "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "X-Requested-With": "XMLHttpRequest",
        })
        _thread_local.session = s
    _apply_proxy_to_session(_thread_local.session)
    return _thread_local.session


def _apply_proxy_to_session(s: requests.Session, proxy_url: str = None):
    """Point session at the right proxy: Webshare (round-robin) or free pool."""
    if proxy_url:
        url = proxy_url
    elif _using_webshare:
        url = _next_proxy()
    else:
        url = _free_pool.next() if _free_pool else _next_proxy()
    s.proxies.update({"http": url, "https": url})



# ══════════════════════════════════════════════════════════════════════════════
#  FREE PROXY SCRAPER  (used only when all 10 Webshare IPs are dead)
# ══════════════════════════════════════════════════════════════════════════════

def scrape_free_proxies():
    proxies = []
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

    def _fetch_source(name, fn):
        try:
            before = len(proxies)
            fn(proxies, headers)
            print(f"  [+] {name:<28}: {len(proxies) - before} proxies")
        except Exception as e:
            print(f"  [-] {name} failed: {e}")

    def _src_fpl(acc, h):
        from bs4 import BeautifulSoup
        r = requests.get("https://free-proxy-list.net/", headers=h, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")
        table = soup.find("table")
        if table:
            for row in table.find_all("tr")[1:]:
                cols = row.find_all("td")
                if len(cols) >= 7:
                    ip     = cols[0].text.strip()
                    port   = cols[1].text.strip()
                    scheme = "https" if cols[6].text.strip().lower() == "yes" else "http"
                    acc.append(f"{scheme}://{ip}:{port}")

    def _src_ssl(acc, h):
        from bs4 import BeautifulSoup
        r = requests.get("https://www.sslproxies.org/", headers=h, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")
        table = soup.find("table")
        if table:
            for row in table.find_all("tr")[1:]:
                cols = row.find_all("td")
                if len(cols) >= 2:
                    acc.append(f"https://{cols[0].text.strip()}:{cols[1].text.strip()}")

    def _src_psc(acc, h):
        url = (
            "https://api.proxyscrape.com/v2/?request=displayproxies"
            "&protocol=http&timeout=5000&country=all&ssl=all&anonymity=all"
        )
        r = requests.get(url, headers=h, timeout=10)
        for line in r.text.strip().splitlines():
            line = line.strip()
            if ":" in line:
                acc.append(f"http://{line}")

    def _src_geo(acc, h):
        url = (
            "https://proxylist.geonode.com/api/proxy-list"
            "?limit=100&page=1&sort_by=lastChecked&sort_type=desc&protocols=http,https"
        )
        r    = requests.get(url, headers=h, timeout=10)
        data = r.json()
        for entry in data.get("data", []):
            ip, port = entry.get("ip", ""), entry.get("port", "")
            proto = entry.get("protocols", ["http"])[0]
            if ip and port:
                acc.append(f"{proto}://{ip}:{port}")

    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = {
            ex.submit(_fetch_source, "free-proxy-list.net", _src_fpl): None,
            ex.submit(_fetch_source, "sslproxies.org",      _src_ssl): None,
            ex.submit(_fetch_source, "proxyscrape API",     _src_psc): None,
            ex.submit(_fetch_source, "geonode API",         _src_geo): None,
        }
        for f in as_completed(futs):
            f.result()

    proxies = list(dict.fromkeys(proxies))
    print(f"  [*] Total unique proxies scraped: {len(proxies)}")
    return proxies


# ══════════════════════════════════════════════════════════════════════════════
#  FREE PROXY POOL  (round-robin deque, same as before)
# ══════════════════════════════════════════════════════════════════════════════

class ProxyPool:
    def __init__(self, proxies, test_url=HTTPS_TEST_URL, timeout=10):
        from collections import deque
        self._all      = proxies
        self._live     = deque()
        self._lock     = threading.Lock()
        self._test_url = test_url
        self._timeout  = timeout
        self._validate_all()

    def _validate_all(self):
        print(f"[*] Validating {len(self._all)} free proxies …")
        with ThreadPoolExecutor(max_workers=min(100, len(self._all) or 1)) as ex:
            list(ex.map(self._check, self._all))
        print(f"[*] {len(self._live)} / {len(self._all)} free proxies live.")
        if not self._live:
            raise RuntimeError("[!] No live free proxies found.")

    def _check(self, proxy_url):
        try:
            r = requests.get(
                self._test_url,
                proxies={"http": proxy_url, "https": proxy_url},
                timeout=self._timeout, verify=False, allow_redirects=True,
            )
            if r.status_code == 200:
                with self._lock:
                    self._live.append(proxy_url)
                print(f"  [+] Live: {proxy_url}")
        except Exception:
            pass

    def next(self):
        with self._lock:
            if not self._live:
                raise RuntimeError("[!] Free proxy pool empty.")
            p = self._live.popleft()
            self._live.append(p)
            return p

    def remove(self, proxy_url):
        with self._lock:
            try:
                self._live.remove(proxy_url)
                print(f"  [x] Removed dead free proxy: {proxy_url} | Remaining: {len(self._live)}")
            except ValueError:
                pass

    def size(self):
        with self._lock:
            return len(self._live)

    def is_empty(self):
        with self._lock:
            return len(self._live) == 0

    def refill(self):
        print("[!] Free pool low — re-scraping …")
        from collections import deque
        new_proxies = scrape_free_proxies()
        found, lock = [], threading.Lock()

        def _check_new(proxy_url):
            try:
                r = requests.get(
                    self._test_url,
                    proxies={"http": proxy_url, "https": proxy_url},
                    timeout=self._timeout, verify=False, allow_redirects=True,
                )
                if r.status_code == 200:
                    with lock:
                        found.append(proxy_url)
            except Exception:
                pass

        with ThreadPoolExecutor(max_workers=min(100, len(new_proxies) or 1)) as ex:
            list(ex.map(_check_new, new_proxies))

        added = 0
        with self._lock:
            existing = set(self._live)
            for p in found:
                if p not in existing:
                    self._live.append(p)
                    added += 1
        print(f"[*] Refill done. Added {added}. Pool size: {self.size()}")


# ══════════════════════════════════════════════════════════════════════════════
#  PROXY MANAGEMENT
# ══════════════════════════════════════════════════════════════════════════════

def _is_rate_limited_or_blocked(status_or_exc):
    if isinstance(status_or_exc, int):
        return status_or_exc in (403, 419, 429, 503)
    msg = str(status_or_exc).lower()
    return any(kw in msg for kw in (
        "419", "429", "too many", "rate limit", "forbidden", "403",
        "blocked", "connection refused", "timeout", "timed out",
        "remote disconnected", "reset by peer",
    ))


def _init_free_pool():
    """Scrape & validate free proxies; store in global _free_pool."""
    global _free_pool
    print("[*] Falling back to free proxy pool …")
    raw = scrape_free_proxies()
    _free_pool = ProxyPool(raw, test_url=detect_target_host(TARGET_JSON))


def report_bad_proxy(proxy_url=None):
    """On failure: rotate Webshare proxy or (if all dead) use free pool.
    Returns the new proxy URL to apply to the session."""
    global _free_pool

    if _using_webshare:
        new_proxy = _rotate_proxy()   # marks current IP bad, advances cursor
                                      # triggers _init_free_pool if all 10 dead
        delay = random.uniform(3.0, 7.0)
        print(f"  [proxy] Back-off {delay:.1f}s …")
        time.sleep(delay)
        # After possible fallback, pick correct source
        with _proxy_lock:
            if not _using_webshare and _free_pool:
                return _free_pool.next()
        return new_proxy
    else:
        # Already on free pool — rotate within it
        if proxy_url and _free_pool:
            _free_pool.remove(proxy_url)
        if _free_pool and _free_pool.is_empty():
            _free_pool.refill()
        return _free_pool.next() if _free_pool else None


def reset_webshare_failures():
    pass   # no-op kept for call-site compatibility


def init_proxy():
    """Test all 10 Webshare proxies at startup; fall back immediately if none work."""
    global _using_webshare
    print(f"[*] Verifying Webshare proxy pool ({len(WEBSHARE_POOL)} proxies) …")
    ok = 0
    for proxy_url in WEBSHARE_POOL:
        try:
            r = requests.get(
                HTTPS_TEST_URL,
                proxies={"http": proxy_url, "https": proxy_url},
                timeout=10, verify=False, allow_redirects=True,
            )
            if r.status_code == 200:
                ok += 1
        except Exception:
            pass
    if ok == 0:
        print("  [proxy] All Webshare proxies unreachable at startup — switching to free pool.")
        with _proxy_lock:
            _using_webshare = False
        _init_free_pool()
    else:
        print(f"  [proxy] {ok}/{len(WEBSHARE_POOL)} Webshare proxies OK.")


# ══════════════════════════════════════════════════════════════════════════════
#  FILE I/O HELPERS  (thread-safe)
# ══════════════════════════════════════════════════════════════════════════════

_file_lock = threading.Lock()


def init_files():
    for f in (PROCESSED_FILE, ERROR_FILE):
        if not os.path.exists(f):
            open(f, "w", encoding="utf-8").close()
    with open(PROCESSED_FILE, "r", encoding="utf-8") as f:
        return set(line.strip() for line in f if line.strip())

def log_processed(url):
    with _file_lock:
        with open(PROCESSED_FILE, "a", encoding="utf-8") as f:
            f.write(url + "\n")

def log_error(url, msg):
    with _file_lock:
        with open(ERROR_FILE, "a", encoding="utf-8") as f:
            f.write(f"{url} | ERROR: {msg}\n")


# ══════════════════════════════════════════════════════════════════════════════
#  TMDB LOOKUP  (direct, no proxy)
# ══════════════════════════════════════════════════════════════════════════════

_tmdb_cache      = {}
_tmdb_cache_lock = threading.Lock()


def get_tmdb_id_from_imdb(imdb_id):
    if not TMDB_API_KEY or not imdb_id:
        return ""
    with _tmdb_cache_lock:
        if imdb_id in _tmdb_cache:
            return _tmdb_cache[imdb_id]
    url = (
        f"https://api.themoviedb.org/3/find/{imdb_id}"
        f"?api_key={TMDB_API_KEY}&external_source=imdb_id"
    )
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        tmdb = ""
        if data.get("movie_results"):
            tmdb = str(data["movie_results"][0]["id"])
        elif data.get("tv_results"):
            tmdb = str(data["tv_results"][0]["id"])
        with _tmdb_cache_lock:
            _tmdb_cache[imdb_id] = tmdb
        return tmdb
    except Exception as e:
        print(f"  [!] TMDb lookup failed for {imdb_id}: {e}")
    return ""


# ══════════════════════════════════════════════════════════════════════════════
#  HTML HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def base(url):
    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}"

def csrf(html):
    m = re.search(r'<meta name="csrf-token" content="([^"]+)"', html)
    token = m.group(1) if m else ""
    if not token:
        print("  [DEBUG] WARNING: csrf-token NOT found")
    else:
        print(f"  [DEBUG] csrf-token: {token[:20]}…")
    return token

def spans(html):
    """Extract (label, data) tuples from <span data=...> elements — pure regex,
    faster than full BeautifulSoup parse on large HTML."""
    # Try fast regex path first
    pairs = re.findall(r'<span[^>]+\bdata=["\']([^"\']{10,})["\'][^>]*>(.*?)</span>', html, re.S)
    if pairs:
        result = [(label.strip(), d) for d, label in pairs]
        print(f"  [DEBUG] spans() — {len(result)} valid server(s) found (regex fast path)")
        return result

    # Fall back to BS4 for malformed HTML
    soup = BeautifulSoup(html, "html.parser")
    all_s = soup.find_all("span", attrs={"data": True})
    result = [
        (s.get_text(strip=True), s["data"])
        for s in all_s
        if len(s.get("data", "")) > 10
    ]
    print(f"  [DEBUG] spans() — {len(result)} valid server(s) (BS4 fallback)")
    if not result:
        print(f"  [DEBUG] HTML snippet: {html[:1500].replace(chr(10), ' ')}")
    return result

def iframe(html):
    m = re.search(r'<iframe[^>]+src=["\']([^"\']+)["\']', html)
    url = m.group(1) if m else ""
    if not url:
        print(f"  [DEBUG] iframe() — none found. Snippet: {html[:300]}")
    else:
        print(f"  [DEBUG] iframe() — {url[:80]}")
    return url

def do_post(session, url, data, ref):
    """Rate-limited POST through the given session."""
    r = rate_limited_request(
        session, "post", url,
        data=data,
        headers={"Referer": ref, "Content-Type": "application/x-www-form-urlencoded"},
        timeout=15,
        verify=False,
    )
    r.raise_for_status()
    return r.text

def do_get(session, url, **kwargs):
    """Rate-limited GET through the given session."""
    kwargs.setdefault("timeout", 15)
    kwargs.setdefault("verify", False)
    r = rate_limited_request(session, "get", url, **kwargs)
    return r


# ══════════════════════════════════════════════════════════════════════════════
#  SERVER FETCH — one /ajax POST per server, run in parallel
# ══════════════════════════════════════════════════════════════════════════════

def _fetch_one_server(root, token, label, data_val, ref_url):
    """Fetch a single embed URL from /ajax. Called in a thread pool."""
    session = get_session()
    try:
        print(f"  [DEBUG] /ajax POST label='{label}' data='{data_val[:30]}…'")
        embed_html = do_post(
            session,
            f"{root}/ajax",
            {"m4u": data_val, "_token": token},
            ref_url,
        )
        return iframe(embed_html)
    except Exception as e:
        print(f"  [DEBUG] /ajax POST failed for label='{label}': {e}")
        return ""


def fetch_all_servers_parallel(root, token, servers, ref_url):
    """Fetch all server embed URLs for a given set of (label, data) pairs in
    parallel using up to SERVER_WORKERS threads."""
    if not servers:
        return []
    embeds = []
    with ThreadPoolExecutor(max_workers=min(SERVER_WORKERS, len(servers))) as ex:
        futs = {
            ex.submit(_fetch_one_server, root, token, label, dv, ref_url): idx
            for idx, (label, dv) in enumerate(servers)
        }
        # Collect in submission order to preserve server numbering
        ordered = [None] * len(servers)
        for fut in as_completed(futs):
            idx = futs[fut]
            try:
                url = fut.result()
                if url:
                    ordered[idx] = url
            except Exception as e:
                print(f"  [!] Server fetch exception: {e}")

    embeds = [u for u in ordered if u]
    return embeds


# ══════════════════════════════════════════════════════════════════════════════
#  EPISODE HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def get_all_episode_ids(html):
    seen, ordered = set(), []
    for ep_id in re.findall(r'idepisode=["\'](\w+)["\']', html):
        if ep_id not in seen:
            seen.add(ep_id)
            ordered.append(ep_id)
    return ordered


def fetch_servers_for_episode(root, token, ep_id, target_url, max_retries=3):
    """Fetch the server list for one episode, then resolve all embeds in
    parallel.  Retries with proxy rotation on failure."""
    session = get_session()
    for attempt in range(max_retries):
        proxy_used = session.proxies.get("https") or session.proxies.get("http")
        try:
            server_html = do_post(
                session,
                f"{root}/ajaxtv",
                {"idepisode": ep_id, "_token": token},
                target_url,
            )
            servers = spans(server_html)
            embeds  = fetch_all_servers_parallel(root, token, servers, target_url)
            # Polite pause after each episode to avoid 419/429 rate-bans
            time.sleep(random.uniform(EPISODE_DELAY_MIN, EPISODE_DELAY_MAX))
            return embeds

        except requests.exceptions.RequestException as e:
            print(f"    [!] Episode {ep_id} attempt {attempt + 1}/{max_retries}: {e}")
            if attempt < max_retries - 1:
                new_proxy = report_bad_proxy(proxy_used)
                _apply_proxy_to_session(session, new_proxy)
            else:
                print(f"    [!] Giving up on episode {ep_id}.")
                return []
        except Exception as e:
            print(f"    [!] Episode {ep_id} unexpected error: {e}")
            return []


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


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN EXTRACTION: MOVIES
# ══════════════════════════════════════════════════════════════════════════════

def extract_movie_servers(target_url, max_retries=3):
    session = get_session()
    for attempt in range(max_retries):
        proxy_used = session.proxies.get("https") or session.proxies.get("http")
        try:
            print(f"  [DEBUG] GET {target_url}")
            r = do_get(session, target_url, allow_redirects=True)
            print(f"  [DEBUG] Status: {r.status_code}")

            if _is_rate_limited_or_blocked(r.status_code):
                raise requests.exceptions.RequestException(
                    f"HTTP {r.status_code} — proxy blocked"
                )

            html    = r.text
            token   = csrf(html)
            root    = base(target_url)
            servers = spans(html)

            if not servers:
                log_error(target_url, "spans() empty — page structure changed")
                return []

            embeds = fetch_all_servers_parallel(root, token, servers, target_url)
            return embeds

        except requests.exceptions.RequestException as e:
            print(f"  [!] Attempt {attempt + 1}/{max_retries}: {e}")
            if attempt < max_retries - 1:
                new_proxy = report_bad_proxy(proxy_used)
                _apply_proxy_to_session(session, new_proxy)
            else:
                log_error(target_url, f"Failed after {max_retries} retries: {e}")
                return []
        except Exception as e:
            print(f"  [!] Unexpected: {e}")
            log_error(target_url, f"Unexpected: {e}")
            return []


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN EXTRACTION: SERIES
#  Episodes themselves are fetched sequentially (one page load → many ep_ids),
#  but each episode's server POSTs are parallelised inside fetch_servers_for_episode.
#  If you want episodes themselves to run in parallel too, enable the
#  ThreadPoolExecutor block below (EPISODE_WORKERS).
# ══════════════════════════════════════════════════════════════════════════════

EPISODE_WORKERS = 1    # sequential episodes — site 419s on concurrent bursts


def extract_series_all_episodes(target_url, max_retries=3):
    session = get_session()

    # ── Step 1: load the series page ────────────────────────────────────────
    html = token = root = None
    for attempt in range(max_retries):
        proxy_used = session.proxies.get("https") or session.proxies.get("http")
        try:
            print(f"  [DEBUG] GET series page: {target_url}")
            r = do_get(session, target_url, allow_redirects=True)
            print(f"  [DEBUG] Status: {r.status_code}")

            if _is_rate_limited_or_blocked(r.status_code):
                raise requests.exceptions.RequestException(
                    f"HTTP {r.status_code} — proxy blocked"
                )

            html  = r.text
            token = csrf(html)
            root  = base(target_url)
            break

        except requests.exceptions.RequestException as e:
            print(f"  [!] Series page attempt {attempt + 1}/{max_retries}: {e}")
            if attempt < max_retries - 1:
                new_proxy = report_bad_proxy(proxy_used)
                _apply_proxy_to_session(session, new_proxy)
            else:
                log_error(target_url, f"Series page failed: {e}")
                return None
        except Exception as e:
            log_error(target_url, f"Unexpected series page error: {e}")
            return None

    # ── Step 2: gather episode IDs ───────────────────────────────────────────
    ep_ids = get_all_episode_ids(html)
    print(f"  [DEBUG] Episode IDs: {ep_ids[:5]}{'…' if len(ep_ids) > 5 else ''}")
    if not ep_ids:
        log_error(target_url, "No episode IDs found.")
        return None
    print(f"  [*] {len(ep_ids)} episodes | {EPISODE_WORKERS} parallel episode workers")

    result = {
        "total_episodes": len(ep_ids),
        "episodes": {},
        "imdb_id": "",
    }
    episodes_lock = threading.Lock()

    # ── Step 3: fetch all episodes in parallel ───────────────────────────────
    def _process_episode(args):
        ep_num, ep_id = args
        embeds = fetch_servers_for_episode(root, token, ep_id, target_url)
        return ep_num, ep_id, embeds

    with ThreadPoolExecutor(max_workers=min(EPISODE_WORKERS, len(ep_ids))) as ex:
        futs = {
            ex.submit(_process_episode, (ep_num, ep_id)): ep_num
            for ep_num, ep_id in enumerate(ep_ids, start=1)
        }
        for fut in as_completed(futs):
            try:
                ep_num, ep_id, embeds = fut.result()
                with episodes_lock:
                    result["episodes"][str(ep_num)] = embeds if embeds else []
                    if embeds and not result["imdb_id"]:
                        for eu in embeds:
                            m = re.search(r'(tt\d{7,10})', eu)
                            if m:
                                result["imdb_id"] = m.group(1)
                                break
                status = f"{len(embeds)} server(s)" if embeds else "no servers"
                print(f"    -> Episode {ep_num}/{len(ep_ids)} (id={ep_id}): {status}")
            except Exception as e:
                ep_num = futs[fut]
                print(f"    [!] Episode {ep_num} worker exception: {e}")
                with episodes_lock:
                    result["episodes"][str(ep_num)] = []

    return result


# ══════════════════════════════════════════════════════════════════════════════
#  APPLY SERIES RESULT
# ══════════════════════════════════════════════════════════════════════════════

def apply_series_result(item, series_data):
    for k in ["server1", "server2", "server3", "server4"]:
        item.pop(k, None)
    item["total_episodes"] = series_data["total_episodes"]
    for ep_num_str, embeds in series_data["episodes"].items():
        for server_idx, embed_url in enumerate(embeds, start=1):
            item[f"episode-{ep_num_str}-server{server_idx}"] = embed_url

def series_already_done(item):
    return bool(item.get("episode-1-server1", ""))


# ══════════════════════════════════════════════════════════════════════════════
#  PROCESS ONE ITEM  (called by each item-worker thread)
# ══════════════════════════════════════════════════════════════════════════════

def process_item(item):
    """Process a single movie/series item. Returns the mutated item dict."""
    target_url = item["url"]
    title      = item.get("title", "Unknown Title")
    print(f"\n-> [{threading.current_thread().name}] {title}")
    print(f"   URL: {target_url}")

    try:
        if IS_SERIES:
            series_data = extract_series_all_episodes(target_url)
            if not series_data:
                log_error(target_url, "Series extraction returned nothing.")
                return item, False

            apply_series_result(item, series_data)

            found_imdb = series_data.get("imdb_id", "")
            if found_imdb:
                item["imdb_id"] = found_imdb
                print(f"   IMDb: {found_imdb}")
                tmdb = get_tmdb_id_from_imdb(found_imdb)
                if tmdb:
                    item["tmdb_id"] = tmdb
                    print(f"   TMDb: {tmdb}")

            print(f"   Done — {series_data['total_episodes']} episode(s) written.")
            return item, True

        else:
            embeds = extract_movie_servers(target_url)
            if not embeds:
                log_error(target_url, "No embeds found.")
                return item, False

            for i in range(1, 5):
                item[f"server{i}"] = embeds[i - 1] if i <= len(embeds) else ""

            found_imdb = ""
            for eu in embeds:
                m = re.search(r'(tt\d{7,10})', eu)
                if m:
                    found_imdb = m.group(1)
                    break

            if found_imdb:
                item["imdb_id"] = found_imdb
                print(f"   IMDb: {found_imdb}")
                tmdb = get_tmdb_id_from_imdb(found_imdb)
                if tmdb:
                    item["tmdb_id"] = tmdb
                    print(f"   TMDb: {tmdb}")

            print(f"   Mapped {len(embeds)} server(s).")
            return item, True

    except Exception as e:
        print(f"  [!] process_item crash: {e}")
        log_error(target_url, f"Crash: {e}")
        return item, False


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    pass  # banner removed

    if not os.path.exists(TARGET_JSON):
        print(f"[!] {TARGET_JSON} not found.")
        sys.exit(1)

    init_proxy()

    processed_urls = init_files()

    with open(TARGET_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)

    print(f"\n[*] Records in {TARGET_JSON}: {len(data)}")

    # ── Optional: restrict to specific serial_no values ─────────────────────
    # Set via env var SERIAL_NOS="180,183,205" (comma-separated, no spaces needed)
    _serial_nos_env = os.getenv("SERIAL_NOS", "").strip()
    _target_serials: set[int] | None = None
    if _serial_nos_env:
        try:
            _target_serials = {int(x.strip()) for x in _serial_nos_env.split(",") if x.strip()}
            print(f"[*] Serial-no filter : {sorted(_target_serials)}")
        except ValueError:
            print(f"[!] Invalid SERIAL_NOS value '{_serial_nos_env}' — ignored.")

    if IS_SERIES:
        queue = [
            item for item in data
            if item.get("url")
            and not series_already_done(item)
            and item["url"] not in processed_urls
            and (_target_serials is None or item.get("serial_no") in _target_serials)
        ]
    else:
        queue = [
            item for item in data
            if item.get("url")
            and not item.get("server1")
            and item["url"] not in processed_urls
            and (_target_serials is None or item.get("serial_no") in _target_serials)
        ]

    # URL_LIMIT is ignored when serial_nos are explicitly specified
    if URL_LIMIT is not None and _target_serials is None:
        queue = queue[:URL_LIMIT]

    print(f"[*] Items queued: {len(queue)}")

    # Build a url→index map so we can patch `data` in place after workers finish
    url_to_idx = {item["url"]: i for i, item in enumerate(data)}

    completed = 0
    failed    = 0

    try:
        with ThreadPoolExecutor(
            max_workers=ITEM_WORKERS,
            thread_name_prefix="worker",
        ) as ex:
            futs = {ex.submit(process_item, item): item for item in queue}

            for fut in as_completed(futs):
                orig_item = futs[fut]
                try:
                    updated_item, ok = fut.result()

                    # Patch the original data list in place (thread-safe: different indices)
                    idx = url_to_idx.get(orig_item["url"])
                    if idx is not None:
                        data[idx] = updated_item

                    if ok:
                        completed += 1
                        processed_urls.add(orig_item["url"])
                        log_processed(orig_item["url"])
                    else:
                        failed += 1

                except Exception as e:
                    failed += 1
                    print(f"  [!] Worker future exception: {e}")
                    log_error(orig_item.get("url", "?"), f"Future exception: {e}")

                with _proxy_lock:
                    if _using_webshare:
                        cur_proxy = "WS:" + WEBSHARE_POOL[_proxy_index % len(WEBSHARE_POOL)].split("@")[1]
                    else:
                        cur_proxy = f"FreePool({_free_pool.size() if _free_pool else 0} live)"
                print(
                    f"  [progress] done={completed} failed={failed} "
                    f"remaining={len(queue) - completed - failed} "
                    f"proxy={cur_proxy}"
                )

    except KeyboardInterrupt:
        print("\n[!] Interrupted. Saving …")
    finally:
        with open(TARGET_JSON, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)
        print(f"\n[*] Saved {TARGET_JSON}.")
        print(f"[*] Completed={completed} | Failed={failed} | Proxy=Webshare (rotating pool)")


if __name__ == "__main__":
    main()
