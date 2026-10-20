#!/usr/bin/env python3
"""Ryuzi Anime News Bot — single-file, standard-library edition.

Python 3.10+; no pip packages, MongoDB, or separate requirements file needed.
Required environment variables: BOT_TOKEN, CHANNEL_ID, OWNER_ID.
Optional: CHECK_INTERVAL_SECONDS (default 3600), INITIAL_POST_COUNT (default 5), PORT (default 10000), DB_PATH.
"""
from __future__ import annotations

import hashlib
import html
import json
import logging
import getpass
import sys
import os
import re
import sqlite3
import threading
import time
import traceback
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from html.parser import HTMLParser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from email.utils import parsedate_to_datetime

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
CHANNEL_ID = os.getenv("CHANNEL_ID", "").strip()
OWNER_ID_RAW = os.getenv("OWNER_ID", "").strip()
OWNER_ID = int(OWNER_ID_RAW) if OWNER_ID_RAW.isdigit() else None
def env_int(name, default, minimum):
    raw = os.getenv(name, str(default)).strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise SystemExit(f"{name} must be a whole number; received {raw!r}.") from exc
    if value < minimum:
        log.warning("%s=%s is below the minimum; using %s", name, value, minimum)
        value = minimum
    return value


CHECK_INTERVAL_SECONDS = env_int("CHECK_INTERVAL_SECONDS", 3600, 30)
INITIAL_POST_COUNT = min(10, env_int("INITIAL_POST_COUNT", 5, 1))
PORT = env_int("PORT", 10000, 1)
DB_PATH = os.getenv("DB_PATH", "ryuzi_anime_news.sqlite3")
MAX_SUMMARY_CHARS = 650
USER_AGENT = "RyuziAnimeNewsBot/2.0 (+RSS reader; contact channel administrator)"

FOOTER = """❗ 𝕁𝕠𝕚𝕟 ℕ𝕠𝕨:-@RyuziSenpai

➡️ 𝐓𝐞𝐥𝐞𝐠𝐫𝐚𝐦 𝐂𝐡𝐚𝐭𝐭𝐢𝐧𝐠 𝐉𝐨𝐢𝐧 𝐍𝐨𝐰:-https://t.me/ryuzisenpaianime

➡️ 𝐈𝐧𝐬𝐭𝐚𝐠𝐫𝐚𝐦 𝐂𝐡𝐚𝐧𝐧𝐞𝐥:-https://www.instagram.com/channel/AbZ_k3-0gWwsdMp2/

➡️ 𝐈𝐧𝐬𝐭𝐚𝐠𝐫𝐚𝐦 𝐃𝐢𝐬𝐜𝐮𝐬𝐬𝐢𝐨𝐧 𝐂𝐡𝐚𝐧𝐧𝐞𝐥:-https://ig.me/j/AbaNLt8RNCJVChLl/"""

SOURCES = [
    ("Anime News Network", "https://www.animenewsnetwork.com/all/rss.xml?ann-edition=us"),
    ("Anime Corner", "https://animecorner.me/feed/"),
    ("Crunchyroll News", "https://cr-news-api-service.prd.crunchyrollsvc.com/v1/en-US/rss"),
    ("Anime News India", "https://animenewsindia.com/feed/"),
]

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper(),
                    format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("ryuzi-anime-news")
started_at = time.time()
status_lock = threading.Lock()
scan_lock = threading.Lock()
last_check = "Not checked yet"
last_result = "Starting"
next_update_id = 0


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def request_bytes(url, data=None, headers=None, timeout=20):
    h = {"User-Agent": USER_AGENT}
    if headers:
        h.update(headers)
    req = urllib.request.Request(url, data=data, headers=h)
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return response.read(), response.headers.get("Content-Type", "")


def telegram(method, payload=None, timeout=35):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    body = json.dumps(payload or {}).encode("utf-8")
    raw, _ = request_bytes(url, data=body, headers={"Content-Type": "application/json"}, timeout=timeout)
    result = json.loads(raw.decode("utf-8"))
    if not result.get("ok"):
        raise RuntimeError(f"Telegram {method} failed: {result.get('description', 'unknown error')}")
    return result.get("result")


def db_connect():
    conn = sqlite3.connect(DB_PATH, timeout=20)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""CREATE TABLE IF NOT EXISTS articles (
        link TEXT PRIMARY KEY, title_key TEXT NOT NULL, title TEXT NOT NULL,
        source TEXT NOT NULL, published TEXT, seen_at TEXT NOT NULL,
        is_published INTEGER NOT NULL DEFAULT 0
    )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_articles_title ON articles(title_key)")
    conn.execute("CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    conn.commit()
    return conn


def normalize_url(url):
    try:
        p = urllib.parse.urlsplit((url or "").strip())
        if not p.hostname:
            return (url or "").strip().rstrip("/")
        host = p.hostname.lower().removeprefix("www.")
        port = f":{p.port}" if p.port and p.port not in (80, 443) else ""
        tracking = {"utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content", "fbclid", "gclid", "mc_cid", "mc_eid", "ref", "source"}
        query = urllib.parse.urlencode([(k, v) for k, v in urllib.parse.parse_qsl(p.query, keep_blank_values=True) if k.lower() not in tracking])
        path = re.sub(r"/{2,}", "/", p.path or "/").rstrip("/") or "/"
        return urllib.parse.urlunsplit((p.scheme.lower() or "https", host + port, path, query, ""))
    except Exception:
        return (url or "").strip().rstrip("/")


def plain_text(value):
    value = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", value or "")
    value = re.sub(r"(?s)<[^>]+>", " ", value)
    return re.sub(r"\s+", " ", html.unescape(value)).strip()


def title_key(title):
    # Preserve non-Latin titles (for example, Japanese anime titles) for deduplication.
    normalized = unicodedata.normalize("NFKC", title or "").casefold()
    normalized = re.sub(r"[^\w]+", " ", normalized, flags=re.UNICODE).strip()
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest() if normalized else ""


def local_name(tag):
    return tag.rsplit("}", 1)[-1].lower()


def child_text(element, wanted):
    for child in element.iter():
        if local_name(child.tag) == wanted.lower() and child is not element:
            text = " ".join(child.itertext()).strip()
            if text:
                return text
    return ""


def entry_link(entry):
    for child in list(entry):
        if local_name(child.tag) == "link":
            href = child.attrib.get("href", "").strip()
            rel = child.attrib.get("rel", "alternate")
            if href and rel in ("alternate", ""):
                return href
            if child.text and child.text.strip():
                return child.text.strip()
    return child_text(entry, "link")


def published_date(entry):
    raw = child_text(entry, "published") or child_text(entry, "pubdate") or child_text(entry, "date") or child_text(entry, "updated")
    if not raw:
        return ""
    try:
        try:
            dt = parsedate_to_datetime(raw)
        except Exception:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        return ""


def fetch_feed(source, url):
    raw, _ = request_bytes(url, headers={"Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml;q=0.9, */*;q=0.5"}, timeout=20)
    root = ET.fromstring(raw)
    entries = [e for e in root.iter() if local_name(e.tag) in ("item", "entry")]
    found = []
    for entry in entries[:40]:
        title = plain_text(child_text(entry, "title"))
        link = entry_link(entry)
        if not title or not link or urllib.parse.urlsplit(link).scheme not in ("http", "https"):
            continue
        summary = plain_text(child_text(entry, "encoded") or child_text(entry, "description") or child_text(entry, "summary") or child_text(entry, "content"))
        found.append({"source": source, "title": title, "link": normalize_url(link), "summary": summary, "published": published_date(entry)})
    return found


class ArticleHTMLParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.meta = {}
        self.skip_depth = 0
        self.capture = False
        self.paragraph_depth = 0
        self.current = []
        self.paragraphs = []
        self.stack = []

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        tag = tag.lower()
        if tag == "meta":
            key = (attrs.get("property") or attrs.get("name") or "").lower()
            if key and attrs.get("content"):
                self.meta[key] = attrs["content"].strip()
        if tag in ("script", "style", "nav", "footer", "header", "aside", "form", "noscript"):
            self.skip_depth += 1
        if tag in ("article", "main") or attrs.get("itemprop") == "articleBody" or any(k in (attrs.get("class") or "").lower() for k in ("entry-content", "post-content", "article-content")):
            self.capture = True
        if tag in ("p", "h2", "h3") and self.capture and self.skip_depth == 0:
            self.paragraph_depth += 1
            self.current = []
        self.stack.append(tag)

    def handle_endtag(self, tag):
        tag = tag.lower()
        if tag in ("script", "style", "nav", "footer", "header", "aside", "form", "noscript") and self.skip_depth:
            self.skip_depth -= 1
        if tag in ("p", "h2", "h3") and self.paragraph_depth:
            text = re.sub(r"\s+", " ", " ".join(self.current)).strip()
            if text:
                self.paragraphs.append(text)
            self.current = []
            self.paragraph_depth -= 1
        if tag in ("article", "main"):
            # Keep capture active for nested article/main structures; extracted text is still bounded.
            self.capture = any(t in ("article", "main") for t in self.stack[:-1])
        if self.stack:
            self.stack.pop()

    def handle_data(self, data):
        if self.capture and self.skip_depth == 0 and self.paragraph_depth:
            self.current.append(data)


def concise_points(text, max_chars=MAX_SUMMARY_CHARS):
    """Return at most three short, useful points without publisher attribution."""
    text = re.sub(r"\s+", " ", plain_text(text or "")).strip()
    if not text:
        return "Important details are not available yet."

    # Split at sentence endings regardless of whether the next sentence starts
    # with an uppercase letter; RSS summaries often use lowercase starts.
    sentences = re.split(r"(?<=[.!?。！？])\s+", text)
    picked, seen = [], set()
    noise = (
        "cookie policy", "privacy policy", "subscribe to", "sign up for our newsletter",
        "read more on", "click here to read", "follow us on", "all rights reserved",
    )
    for sentence in sentences:
        sentence = sentence.strip(" -•|\t")
        key = re.sub(r"[^\w]+", " ", sentence.casefold(), flags=re.UNICODE).strip()
        if len(sentence) < 28 or not key or key in seen:
            continue
        if any(phrase in sentence.casefold() for phrase in noise):
            continue
        # Avoid copying a source-credit sentence into the post.
        if re.match(r"^(source|via|credit)\s*[:：-]", sentence, flags=re.IGNORECASE):
            continue
        candidate = "• " + sentence
        if len("\n".join(picked)) + len(candidate) + 1 > max_chars:
            remaining = max_chars - len("\n".join(picked)) - 1
            if remaining >= 80:
                clipped = sentence[:max(1, remaining - 2)].rsplit(" ", 1)[0].rstrip(" ,;:-")
                if len(clipped) >= 50:
                    picked.append("• " + clipped + "…")
            break
        picked.append(candidate)
        seen.add(key)
        if len(picked) == 3:
            break

    if picked:
        return "\n\n".join(picked)
    # If the feed gives one long sentence, keep it short rather than dumping the article.
    short = text[:max_chars].rsplit(" ", 1)[0].rstrip(" ,;:-")
    return "• " + short + ("…" if len(text) > len(short) else "")


def article_context(article):
    fallback = article.get("summary", "")
    text = fallback
    try:
        raw, content_type = request_bytes(article["link"], timeout=15)
        if "html" in content_type.lower():
            parser = ArticleHTMLParser()
            parser.feed(raw.decode("utf-8", errors="replace"))
            text = " ".join(parser.paragraphs) or fallback
    except Exception as exc:
        log.debug("Article extraction failed for %s: %s", article["link"], exc)
    return concise_points(text)


def article_image(article):
    try:
        raw, content_type = request_bytes(article["link"], timeout=12)
        if "html" not in content_type.lower():
            return ""
        parser = ArticleHTMLParser()
        parser.feed(raw.decode("utf-8", errors="replace"))
        image_url = (parser.meta.get("og:image")
                     or parser.meta.get("og:image:secure_url")
                     or parser.meta.get("twitter:image")
                     or parser.meta.get("twitter:image:src")
                     or parser.meta.get("twitter:image:src:0"))
        if image_url:
            image_url = urllib.parse.urljoin(article["link"], image_url.strip())
            parsed = urllib.parse.urlsplit(image_url)
            if parsed.scheme in ("http", "https") and parsed.netloc:
                return image_url
    except Exception as exc:
        log.debug("Image extraction failed for %s: %s", article["link"], exc)
    return ""


def category(article):
    text = f"{article['title']} {article.get('summary', '')} {article['source']}".lower()
    if any(x in text for x in ("hindi dub", "hindi-dub", "hindi dubbed", "hindi audio")):
        return "🎙️ Hindi Dub Updates"
    if any(x in text for x in ("tamil dub", "telugu dub", "tamil dubbed", "telugu dubbed", "tamil audio", "telugu audio")):
        return "🎧 Tamil/Telugu Dub Updates"
    if any(x in text for x in ("release date", "premiere", "season 2", "season 3", "season 4", "new season", "air date", "broadcast date")):
        return "📅 Release Date & Season Announcements"
    if any(x in text for x in ("streaming", "streams", "stream on", "ott", "netflix", "crunchyroll", "prime video", "hotstar", "disney+", "platform")):
        return "📺 OTT & Streaming Updates"
    if article["source"] == "Anime News India" or any(x in text for x in ("available in india", "india release", "indian audience", "india streaming", "india premiere")):
        return "🇮🇳 India Anime News"
    return "🌍 Global Anime News"


def format_post(article, summary):
    # Do not expose the publisher name or the original article URL in the Telegram post.
    prefix = f"{category(article)}\n\n📰 <b>Title:</b> {html.escape(article['title'])}\n\n📌 <b>Key Points:</b>\n"
    tail = f"\n\n{html.escape(FOOTER)}"
    # article_context() has already condensed this into at most three points.
    details = html.escape(summary or "• Important details are not available yet.")
    available = max(100, 3900 - len(prefix) - len(tail))
    if len(details) > available:
        details = details[:available - 1].rsplit(" ", 1)[0] + "…"
    return prefix + details + tail


def is_known(conn, article):
    row = conn.execute("SELECT 1 FROM articles WHERE link=? OR title_key=? LIMIT 1",
                       (article["link"], title_key(article["title"]))).fetchone()
    return row is not None


def remember(conn, article, published=False):
    conn.execute("""INSERT INTO articles(link,title_key,title,source,published,seen_at,is_published)
        VALUES(?,?,?,?,?,?,?) ON CONFLICT(link) DO UPDATE SET is_published=MAX(articles.is_published, excluded.is_published)""",
        (article["link"], title_key(article["title"]), article["title"], article["source"], article.get("published") or None, utc_now(), int(published)))
    conn.commit()


def publish(article):
    """Publish anime news with its article image when one can be found.

    Telegram photo captions are limited to 1024 characters. Short posts are
    sent as one photo-with-caption message; longer posts get the image followed
    by the complete text post so that no news or footer text is silently cut.
    If the image URL is missing or Telegram rejects it, fall back to text only.
    """
    summary = article_context(article)
    post_text = format_post(article, summary)
    image_url = article_image(article)

    if image_url:
        try:
            # Telegram caption limit is 1024 characters after entity parsing.
            if len(post_text) <= 1000:
                telegram("sendPhoto", {
                    "chat_id": CHANNEL_ID,
                    "photo": image_url,
                    "caption": post_text,
                    "parse_mode": "HTML",
                }, timeout=35)
            else:
                # Keep the complete news and footer intact when the caption is
                # too long. This is an image message plus its full text, not a
                # duplicate news post: Telegram cannot put >1024 chars in a photo caption.
                telegram("sendPhoto", {
                    "chat_id": CHANNEL_ID,
                    "photo": image_url,
                }, timeout=35)
                telegram("sendMessage", {
                    "chat_id": CHANNEL_ID,
                    "text": post_text,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                }, timeout=30)
            log.info("Published news with image when available: %s", article.get("title", "(untitled)"))
            return
        except Exception as exc:
            log.warning("Could not publish image for %s; falling back to text: %s", article.get("title", "(untitled)"), exc)

    telegram("sendMessage", {
        "chat_id": CHANNEL_ID,
        "text": post_text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }, timeout=30)
    log.info("Published text-only news (no usable image): %s", article.get("title", "(untitled)"))


def scan_news(force=False):
    global last_check, last_result
    if not scan_lock.acquire(blocking=False):
        return {"found": 0, "posted": 0, "failures": 0, "message": "A scan is already running."}
    with status_lock:
        last_check = utc_now()
        last_result = "Checking sources"
    conn = None
    posted = failures = 0
    try:
        conn = db_connect()
        found, titles_seen, links_seen = [], set(), set()
        # Fetch feeds concurrently, keeping source response latency from stacking up.
        from concurrent.futures import ThreadPoolExecutor, as_completed
        with ThreadPoolExecutor(max_workers=len(SOURCES)) as pool:
            futures = {pool.submit(fetch_feed, name, url): name for name, url in SOURCES}
            for future in as_completed(futures):
                name = futures[future]
                try:
                    for article in future.result():
                        tkey = title_key(article["title"])
                        if article["link"] in links_seen or (tkey and tkey in titles_seen):
                            continue
                        links_seen.add(article["link"])
                        if tkey:
                            titles_seen.add(tkey)
                        found.append(article)
                except Exception as exc:
                    failures += 1
                    log.warning("Source %s failed: %s", name, exc)
        found.sort(key=lambda a: a.get("published") or "", reverse=True)
        bootstrap = conn.execute("SELECT value FROM metadata WHERE key='bootstrap_complete'").fetchone()
        if not bootstrap and found:
            candidates = [a for a in found if not is_known(conn, a)]
            selected = candidates[:INITIAL_POST_COUNT]
            for article in selected:
                try:
                    publish(article)
                    remember(conn, article, published=True)
                    posted += 1
                    time.sleep(0.5)
                except Exception:
                    log.exception("Failed to publish bootstrap article: %s", article["link"])
            for article in found:
                if article not in selected and not is_known(conn, article):
                    remember(conn, article, published=False)
            if posted or not selected:
                conn.execute("INSERT OR REPLACE INTO metadata(key,value) VALUES('bootstrap_complete',?)", (utc_now(),))
                conn.commit()
        else:
            for article in found:
                if is_known(conn, article):
                    continue
                try:
                    publish(article)
                    remember(conn, article, published=True)
                    posted += 1
                    time.sleep(0.5)
                except Exception:
                    log.exception("Failed to publish article; it will be retried next scan: %s", article["link"])
        with status_lock:
            last_result = f"OK — found {len(found)}, posted {posted}, source failures {failures}"
        log.info("Scan complete: found=%d posted=%d source_failures=%d", len(found), posted, failures)
        return {"found": len(found), "posted": posted, "failures": failures}
    except Exception as exc:
        log.exception("News scan failed")
        with status_lock:
            last_result = f"Error: {exc}"
        return {"found": 0, "posted": posted, "failures": failures + 1, "error": str(exc)}
    finally:
        if conn:
            conn.close()
        scan_lock.release()


def status_text():
    with status_lock:
        last, result = last_check, last_result
    uptime = int(time.time() - started_at)
    return ("🤖 Ryuzi Anime News Bot\nStatus: running\n"
            f"Uptime: {uptime // 3600}h {(uptime % 3600) // 60}m {uptime % 60}s\n"
            f"Last check (UTC): {last}\nLast result: {result}\n"
            f"Check interval: {CHECK_INTERVAL_SECONDS}s\nDatabase: local SQLite")


def handle_command(message):
    chat_id = message.get("chat", {}).get("id")
    user_id = message.get("from", {}).get("id")
    text = (message.get("text") or "").strip()
    command = text.split()[0].split("@")[0].lower() if text.startswith("/") else ""
    if not command:
        return
    if command in ("/start", "/help"):
        telegram("sendMessage", {"chat_id": chat_id, "text": "Ryuzi Anime News Bot is online! 🤖\n/start — bot information\n/help — commands\n/status — health and last scan\n/checknow — owner-only immediate scan"})
    elif command == "/status":
        telegram("sendMessage", {"chat_id": chat_id, "text": status_text()})
    elif command == "/checknow":
        if OWNER_ID is None or user_id != OWNER_ID:
            telegram("sendMessage", {"chat_id": chat_id, "text": "⛔ This command is restricted to the configured bot owner."})
            return
        telegram("sendMessage", {"chat_id": chat_id, "text": "Starting a news scan…"})
        result = scan_news(force=True)
        telegram("sendMessage", {"chat_id": chat_id, "text": f"Scan complete.\nFound: {result.get('found', 0)}\nPosted: {result.get('posted', 0)}\nSource failures: {result.get('failures', 0)}"})


def polling_loop():
    global next_update_id
    while True:
        try:
            updates = telegram("getUpdates", {"offset": next_update_id, "timeout": 25, "allowed_updates": ["message"]}, timeout=35)
            for update in updates:
                next_update_id = max(next_update_id, update["update_id"] + 1)
                message = update.get("message")
                if message:
                    try:
                        handle_command(message)
                    except Exception:
                        log.exception("Could not process Telegram command")
        except Exception as exc:
            log.warning("Telegram polling issue: %s", exc)
            time.sleep(3)


def periodic_loop():
    # Start immediately, then use a fixed wait between scans.
    while True:
        try:
            scan_news()
        except Exception:
            log.exception("Unexpected periodic scan failure")
        time.sleep(CHECK_INTERVAL_SECONDS)


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path not in ("/", "/health"):
            self.send_error(404)
            return
        body = json.dumps({"ok": True, "service": "ryuzi-anime-news-bot", "last_result": last_result}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        log.debug("Health server: " + fmt, *args)


def main():
    global BOT_TOKEN, CHANNEL_ID, OWNER_ID_RAW, OWNER_ID
    # For local terminal use, prompt for missing secrets instead of requiring a .env file.
    # On hosting platforms, set environment variables so restarts remain unattended.
    if sys.stdin.isatty():
        if not BOT_TOKEN:
            BOT_TOKEN = getpass.getpass("Telegram bot token (from @BotFather): ").strip()
        if not CHANNEL_ID:
            CHANNEL_ID = input("Target channel username or numeric chat ID: ").strip()
        if not OWNER_ID_RAW:
            OWNER_ID_RAW = input("Your numeric Telegram user ID: ").strip()
            OWNER_ID = int(OWNER_ID_RAW) if OWNER_ID_RAW.isdigit() else None
    missing = [name for name, value in (("BOT_TOKEN", BOT_TOKEN), ("CHANNEL_ID", CHANNEL_ID), ("OWNER_ID", OWNER_ID_RAW)) if not value]
    if missing:
        raise SystemExit("Missing required environment variable(s): " + ", ".join(missing) + ". Set them in your hosting settings or run this file from a terminal to be prompted.")
    if OWNER_ID is None:
        raise SystemExit("OWNER_ID must be your numeric Telegram user ID.")
    if not BOT_TOKEN or ":" not in BOT_TOKEN:
        raise SystemExit("BOT_TOKEN does not look valid; copy it from BotFather.")
    # Long polling cannot work while a webhook is configured. Remove it safely
    # without discarding pending updates, then validate the token before starting.
    telegram("deleteWebhook", {"drop_pending_updates": False})
    telegram("getMe", {})
    conn = db_connect()
    conn.close()
    threading.Thread(target=periodic_loop, name="news-scanner", daemon=True).start()
    threading.Thread(target=polling_loop, name="telegram-polling", daemon=True).start()
    server = ThreadingHTTPServer(("0.0.0.0", PORT), HealthHandler)
    log.info("Ryuzi Anime News Bot started; interval=%ss; local database=%s", CHECK_INTERVAL_SECONDS, DB_PATH)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("Bot stopped by keyboard interrupt")
    finally:
        server.server_close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
    except Exception:
        log.error("Fatal startup error:\n%s", traceback.format_exc())
        raise
