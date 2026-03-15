#!/usr/bin/env python3
"""
Import posts from all_retreat_accounts.csv into Notion Posts DB.
Reads NOTION_POSTS_DB_ID from .env file (created by setup_full.py).
Usage: NOTION_API_TOKEN=... python3 import_posts.py [path/to/file.csv]
"""
import csv
import os
import sys
import time
from urllib.parse import urlparse

from notion_client import Client


# ── Config ────────────────────────────────────────────────────────────────────

def load_env(path=".env"):
    """Load key=value pairs from .env file into os.environ."""
    if not os.path.exists(path):
        return
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

load_env()

TOKEN = os.environ.get("NOTION_API_TOKEN", "")
POSTS_DB_ID = os.environ.get("NOTION_POSTS_DB_ID", "")
CSV_PATH = sys.argv[1] if len(sys.argv) > 1 else "all_retreat_accounts.csv"

if not TOKEN:
    sys.exit("ERROR: NOTION_API_TOKEN is not set")
if not POSTS_DB_ID:
    sys.exit("ERROR: NOTION_POSTS_DB_ID not found. Run setup_full.py first.")

notion = Client(auth=TOKEN)

# ── Helpers ───────────────────────────────────────────────────────────────────

TYPE_MAP = {
    "sidecar": "Carousel",
    "carousel": "Carousel",
    "image": "Image",
    "photo": "Image",
    "reel": "Reel",
    "video": "Reel",
}

def map_type(raw: str) -> str:
    return TYPE_MAP.get(raw.strip().lower(), "Image")

def post_id_from_url(url: str, username: str, ts: str) -> str:
    slug = urlparse(url).path.rstrip("/").split("/")[-1] if url else ""
    date = ts[:10].replace("-", "") if ts else "unknown"
    return f"{username}_{date}_{slug}" if slug else f"{username}_{date}"

def parse_int(v: str) -> int:
    try:
        return int(str(v).strip().replace(",", ""))
    except (ValueError, TypeError):
        return 0

def load_existing_urls() -> set:
    """Fetch all existing Post URLs from Notion to avoid duplicates."""
    urls = set()
    cursor = None
    while True:
        resp = notion.databases.query(
            database_id=POSTS_DB_ID,
            page_size=100,
            start_cursor=cursor,
        ) if cursor else notion.databases.query(
            database_id=POSTS_DB_ID,
            page_size=100,
        )
        for page in resp.get("results", []):
            url = page.get("properties", {}).get("Post URL", {}).get("url")
            if url:
                urls.add(url.strip())
        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")
    return urls

def add_post(row: dict) -> bool:
    username = row.get("username", "").strip()
    ts       = row.get("timestamp", "").strip()
    url      = row.get("url", "").strip()
    caption  = row.get("caption", "").strip()
    location = row.get("location", "").strip()
    post_type = map_type(row.get("type", ""))
    likes     = parse_int(row.get("likes", 0))
    comments  = parse_int(row.get("comments", 0))
    engagement= parse_int(row.get("engagement", 0))
    post_id   = post_id_from_url(url, username, ts)

    # Normalize timestamp to ISO format
    ts_clean = ts.replace("Z", "+00:00") if ts.endswith("Z") else ts

    props = {
        "Post ID":   {"title": [{"text": {"content": post_id[:2000]}}]},
        "Username":  {"rich_text": [{"text": {"content": username[:2000]}}]},
        "Post Type": {"select": {"name": post_type}},
        "Likes":     {"number": likes},
        "Comments":  {"number": comments},
        "Engagement":{"number": engagement},
    }
    if ts_clean:
        props["Timestamp"] = {"date": {"start": ts_clean}}
    if url:
        props["Post URL"] = {"url": url}
    if caption:
        props["Caption"] = {"rich_text": [{"text": {"content": caption[:2000]}}]}
    if location:
        props["Location"] = {"rich_text": [{"text": {"content": location[:2000]}}]}

    try:
        notion.pages.create(parent={"database_id": POSTS_DB_ID}, properties=props)
        time.sleep(0.35)
        return True
    except Exception as e:
        print(f"  ! {username} {ts[:10]}: {str(e)[:120]}")
        return False


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if not os.path.exists(CSV_PATH):
        sys.exit(f"ERROR: CSV not found: {CSV_PATH}")

    print(f"Loading existing posts from Notion...")
    existing = load_existing_urls()
    print(f"  {len(existing)} posts already in DB")

    with open(CSV_PATH, newline="", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))

    print(f"CSV has {len(rows)} rows. Importing new ones...")
    created = skipped = errors = 0

    for i, row in enumerate(rows):
        url = row.get("url", "").strip()
        if url and url in existing:
            skipped += 1
            continue
        ok = add_post(row)
        if ok:
            created += 1
            existing.add(url)
            username = row.get("username","")
            ts = row.get("timestamp","")[:10]
            print(f"  [{i+1}/{len(rows)}] + {username} {ts}")
        else:
            errors += 1

    print(f"\nDone: {created} imported, {skipped} skipped, {errors} errors")

if __name__ == "__main__":
    main()
