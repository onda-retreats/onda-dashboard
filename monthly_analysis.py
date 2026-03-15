#!/usr/bin/env python3
"""
Monthly analysis script — runs on the 1st of each month via cron.
1. Scrapes new posts from tracked accounts via Apify
2. Imports new posts into Notion Posts DB
3. Analyzes engagement patterns
4. Creates new Insights in Notion Insights DB
5. Logs a report to monthly_reports/YYYY-MM.txt

Required env vars (from .env):
  NOTION_API_TOKEN
  NOTION_POSTS_DB_ID
  NOTION_INSIGHTS_DB_ID
  APIFY_API_TOKEN
"""
import csv
import io
import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path

from notion_client import Client


# ── Config ────────────────────────────────────────────────────────────────────

def load_env(path=None):
    if path is None:
        path = Path(__file__).parent / ".env"
    if not Path(path).exists():
        return
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

load_env()

NOTION_TOKEN     = os.environ.get("NOTION_API_TOKEN", "")
POSTS_DB_ID      = os.environ.get("NOTION_POSTS_DB_ID", "")
INSIGHTS_DB_ID   = os.environ.get("NOTION_INSIGHTS_DB_ID", "")
APIFY_TOKEN      = os.environ.get("APIFY_API_TOKEN", "")

ACCOUNTS_TO_SCRAPE = [
    "bonanzacollective",
    "scapers.club",
    "sheshe_retreats",
    "recreate.surfcamp",
    "balance___trip",
    "wylder.retreats",
    "viter.vie",
    "retreats.collective",
    "onda.retreats",
]

POSTS_PER_ACCOUNT = 30  # scrape last N posts per account
REPORT_DIR = Path(__file__).parent / "monthly_reports"

TYPE_MAP = {
    "sidecar": "Carousel", "carousel": "Carousel",
    "image": "Image", "photo": "Image",
    "reel": "Reel", "video": "Reel",
}

# ── Notion helpers ────────────────────────────────────────────────────────────

notion = Client(auth=NOTION_TOKEN) if NOTION_TOKEN else None

def TX(v): return {"rich_text": [{"text": {"content": str(v)[:2000]}}]}
def S(v):  return {"select": {"name": v}}
def C(v):  return {"checkbox": bool(v)}

def notion_add(db_id, props):
    try:
        notion.pages.create(parent={"database_id": db_id}, properties=props)
        time.sleep(0.35)
        return True
    except Exception as e:
        print(f"  ! Notion error: {str(e)[:100]}")
        return False

def load_existing_urls():
    if not notion or not POSTS_DB_ID:
        return set()
    urls = set()
    cursor = None
    while True:
        kwargs = {"database_id": POSTS_DB_ID, "page_size": 100}
        if cursor:
            kwargs["start_cursor"] = cursor
        resp = notion.databases.query(**kwargs)
        for page in resp.get("results", []):
            url = page.get("properties", {}).get("Post URL", {}).get("url")
            if url:
                urls.add(url.strip())
        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")
    return urls

def load_all_posts_this_month():
    """Fetch all posts from Notion created/timestamped in the previous month."""
    if not notion or not POSTS_DB_ID:
        return []
    now = datetime.now(timezone.utc)
    # analyze posts from last 30 days
    since = (now - timedelta(days=30)).isoformat()
    posts = []
    cursor = None
    while True:
        kwargs = {
            "database_id": POSTS_DB_ID,
            "page_size": 100,
            "filter": {
                "property": "Timestamp",
                "date": {"on_or_after": since}
            }
        }
        if cursor:
            kwargs["start_cursor"] = cursor
        resp = notion.databases.query(**kwargs)
        for page in resp.get("results", []):
            props = page.get("properties", {})
            def txt(key):
                parts = props.get(key, {}).get("rich_text", [])
                return "".join(p.get("plain_text","") for p in parts)
            def num(key):
                return props.get(key, {}).get("number") or 0
            def sel(key):
                s = props.get(key, {}).get("select")
                return s["name"] if s else ""
            posts.append({
                "username":   txt("Username"),
                "engagement": num("Engagement"),
                "likes":      num("Likes"),
                "comments":   num("Comments"),
                "post_type":  sel("Post Type"),
                "caption":    txt("Caption"),
            })
        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")
    return posts

# ── Apify scraper ─────────────────────────────────────────────────────────────

def scrape_account(username: str) -> list[dict]:
    """Scrape recent posts for one account via Apify."""
    try:
        from apify_client import ApifyClient
    except ImportError:
        print("  apify-client not installed. Run: pip install apify-client")
        return []

    if not APIFY_TOKEN:
        print("  APIFY_API_TOKEN not set — skipping scrape")
        return []

    client = ApifyClient(APIFY_TOKEN)
    run = client.actor("apify/instagram-scraper").call(run_input={
        "directUrls": [f"https://www.instagram.com/{username}/"],
        "resultsType": "posts",
        "resultsLimit": POSTS_PER_ACCOUNT,
        "addParentData": False,
    })
    items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
    posts = []
    for item in items:
        posts.append({
            "username":   username,
            "timestamp":  item.get("timestamp", ""),
            "url":        item.get("url", ""),
            "type":       item.get("type", "Image"),
            "likes":      item.get("likesCount") or 0,
            "comments":   item.get("commentsCount") or 0,
            "engagement": (item.get("likesCount") or 0) + (item.get("commentsCount") or 0),
            "caption":    (item.get("caption") or "")[:2000],
            "location":   item.get("locationName") or "",
        })
    return posts

# ── Import to Notion ──────────────────────────────────────────────────────────

def import_post(row: dict, existing_urls: set) -> bool:
    from urllib.parse import urlparse
    url = row.get("url", "").strip()
    if url in existing_urls:
        return False  # skip duplicate

    username  = row.get("username", "").strip()
    ts        = row.get("timestamp", "").strip()
    caption   = row.get("caption", "").strip()
    location  = row.get("location", "").strip()
    post_type = TYPE_MAP.get(str(row.get("type","")).strip().lower(), "Image")
    likes     = int(row.get("likes") or 0)
    comments  = int(row.get("comments") or 0)
    engagement= int(row.get("engagement") or 0)

    slug   = urlparse(url).path.rstrip("/").split("/")[-1] if url else ""
    date   = ts[:10].replace("-","") if ts else "unknown"
    post_id = f"{username}_{date}_{slug}" if slug else f"{username}_{date}"
    ts_iso  = ts.replace("Z", "+00:00") if ts.endswith("Z") else ts

    props = {
        "Post ID":    {"title": [{"text": {"content": post_id[:2000]}}]},
        "Username":   TX(username),
        "Post Type":  S(post_type),
        "Likes":      {"number": likes},
        "Comments":   {"number": comments},
        "Engagement": {"number": engagement},
    }
    if ts_iso:
        props["Timestamp"] = {"date": {"start": ts_iso}}
    if url:
        props["Post URL"] = {"url": url}
    if caption:
        props["Caption"] = TX(caption)
    if location:
        props["Location"] = TX(location)

    ok = notion_add(POSTS_DB_ID, props)
    if ok:
        existing_urls.add(url)
    return ok

# ── Analysis ──────────────────────────────────────────────────────────────────

def analyze_posts(posts: list[dict]) -> dict:
    """Compute engagement stats by type, account, identify top posts."""
    if not posts:
        return {}

    by_type = defaultdict(list)
    by_account = defaultdict(list)
    for p in posts:
        by_type[p["post_type"]].append(p["engagement"])
        by_account[p["username"]].append(p["engagement"])

    type_avg = {t: int(sum(v)/len(v)) for t, v in by_type.items() if v}
    acct_avg = {a: int(sum(v)/len(v)) for a, v in by_account.items() if v}

    total_eng = [p["engagement"] for p in posts]
    overall_avg = int(sum(total_eng) / len(total_eng)) if total_eng else 0

    top_posts = sorted(posts, key=lambda p: p["engagement"], reverse=True)[:5]

    best_type = max(type_avg, key=type_avg.get) if type_avg else "N/A"
    best_account = max(acct_avg, key=acct_avg.get) if acct_avg else "N/A"

    return {
        "total_posts": len(posts),
        "overall_avg": overall_avg,
        "by_type": type_avg,
        "by_account": acct_avg,
        "best_type": best_type,
        "best_account": best_account,
        "top_posts": top_posts,
    }

def build_insights(stats: dict, month_str: str) -> list[dict]:
    """Generate Insight entries based on analysis."""
    insights = []
    if not stats:
        return insights

    best_type = stats.get("best_type", "")
    type_avgs = stats.get("by_type", {})
    overall = stats.get("overall_avg", 0)

    # Insight 1: Best performing post type this month
    if best_type and type_avgs:
        best_avg = type_avgs.get(best_type, 0)
        summary_parts = [f"{t}: avg {v}" for t, v in sorted(type_avgs.items(), key=lambda x: -x[1])]
        insights.append({
            "title": f"[{month_str}] Best format: {best_type} (avg {best_avg})",
            "type": "Format Performance",
            "summary": "Monthly format breakdown — " + ", ".join(summary_parts),
            "impact": "High (>500 avg)" if best_avg > 500 else ("Medium (200-500)" if best_avg > 200 else "Low (<200)"),
            "conf": "Confirmed (3+ posts)",
            "priority": "P2 This Month",
        })

    # Insight 2: Top account this month
    best_acct = stats.get("best_account", "")
    acct_avgs = stats.get("by_account", {})
    if best_acct:
        acct_avg = acct_avgs.get(best_acct, 0)
        insights.append({
            "title": f"[{month_str}] Top account: @{best_acct} (avg {acct_avg})",
            "type": "Competitor Move",
            "summary": f"@{best_acct} led engagement this month with avg {acct_avg} vs overall {overall}.",
            "impact": "High (>500 avg)" if acct_avg > 500 else "Medium (200-500)",
            "conf": "Confirmed (3+ posts)",
            "priority": "P2 This Month",
        })

    # Insight 3: Top post hook
    top_posts = stats.get("top_posts", [])
    if top_posts:
        top = top_posts[0]
        caption_preview = top.get("caption","")[:100]
        insights.append({
            "title": f"[{month_str}] Top post: @{top['username']} ({top['engagement']} eng)",
            "type": "Hook Pattern",
            "summary": f"Highest engagement post this month: {caption_preview}...",
            "impact": "High (>500 avg)" if top["engagement"] > 500 else "Medium (200-500)",
            "conf": "Emerging (2 posts)",
            "priority": "P1 Urgent",
        })

    return insights

# ── Report ────────────────────────────────────────────────────────────────────

def write_report(stats: dict, new_posts: int, month_str: str):
    REPORT_DIR.mkdir(exist_ok=True)
    path = REPORT_DIR / f"{month_str}.txt"
    lines = [
        f"Monthly Analysis Report — {month_str}",
        f"Generated: {datetime.now().isoformat()}",
        "=" * 50,
        f"New posts scraped & imported: {new_posts}",
        f"Posts analyzed (last 30 days): {stats.get('total_posts', 0)}",
        f"Overall avg engagement: {stats.get('overall_avg', 0)}",
        "",
        "Engagement by post type:",
    ]
    for t, avg in sorted(stats.get("by_type", {}).items(), key=lambda x: -x[1]):
        lines.append(f"  {t}: {avg}")
    lines += ["", "Engagement by account:"]
    for a, avg in sorted(stats.get("by_account", {}).items(), key=lambda x: -x[1]):
        lines.append(f"  @{a}: {avg}")
    lines += ["", "Top 5 posts:"]
    for i, p in enumerate(stats.get("top_posts", []), 1):
        lines.append(f"  {i}. @{p['username']} — {p['engagement']} eng")
        lines.append(f"     {p.get('caption','')[:80]}")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"Report saved: {path}")

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if not NOTION_TOKEN:
        sys.exit("ERROR: NOTION_API_TOKEN not set")
    if not POSTS_DB_ID:
        sys.exit("ERROR: NOTION_POSTS_DB_ID not set (run setup_full.py first)")

    month_str = datetime.now().strftime("%Y-%m")
    print(f"\n=== Monthly Analysis — {month_str} ===\n")

    # 1. Scrape new posts
    print("Step 1: Scraping accounts via Apify...")
    all_new = []
    for username in ACCOUNTS_TO_SCRAPE:
        print(f"  Scraping @{username}...")
        posts = scrape_account(username)
        print(f"    {len(posts)} posts fetched")
        all_new.extend(posts)
    print(f"Total scraped: {len(all_new)}")

    # 2. Import new posts
    print("\nStep 2: Importing new posts to Notion...")
    existing = load_existing_urls()
    imported = sum(1 for p in all_new if import_post(p, existing))
    print(f"Imported: {imported} new posts")

    # 3. Analyze
    print("\nStep 3: Analyzing patterns...")
    posts_to_analyze = load_all_posts_this_month()
    stats = analyze_posts(posts_to_analyze)
    print(f"Analyzed {stats.get('total_posts',0)} posts")
    print(f"Overall avg engagement: {stats.get('overall_avg',0)}")
    print(f"Best format: {stats.get('best_type','N/A')}")

    # 4. Write insights to Notion
    if INSIGHTS_DB_ID:
        print("\nStep 4: Writing insights to Notion...")
        insights = build_insights(stats, month_str)
        today = datetime.now().strftime("%Y-%m-%d")
        for ins in insights:
            props = {
                "Insight Title": {"title": [{"text": {"content": ins["title"]}}]},
                "Insight Type":  S(ins["type"]),
                "Summary":       TX(ins["summary"]),
                "Engagement Impact": S(ins["impact"]),
                "Confidence":    S(ins["conf"]),
                "Applicable to Onda": C(True),
                "Status":        S("New"),
                "Priority":      S(ins["priority"]),
                "Date Detected": {"date": {"start": today}},
            }
            if notion_add(INSIGHTS_DB_ID, props):
                print(f"  + {ins['title'][:60]}")
    else:
        print("Step 4: NOTION_INSIGHTS_DB_ID not set — skipping insights")

    # 5. Save local report
    print("\nStep 5: Saving local report...")
    write_report(stats, imported, month_str)

    print(f"\nDone! Monthly analysis complete for {month_str}")

if __name__ == "__main__":
    main()
