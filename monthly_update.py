#!/usr/bin/env python3
"""
Monthly update script — Onda Retreats Content Intelligence.
Runs on the 1st of each month at 09:00 via cron:
  0 9 1 * * cd /Users/katya/WORK/instagram_ai && python3 monthly_update.py >> logs/monthly.log 2>&1

Required env vars (.env or environment):
  APIFY_API_TOKEN       — Instagram scraping (вже є)
  ANTHROPIC_API_KEY     — AI recommendations (console.anthropic.com)
  GITHUB_TOKEN          — git push to GitHub Pages (github.com/settings/tokens, scope: repo)
  TELEGRAM_BOT_TOKEN    — Telegram notifications (від @BotFather)
  TELEGRAM_CHAT_ID      — Telegram chat/channel ID (від @userinfobot)
"""

import csv
import json
import os
import re
import subprocess
import sys
import time
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────

BASE_DIR = Path(__file__).parent


def load_env(path=None):
    p = Path(path or BASE_DIR / ".env")
    if not p.exists():
        return
    with open(p) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())


load_env()

APIFY_TOKEN      = os.environ.get("APIFY_API_TOKEN", "")
ANTHROPIC_KEY    = os.environ.get("ANTHROPIC_API_KEY", "")
GITHUB_TOKEN     = os.environ.get("GITHUB_TOKEN", "")
TG_BOT_TOKEN     = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TG_CHAT_ID       = os.environ.get("TELEGRAM_CHAT_ID", "")

CSV_PATH         = BASE_DIR / "all_retreat_accounts.csv"
ACCOUNTS_PATH    = BASE_DIR / "accounts.json"
SNAPSHOTS_DIR    = BASE_DIR / "monthly_snapshots"
LOGS_DIR         = BASE_DIR / "logs"
DASHBOARD_PATH   = BASE_DIR / "dashboard.html"

POSTS_PER_ACCOUNT = 50  # scrape last N posts

TYPE_MAP = {
    "sidecar": "Carousel", "carousel": "Carousel",
    "image": "Image",      "photo": "Image",
    "reel": "Reel",        "video": "Reel",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def load_accounts():
    if not ACCOUNTS_PATH.exists():
        return {
            "own": ["onda.retreats"],
            "competitors": [
                "bonanzacollective", "scapers.club", "sheshe_retreats",
                "recreate.surfcamp", "balance___trip", "wylder.retreats",
                "viter.vie", "retreats.collective",
            ],
            "inspiration": [],
        }
    with open(ACCOUNTS_PATH) as f:
        return json.load(f)


def get_category(username, accounts):
    if username in accounts.get("own", []):
        return "own"
    if username in accounts.get("competitors", []):
        return "competitor"
    if username in accounts.get("inspiration", []):
        return "inspiration"
    return "competitor"


# ── Step 1: Scrape via Apify ──────────────────────────────────────────────────

def scrape_account(username: str) -> list[dict]:
    if not APIFY_TOKEN:
        log(f"  APIFY_API_TOKEN not set — skipping @{username}")
        return []
    try:
        from apify_client import ApifyClient
    except ImportError:
        log("  apify-client not installed. Run: pip install apify-client")
        return []

    log(f"  Scraping @{username}...")
    try:
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
                "username":  username,
                "timestamp": item.get("timestamp", ""),
                "url":       item.get("url", ""),
                "type":      TYPE_MAP.get(str(item.get("type", "")).lower(), "Image"),
                "likes":     item.get("likesCount") or 0,
                "comments":  item.get("commentsCount") or 0,
                "engagement": (item.get("likesCount") or 0) + (item.get("commentsCount") or 0),
                "caption":   (item.get("caption") or "")[:2000],
                "location":  item.get("locationName") or "",
            })
        log(f"    → {len(posts)} posts")
        return posts
    except Exception as e:
        log(f"    ! Scrape error for @{username}: {str(e)[:120]}")
        return []


def scrape_all(accounts: dict) -> list[dict]:
    all_accounts = (
        accounts.get("own", []) +
        accounts.get("competitors", []) +
        accounts.get("inspiration", [])
    )
    results = []
    for username in all_accounts:
        posts = scrape_account(username)
        results.extend(posts)
        time.sleep(2)  # rate limiting
    return results


# ── Step 2: Update CSV ────────────────────────────────────────────────────────

def update_csv(new_posts: list[dict], accounts: dict) -> int:
    # Load existing
    existing_urls = set()
    existing_rows = []
    fieldnames = ["username", "timestamp", "url", "type", "likes", "comments",
                  "engagement", "caption", "location", "account_category"]

    if CSV_PATH.exists():
        with open(CSV_PATH, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames:
                fieldnames = list(reader.fieldnames)
                if "account_category" not in fieldnames:
                    fieldnames.append("account_category")
            for row in reader:
                existing_rows.append(row)
                if row.get("url"):
                    existing_urls.add(row["url"].strip())

    added = 0
    for post in new_posts:
        url = post.get("url", "").strip()
        if url and url in existing_urls:
            continue
        row = {
            "username":          post.get("username", ""),
            "timestamp":         post.get("timestamp", ""),
            "url":               url,
            "type":              post.get("type", "Image"),
            "likes":             post.get("likes", 0),
            "comments":          post.get("comments", 0),
            "engagement":        post.get("engagement", 0),
            "caption":           post.get("caption", ""),
            "location":          post.get("location", ""),
            "account_category":  get_category(post.get("username", ""), accounts),
        }
        existing_rows.append(row)
        if url:
            existing_urls.add(url)
        added += 1

    # Write back
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(existing_rows)

    log(f"CSV updated: {added} new posts added ({len(existing_rows)} total)")
    return added


# ── Step 3: Analyse ───────────────────────────────────────────────────────────

def analyse(accounts: dict) -> dict:
    rows = []
    with open(CSV_PATH, newline="", encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            rows.append(r)

    now_ts = datetime.now(timezone.utc)
    month_str = now_ts.strftime("%Y-%m")
    prev_month_str = (now_ts.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")

    def eng(r):
        return int(r.get("engagement") or 0)

    # Current month posts
    curr = [r for r in rows if r.get("timestamp", "")[:7] == month_str]
    prev_snap_path = SNAPSHOTS_DIR / f"{prev_month_str}.json"
    prev_stats = {}
    if prev_snap_path.exists():
        with open(prev_snap_path) as f:
            prev_stats = json.load(f)

    own_accts = accounts.get("own", [])
    comp_accts = accounts.get("competitors", [])
    insp_accts = accounts.get("inspiration", [])

    onda_posts = [r for r in curr if r.get("username") in own_accts]
    comp_posts = [r for r in curr if r.get("username") in comp_accts]
    insp_posts = [r for r in curr if r.get("username") in insp_accts]

    def avg(lst):
        return int(sum(eng(r) for r in lst) / len(lst)) if lst else 0

    onda_avg = avg(onda_posts)
    comp_avg = avg(comp_posts)

    by_type = defaultdict(list)
    for r in curr:
        t = TYPE_MAP.get(r.get("type", "").lower(), "Image")
        by_type[t].append(eng(r))
    type_avgs = {t: int(sum(v) / len(v)) for t, v in by_type.items() if v}
    best_type = max(type_avgs, key=type_avgs.get) if type_avgs else "Carousel"

    top_posts = sorted(rows, key=eng, reverse=True)[:20]

    stats = {
        "month": month_str,
        "onda_avg": onda_avg,
        "comp_avg": comp_avg,
        "best_type": best_type,
        "best_type_avg": type_avgs.get(best_type, 0),
        "type_avgs": type_avgs,
        "new_posts_count": len(curr),
        "insp_posts_count": len(insp_posts),
        "top_posts": [{
            "username": p.get("username", ""),
            "timestamp": p.get("timestamp", "")[:10],
            "type": TYPE_MAP.get(p.get("type", "").lower(), "Image"),
            "engagement": eng(p),
            "caption": (p.get("caption") or "")[:200],
            "url": p.get("url", ""),
            "category": p.get("account_category", "competitor"),
        } for p in top_posts],
        "insp_top_posts": [{
            "username": p.get("username", ""),
            "timestamp": p.get("timestamp", "")[:10],
            "type": TYPE_MAP.get(p.get("type", "").lower(), "Image"),
            "engagement": eng(p),
            "caption": (p.get("caption") or "")[:300],
            "url": p.get("url", ""),
        } for p in sorted(insp_posts, key=eng, reverse=True)[:10]],
    }

    # Deltas
    stats["onda_delta"] = (
        round((onda_avg - prev_stats.get("onda_avg", onda_avg)) / max(prev_stats.get("onda_avg", 1), 1) * 100, 1)
        if prev_stats else None
    )
    stats["comp_delta"] = (
        round((comp_avg - prev_stats.get("comp_avg", comp_avg)) / max(prev_stats.get("comp_avg", 1), 1) * 100, 1)
        if prev_stats else None
    )

    # Save snapshot
    SNAPSHOTS_DIR.mkdir(exist_ok=True)
    snap_path = SNAPSHOTS_DIR / f"{month_str}.json"
    with open(snap_path, "w") as f:
        json.dump({k: v for k, v in stats.items() if k not in ("top_posts", "insp_top_posts")}, f)

    return stats


# ── Step 4: AI Recommendations ────────────────────────────────────────────────

def format_top_posts(posts: list[dict]) -> str:
    lines = []
    for i, p in enumerate(posts, 1):
        lines.append(
            f"{i}. @{p['username']} [{p['type']}] {p['engagement']} eng\n"
            f"   \"{p['caption'][:100]}\""
        )
    return "\n".join(lines)


def get_ai_recommendations(stats: dict) -> dict:
    if not ANTHROPIC_KEY:
        log("  ANTHROPIC_API_KEY not set — skipping AI recommendations")
        return {}
    try:
        import anthropic
    except ImportError:
        log("  anthropic not installed. Run: pip install anthropic")
        return {}

    month_str = stats.get("month", "")
    delta_str = f"{stats['onda_delta']:+.1f}%" if stats.get("onda_delta") is not None else "немає даних"

    prompt = f"""Ти контент-стратег для Onda Retreats — жіночого wellness retreat бренду в Україні.

Дані за {month_str}:
- Onda avg engagement: {stats['onda_avg']} (зміна vs минулий місяць: {delta_str})
- Конкуренти avg: {stats['comp_avg']}
- Найкращий формат: {stats['best_type']} (avg {stats['best_type_avg']})
- Нових постів в датасеті: {stats['new_posts_count']}

Топ-5 постів цього місяця:
{format_top_posts(stats['top_posts'][:5])}

Пости з inspiration акаунтів (топ-5):
{format_top_posts(stats['insp_top_posts'][:5]) if stats['insp_top_posts'] else 'Поки немає inspiration акаунтів.'}

Згенеруй відповідь у JSON форматі:
{{
  "insights": [
    "Інсайт 1 з конкретними числами",
    "Інсайт 2 з конкретними числами",
    "Інсайт 3 з конкретними числами"
  ],
  "whats_good": "Що Onda робить добре — 2-3 речення з даними",
  "improve": "Що покращити — 2-3 конкретних дії з прикладами від конкурентів",
  "reel_ideas": [
    {{"hook": "Хук рілсу", "desc": "Короткий опис концепції", "cat": "Трансформація"}},
    {{"hook": "...", "desc": "...", "cat": "..."}},
    {{"hook": "...", "desc": "...", "cat": "..."}},
    {{"hook": "...", "desc": "...", "cat": "..."}},
    {{"hook": "...", "desc": "...", "cat": "..."}}
  ],
  "inspiration_adaptations": [
    {{
      "original_account": "@назва",
      "original_hook": "Перший рядок їх caption",
      "adaptation_hook": "Хук для Onda в їхньому стилі",
      "adaptation_concept": "Короткий опис як зробити в стилі Onda",
      "format": "Carousel",
      "difficulty": "Легко"
    }}
  ],
  "content_plan": [
    {{"date": "пн 7 {month_str[-2:]}.", "format": "Carousel", "type": "UGC", "hook": "Хук посту"}},
    {{"date": "ср 9 {month_str[-2:]}.", "format": "Reel", "type": "Практика", "hook": "..."}},
    {{"date": "пт 11 {month_str[-2:]}.", "format": "Carousel", "type": "FOMO", "hook": "..."}},
    {{"date": "пн 14 {month_str[-2:]}.", "format": "Carousel", "type": "Освіта", "hook": "..."}},
    {{"date": "ср 16 {month_str[-2:]}.", "format": "Carousel", "type": "Storytelling", "hook": "..."}},
    {{"date": "пт 18 {month_str[-2:]}.", "format": "Image", "type": "Цитата", "hook": "..."}},
    {{"date": "пн 21 {month_str[-2:]}.", "format": "Carousel", "type": "Закулісся", "hook": "..."}},
    {{"date": "ср 23 {month_str[-2:]}.", "format": "Reel", "type": "Локація", "hook": "..."}},
    {{"date": "пт 25 {month_str[-2:]}.", "format": "Carousel", "type": "Анонс", "hook": "..."}},
    {{"date": "пн 28 {month_str[-2:]}.", "format": "Carousel", "type": "UGC", "hook": "..."}},
    {{"date": "ср 30 {month_str[-2:]}.", "format": "Carousel", "type": "Спільнота", "hook": "..."}},
    {{"date": "пт 1.", "format": "Image", "type": "Підсумок", "hook": "..."}}
  ]
}}

Відповідай тільки валідним JSON. Мова: українська. Будь конкретним з числами."""

    log("  Calling Claude API...")
    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
        msg = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=3000,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = msg.content[0].text.strip()
        # Extract JSON if wrapped in markdown
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        if m:
            raw = m.group(0)
        result = json.loads(raw)
        log(f"  AI: {len(result.get('reel_ideas', []))} reel ideas, "
            f"{len(result.get('content_plan', []))} plan slots")
        return result
    except Exception as e:
        log(f"  ! AI error: {str(e)[:200]}")
        return {}


# ── Step 5: Update dashboard data ─────────────────────────────────────────────

def update_dashboard(stats: dict, ai_recs: dict, month_str: str):
    """
    Regenerate dashboard.html by calling build_dashboard.py if it exists,
    otherwise update the data JSON block in-place using a safe delimiter approach.
    """
    build_script = BASE_DIR / "build_dashboard.py"
    if build_script.exists():
        log("  Running build_dashboard.py...")
        env = os.environ.copy()
        result = subprocess.run(
            [sys.executable, str(build_script)],
            cwd=BASE_DIR, capture_output=True, text=True, env=env,
        )
        if result.returncode != 0:
            log(f"  ! build_dashboard.py error: {result.stderr[:300]}")
        else:
            log("  Dashboard rebuilt.")
        return

    # Fallback: inject new data via safe split on unique marker
    if not DASHBOARD_PATH.exists():
        log("  dashboard.html not found — skipping update")
        return

    log("  Updating dashboard data in-place...")
    with open(DASHBOARD_PATH, encoding="utf-8") as f:
        html = f.read()

    # Build updated data JSON from CSV
    import csv as csv_mod
    from collections import defaultdict as dd

    rows = []
    with open(CSV_PATH, newline="", encoding="utf-8-sig") as f:
        for r in csv_mod.DictReader(f):
            rows.append(r)

    def _eng(r):
        return int(r.get("engagement") or 0)

    # Compute all stats
    acct_data = {}
    for r in rows:
        u = r["username"]
        if u not in acct_data:
            acct_data[u] = {"username": u, "count": 0, "total": 0, "max": 0,
                            "carousel": [], "reel": [], "image": []}
        e = _eng(r)
        t = TYPE_MAP.get(r.get("type", "").lower(), "Image").lower()
        acct_data[u]["count"] += 1
        acct_data[u]["total"] += e
        acct_data[u]["max"] = max(acct_data[u]["max"], e)
        acct_data[u][t].append(e)

    acct_stats = []
    for u, d in acct_data.items():
        acct_stats.append({
            "username": u, "count": d["count"],
            "avg": d["total"] // d["count"],
            "max": d["max"], "total": d["total"],
            "carousel_avg": int(sum(d["carousel"]) / len(d["carousel"])) if d["carousel"] else 0,
            "carousel_count": len(d["carousel"]),
            "reel_avg": int(sum(d["reel"]) / len(d["reel"])) if d["reel"] else 0,
            "reel_count": len(d["reel"]),
            "image_avg": int(sum(d["image"]) / len(d["image"])) if d["image"] else 0,
            "image_count": len(d["image"]),
        })

    by_type_d = dd(list)
    for r in rows:
        by_type_d[TYPE_MAP.get(r.get("type", "").lower(), "Image")].append(_eng(r))
    type_stats = [{"type": t, "count": len(v), "avg": int(sum(v) / len(v)), "max": max(v)}
                  for t, v in by_type_d.items()]

    monthly_all = dd(list)
    monthly_onda = dd(list)
    monthly_comp = dd(list)
    for r in rows:
        m = r.get("timestamp", "")[:7]
        e = _eng(r)
        monthly_all[m].append(e)
        if r.get("username") == "onda.retreats":
            monthly_onda[m].append(e)
        else:
            monthly_comp[m].append(e)

    all_months = sorted(monthly_all.keys())[-12:]
    monthly_trend = [{
        "month": m,
        "all_avg": int(sum(monthly_all[m]) / len(monthly_all[m])) if monthly_all.get(m) else 0,
        "onda_avg": int(sum(monthly_onda[m]) / len(monthly_onda[m])) if monthly_onda.get(m) else 0,
        "comp_avg": int(sum(monthly_comp[m]) / len(monthly_comp[m])) if monthly_comp.get(m) else 0,
        "all_count": len(monthly_all.get(m, [])),
    } for m in all_months]

    top30 = sorted(rows, key=_eng, reverse=True)[:30]
    onda_posts = sorted([r for r in rows if r.get("username") == "onda.retreats"], key=_eng, reverse=True)
    all_posts = sorted([{
        "username": r["username"], "timestamp": r.get("timestamp", "")[:10],
        "type": TYPE_MAP.get(r.get("type", "").lower(), "Image"),
        "likes": int(r.get("likes") or 0), "comments": int(r.get("comments") or 0),
        "engagement": _eng(r), "caption": (r.get("caption") or "")[:80],
        "url": r.get("url", ""),
    } for r in rows], key=lambda x: -x["engagement"])

    def to_row(r):
        return {
            "username": r.get("username", ""),
            "timestamp": r.get("timestamp", "")[:10],
            "type": TYPE_MAP.get(r.get("type", "").lower(), "Image"),
            "likes": int(r.get("likes") or 0), "comments": int(r.get("comments") or 0),
            "engagement": _eng(r), "caption": (r.get("caption") or "")[:120],
            "url": r.get("url", ""),
        }

    # Inspiration posts
    insp_accounts = load_accounts().get("inspiration", [])
    insp_posts = sorted([r for r in rows if r.get("username") in insp_accounts], key=_eng, reverse=True)
    insp_data = [to_row(r) for r in insp_posts[:30]]

    # AI reel ideas
    new_reel_ideas = ai_recs.get("reel_ideas", [])
    inspiration_adaptations = ai_recs.get("inspiration_adaptations", [])

    # Extract existing reel_ideas from current HTML to preserve them
    m = re.search(r'"reel_ideas"\s*:\s*(\[.*?\])\s*,\s*"pipeline"', html, re.DOTALL)
    existing_reel_ideas = []
    if m:
        try:
            existing_reel_ideas = json.loads(m.group(1))
        except Exception:
            pass

    # Merge: add new ideas with new IDs
    max_id = max((r.get("id", 0) for r in existing_reel_ideas), default=50)
    for i, idea in enumerate(new_reel_ideas):
        idea["id"] = max_id + i + 1
        if "tags" not in idea:
            idea["tags"] = [idea.get("cat", "").lower().replace(" ", "-")]
    merged_ideas = existing_reel_ideas + [
        {"id": r["id"], "cat": r.get("cat", "Нові"), "hook": r.get("hook", ""),
         "desc": r.get("desc", ""), "tags": r.get("tags", [])}
        for r in new_reel_ideas
    ]

    # Pipeline from AI content plan
    ai_plan = ai_recs.get("content_plan", [])

    # Read current pipeline data from HTML
    existing_data_match = re.search(r'var DASH_DATA\s*=\s*(\{.*?\});\s*</script>', html, re.DOTALL)
    pipeline_data = []
    if existing_data_match:
        try:
            existing_data = json.loads(existing_data_match.group(1))
            pipeline_data = existing_data.get("pipeline", [])
        except Exception:
            pass

    new_data = {
        "acct_stats": sorted(acct_stats, key=lambda x: -x["avg"]),
        "type_stats": type_stats,
        "monthly_trend": monthly_trend,
        "top30": [to_row(r) for r in top30],
        "onda_posts": [to_row(r) for r in onda_posts],
        "all_posts": all_posts,
        "reel_ideas": merged_ideas,
        "pipeline": pipeline_data,
        "insp_posts": insp_data,
        "inspiration_adaptations": inspiration_adaptations,
        "totals": {
            "posts": len(rows),
            "accounts": len(acct_data),
            "avg_eng": int(sum(_eng(r) for r in rows) / len(rows)) if rows else 0,
            "onda_avg": acct_data.get("onda.retreats", {}).get("total", 0) // max(acct_data.get("onda.retreats", {}).get("count", 1), 1),
            "comp_avg": int(sum(d["total"] for u, d in acct_data.items() if u != "onda.retreats") /
                           max(sum(d["count"] for u, d in acct_data.items() if u != "onda.retreats"), 1)),
            "month": month_str,
        },
    }

    new_data_json = json.dumps(new_data, ensure_ascii=False)

    # Safe replacement using script tag split
    marker_start = "var DASH_DATA = "
    marker_end = ";\n</script>"
    if marker_start in html:
        idx_s = html.index(marker_start)
        idx_e = html.index(marker_end, idx_s) + len(marker_end)
        html_new = (html[:idx_s] + marker_start + new_data_json + ";\n</script>" + html[idx_e:])

        # Also update header date
        html_new = re.sub(
            r'березень \d{4}|квітень \d{4}|травень \d{4}|червень \d{4}|'
            r'липень \d{4}|серпень \d{4}|вересень \d{4}|жовтень \d{4}|'
            r'листопад \d{4}|грудень \d{4}|січень \d{4}|лютий \d{4}',
            month_str, html_new
        )

        with open(DASHBOARD_PATH, "w", encoding="utf-8") as f:
            f.write(html_new)
        log("  Dashboard data updated.")
    else:
        log("  ! Could not find DASH_DATA marker — dashboard not updated")


# ── Step 6: Git push ──────────────────────────────────────────────────────────

def git_push(month_str: str):
    if not GITHUB_TOKEN:
        log("  GITHUB_TOKEN not set — skipping git push")
        return False

    env = os.environ.copy()
    # Configure git to use token for HTTPS
    remote_url = subprocess.run(
        ["git", "remote", "get-url", "origin"],
        cwd=BASE_DIR, capture_output=True, text=True
    ).stdout.strip()
    if remote_url.startswith("https://"):
        # Strip any existing token, then inject fresh one
        clean_url = re.sub(r"https://[^@]+@", "https://", remote_url)
        auth_url = clean_url.replace("https://", f"https://{GITHUB_TOKEN}@", 1)
        subprocess.run(["git", "remote", "set-url", "origin", auth_url],
                       cwd=BASE_DIR, capture_output=True)

    # GitHub Pages requires index.html
    import shutil
    shutil.copy(BASE_DIR / "dashboard.html", BASE_DIR / "index.html")

    cmds = [
        ["git", "add", "dashboard.html", "index.html", "all_retreat_accounts.csv", "accounts.json"],
        ["git", "commit", "-m", f"Monthly update: {month_str}"],
        ["git", "push", "origin", "main"],
    ]
    for cmd in cmds:
        result = subprocess.run(cmd, cwd=BASE_DIR, capture_output=True, text=True, env=env)
        if result.returncode != 0:
            out = result.stdout + result.stderr
            log(f"  Git ({' '.join(cmd[:2])}): {out[:200]}")
            skip_phrases = ("nothing to commit", "nothing added to commit",
                            "Changes not staged", "up to date", "already up to date")
            if any(p in out for p in skip_phrases):
                continue
            return False
    log("  Pushed to GitHub.")
    return True


# ── Step 7: Telegram notification ────────────────────────────────────────────

def send_telegram(message: str):
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        log("  TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set — skipping")
        return
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    data = json.dumps({
        "chat_id": TG_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": False,
    }).encode("utf-8")
    req = urllib.request.Request(url, data=data,
                                  headers={"Content-Type": "application/json"})
    try:
        urllib.request.urlopen(req, timeout=15)
        log("  Telegram notification sent.")
    except Exception as e:
        log(f"  ! Telegram error: {e}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    LOGS_DIR.mkdir(exist_ok=True)
    SNAPSHOTS_DIR.mkdir(exist_ok=True)

    month_str = datetime.now().strftime("%Y-%m")
    log(f"\n{'='*60}")
    log(f"Monthly Update — {month_str}")
    log(f"{'='*60}\n")

    accounts = load_accounts()
    all_accts = accounts["own"] + accounts["competitors"] + accounts.get("inspiration", [])
    log(f"Accounts: {len(all_accts)} total "
        f"({len(accounts['own'])} own, {len(accounts['competitors'])} competitors, "
        f"{len(accounts.get('inspiration', []))} inspiration)\n")

    results = {"scraped": 0, "new_posts": 0, "ai": False, "dashboard": False, "git": False, "telegram": False}

    # Step 1
    log("── Step 1: Scraping ─────────────────────────────────")
    new_posts = scrape_all(accounts)
    results["scraped"] = len(new_posts)
    log(f"Total scraped: {len(new_posts)} posts\n")

    # Step 2
    log("── Step 2: Updating CSV ─────────────────────────────")
    added = update_csv(new_posts, accounts)
    results["new_posts"] = added
    log("")

    # Step 3
    log("── Step 3: Analysis ─────────────────────────────────")
    stats = analyse(accounts)
    log(f"Onda avg: {stats['onda_avg']}  |  Competitors avg: {stats['comp_avg']}")
    log(f"Best format: {stats['best_type']} ({stats['best_type_avg']} avg)\n")

    # Step 4
    log("── Step 4: AI Recommendations ───────────────────────")
    ai_recs = get_ai_recommendations(stats)
    if ai_recs:
        results["ai"] = True
        for insight in ai_recs.get("insights", []):
            log(f"  💡 {insight}")
    log("")

    # Step 5
    log("── Step 5: Update Dashboard ─────────────────────────")
    try:
        update_dashboard(stats, ai_recs, month_str)
        results["dashboard"] = True
    except Exception as e:
        log(f"  ! Dashboard update error: {e}")
    log("")

    # Step 6
    log("── Step 6: Git Push ──────────────────────────────────")
    results["git"] = git_push(month_str)
    log("")

    # Step 7
    log("── Step 7: Notifications ────────────────────────────")
    first_insight = ai_recs.get("insights", [""])[0] if ai_recs else "—"
    msg = (
        f"✅ <b>Onda Dashboard оновлено за {month_str}</b>\n\n"
        f"📊 Нових постів: {results['new_posts']}\n"
        f"📈 Onda avg engagement: {stats['onda_avg']}"
        + (f" ({stats['onda_delta']:+.1f}%)" if stats.get('onda_delta') is not None else "") + "\n"
        f"🏆 Топ формат: {stats['best_type']} ({stats['best_type_avg']} avg)\n\n"
        f"💡 Топ інсайт: {first_insight}\n\n"
        f"🔗 Відкрити: <a href='https://onda-retreats.github.io/onda-dashboard/'>dashboard</a>"
    )
    send_telegram(msg)
    results["telegram"] = True
    log("")

    # Summary
    log("── Summary ───────────────────────────────────────────")
    log(f"  Scraped:       {results['scraped']} posts")
    log(f"  New in CSV:    {results['new_posts']}")
    log(f"  AI recs:       {'✓' if results['ai'] else '✗ (no ANTHROPIC_API_KEY)' if not ANTHROPIC_KEY else '✗'}")
    log(f"  Dashboard:     {'✓' if results['dashboard'] else '✗'}")
    log(f"  Git push:      {'✓' if results['git'] else '✗ (no GITHUB_TOKEN)' if not GITHUB_TOKEN else '✗ (push failed)'}")
    log(f"  Telegram:      {'✓' if results['telegram'] else '✗ (no token/chat_id)'}")
    log(f"\nDone — {month_str}\n")


if __name__ == "__main__":
    main()
