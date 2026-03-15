#!/usr/bin/env python3
"""
Onda Content Intelligence — Telegram Bot
=========================================
Команди:
  /add_competitor @username    — додати конкурента
  /add_inspiration @username   — додати натхнення
  /remove @username            — видалити (з будь-якої категорії)
  /list                        — всі акаунти
  /run                         — повний аналіз
  /run_inspiration             — тільки inspiration
  /run_competitors             — тільки competitors
  /status                      — статус дашборду

Без команди: instagram.com/... або @username → бот питає куди додати.

Запуск:
  python3 -m pip install "python-telegram-bot==20.7"
  python3 tg_bot.py
"""
import json
import logging
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# ── Config ────────────────────────────────────────────────────────────────────

BASE_DIR = Path(__file__).parent


def load_env():
    env_path = BASE_DIR / ".env"
    if not env_path.exists():
        return
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())


load_env()

BOT_TOKEN     = os.environ.get("TELEGRAM_BOT_TOKEN", "")
ALLOWED_CHAT  = os.environ.get("TELEGRAM_CHAT_ID", "")
DASHBOARD_URL = os.environ.get("DASHBOARD_URL", "https://github.com")
ACCOUNTS_PATH = BASE_DIR / "accounts.json"
SNAPSHOTS_DIR = BASE_DIR / "monthly_snapshots"
UPDATE_SCRIPT = BASE_DIR / "monthly_update.py"
LOG_DIR       = BASE_DIR / "logs"

MONTHS_UA = {
    "01": "січня", "02": "лютого", "03": "березня", "04": "квітня",
    "05": "травня", "06": "червня", "07": "липня", "08": "серпня",
    "09": "вересня", "10": "жовтня", "11": "листопада", "12": "грудня",
}

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger(__name__)


# ── accounts.json helpers ─────────────────────────────────────────────────────

def read_accounts():
    if not ACCOUNTS_PATH.exists():
        return {"own": [], "competitors": [], "inspiration": []}
    with open(ACCOUNTS_PATH, encoding="utf-8") as f:
        return json.load(f)


def write_accounts(data):
    with open(ACCOUNTS_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def find_category(username, accounts):
    if username in accounts.get("own", []):
        return "own"
    if username in accounts.get("competitors", []):
        return "competitors"
    if username in accounts.get("inspiration", []):
        return "inspiration"
    return None


def do_add(username, category):
    """Додає username в категорію. Повертає (success, message)."""
    accounts = read_accounts()
    current = find_category(username, accounts)

    if current == category:
        labels = {"own": "Мій акаунт", "competitors": "Конкуренти", "inspiration": "Натхнення"}
        return False, f"@{username} вже є в «{labels.get(category, category)}» ✔"
    if current == "own":
        return False, f"@{username} — власний акаунт, не переношу."

    # Remove from current category if moving
    if current and current != "own":
        accounts[current] = [u for u in accounts[current] if u != username]

    accounts.setdefault(category, []).append(username)
    write_accounts(accounts)

    labels = {"competitors": "Конкуренти", "inspiration": "Натхнення"}
    return True, f"✅ Додано @{username} в {labels.get(category, category)}"


def do_remove(username):
    accounts = read_accounts()
    cat = find_category(username, accounts)
    if not cat:
        return False, f"@{username} не знайдено в жодній категорії."
    if cat == "own":
        return False, f"@{username} — власний акаунт, не видаляю."
    accounts[cat] = [u for u in accounts[cat] if u != username]
    write_accounts(accounts)
    labels = {"competitors": "Конкуренти", "inspiration": "Натхнення"}
    return True, f"🗑 @{username} видалено з «{labels.get(cat, cat)}»"


def extract_username(text):
    text = text.strip()
    m = re.search(r"instagram\.com/([A-Za-z0-9_.]+)/?", text)
    if m:
        u = m.group(1).lower()
        return u if u not in ("p", "reel", "stories", "explore") else None
    m = re.match(r"@([A-Za-z0-9_.]+)$", text)
    if m:
        return m.group(1).lower()
    return None


# ── Status helpers ────────────────────────────────────────────────────────────

def get_status_text():
    snapshots = sorted(SNAPSHOTS_DIR.glob("*.json")) if SNAPSHOTS_DIR.exists() else []
    accounts  = read_accounts()
    total = (
        len(accounts.get("own", [])) +
        len(accounts.get("competitors", [])) +
        len(accounts.get("inspiration", []))
    )

    now = datetime.now()
    if now.month == 12:
        next_run = datetime(now.year + 1, 1, 1)
    else:
        next_run = datetime(now.year, now.month + 1, 1)
    next_str = f"1 {MONTHS_UA[next_run.strftime('%m')]} {next_run.year}"

    if not snapshots:
        return (
            "📅 <b>Статус</b>\n\n"
            "📭 Оновлень ще не було.\n\n"
            f"📊 Акаунтів відстежується: {total}\n"
            f"⏰ Наступне автооновлення: {next_str}"
        )

    latest = snapshots[-1]
    month_key = latest.stem  # 2026-03
    y, m = month_key.split("-")
    month_ua = f"{MONTHS_UA.get(m, m)} {y}"

    try:
        with open(latest, encoding="utf-8") as f:
            snap = json.load(f)
        posts_total = snap.get("total_posts", "—")
        onda_avg    = snap.get("onda_avg", "—")
        best_type   = snap.get("best_type", "—")
        delta       = snap.get("onda_delta")
        delta_str   = f" ({delta:+.1f}%)" if isinstance(delta, (int, float)) else ""
        extra = (
            f"\n📊 Постів в базі: {posts_total}"
            f"\n📈 Onda avg engagement: {onda_avg}{delta_str}"
            f"\n🏆 Кращий формат: {best_type}"
        )
    except Exception:
        extra = ""

    return (
        f"📅 <b>Останнє оновлення:</b> {month_ua}\n"
        f"🔗 <a href='{DASHBOARD_URL}'>Відкрити дашборд</a>"
        f"{extra}\n\n"
        f"⏰ Наступне автооновлення: {next_str}"
    )


def format_list():
    accounts = read_accounts()
    own   = accounts.get("own", [])
    comps = accounts.get("competitors", [])
    insp  = accounts.get("inspiration", [])

    def lines(lst):
        return "\n".join(f"  — {u}" for u in lst) if lst else "  — (порожньо)"

    return (
        "📊 <b>МІЙ АКАУНТ:</b>\n" + lines(own) + "\n\n"
        f"🔴 <b>КОНКУРЕНТИ ({len(comps)}):</b>\n" + lines(comps) + "\n\n"
        f"✨ <b>НАТХНЕННЯ ({len(insp)}):</b>\n" + lines(insp)
    )


# ── Auth ──────────────────────────────────────────────────────────────────────

def is_allowed(update):
    if not ALLOWED_CHAT:
        return True
    chat_id = (
        update.effective_chat.id
        if update.effective_chat
        else update.callback_query.message.chat.id
    )
    return str(chat_id) == str(ALLOWED_CHAT)


async def deny(update):
    if update.message:
        await update.message.reply_text("⛔ Немає доступу.")
    elif update.callback_query:
        await update.callback_query.answer("⛔ Немає доступу.", show_alert=True)


# ── Inline keyboards ──────────────────────────────────────────────────────────

def kb_category(username):
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("⚔️ Конкурент",  callback_data=f"add:competitors:{username}"),
        InlineKeyboardButton("✨ Натхнення",   callback_data=f"add:inspiration:{username}"),
        InlineKeyboardButton("❌ Скасувати",  callback_data=f"cancel:{username}"),
    ]])


def kb_confirm_remove(username):
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Так, видалити", callback_data=f"remove_ok:{username}"),
        InlineKeyboardButton("❌ Ні",            callback_data=f"cancel:{username}"),
    ]])


def kb_run_now(category="all"):
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("🚀 Запустити зараз", callback_data=f"run:{category}"),
        InlineKeyboardButton("⏰ Наступного місяця", callback_data="cancel:run"),
    ]])


# ── Command handlers ──────────────────────────────────────────────────────────

async def cmd_start(update, ctx):
    if not is_allowed(update):
        await deny(update)
        return
    text = (
        "🌊 <b>Onda Content Bot</b>\n\n"
        "<b>Додати акаунт:</b>\n"
        "/add_competitor @username\n"
        "/add_inspiration @username\n\n"
        "<b>Видалити:</b>\n"
        "/remove @username\n\n"
        "<b>Переглянути:</b>\n"
        "/list — всі акаунти\n"
        "/status — статус дашборду\n\n"
        "<b>Запустити аналіз:</b>\n"
        "/run — повний\n"
        "/run_inspiration — тільки натхнення\n"
        "/run_competitors — тільки конкуренти\n\n"
        "Або просто надішли посилання:\n"
        "<code>instagram.com/username</code>"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


async def cmd_add_competitor(update, ctx):
    if not is_allowed(update):
        await deny(update)
        return
    if not ctx.args:
        await update.message.reply_text("Використання: /add_competitor @username")
        return
    username = extract_username(ctx.args[0])
    if not username:
        await update.message.reply_text("Не можу розпізнати username.")
        return
    ok, msg = do_add(username, "competitors")
    text = msg
    if ok:
        text += "\n\nХочеш запустити аналіз зараз?"
        await update.message.reply_text(text, parse_mode=ParseMode.HTML,
                                         reply_markup=kb_run_now("competitors"))
    else:
        await update.message.reply_text(text, parse_mode=ParseMode.HTML)


async def cmd_add_inspiration(update, ctx):
    if not is_allowed(update):
        await deny(update)
        return
    if not ctx.args:
        await update.message.reply_text("Використання: /add_inspiration @username")
        return
    username = extract_username(ctx.args[0])
    if not username:
        await update.message.reply_text("Не можу розпізнати username.")
        return
    ok, msg = do_add(username, "inspiration")
    text = msg
    if ok:
        text += "\n\nХочеш запустити аналіз зараз?"
        await update.message.reply_text(text, parse_mode=ParseMode.HTML,
                                         reply_markup=kb_run_now("inspiration"))
    else:
        await update.message.reply_text(text, parse_mode=ParseMode.HTML)


async def cmd_remove(update, ctx):
    if not is_allowed(update):
        await deny(update)
        return
    if not ctx.args:
        await update.message.reply_text("Використання: /remove @username")
        return
    username = extract_username(ctx.args[0])
    if not username:
        await update.message.reply_text("Не можу розпізнати username.")
        return
    accounts = read_accounts()
    cat = find_category(username, accounts)
    if not cat or cat == "own":
        _, msg = do_remove(username)
        await update.message.reply_text(msg)
        return
    labels = {"competitors": "Конкуренти", "inspiration": "Натхнення"}
    await update.message.reply_text(
        f"Видалити @{username} з «{labels.get(cat, cat)}»?",
        reply_markup=kb_confirm_remove(username),
    )


async def cmd_list(update, ctx):
    if not is_allowed(update):
        await deny(update)
        return
    await update.message.reply_text(format_list(), parse_mode=ParseMode.HTML)


async def cmd_status(update, ctx):
    if not is_allowed(update):
        await deny(update)
        return
    await update.message.reply_text(
        get_status_text(),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )


async def _run_script(update_or_query, send_fn, category="all"):
    """Запускає monthly_update.py з фільтром по категорії."""
    if not UPDATE_SCRIPT.exists():
        await send_fn("❌ monthly_update.py не знайдено.")
        return

    await send_fn(
        "⏳ Запускаю аналіз"
        + (" (тільки inspiration)" if category == "inspiration" else
           " (тільки competitors)" if category == "competitors" else "")
        + "...\nЦе займе кілька хвилин. Надішлю результат коли завершиться."
    )

    env = os.environ.copy()
    if category != "all":
        env["RUN_CATEGORY"] = category

    log.info(f"Running monthly_update.py category={category}")
    try:
        result = subprocess.run(
            [sys.executable, str(UPDATE_SCRIPT)],
            capture_output=True, text=True, timeout=1800, env=env,
        )
        if result.returncode == 0:
            tail = "\n".join(result.stdout.strip().splitlines()[-10:])
            await send_fn(
                f"✅ <b>Аналіз завершено!</b>\n\n<pre>{tail[:800]}</pre>"
            )
        else:
            err = (result.stderr or result.stdout).strip()[-600:]
            await send_fn(f"❌ <b>Помилка:</b>\n<pre>{err}</pre>")
    except subprocess.TimeoutExpired:
        await send_fn("⏱ Timeout — скрипт виконується більше 30 хвилин.")
    except Exception as e:
        await send_fn(f"❌ {e}")


async def cmd_run(update, ctx):
    if not is_allowed(update):
        await deny(update)
        return
    async def send(t):
        await update.message.reply_text(t, parse_mode=ParseMode.HTML)
    await _run_script(update, send, "all")


async def cmd_run_inspiration(update, ctx):
    if not is_allowed(update):
        await deny(update)
        return
    async def send(t):
        await update.message.reply_text(t, parse_mode=ParseMode.HTML)
    await _run_script(update, send, "inspiration")


async def cmd_run_competitors(update, ctx):
    if not is_allowed(update):
        await deny(update)
        return
    async def send(t):
        await update.message.reply_text(t, parse_mode=ParseMode.HTML)
    await _run_script(update, send, "competitors")


# ── Message handler — Instagram links ────────────────────────────────────────

async def handle_message(update, ctx):
    if not is_allowed(update):
        return
    text = update.message.text or ""
    username = extract_username(text)

    if not username:
        return

    accounts  = read_accounts()
    existing  = find_category(username, accounts)
    labels    = {"own": "Мій акаунт", "competitors": "Конкуренти", "inspiration": "Натхнення"}

    if existing:
        await update.message.reply_text(
            f"📸 @{username} вже є в «{labels.get(existing, existing)}» ✔"
        )
        return

    await update.message.reply_text(
        f"📸 Знайшла акаунт <b>@{username}</b>\nКуди додати?",
        parse_mode=ParseMode.HTML,
        reply_markup=kb_category(username),
    )


# ── Callback query handler ────────────────────────────────────────────────────

async def handle_callback(update, ctx):
    query = update.callback_query
    await query.answer()

    if not is_allowed(update):
        await query.answer("⛔ Немає доступу.", show_alert=True)
        return

    data = query.data  # e.g. "add:inspiration:username"

    # add:<category>:<username>
    if data.startswith("add:"):
        _, category, username = data.split(":", 2)
        ok, msg = do_add(username, category)
        text = msg
        if ok:
            text += "\n\nХочеш запустити аналіз зараз?"
            await query.edit_message_text(
                text, parse_mode=ParseMode.HTML,
                reply_markup=kb_run_now(category),
            )
        else:
            await query.edit_message_text(text, parse_mode=ParseMode.HTML)

    # remove_ok:<username>
    elif data.startswith("remove_ok:"):
        username = data.split(":", 1)[1]
        _, msg = do_remove(username)
        await query.edit_message_text(msg)

    # run:<category>
    elif data.startswith("run:"):
        category = data.split(":", 1)[1]
        await query.edit_message_text(
            "⏳ Запускаю аналіз...", parse_mode=ParseMode.HTML
        )
        async def send(t):
            await query.message.reply_text(t, parse_mode=ParseMode.HTML)
        await _run_script(update, send, category)

    # cancel:*
    elif data.startswith("cancel:"):
        await query.edit_message_text("Скасовано.")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if not BOT_TOKEN:
        sys.exit(
            "ERROR: TELEGRAM_BOT_TOKEN не встановлено.\n"
            "Додай в .env: TELEGRAM_BOT_TOKEN=<токен від @BotFather>"
        )

    LOG_DIR.mkdir(exist_ok=True)

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start",            cmd_start))
    app.add_handler(CommandHandler("add_competitor",   cmd_add_competitor))
    app.add_handler(CommandHandler("add_inspiration",  cmd_add_inspiration))
    app.add_handler(CommandHandler("remove",           cmd_remove))
    app.add_handler(CommandHandler("list",             cmd_list))
    app.add_handler(CommandHandler("status",           cmd_status))
    app.add_handler(CommandHandler("run",              cmd_run))
    app.add_handler(CommandHandler("run_inspiration",  cmd_run_inspiration))
    app.add_handler(CommandHandler("run_competitors",  cmd_run_competitors))
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    log.info("Bot started. Polling...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
