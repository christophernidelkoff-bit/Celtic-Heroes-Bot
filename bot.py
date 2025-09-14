# bot.py — Celtic Heroes Boss Tracker + Market/Lixing 
# Section 1/4: imports, env/globals, helpers, DB (preflight+async), meta, auth gate, categories/colors/emojis

from __future__ import annotations

import os
import re
import io
import atexit
import signal
import shutil
import asyncio
import logging
from typing import Optional, Tuple, List, Dict, Any, Set
from datetime import datetime, timezone

import aiosqlite
import discord
from discord.ext import commands, tasks
from discord import app_commands
from dotenv import load_dotenv

# -------------------- ENV / GLOBALS --------------------
load_dotenv()

TOKEN = os.getenv("DISCORD_TOKEN")
if not TOKEN:
    raise SystemExit("DISCORD_TOKEN missing in .env")

# Optional host-guard for /ps (PowerShell) and auth default
ALLOW_POWERSHELL = os.getenv("ALLOW_POWERSHELL", "0").lower() in {"1", "true", "yes"}
DEFAULT_AUTH_USER_ID = int(os.getenv("GATEKEEPER_USER_ID", "0") or "0")  # used if admin hasn’t set it in DB yet

DB_PATH = os.getenv("DB_PATH", "bosses.db")
DEFAULT_PREFIX = "!"
CHECK_INTERVAL_SECONDS = 15
DEFAULT_UPTIME_MINUTES = 60
NADA_GRACE_SECONDS = 1800  # after window closes, only flip to -Nada once this grace passes

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ch-bossbot")

intents = discord.Intents.default()
intents.message_content = True
intents.reactions = True
intents.guilds = True
intents.members = True

bot = commands.Bot(command_prefix=lambda b, m: DEFAULT_PREFIX, intents=intents, help_command=None)
tree = bot.tree  # IMPORTANT: define tree before any @tree.command decorators (fixes slash-command registration)

_last_timer_tick_ts: int = 0
_prev_timer_tick_ts: int = 0

SEED_VERSION = "v2025-09-13-full-stable-with-market-lixing"

# -------------------- TIME HELPERS --------------------
def now_ts() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp())

def ts_to_utc(ts: int) -> str:
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return "—"

_nat_re = re.compile(r'(\d+|\D+)')
def natural_key(s: str) -> List[Any]:
    s = (s or "").strip().lower()
    return [int(p) if p.isdigit() else p for p in _nat_re.findall(s)]

def fmt_delta_for_list(delta_s: int) -> str:
    # Positive time → h/m/s; Negative shows "-Xm" until grace ends; then "-Nada"
    if delta_s <= 0:
        overdue = -delta_s
        return "-Nada" if overdue > NADA_GRACE_SECONDS else f"-{overdue // 60}m"
    m, s = divmod(delta_s, 60)
    h, m = divmod(m, 60)
    parts = []
    if h: parts.append(f"{h}h")
    if m: parts.append(f"{m}m")
    if not parts: parts.append(f"{s}s")
    return " ".join(parts)

def human_ago(seconds: int) -> str:
    if seconds < 60: return "just now"
    m, _ = divmod(seconds, 60)
    h, m = divmod(m, 60)
    return f"{h}h {m}m ago" if h else f"{m}m ago"

def window_label(now: int, next_ts: int, window_m: int) -> str:
    """
    Rules:
      - If next_ts > now → "Xm (pending)" where X = window_m
      - If window open (now within [next_ts, next_ts+window]) → "Ym left (open)"
      - After window closes but within grace → "closed"
      - After grace → "-Nada"
    """
    delta = next_ts - now
    if delta >= 0:
        return f"{window_m}m (pending)"
    open_secs = -delta
    win_secs = max(0, int(window_m) * 60)
    if open_secs <= win_secs:
        left_m = max(0, (win_secs - open_secs) // 60)
        return f"{left_m}m left (open)"
    after_close = open_secs - win_secs
    if after_close <= NADA_GRACE_SECONDS:
        return "closed"
    return "-Nada"

# -------------------- CATEGORY / COLORS / EMOJIS --------------------
CATEGORY_ORDER = ["Warden", "Meteoric", "Frozen", "DL", "EDL", "Midraids", "Rings", "EG", "Default"]

def norm_cat(c: Optional[str]) -> str:
    c = (c or "Default").strip(); cl = c.lower()
    if "warden" in cl: return "Warden"
    if "meteoric" in cl: return "Meteoric"
    if "frozen" in cl: return "Frozen"
    if cl.startswith("dl"): return "DL"
    if cl.startswith("edl"): return "EDL"
    if "midraid" in cl: return "Midraids"
    if "ring" in cl: return "Rings"
    if cl.startswith("eg"): return "EG"
    return "Default"

def category_emoji(c: str) -> str:
    c = norm_cat(c)
    return {
        "Warden": "🛡️", "Meteoric": "☄️", "Frozen": "🧊", "DL": "🐉",
        "EDL": "🐲", "Midraids": "⚔️", "Rings": "💍", "EG": "🔱", "Default": "📜",
    }.get(c, "📜")

DEFAULT_COLORS = {
    "Warden": 0x2ecc71, "Meteoric": 0xe67e22, "Frozen": 0x3498db,
    "DL": 0xe74c3c, "EDL": 0x8e44ad, "Midraids": 0x34495e,
    "Rings": 0x1abc9c, "EG": 0xf1c40f, "Default": 0x95a5a6,
}

EMOJI_PALETTE = [
    "🟥","🟧","🟨","🟩","🟦","🟪","⬛","⬜","🟫",
    "🔴","🟠","🟡","🟢","🔵","🟣","⚫","⚪","🟤",
    "⭐","✨","⚡","🔥","⚔️","🗡️","🛡️","🏹","🗿","🧪","🧿","👑","🎯","🪙",
    "🐉","🐲","🔱","☄️","🧊","🌋","🌪️","🌊","🌫️","🌩️","🪽","🪓",
    "0️⃣","1️⃣","2️⃣","3️⃣","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟",
]
EXTRA_EMOJIS = [
    "⓪","①","②","③","④","⑤","⑥","⑦","⑧","⑨","⑩","⑪","⑫","⑬","⑭","⑮","⑯","⑰","⑱","⑲","⑳",
    "🅰️","🅱️","🆎","🆑","🆒","🆓","🆔","🆕","🆖","🅾️","🆗","🅿️","🆘","🆙","🆚",
    "♈","♉","♊","♋","♌","♍","♎","♏","♐","♑","♒","♓",
]

# -------------------- DB PREFLIGHT (sync, for container boot) --------------------
def preflight_migrate_sync():
    import sqlite3
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # core tables
    cur.execute("""CREATE TABLE IF NOT EXISTS bosses (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        guild_id INTEGER NOT NULL,
        channel_id INTEGER,
        name TEXT NOT NULL,
        spawn_minutes INTEGER NOT NULL,
        next_spawn_ts INTEGER NOT NULL,
        pre_announce_min INTEGER DEFAULT 10,
        trusted_role_id INTEGER DEFAULT NULL,
        created_by INTEGER,
        notes TEXT DEFAULT '',
        category TEXT DEFAULT 'Default',
        sort_key TEXT DEFAULT '',
        window_minutes INTEGER DEFAULT 0
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS guild_config (
        guild_id INTEGER PRIMARY KEY,
        default_channel INTEGER DEFAULT NULL,
        prefix TEXT DEFAULT NULL,
        sub_channel_id INTEGER DEFAULT NULL,
        sub_message_id INTEGER DEFAULT NULL,
        uptime_minutes INTEGER DEFAULT NULL,
        heartbeat_channel_id INTEGER DEFAULT NULL,
        show_eta INTEGER DEFAULT 0,
        sub_ping_channel_id INTEGER DEFAULT NULL,
        auth_user_id INTEGER DEFAULT NULL,
        market_digest_channel_id INTEGER DEFAULT NULL,
        lixing_digest_channel_id INTEGER DEFAULT NULL
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)""")
    cur.execute("""CREATE TABLE IF NOT EXISTS category_colors (
        guild_id INTEGER NOT NULL,
        category TEXT NOT NULL,
        color_hex TEXT NOT NULL,
        PRIMARY KEY (guild_id, category)
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS subscription_emojis (
        guild_id INTEGER NOT NULL,
        boss_id INTEGER NOT NULL,
        emoji TEXT NOT NULL,
        PRIMARY KEY (guild_id, boss_id)
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS subscription_members (
        guild_id INTEGER NOT NULL,
        boss_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        PRIMARY KEY (guild_id, boss_id, user_id)
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS boss_aliases (
        guild_id INTEGER NOT NULL,
        boss_id INTEGER NOT NULL,
        alias TEXT NOT NULL,
        UNIQUE (guild_id, alias)
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS category_channels (
        guild_id INTEGER NOT NULL,
        category TEXT NOT NULL,
        channel_id INTEGER NOT NULL,
        PRIMARY KEY (guild_id, category)
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS user_timer_prefs (
        guild_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        categories TEXT NOT NULL,
        PRIMARY KEY (guild_id, user_id)
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS subscription_panels (
        guild_id INTEGER NOT NULL,
        category TEXT NOT NULL,
        message_id INTEGER NOT NULL,
        channel_id INTEGER DEFAULT NULL,
        PRIMARY KEY (guild_id, category)
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS rr_panels (
        message_id INTEGER PRIMARY KEY,
        guild_id INTEGER NOT NULL,
        channel_id INTEGER NOT NULL,
        title TEXT DEFAULT ''
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS rr_map (
        panel_message_id INTEGER NOT NULL,
        emoji TEXT NOT NULL,
        role_id INTEGER NOT NULL,
        PRIMARY KEY (panel_message_id, emoji)
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS blacklist (
        guild_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        PRIMARY KEY (guild_id, user_id)
    )""")

    # MARKET / LIXING (simple normalized tables)
    cur.execute("""CREATE TABLE IF NOT EXISTS market_listings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        guild_id INTEGER NOT NULL,
        owner_id INTEGER NOT NULL,
        item_name TEXT NOT NULL,
        quantity INTEGER NOT NULL,
        price TEXT NOT NULL,
        accepts_trades INTEGER NOT NULL DEFAULT 0,
        taking_offers INTEGER NOT NULL DEFAULT 0,
        is_buy INTEGER NOT NULL DEFAULT 0,
        created_ts INTEGER NOT NULL
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS market_offers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        guild_id INTEGER NOT NULL,
        listing_id INTEGER NOT NULL,
        bidder_id INTEGER NOT NULL,
        text TEXT NOT NULL,
        created_ts INTEGER NOT NULL
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS lixing_posts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        guild_id INTEGER NOT NULL,
        owner_id INTEGER NOT NULL,
        char_name TEXT NOT NULL,
        char_class TEXT NOT NULL,
        char_level TEXT NOT NULL,
        desired_lixes TEXT NOT NULL,
        created_ts INTEGER NOT NULL
    )""")

    # column backfills (if older DB present)
    def col_exists(table, col):
        cur.execute(f"PRAGMA table_info({table})")
        return any(row[1] == col for row in cur.fetchall())

    if not col_exists("guild_config", "auth_user_id"):
        cur.execute("ALTER TABLE guild_config ADD COLUMN auth_user_id INTEGER DEFAULT NULL")
    if not col_exists("guild_config", "market_digest_channel_id"):
        cur.execute("ALTER TABLE guild_config ADD COLUMN market_digest_channel_id INTEGER DEFAULT NULL")
    if not col_exists("guild_config", "lixing_digest_channel_id"):
        cur.execute("ALTER TABLE guild_config ADD COLUMN lixing_digest_channel_id INTEGER DEFAULT NULL")

    conn.commit(); conn.close()

preflight_migrate_sync()

# -------------------- ASYNC MIGRATIONS / META --------------------
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""CREATE TABLE IF NOT EXISTS bosses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id INTEGER NOT NULL,
            channel_id INTEGER,
            name TEXT NOT NULL,
            spawn_minutes INTEGER NOT NULL,
            next_spawn_ts INTEGER NOT NULL,
            pre_announce_min INTEGER DEFAULT 10,
            trusted_role_id INTEGER DEFAULT NULL,
            created_by INTEGER,
            notes TEXT DEFAULT '',
            category TEXT DEFAULT 'Default',
            sort_key TEXT DEFAULT '',
            window_minutes INTEGER DEFAULT 0
        )""")
        await db.execute("""CREATE TABLE IF NOT EXISTS guild_config (
            guild_id INTEGER PRIMARY KEY,
            default_channel INTEGER DEFAULT NULL,
            prefix TEXT DEFAULT NULL,
            sub_channel_id INTEGER DEFAULT NULL,
            sub_message_id INTEGER DEFAULT NULL,
            uptime_minutes INTEGER DEFAULT NULL,
            heartbeat_channel_id INTEGER DEFAULT NULL,
            show_eta INTEGER DEFAULT 0,
            sub_ping_channel_id INTEGER DEFAULT NULL,
            auth_user_id INTEGER DEFAULT NULL,
            market_digest_channel_id INTEGER DEFAULT NULL,
            lixing_digest_channel_id INTEGER DEFAULT NULL
        )""")
        await db.execute("""CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS category_colors (guild_id INTEGER NOT NULL, category TEXT NOT NULL, color_hex TEXT NOT NULL, PRIMARY KEY (guild_id, category))""")
        await db.execute("""CREATE TABLE IF NOT EXISTS subscription_emojis (guild_id INTEGER NOT NULL, boss_id INTEGER NOT NULL, emoji TEXT NOT NULL, PRIMARY KEY (guild_id, boss_id))""")
        await db.execute("""CREATE TABLE IF NOT EXISTS subscription_members (guild_id INTEGER NOT NULL, boss_id INTEGER NOT NULL, user_id INTEGER NOT NULL, PRIMARY KEY (guild_id, boss_id, user_id))""")
        await db.execute("""CREATE TABLE IF NOT EXISTS boss_aliases (guild_id INTEGER NOT NULL, boss_id INTEGER NOT NULL, alias TEXT NOT NULL, UNIQUE (guild_id, alias))""")
        await db.execute("""CREATE TABLE IF NOT EXISTS category_channels (guild_id INTEGER NOT NULL, category TEXT NOT NULL, channel_id INTEGER NOT NULL, PRIMARY KEY (guild_id, category))""")
        await db.execute("""CREATE TABLE IF NOT EXISTS user_timer_prefs (guild_id INTEGER NOT NULL, user_id INTEGER NOT NULL, categories TEXT NOT NULL, PRIMARY KEY (guild_id, user_id))""")
        await db.execute("""CREATE TABLE IF NOT EXISTS subscription_panels (guild_id INTEGER NOT NULL, category TEXT NOT NULL, message_id INTEGER NOT NULL, channel_id INTEGER DEFAULT NULL, PRIMARY KEY (guild_id, category))""")
        await db.execute("""CREATE TABLE IF NOT EXISTS rr_panels (message_id INTEGER PRIMARY KEY, guild_id INTEGER NOT NULL, channel_id INTEGER NOT NULL, title TEXT DEFAULT '')""")
        await db.execute("""CREATE TABLE IF NOT EXISTS rr_map (panel_message_id INTEGER NOT NULL, emoji TEXT NOT NULL, role_id INTEGER NOT NULL, PRIMARY KEY (panel_message_id, emoji))""")
        await db.execute("""CREATE TABLE IF NOT EXISTS blacklist (guild_id INTEGER NOT NULL, user_id INTEGER NOT NULL, PRIMARY KEY (guild_id, user_id))""")
        await db.execute("""CREATE TABLE IF NOT EXISTS market_listings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id INTEGER NOT NULL,
            owner_id INTEGER NOT NULL,
            item_name TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            price TEXT NOT NULL,
            accepts_trades INTEGER NOT NULL DEFAULT 0,
            taking_offers INTEGER NOT NULL DEFAULT 0,
            is_buy INTEGER NOT NULL DEFAULT 0,
            created_ts INTEGER NOT NULL
        )""")
        await db.execute("""CREATE TABLE IF NOT EXISTS market_offers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id INTEGER NOT NULL,
            listing_id INTEGER NOT NULL,
            bidder_id INTEGER NOT NULL,
            text TEXT NOT NULL,
            created_ts INTEGER NOT NULL
        )""")
        await db.execute("""CREATE TABLE IF NOT EXISTS lixing_posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id INTEGER NOT NULL,
            owner_id INTEGER NOT NULL,
            char_name TEXT NOT NULL,
            char_class TEXT NOT NULL,
            char_level TEXT NOT NULL,
            desired_lixes TEXT NOT NULL,
            created_ts INTEGER NOT NULL
        )""")
        await db.commit()

async def meta_set(key: str, value: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value)
        ); await db.commit()

async def meta_get(key: str) -> Optional[str]:
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT value FROM meta WHERE key=?", (key,))
        r = await c.fetchone()
        return r[0] if r else None

# -------------------- AUTH / GUILD GATE --------------------
async def _get_auth_user_id_from_db(guild_id: int) -> Optional[int]:
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT auth_user_id FROM guild_config WHERE guild_id=?", (guild_id,))
        r = await c.fetchone()
        return int(r[0]) if r and r[0] else None

async def upsert_guild_defaults(guild_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO guild_config (guild_id, prefix, uptime_minutes, show_eta, auth_user_id) VALUES (?,?,?,?,?) "
            "ON CONFLICT(guild_id) DO NOTHING",
            (guild_id, DEFAULT_PREFIX, DEFAULT_UPTIME_MINUTES, 0, (DEFAULT_AUTH_USER_ID or None))
        ); await db.commit()

async def ensure_guild_auth(guild: discord.Guild) -> bool:
    """Return True if the required auth user is present in this guild (or no requirement set)."""
    if not guild:
        return False
    required_id = await _get_auth_user_id_from_db(guild.id)
    if not required_id:
        # If still not set, fall back to env default; if that is 0, treat as no requirement.
        if DEFAULT_AUTH_USER_ID <= 0:
            return True
        required_id = DEFAULT_AUTH_USER_ID
    member = guild.get_member(required_id) or (await guild.fetch_member(required_id) if guild and required_id else None)
    return member is not None

def guild_auth_check():
    async def predicate(ctx: commands.Context) -> bool:
        if not ctx.guild:
            return False
        ok = await ensure_guild_auth(ctx.guild)
        if not ok:
            try:
                await ctx.send(":no_entry: Bot is locked — required user is not in this server.")
            except Exception:
                pass
        return ok
    return commands.check(predicate)

bot.add_check(guild_auth_check())
# bot.py — Section 2/4: data helpers, seeding, boss utils, embeds, subscription panels

# -------------------- SIMPLE DB HELPERS --------------------
async def db_execute(sql: str, *params):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(sql, params)
        await db.commit()

async def db_fetchone(sql: str, *params) -> Optional[aiosqlite.Row]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(sql, params)
        return await cur.fetchone()

async def db_fetchall(sql: str, *params) -> List[aiosqlite.Row]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(sql, params)
        return await cur.fetchall()

# -------------------- COLORS --------------------
async def get_color_for_category(guild_id: int, category: str) -> discord.Color:
    category = norm_cat(category)
    row = await db_fetchone("SELECT color_hex FROM category_colors WHERE guild_id=? AND category=?",
                            guild_id, category)
    if row and row["color_hex"]:
        try:
            return discord.Color(int(row["color_hex"], 16))
        except Exception:
            pass
    return discord.Color(DEFAULT_COLORS.get(category, DEFAULT_COLORS["Default"]))

# -------------------- BOSS CRUD / ALIASES --------------------
async def upsert_boss(guild_id: int, name: str, spawn_m: int, window_m: int = 0,
                      category: str = "Default", channel_id: Optional[int] = None,
                      pre_announce_min: int = 10, trusted_role_id: Optional[int] = None,
                      created_by: Optional[int] = None, notes: str = "") -> int:
    name = name.strip()
    category = norm_cat(category)
    # Check existing
    e = await db_fetchone("SELECT id FROM bosses WHERE guild_id=? AND lower(name)=lower(?)",
                          guild_id, name)
    next_ts = now_ts() + spawn_m * 60  # reasonable default next
    if e:
        await db_execute("""UPDATE bosses SET spawn_minutes=?, window_minutes=?, category=?,
                           channel_id=?, pre_announce_min=?, trusted_role_id=?, notes=? WHERE id=?""",
                         spawn_m, window_m, category, channel_id, pre_announce_min,
                         trusted_role_id, notes, e["id"])
        return int(e["id"])
    await db_execute("""INSERT INTO bosses (guild_id, channel_id, name, spawn_minutes, next_spawn_ts,
                     pre_announce_min, trusted_role_id, created_by, notes, category, sort_key, window_minutes)
                     VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                     guild_id, channel_id, name, spawn_m, next_ts, pre_announce_min,
                     trusted_role_id, created_by, notes, category, name.lower(), window_m)
    r = await db_fetchone("SELECT id FROM bosses WHERE guild_id=? AND lower(name)=lower(?)", guild_id, name)
    return int(r["id"])

async def set_boss_next(guild_id: int, boss_id: int, next_ts: int):
    await db_execute("UPDATE bosses SET next_spawn_ts=? WHERE guild_id=? AND id=?", next_ts, guild_id, boss_id)

async def add_alias(guild_id: int, boss_id: int, alias: str) -> bool:
    alias = alias.strip()
    try:
        await db_execute("INSERT INTO boss_aliases (guild_id, boss_id, alias) VALUES (?,?,?)", guild_id, boss_id, alias)
        return True
    except Exception:
        return False

async def resolve_boss_by_name_or_alias(guild_id: int, name_or_alias: str) -> Optional[aiosqlite.Row]:
    q = name_or_alias.strip().lower()
    row = await db_fetchone("""SELECT b.* FROM bosses b
                               LEFT JOIN boss_aliases a ON a.guild_id=b.guild_id AND a.boss_id=b.id
                               WHERE b.guild_id=? AND (lower(b.name)=? OR lower(a.alias)=?)""",
                            guild_id, q, q)
    return row

async def get_bosses_by_cat(guild_id: int, category: str) -> List[aiosqlite.Row]:
    return await db_fetchall("SELECT * FROM bosses WHERE guild_id=? AND category=? ORDER BY sort_key ASC",
                             guild_id, norm_cat(category))

async def get_all_bosses_grouped(guild_id: int) -> Dict[str, List[aiosqlite.Row]]:
    rows = await db_fetchall("SELECT * FROM bosses WHERE guild_id=? ORDER BY category, sort_key", guild_id)
    out: Dict[str, List[aiosqlite.Row]] = {k: [] for k in CATEGORY_ORDER}
    out.setdefault("Default", [])
    for r in rows:
        out.setdefault(norm_cat(r["category"]), []).append(r)
    return out

# -------------------- SUBSCRIPTIONS --------------------
async def get_or_assign_emoji(guild_id: int, boss_id: int) -> str:
    row = await db_fetchone("SELECT emoji FROM subscription_emojis WHERE guild_id=? AND boss_id=?",
                            guild_id, boss_id)
    if row:
        return row["emoji"]

    # assign next available emoji deterministically by boss_id
    palette = EMOJI_PALETTE + EXTRA_EMOJIS
    idx = boss_id % len(palette)
    chosen = palette[idx]
    # ensure uniqueness
    existing = await db_fetchall("SELECT emoji FROM subscription_emojis WHERE guild_id=?", guild_id)
    used = {e["emoji"] for e in existing}
    if chosen in used:
        for e in palette:
            if e not in used:
                chosen = e; break
    await db_execute("INSERT OR REPLACE INTO subscription_emojis (guild_id,boss_id,emoji) VALUES (?,?,?)",
                     guild_id, boss_id, chosen)
    return chosen

async def get_subscribers(guild_id: int, boss_id: int) -> List[int]:
    rows = await db_fetchall("SELECT user_id FROM subscription_members WHERE guild_id=? AND boss_id=?",
                             guild_id, boss_id)
    return [int(r["user_id"]) for r in rows]

async def toggle_subscription(guild_id: int, user_id: int, boss_id: int, on: Optional[bool]=None) -> bool:
    cur = await db_fetchone("SELECT user_id FROM subscription_members WHERE guild_id=? AND boss_id=? AND user_id=?",
                            guild_id, boss_id, user_id)
    if on is None:
        # toggle
        if cur:
            await db_execute("DELETE FROM subscription_members WHERE guild_id=? AND boss_id=? AND user_id=?",
                             guild_id, boss_id, user_id)
            return False
        await db_execute("INSERT INTO subscription_members (guild_id,boss_id,user_id) VALUES (?,?,?)",
                         guild_id, boss_id, user_id)
        return True
    else:
        if on and not cur:
            await db_execute("INSERT INTO subscription_members (guild_id,boss_id,user_id) VALUES (?,?,?)",
                             guild_id, boss_id, user_id)
            return True
        if not on and cur:
            await db_execute("DELETE FROM subscription_members WHERE guild_id=? AND boss_id=? AND user_id=?",
                             guild_id, boss_id, user_id)
        return False

# -------------------- GUILD CONFIG --------------------
async def set_config_value(guild_id: int, key: str, value: Optional[int]):
    await db_execute(f"INSERT INTO guild_config (guild_id,{key}) VALUES (?,?) "
                     f"ON CONFLICT(guild_id) DO UPDATE SET {key}=excluded.{key}", guild_id, value)

async def get_config_int(guild_id: int, key: str) -> Optional[int]:
    r = await db_fetchone(f"SELECT {key} FROM guild_config WHERE guild_id=?", guild_id)
    if r and r[key] is not None:
        return int(r[key])
    return None

# -------------------- SEED BOSSES (includes DL '180' override) --------------------
DEFAULT_SEED = [
    # name, category, spawn_m, window_m
    ("Aggy", "DL", 180, 5),
    ("Snorri", "EDL", 180, 5),
    ("Proteus", "EG", 240, 10),
    ("Mordris", "Midraids", 180, 5),
    ("Hrungnir", "Midraids", 180, 5),
    ("Gele", "EG", 360, 10),
    ("Necro", "Midraids", 240, 5),
    ("180", "DL", 88, 3),  # ← rule: '180' spawn 88m, window 3m
]

async def ensure_seed_for_guild(guild: discord.Guild):
    await upsert_guild_defaults(guild.id)

    applied = await meta_get(f"seed:{guild.id}:{SEED_VERSION}")
    if applied == "1":
        return

    # seed basic bosses if not present
    for name, cat, spawn_m, win_m in DEFAULT_SEED:
        await upsert_boss(guild.id, name, spawn_m, window_m=win_m, category=cat)

    # fix any legacy wrong '180'
    r = await resolve_boss_by_name_or_alias(guild.id, "180")
    if r and (r["spawn_minutes"] != 88 or r["window_minutes"] != 3):
        await db_execute("UPDATE bosses SET spawn_minutes=88, window_minutes=3 WHERE id=?", r["id"])

    await meta_set(f"seed:{guild.id}:{SEED_VERSION}", "1")
    log.info("Seed applied for guild %s", guild.id)

# -------------------- TIMER / EMBED BUILDERS --------------------
def _boss_line(now: int, b: aiosqlite.Row) -> str:
    # window label + relative next
    win_m = int(b["window_minutes"] or 0)
    nxt = int(b["next_spawn_ts"])
    wlabel = window_label(now, nxt, win_m)
    mins_until = max(0, (nxt - now) // 60) if nxt > now else -((now - nxt) // 60)
    eta = f"in {mins_until}m" if nxt > now else f"{-mins_until}m ago"
    return f"**{b['name']}** — {wlabel} • next {eta}"

async def build_timer_embeds_for_categories(guild: discord.Guild, shown: Optional[List[str]]=None) -> List[discord.Embed]:
    shown = shown or CATEGORY_ORDER
    now = now_ts()
    out: List[discord.Embed] = []
    for cat in shown:
        bosses = await get_bosses_by_cat(guild.id, cat)
        if not bosses:  # skip empty
            continue
        color = await get_color_for_category(guild.id, cat)
        e = discord.Embed(
            title=f"{category_emoji(cat)} {cat}",
            description="\n".join(_boss_line(now, b) for b in bosses),
            color=color
        )
        e.set_footer(text="Window shows minutes • 'closed' until grace ends • '-Nada' after")
        out.append(e)
    return out

# -------------------- SUBSCRIPTION PANELS (IN-PLACE REFRESH) --------------------
async def _panel_embed_for_category(guild: discord.Guild, category: str) -> discord.Embed:
    bosses = await get_bosses_by_cat(guild.id, category)
    if not bosses:
        color = await get_color_for_category(guild.id, category)
        return discord.Embed(title=f"{category_emoji(category)} {category} — Subscriptions",
                             description="No bosses in this category.", color=color)

    # Build list with emojis and subscriber counts
    lines = []
    color = await get_color_for_category(guild.id, category)
    for b in bosses:
        emoji = await get_or_assign_emoji(guild.id, b["id"])
        subs = await db_fetchone("SELECT COUNT(*) c FROM subscription_members WHERE guild_id=? AND boss_id=?",
                                 guild.id, b["id"])
        count = subs["c"] if subs else 0
        lines.append(f"{emoji}  **{b['name']}**  — {count} subs")

    e = discord.Embed(
        title=f"{category_emoji(category)} {category} — Subscriptions",
        description="\n".join(lines) if lines else "—",
        color=color
    )
    e.set_footer(text="React with the emoji to (un)subscribe. Use !sub list to view your subs.")
    return e

async def ensure_panel_record(guild_id: int, category: str, channel_id: Optional[int]=None, message_id: Optional[int]=None):
    # Make sure a row exists
    row = await db_fetchone("SELECT message_id, channel_id FROM subscription_panels WHERE guild_id=? AND category=?",
                            guild_id, norm_cat(category))
    if row:
        # allow channel/message update
        if channel_id or message_id:
            cid = channel_id or row["channel_id"]
            mid = message_id or row["message_id"]
            await db_execute("UPDATE subscription_panels SET channel_id=?, message_id=? WHERE guild_id=? AND category=?",
                             cid, mid, guild_id, norm_cat(category))
        return
    await db_execute("INSERT INTO subscription_panels (guild_id, category, message_id, channel_id) VALUES (?,?,?,?)",
                     guild_id, norm_cat(category), int(message_id or 0), int(channel_id or 0))

async def post_or_refresh_sub_panel(guild: discord.Guild, category: str) -> Optional[discord.Message]:
    """
    Create panel if missing, otherwise edit in-place. Reactions are synced without nuking existing ones.
    """
    cat = norm_cat(category)
    # where to put panels
    panel_channel_id = await get_config_int(guild.id, "sub_channel_id")
    channel: Optional[discord.TextChannel] = guild.get_channel(panel_channel_id) if panel_channel_id else None
    if not channel:
        log.warning("No sub_channel_id set for guild %s; skipping panel for %s", guild.id, cat)
        return None

    row = await db_fetchone("SELECT message_id FROM subscription_panels WHERE guild_id=? AND category=?",
                            guild.id, cat)
    embed = await _panel_embed_for_category(guild, cat)

    message: Optional[discord.Message] = None
    try:
        if row and row["message_id"] != 0:
            message = await channel.fetch_message(int(row["message_id"]))
            await message.edit(embed=embed, view=None)
        else:
            message = await channel.send(embed=embed)
            await ensure_panel_record(guild.id, cat, channel_id=channel.id, message_id=message.id)
    except discord.NotFound:
        # message missing; recreate
        message = await channel.send(embed=embed)
        await ensure_panel_record(guild.id, cat, channel_id=channel.id, message_id=message.id)

    # Ensure reactions—in sync but do not clear; add only missing
    bosses = await get_bosses_by_cat(guild.id, cat)
    needed = [await get_or_assign_emoji(guild.id, b["id"]) for b in bosses]
    existing = {str(r.emoji) for r in (message.reactions or [])}
    for emo in needed:
        if emo not in existing:
            try:
                await message.add_reaction(emo)
                await asyncio.sleep(0.25)  # avoid rate limits on bulk add
            except Exception as e:
                log.warning("Reaction add failed %s on panel %s: %s", emo, cat, e)

    return message

async def refresh_all_sub_panels(guild: discord.Guild):
    for cat in CATEGORY_ORDER:
        await post_or_refresh_sub_panel(guild, cat)

# -------------------- SAFE PING FUNCTION --------------------
async def send_subscription_ping(guild: discord.Guild, boss_row: aiosqlite.Row, text: str):
    sub_ping_channel_id = await get_config_int(guild.id, "sub_ping_channel_id")
    channel: Optional[discord.TextChannel] = guild.get_channel(sub_ping_channel_id) if sub_ping_channel_id else None
    if not channel:
        # fall back to default_channel
        def_chan_id = await get_config_int(guild.id, "default_channel")
        channel = guild.get_channel(def_chan_id) if def_chan_id else None
    if not channel:
        log.warning("No ping channel configured for guild %s; dropping ping.", guild.id); return

    users = await get_subscribers(guild.id, int(boss_row["id"]))
    mentions = " ".join(f"<@{u}>" for u in users) if users else "@here"
    try:
        await channel.send(f"{mentions} • **{boss_row['name']}** — {text}")
    except Exception as e:
        log.warning("Failed to send subscription ping: %s", e)

# -------------------- BLACKLIST CHECK --------------------
async def is_blacklisted(guild_id: int, user_id: int) -> bool:
    r = await db_fetchone("SELECT 1 FROM blacklist WHERE guild_id=? AND user_id=?", guild_id, user_id)
    return bool(r)

# -------------------- MARKET / LIXING HELPERS --------------------
async def market_create_listing(guild_id: int, owner_id: int, item_name: str, qty: int,
                                price: str, accepts_trades: bool, taking_offers: bool, is_buy: bool) -> int:
    ts = now_ts()
    await db_execute("""INSERT INTO market_listings (guild_id, owner_id, item_name, quantity, price,
                       accepts_trades, taking_offers, is_buy, created_ts)
                       VALUES (?,?,?,?,?,?,?,?,?)""",
                     guild_id, owner_id, item_name.strip(), int(qty), price.strip(),
                     1 if accepts_trades else 0, 1 if taking_offers else 0, 1 if is_buy else 0, ts)
    r = await db_fetchone("""SELECT id FROM market_listings WHERE guild_id=? AND owner_id=? AND item_name=? 
                             ORDER BY id DESC LIMIT 1""", guild_id, owner_id, item_name.strip())
    return int(r["id"])

async def market_list(guild_id: int, mode: Optional[str]=None, text: Optional[str]=None) -> List[aiosqlite.Row]:
    sql = "SELECT * FROM market_listings WHERE guild_id=?"
    params: List[Any] = [guild_id]
    if mode in {"buy","sell"}:
        sql += " AND is_buy=?"; params.append(1 if mode=="buy" else 0)
    if text:
        sql += " AND lower(item_name) LIKE ?"; params.append(f"%{text.lower()}%")
    sql += " ORDER BY created_ts DESC"
    return await db_fetchall(sql, *params)

async def market_offer(guild_id: int, listing_id: int, bidder_id: int, text: str) -> int:
    ts = now_ts()
    await db_execute("""INSERT INTO market_offers (guild_id, listing_id, bidder_id, text, created_ts)
                        VALUES (?,?,?,?,?)""", guild_id, listing_id, bidder_id, text.strip(), ts)
    r = await db_fetchone("""SELECT id FROM market_offers WHERE guild_id=? AND listing_id=? 
                             ORDER BY created_ts DESC LIMIT 1""", guild_id, listing_id)
    return int(r["id"])

async def market_offers_for_listing(guild_id: int, listing_id: int) -> List[aiosqlite.Row]:
    return await db_fetchall("""SELECT * FROM market_offers WHERE guild_id=? AND listing_id=? 
                                ORDER BY created_ts DESC""", guild_id, listing_id)

async def lixing_create_post(guild_id: int, owner_id: int, char_name: str, char_class: str,
                             char_level: str, desired_lixes: str) -> int:
    ts = now_ts()
    await db_execute("""INSERT INTO lixing_posts (guild_id, owner_id, char_name, char_class, char_level,
                       desired_lixes, created_ts) VALUES (?,?,?,?,?,?,?)""",
                     guild_id, owner_id, char_name.strip(), char_class.strip(),
                     char_level.strip(), desired_lixes.strip() or "N/A", ts)
    r = await db_fetchone("""SELECT id FROM lixing_posts WHERE guild_id=? AND owner_id=? 
                             ORDER BY id DESC LIMIT 1""", guild_id, owner_id)
    return int(r["id"])

async def lixing_list(guild_id: int, text: Optional[str]=None) -> List[aiosqlite.Row]:
    sql = "SELECT * FROM lixing_posts WHERE guild_id=?"
    params: List[Any] = [guild_id]
    if text:
        sql += " AND (lower(char_name) LIKE ? OR lower(char_class) LIKE ?)"
        params.extend([f"%{text.lower()}%", f"%{text.lower()}%"])
    sql += " ORDER BY created_ts DESC"
    return await db_fetchall(sql, *params)

# -------------------- DIGEST RENDERERS --------------------
def render_market_digest(rows: List[aiosqlite.Row]) -> str:
    if not rows: return "_No active listings._"
    lines = []
    for r in rows[:50]:
        tag = "BUY" if r["is_buy"] else "SELL"
        trades = "Trades✓" if r["accepts_trades"] else "Trades×"
        offers = "Offers✓" if r["taking_offers"] else "Offers×"
        lines.append(f"**[{tag}]** {r['item_name']} x{r['quantity']} — {r['price']}  ({trades}, {offers})  — by <@{r['owner_id']}>  `#{r['id']}`")
    return "\n".join(lines)

def render_lixing_digest(rows: List[aiosqlite.Row]) -> str:
    if not rows: return "_No current Lixing posts._"
    lines = []
    for r in rows[:50]:
        lines.append(f"**{r['char_name']}** — {r['char_class']} {r['char_level']} • desired: {r['desired_lixes']} — by <@{r['owner_id']}>  `#{r['id']}`")
    return "\n".join(lines)
# bot.py — Section 3/4: commands, views, modals, UI flows

# -------------------- INTERACTION GATE + SMALL UTILITIES --------------------
async def _guild_gate(inter: discord.Interaction) -> bool:
    """Common gate: must be in a guild, guild authorized, and user not blacklisted."""
    if not inter.guild:
        await inter.response.send_message("This command can only be used in a server.", ephemeral=True)
        return False
    if not await ensure_guild_auth(inter.guild):
        await inter.response.send_message("Bot is not authorized to run on this server.", ephemeral=True)
        return False
    if await is_blacklisted(inter.guild.id, inter.user.id):
        await inter.response.send_message("You are not allowed to use this bot here.", ephemeral=True)
        return False
    return True

def _admin_check(member: discord.Member) -> bool:
    return member.guild_permissions.manage_guild or member.guild_permissions.administrator

def _choice_cat() -> List[app_commands.Choice[str]]:
    return [app_commands.Choice(name=c, value=c) for c in CATEGORY_ORDER]

# -------------------- HELP (clean; no auth/admin/blacklist in help) --------------------
@bot.tree.command(name="help", description="Show bot commands and tips")
async def help_cmd(inter: discord.Interaction):
    if not await _guild_gate(inter): return
    e = discord.Embed(
        title="Celtic Heroes Bot — Help",
        color=discord.Color.blurple(),
        description=(
            "Boss timers, subscriptions, Market, and Lixing tools.\n"
            "Windows show **minutes**: `open` → `closed` → `-Nada` (after grace)."
        )
    )
    e.add_field(
        name="Timers",
        value=(
            "• `/timers` — show all categories\n"
            "• `/timers category <name>` — one category (DL, EDL, EG, Midraids, etc.)"
        ), inline=False
    )
    e.add_field(
        name="Subscriptions",
        value=(
            "• React on the **Subscription Panels** to (un)subscribe\n"
            "• `/sub list` — list your subscriptions\n"
            "• `/sub toggle <boss>` — subscribe/unsubscribe by boss/alias"
        ), inline=False
    )
    e.add_field(
        name="Market",
        value=(
            "• `/market` — open listings menu (create BUY/SELL, filter, offer)\n"
            "• Offers appear on the listing and ping the owner\n"
            "• Optional: `/market_remove <id>` to remove your listing"
        ), inline=False
    )
    e.add_field(
        name="Lixing",
        value=(
            "• `/lixing` — open Lixing menu (create post, refresh)\n"
            "• Optional: `/lixing_remove <id>` to remove your post"
        ), inline=False
    )
    e.add_field(
        name="Tips",
        value=(
            f"• Use `{DEFAULT_PREFIX}<boss>` as a quick kill shorthand (admins/trusted).\n"
            "• Admins can refresh panels with `/subs refresh-all`."
        ), inline=False
    )
    e.set_footer(text="Admins: use /config and /subs to set channels/panels (hidden from this help).")
    await inter.response.send_message(embed=e, ephemeral=True)

# -------------------- TIMERS --------------------
@bot.tree.command(name="timers", description="Show boss timers.")
@app_commands.describe(category="Optional category (e.g., DL, EDL, EG, Midraids)")
@app_commands.choices(category=_choice_cat())
async def slash_timers(inter: discord.Interaction, category: Optional[app_commands.Choice[str]]=None):
    if not await _guild_gate(inter): return
    guild = inter.guild
    await inter.response.defer(ephemeral=False)
    if category:
        embeds = await build_timer_embeds_for_categories(guild, [category.value])
    else:
        embeds = await build_timer_embeds_for_categories(guild, CATEGORY_ORDER)
    if not embeds:
        await inter.followup.send("No timers to show yet. Add bosses or wait for seed to finish.")
        return
    for em in embeds:
        await inter.followup.send(embed=em)

# -------------------- SUBSCRIPTIONS (user) --------------------
sub_group = app_commands.Group(name="sub", description="Manage boss subscriptions")

@sub_group.command(name="list", description="List your subscriptions")
async def sub_list(inter: discord.Interaction):
    if not await _guild_gate(inter): return
    rows = await db_fetchall("""SELECT b.name FROM subscription_members s
                                JOIN bosses b ON b.id=s.boss_id
                                WHERE s.guild_id=? AND s.user_id=?
                                ORDER BY b.category, b.sort_key""",
                             inter.guild.id, inter.user.id)
    if not rows:
        await inter.response.send_message("You aren't subscribed to any bosses. Use the panels or `/sub toggle <boss>`.", ephemeral=True)
        return
    names = ", ".join(r["name"] for r in rows)
    await inter.response.send_message(f"You're subscribed to: **{names}**", ephemeral=True)

@sub_group.command(name="toggle", description="Toggle subscription for a boss")
@app_commands.describe(boss="Boss name or alias")
async def sub_toggle(inter: discord.Interaction, boss: str):
    if not await _guild_gate(inter): return
    b = await resolve_boss_by_name_or_alias(inter.guild.id, boss)
    if not b:
        await inter.response.send_message("Couldn't find that boss.", ephemeral=True)
        return
    on = await toggle_subscription(inter.guild.id, inter.user.id, int(b["id"]))
    await inter.response.send_message(
        f"{'Subscribed to' if on else 'Unsubscribed from'} **{b['name']}**.", ephemeral=True
    )

bot.tree.add_command(sub_group)

# -------------------- SUBSCRIPTIONS (admin/panels) --------------------
subs_admin = app_commands.Group(name="subs", description="Admin: subscription panels")

@subs_admin.command(name="panel", description="Post or refresh a subscription panel for a category")
@app_commands.describe(category="Category name")
@app_commands.choices(category=_choice_cat())
async def subs_panel(inter: discord.Interaction, category: app_commands.Choice[str]):
    if not await _guild_gate(inter): return
    if not _admin_check(inter.user): 
        await inter.response.send_message("Admin only.", ephemeral=True); return
    await inter.response.defer(ephemeral=True)
    m = await post_or_refresh_sub_panel(inter.guild, category.value)
    if m:
        await inter.followup.send(f"Panel ready for **{category.value}** in <#{m.channel.id}>", ephemeral=True)
    else:
        await inter.followup.send("No subscription channel configured. Set it with `/config subpanel-channel`.", ephemeral=True)

@subs_admin.command(name="refresh-all", description="Refresh all subscription panels")
async def subs_refresh_all(inter: discord.Interaction):
    if not await _guild_gate(inter): return
    if not _admin_check(inter.user):
        await inter.response.send_message("Admin only.", ephemeral=True); return
    await inter.response.defer(ephemeral=True)
    await refresh_all_sub_panels(inter.guild)
    await inter.followup.send("All subscription panels refreshed.", ephemeral=True)

bot.tree.add_command(subs_admin)

# -------------------- CONFIG (admin) --------------------
config_group = app_commands.Group(name="config", description="Admin: configure channels and options")

@config_group.command(name="subpanel-channel", description="Set the channel where subscription panels live")
@app_commands.describe(channel="Target channel for subscription panels")
async def cfg_subpanel_channel(inter: discord.Interaction, channel: discord.TextChannel):
    if not await _guild_gate(inter): return
    if not _admin_check(inter.user): 
        await inter.response.send_message("Admin only.", ephemeral=True); return
    await set_config_value(inter.guild.id, "sub_channel_id", channel.id)
    await inter.response.send_message(f"Subscription panel channel set to {channel.mention}", ephemeral=True)

@config_group.command(name="subping-channel", description="Set the channel where subscription pings are sent")
@app_commands.describe(channel="Target channel for subscription pings")
async def cfg_subping_channel(inter: discord.Interaction, channel: discord.TextChannel):
    if not await _guild_gate(inter): return
    if not _admin_check(inter.user):
        await inter.response.send_message("Admin only.", ephemeral=True); return
    await set_config_value(inter.guild.id, "sub_ping_channel_id", channel.id)
    await inter.response.send_message(f"Subscription **pings** will go to {channel.mention}", ephemeral=True)

@config_group.command(name="default-channel", description="Set default output channel for bot messages")
@app_commands.describe(channel="Default channel used by the bot")
async def cfg_default_channel(inter: discord.Interaction, channel: discord.TextChannel):
    if not await _guild_gate(inter): return
    if not _admin_check(inter.user):
        await inter.response.send_message("Admin only.", ephemeral=True); return
    await set_config_value(inter.guild.id, "default_channel", channel.id)
    await inter.response.send_message(f"Default channel set to {channel.mention}", ephemeral=True)

@config_group.command(name="heartbeat-channel", description="Set the heartbeat/status channel")
@app_commands.describe(channel="Destination for uptime heartbeat pings")
async def cfg_heartbeat_channel(inter: discord.Interaction, channel: discord.TextChannel):
    if not await _guild_gate(inter): return
    if not _admin_check(inter.user):
        await inter.response.send_message("Admin only.", ephemeral=True); return
    await set_config_value(inter.guild.id, "heartbeat_channel_id", channel.id)
    await inter.response.send_message(f"Heartbeat channel set to {channel.mention}", ephemeral=True)

@config_group.command(name="uptime-minutes", description="Set minutes between heartbeat pings (0 to disable)")
@app_commands.describe(minutes="Interval in minutes (0 to turn off)")
async def cfg_uptime_minutes(inter: discord.Interaction, minutes: int):
    if not await _guild_gate(inter): return
    if not _admin_check(inter.user):
        await inter.response.send_message("Admin only.", ephemeral=True); return
    await set_config_value(inter.guild.id, "uptime_minutes", max(0, int(minutes)))
    await inter.response.send_message(f"Heartbeat interval set to **{max(0, int(minutes))}m**", ephemeral=True)

@config_group.command(name="category-channel", description="Map a category to a specific announce channel")
@app_commands.describe(category="Boss category", channel="Target announce channel")
@app_commands.choices(category=_choice_cat())
async def cfg_category_channel(inter: discord.Interaction, category: app_commands.Choice[str], channel: discord.TextChannel):
    if not await _guild_gate(inter): return
    if not _admin_check(inter.user):
        await inter.response.send_message("Admin only.", ephemeral=True); return
    await db_execute("""INSERT INTO category_channels (guild_id,category,channel_id) VALUES (?,?,?)
                        ON CONFLICT(guild_id,category) DO UPDATE SET channel_id=excluded.channel_id""",
                     inter.guild.id, category.value, channel.id)
    await inter.response.send_message(f"Category **{category.value}** → {channel.mention}", ephemeral=True)

@config_group.command(name="color", description="Set color for a category embed")
@app_commands.describe(category="Boss category", hex_color="Hex color without #, e.g. 3498db")
@app_commands.choices(category=_choice_cat())
async def cfg_color(inter: discord.Interaction, category: app_commands.Choice[str], hex_color: str):
    if not await _guild_gate(inter): return
    if not _admin_check(inter.user):
        await inter.response.send_message("Admin only.", ephemeral=True); return
    hx = hex_color.strip().lstrip("#")
    try:
        int(hx, 16)
    except ValueError:
        await inter.response.send_message("Invalid hex color.", ephemeral=True); return
    await db_execute("""INSERT INTO category_colors (guild_id,category,color_hex) VALUES (?,?,?)
                        ON CONFLICT(guild_id,category) DO UPDATE SET color_hex=excluded.color_hex""",
                     inter.guild.id, category.value, hx.lower())
    await inter.response.send_message(f"Color for **{category.value}** set to `#{hx.lower()}`", ephemeral=True)

@config_group.command(name="auth-user", description="(Hidden) Set required user to unlock bot in this server")
@app_commands.describe(user="User mention who must be present in the server")
async def cfg_auth_user(inter: discord.Interaction, user: discord.Member):
    if not await _guild_gate(inter): return
    if not _admin_check(inter.user):
        await inter.response.send_message("Admin only.", ephemeral=True); return
    await set_config_value(inter.guild.id, "auth_user_id", user.id)
    await inter.response.send_message(f"Auth gate set. Bot will only run while {user.mention} is in this server.", ephemeral=True)

bot.tree.add_command(config_group)

# -------------------- MARKET UI --------------------
class MarketCreateModal(discord.ui.Modal, title="New Market Listing"):
    def __init__(self, is_buy: bool):
        super().__init__(timeout=900)
        self.is_buy = is_buy
        self.item = discord.ui.TextInput(label="Item name", placeholder="Phoenix Egg", max_length=100)
        self.qty = discord.ui.TextInput(label="Quantity", placeholder="1", default="1", max_length=6)
        self.price = discord.ui.TextInput(label="Price / Range", placeholder="200k or 150k-250k", max_length=50)
        self.trades = discord.ui.TextInput(label="Accept trades? (yes/no)", default="no", max_length=5)
        self.offers = discord.ui.TextInput(label="Taking offers/bids? (yes/no)", default="yes", max_length=5)
        for c in (self.item, self.qty, self.price, self.trades, self.offers):
            self.add_item(c)

    async def on_submit(self, inter: discord.Interaction):
        if not await _guild_gate(inter): return
        try:
            qty = max(1, int(str(self.qty)))
        except Exception:
            await inter.response.send_message("Quantity must be a number.", ephemeral=True); return
        accepts_trades = str(self.trades).strip().lower().startswith("y")
        taking_offers = str(self.offers).strip().lower().startswith("y")
        listing_id = await market_create_listing(
            inter.guild.id, inter.user.id, str(self.item), qty, str(self.price),
            accepts_trades, taking_offers, self.is_buy
        )
        await inter.response.send_message(f"Listing **#{listing_id}** created.", ephemeral=True)

class OfferModal(discord.ui.Modal, title="Make an Offer/Bid"):
    def __init__(self, listing_id: Optional[int]=None):
        super().__init__(timeout=900)
        self.listing_field = discord.ui.TextInput(label="Listing ID", placeholder="#123 or 123", required=(listing_id is None),
                                                  default=str(listing_id) if listing_id else None, max_length=12)
        self.offer_text = discord.ui.TextInput(label="Your offer", placeholder="e.g., 220k or Trade: X + Y", max_length=200)
        self.add_item(self.listing_field)
        self.add_item(self.offer_text)

    async def on_submit(self, inter: discord.Interaction):
        if not await _guild_gate(inter): return
        raw = str(self.listing_field).strip().lstrip("#")
        if not raw.isdigit():
            await inter.response.send_message("Listing ID must be a number like `#123`.", ephemeral=True); return
        lid = int(raw)
        oid = await market_offer(inter.guild.id, lid, inter.user.id, str(self.offer_text))
        # Notify listing owner if possible
        listing = await db_fetchone("SELECT owner_id FROM market_listings WHERE guild_id=? AND id=?",
                                    inter.guild.id, lid)
        if listing:
            try:
                user = inter.guild.get_member(int(listing["owner_id"]))
                if user:
                    await user.send(f"New offer on your Market listing `#{lid}` from {inter.user.mention}: {str(self.offer_text)}")
            except Exception:
                pass
        await inter.response.send_message(f"Offer **#{oid}** posted on listing `#{lid}`.", ephemeral=True)

class MarketMenuView(discord.ui.View):
    def __init__(self, author_id: int):
        super().__init__(timeout=300)
        self.author_id = author_id
        self.mode = "all"
        self.search = None

    async def _refresh(self, inter: discord.Interaction):
        rows = await market_list(inter.guild.id, None if self.mode=="all" else self.mode, self.search)
        e = discord.Embed(title="Market — Listings", color=discord.Color.gold())
        e.description = render_market_digest(rows)
        e.set_footer(text="Click Offer to bid; include #ID if prompted.")
        await inter.response.edit_message(embed=e, view=self)

    @discord.ui.select(placeholder="Filter: all/buy/sell", min_values=1, max_values=1,
                       options=[
                           discord.SelectOption(label="All", value="all"),
                           discord.SelectOption(label="Buy", value="buy"),
                           discord.SelectOption(label="Sell", value="sell"),
                       ])
    async def filter_select(self, inter: discord.Interaction, select: discord.ui.Select):
        if inter.user.id != self.author_id:
            await inter.response.send_message("Open your own menu with `/market`.", ephemeral=True); return
        self.mode = select.values[0]
        await self._refresh(inter)

    @discord.ui.button(label="Create BUY", style=discord.ButtonStyle.success)
    async def btn_create_buy(self, inter: discord.Interaction, btn: discord.ui.Button):
        if inter.user.id != self.author_id:
            await inter.response.send_message("Open your own menu with `/market`.", ephemeral=True); return
        await inter.response.send_modal(MarketCreateModal(is_buy=True))

    @discord.ui.button(label="Create SELL", style=discord.ButtonStyle.primary)
    async def btn_create_sell(self, inter: discord.Interaction, btn: discord.ui.Button):
        if inter.user.id != self.author_id:
            await inter.response.send_message("Open your own menu with `/market`.", ephemeral=True); return
        await inter.response.send_modal(MarketCreateModal(is_buy=False))

    @discord.ui.button(label="Offer", style=discord.ButtonStyle.secondary)
    async def btn_offer(self, inter: discord.Interaction, btn: discord.ui.Button):
        await inter.response.send_modal(OfferModal())

    @discord.ui.button(label="Refresh", style=discord.ButtonStyle.secondary)
    async def btn_refresh(self, inter: discord.Interaction, btn: discord.ui.Button):
        await self._refresh(inter)

# Slash entry for Market
@bot.tree.command(name="market", description="Open the Market menu")
async def market_cmd(inter: discord.Interaction):
    if not await _guild_gate(inter): return
    view = MarketMenuView(inter.user.id)
    rows = await market_list(inter.guild.id)
    e = discord.Embed(title="Market — Listings", color=discord.Color.gold(), description=render_market_digest(rows))
    e.set_footer(text="Use the menu to filter or create listings. Use Offer to bid.")
    await inter.response.send_message(embed=e, view=view, ephemeral=True)

@bot.tree.command(name="market_remove", description="Remove one of your market listings (owner or admin)")
@app_commands.describe(listing_id="Listing ID number (e.g., 123)")
async def market_remove_cmd(inter: discord.Interaction, listing_id: int):
    if not await _guild_gate(inter): return
    row = await db_fetchone("SELECT owner_id FROM market_listings WHERE guild_id=? AND id=?",
                            inter.guild.id, listing_id)
    if not row:
        await inter.response.send_message("Listing not found.", ephemeral=True); return
    if (int(row["owner_id"]) != inter.user.id) and (not _admin_check(inter.user)):
        await inter.response.send_message("Only the owner or an admin can remove this listing.", ephemeral=True); return
    await db_execute("DELETE FROM market_listings WHERE guild_id=? AND id=?", inter.guild.id, listing_id)
    await db_execute("DELETE FROM market_offers WHERE guild_id=? AND listing_id=?", inter.guild.id, listing_id)
    await inter.response.send_message(f"Listing `#{listing_id}` removed.", ephemeral=True)

# -------------------- LIXING UI --------------------
class LixingCreateModal(discord.ui.Modal, title="New Lixing Post"):
    def __init__(self):
        super().__init__(timeout=900)
        self.char_name = discord.ui.TextInput(label="Character name", max_length=50)
        self.char_class = discord.ui.TextInput(label="Class", placeholder="Rogue/Druid/etc.", max_length=40)
        self.char_level = discord.ui.TextInput(label="Level", placeholder="e.g., 180", max_length=20)
        self.desired = discord.ui.TextInput(label="Desired lixes (or N/A)", default="N/A", max_length=40)
        for c in (self.char_name, self.char_class, self.char_level, self.desired):
            self.add_item(c)

    async def on_submit(self, inter: discord.Interaction):
        if not await _guild_gate(inter): return
        post_id = await lixing_create_post(inter.guild.id, inter.user.id, str(self.char_name),
                                           str(self.char_class), str(self.char_level), str(self.desired))
        await inter.response.send_message(f"Lixing post **#{post_id}** created.", ephemeral=True)

class LixingMenuView(discord.ui.View):
    def __init__(self, author_id: int):
        super().__init__(timeout=300)
        self.author_id = author_id
        self.search = None

    async def _refresh(self, inter: discord.Interaction):
        rows = await lixing_list(inter.guild.id, self.search)
        e = discord.Embed(title="Lixing — Posts", color=discord.Color.teal())
        e.description = render_lixing_digest(rows)
        await inter.response.edit_message(embed=e, view=self)

    @discord.ui.button(label="Create Post", style=discord.ButtonStyle.primary)
    async def btn_create(self, inter: discord.Interaction, btn: discord.ui.Button):
        if inter.user.id != self.author_id:
            await inter.response.send_message("Open your own menu with `/lixing`.", ephemeral=True); return
        await inter.response.send_modal(LixingCreateModal())

    @discord.ui.button(label="Refresh", style=discord.ButtonStyle.secondary)
    async def btn_refresh(self, inter: discord.Interaction, btn: discord.ui.Button):
        await self._refresh(inter)

@bot.tree.command(name="lixing", description="Open the Lixing menu")
async def lixing_cmd(inter: discord.Interaction):
    if not await _guild_gate(inter): return
    view = LixingMenuView(inter.user.id)
    rows = await lixing_list(inter.guild.id)
    e = discord.Embed(title="Lixing — Posts", color=discord.Color.teal(), description=render_lixing_digest(rows))
    await inter.response.send_message(embed=e, view=view, ephemeral=True)

@bot.tree.command(name="lixing_remove", description="Remove one of your Lixing posts (owner or admin)")
@app_commands.describe(post_id="Post ID number (e.g., 321)")
async def lixing_remove_cmd(inter: discord.Interaction, post_id: int):
    if not await _guild_gate(inter): return
    row = await db_fetchone("SELECT owner_id FROM lixing_posts WHERE guild_id=? AND id=?",
                            inter.guild.id, post_id)
    if not row:
        await inter.response.send_message("Post not found.", ephemeral=True); return
    if (int(row["owner_id"]) != inter.user.id) and (not _admin_check(inter.user)):
        await inter.response.send_message("Only the owner or an admin can remove this post.", ephemeral=True); return
    await db_execute("DELETE FROM lixing_posts WHERE guild_id=? AND id=?", inter.guild.id, post_id)
    await inter.response.send_message(f"Lixing post `#{post_id}` removed.", ephemeral=True)

# -------------------- BOSS ADMIN (kept; not shown in /help) --------------------
boss_group = app_commands.Group(name="boss", description="Admin: boss management")

@boss_group.command(name="add", description="Add or update a boss")
@app_commands.describe(name="Boss name", category="Category", spawn_minutes="Spawn interval in minutes",
                       window_minutes="Window length in minutes (0 for none)")
@app_commands.choices(category=_choice_cat())
async def boss_add(inter: discord.Interaction, name: str, category: app_commands.Choice[str],
                   spawn_minutes: int, window_minutes: Optional[int]=0):
    if not await _guild_gate(inter): return
    if not _admin_check(inter.user):
        await inter.response.send_message("Admin only.", ephemeral=True); return
    bid = await upsert_boss(inter.guild.id, name, int(spawn_minutes), int(window_minutes or 0), category.value, created_by=inter.user.id)
    await inter.response.send_message(f"Boss **{name}** upserted (id `{bid}`).", ephemeral=True)

@boss_group.command(name="alias", description="Add an alias for a boss")
@app_commands.describe(name="Existing boss name", alias="New alias")
async def boss_alias(inter: discord.Interaction, name: str, alias: str):
    if not await _guild_gate(inter): return
    if not _admin_check(inter.user):
        await inter.response.send_message("Admin only.", ephemeral=True); return
    row = await resolve_boss_by_name_or_alias(inter.guild.id, name)
    if not row:
        await inter.response.send_message("Boss not found.", ephemeral=True); return
    ok = await add_alias(inter.guild.id, int(row["id"]), alias)
    await inter.response.send_message(("Alias added." if ok else "Alias already exists."), ephemeral=True)

@boss_group.command(name="list", description="List bosses")
async def boss_list(inter: discord.Interaction):
    if not await _guild_gate(inter): return
    if not _admin_check(inter.user):
        await inter.response.send_message("Admin only.", ephemeral=True); return
    groups = await get_all_bosses_grouped(inter.guild.id)
    desc = []
    for cat in CATEGORY_ORDER:
        if not groups.get(cat): continue
        desc.append(f"**{cat}**: " + ", ".join(b['name'] for b in groups[cat]))
    e = discord.Embed(title="Bosses", description="\n".join(desc) if desc else "—", color=discord.Color.dark_gray())
    await inter.response.send_message(embed=e, ephemeral=True)

@boss_group.command(name="kill", description="Mark a boss killed now (sets next = now + spawn_minutes)")
@app_commands.describe(boss="Boss name or alias")
async def boss_kill(inter: discord.Interaction, boss: str):
    if not await _guild_gate(inter): return
    if not _admin_check(inter.user): 
        await inter.response.send_message("Admin only.", ephemeral=True); return
    b = await resolve_boss_by_name_or_alias(inter.guild.id, boss)
    if not b:
        await inter.response.send_message("Boss not found.", ephemeral=True); return
    next_ts = now_ts() + int(b["spawn_minutes"]) * 60
    await set_boss_next(inter.guild.id, int(b["id"]), next_ts)
    await inter.response.send_message(f"Marked **{b['name']}** killed. Next in `{b['spawn_minutes']}m`.", ephemeral=True)

@boss_group.command(name="set-next-mins", description="Set next spawn in N minutes from now")
@app_commands.describe(boss="Boss", minutes="Minutes from now")
async def boss_set_next_mins(inter: discord.Interaction, boss: str, minutes: int):
    if not await _guild_gate(inter): return
    if not _admin_check(inter.user): 
        await inter.response.send_message("Admin only.", ephemeral=True); return
    b = await resolve_boss_by_name_or_alias(inter.guild.id, boss)
    if not b:
        await inter.response.send_message("Boss not found.", ephemeral=True); return
    await set_boss_next(inter.guild.id, int(b["id"]), now_ts() + max(0, int(minutes))*60)
    await inter.response.send_message(f"Next spawn for **{b['name']}** set to `{max(0,int(minutes))}m` from now.", ephemeral=True)

@boss_group.command(name="set-window", description="Set window minutes for a boss")
@app_commands.describe(boss="Boss", window_minutes="Window length in minutes")
async def boss_set_window(inter: discord.Interaction, boss: str, window_minutes: int):
    if not await _guild_gate(inter): return
    if not _admin_check(inter.user): 
        await inter.response.send_message("Admin only.", ephemeral=True); return
    b = await resolve_boss_by_name_or_alias(inter.guild.id, boss)
    if not b:
        await inter.response.send_message("Boss not found.", ephemeral=True); return
    await db_execute("UPDATE bosses SET window_minutes=? WHERE guild_id=? AND id=?",
                     max(0, int(window_minutes)), inter.guild.id, int(b["id"]))
    await inter.response.send_message(f"**{b['name']}** window set to `{max(0,int(window_minutes))}m`.", ephemeral=True)

@boss_group.command(name="set-preannounce", description="Set pre-announce minutes for a boss")
@app_commands.describe(boss="Boss", minutes="Minutes before next spawn to pre-announce")
async def boss_set_preannounce(inter: discord.Interaction, boss: str, minutes: int):
    if not await _guild_gate(inter): return
    if not _admin_check(inter.user): 
        await inter.response.send_message("Admin only.", ephemeral=True); return
    b = await resolve_boss_by_name_or_alias(inter.guild.id, boss)
    if not b:
        await inter.response.send_message("Boss not found.", ephemeral=True); return
    await db_execute("UPDATE bosses SET pre_announce_min=? WHERE guild_id=? AND id=?",
                     max(0, int(minutes)), inter.guild.id, int(b["id"]))
    await inter.response.send_message(f"**{b['name']}** pre-announce set to `{max(0,int(minutes))}m`.", ephemeral=True)

@boss_group.command(name="set-channel", description="Set announce channel for a boss (overrides category/default)")
@app_commands.describe(boss="Boss", channel="Announce channel")
async def boss_set_channel(inter: discord.Interaction, boss: str, channel: discord.TextChannel):
    if not await _guild_gate(inter): return
    if not _admin_check(inter.user): 
        await inter.response.send_message("Admin only.", ephemeral=True); return
    b = await resolve_boss_by_name_or_alias(inter.guild.id, boss)
    if not b:
        await inter.response.send_message("Boss not found.", ephemeral=True); return
    await db_execute("UPDATE bosses SET channel_id=? WHERE guild_id=? AND id=?", channel.id, inter.guild.id, int(b["id"]))
    await inter.response.send_message(f"**{b['name']}** announce channel set to {channel.mention}.", ephemeral=True)

@boss_group.command(name="set-trustedrole", description="Set a role allowed to reset this boss via shorthand")
@app_commands.describe(boss="Boss", role="Role mention")
async def boss_set_trustedrole(inter: discord.Interaction, boss: str, role: discord.Role):
    if not await _guild_gate(inter): return
    if not _admin_check(inter.user): 
        await inter.response.send_message("Admin only.", ephemeral=True); return
    b = await resolve_boss_by_name_or_alias(inter.guild.id, boss)
    if not b:
        await inter.response.send_message("Boss not found.", ephemeral=True); return
    await db_execute("UPDATE bosses SET trusted_role_id=? WHERE guild_id=? AND id=?", role.id, inter.guild.id, int(b["id"]))
    await inter.response.send_message(f"Trusted role for **{b['name']}** set to <@&{role.id}>.", ephemeral=True)

@boss_group.command(name="notes", description="Set or clear notes for a boss")
@app_commands.describe(boss="Boss", text="Optional notes (leave empty to clear)")
async def boss_notes(inter: discord.Interaction, boss: str, text: Optional[str]=None):
    if not await _guild_gate(inter): return
    if not _admin_check(inter.user): 
        await inter.response.send_message("Admin only.", ephemeral=True); return
    b = await resolve_boss_by_name_or_alias(inter.guild.id, boss)
    if not b:
        await inter.response.send_message("Boss not found.", ephemeral=True); return
    await db_execute("UPDATE bosses SET notes=? WHERE guild_id=? AND id=?", (text or ""), inter.guild.id, int(b["id"]))
    await inter.response.send_message(f"Notes updated for **{b['name']}**.", ephemeral=True)

@boss_group.command(name="remove", description="Remove a boss")
@app_commands.describe(name="Boss to remove")
async def boss_remove(inter: discord.Interaction, name: str):
    if not await _guild_gate(inter): return
    if not _admin_check(inter.user): 
        await inter.response.send_message("Admin only.", ephemeral=True); return
    b = await resolve_boss_by_name_or_alias(inter.guild.id, name)
    if not b:
        await inter.response.send_message("Boss not found.", ephemeral=True); return
    bid = int(b["id"])
    await db_execute("DELETE FROM subscription_emojis WHERE guild_id=? AND boss_id=?", inter.guild.id, bid)
    await db_execute("DELETE FROM subscription_members WHERE guild_id=? AND boss_id=?", inter.guild.id, bid)
    await db_execute("DELETE FROM boss_aliases WHERE guild_id=? AND boss_id=?", inter.guild.id, bid)
    await db_execute("DELETE FROM bosses WHERE guild_id=? AND id=?", inter.guild.id, bid)
    await inter.response.send_message(f"Boss **{b['name']}** removed.", ephemeral=True)

bot.tree.add_command(boss_group)

# -------------------- BLACKLIST (hidden admin) --------------------
bl_group = app_commands.Group(name="blacklist", description="Admin: block/unblock users (hidden from help)")

@bl_group.command(name="add", description="Blacklist a user from using the bot")
async def bl_add(inter: discord.Interaction, user: discord.Member):
    if not await _guild_gate(inter): return
    if not _admin_check(inter.user): 
        await inter.response.send_message("Admin only.", ephemeral=True); return
    await db_execute("INSERT OR IGNORE INTO blacklist (guild_id,user_id) VALUES (?,?)", inter.guild.id, user.id)
    await inter.response.send_message(f"{user.mention} blacklisted.", ephemeral=True)

@bl_group.command(name="remove", description="Remove a user from blacklist")
async def bl_remove(inter: discord.Interaction, user: discord.Member):
    if not await _guild_gate(inter): return
    if not _admin_check(inter.user): 
        await inter.response.send_message("Admin only.", ephemeral=True); return
    await db_execute("DELETE FROM blacklist WHERE guild_id=? AND user_id=?", inter.guild.id, user.id)
    await inter.response.send_message(f"{user.mention} removed from blacklist.", ephemeral=True)

@bl_group.command(name="list", description="Show blacklisted users")
async def bl_list(inter: discord.Interaction):
    if not await _guild_gate(inter): return
    if not _admin_check(inter.user): 
        await inter.response.send_message("Admin only.", ephemeral=True); return
    rows = await db_fetchall("SELECT user_id FROM blacklist WHERE guild_id=?", inter.guild.id)
    if not rows:
        await inter.response.send_message("No users are blacklisted.", ephemeral=True); return
    text = ", ".join(f"<@{r['user_id']}>" for r in rows)
    await inter.response.send_message(f"Blacklisted: {text}", ephemeral=True)

bot.tree.add_command(bl_group)
# bot.py — Section 4/4: loops, events, reactions, quick shorthand, roles panel, status/health, /ps, shutdown, main

# -------------- CHANNEL/ROLE PERMS --------------
def can_send(channel: Optional[discord.abc.GuildChannel]) -> bool:
    if not channel or not isinstance(channel, (discord.TextChannel, discord.Thread)): return False
    me = channel.guild.me
    if not me: return False
    perms = channel.permissions_for(me)
    return perms.view_channel and perms.send_messages and perms.embed_links and perms.read_message_history

def can_react(channel: Optional[discord.abc.GuildChannel]) -> bool:
    if not channel or not isinstance(channel, (discord.TextChannel, discord.Thread)): return False
    me = channel.guild.me
    if not me: return False
    perms = channel.permissions_for(me)
    return perms.add_reactions and perms.view_channel and perms.read_message_history

# -------------- ANNOUNCE-CHANNEL RESOLUTION --------------
async def resolve_announce_channel(guild: discord.Guild, boss_row: aiosqlite.Row) -> Optional[discord.TextChannel]:
    # Priority: per-boss channel_id → category channel (category_channels) → default_channel
    if boss_row.get("channel_id"):
        ch = guild.get_channel(int(boss_row["channel_id"]))
        if can_send(ch): return ch
    cat_row = await db_fetchone("SELECT channel_id FROM category_channels WHERE guild_id=? AND category=?",
                                guild.id, norm_cat(boss_row["category"]))
    if cat_row and cat_row["channel_id"]:
        ch = guild.get_channel(int(cat_row["channel_id"]))
        if can_send(ch): return ch
    def_id = await get_config_int(guild.id, "default_channel")
    if def_id:
        ch = guild.get_channel(def_id)
        if can_send(ch): return ch
    return None

# -------------- TRUST/PERM CHECK --------------
async def has_trusted(member: discord.Member, guild_id: int, boss_id: Optional[int] = None) -> bool:
    if member.guild_permissions.administrator: return True
    if member.guild_permissions.manage_messages: return True
    if boss_id:
        row = await db_fetchone("SELECT trusted_role_id FROM bosses WHERE id=? AND guild_id=?", boss_id, guild_id)
        if row and row["trusted_role_id"]:
            return any(role.id == int(row["trusted_role_id"]) for role in member.roles)
    return False

# -------------- OFFLINE BOOT CATCH-UP --------------
async def boot_offline_processing():
    boot = now_ts()
    off_since: Optional[int] = None
    off_explicit = await meta_get("offline_since")
    if off_explicit and off_explicit.isdigit():
        off_since = int(off_explicit)
    last_tick = await meta_get("last_tick_ts")
    if (off_since is None) and last_tick and last_tick.isdigit():
        last_tick_i = int(last_tick)
        if boot - last_tick_i > CHECK_INTERVAL_SECONDS * 2:
            off_since = last_tick_i
    await meta_set("offline_since", "")

    # Examine bosses and announce any windows that opened while we were offline
    for g in bot.guilds:
        rows = await db_fetchall("SELECT * FROM bosses WHERE guild_id=?", g.id)
        due = [r for r in rows if int(r["next_spawn_ts"]) <= boot]
        just_due = [r for r in due if off_since and off_since <= int(r["next_spawn_ts"]) <= boot] if off_since else []
        for b in just_due:
            ch = await resolve_announce_channel(g, b)
            if ch and can_send(ch):
                ago = human_ago(boot - int(b["next_spawn_ts"]))
                try:
                    await ch.send(f":zzz: While I was offline, **{b['name']}** spawned ({ago}).")
                except Exception:
                    pass
            await send_subscription_ping(g, b, "Spawn Window has opened!")

# -------------- LOOPS: TIMERS + HEARTBEAT + DIGESTS --------------
_last_timer_tick_ts = 0
_prev_timer_tick_ts = 0

@tasks.loop(seconds=CHECK_INTERVAL_SECONDS)
async def timers_tick():
    global _last_timer_tick_ts, _prev_timer_tick_ts
    now = now_ts()
    prev = _last_timer_tick_ts or (now - CHECK_INTERVAL_SECONDS)
    _prev_timer_tick_ts = prev
    _last_timer_tick_ts = now
    try: await meta_set("last_tick_ts", str(_last_timer_tick_ts))
    except Exception: pass

    # Pre-announces for future spawns that enter pre-window
    future = await db_fetchall("SELECT * FROM bosses WHERE next_spawn_ts > ?", now)
    for b in future:
        pre = int(b["pre_announce_min"] or 0)
        if pre <= 0: continue
        pre_ts = int(b["next_spawn_ts"]) - pre * 60
        if not (prev < pre_ts <= now):
            continue
        guild = bot.get_guild(int(b["guild_id"]))
        if not guild: continue
        ch = await resolve_announce_channel(guild, b)
        left = max(0, int(b["next_spawn_ts"]) - now)
        txt = f"⏳ **{b['name']}** — Spawn Time: `{fmt_delta_for_list(left)}` (almost up)."
        if ch and can_send(ch):
            try: await ch.send(txt)
            except Exception: pass
        await send_subscription_ping(guild, b, f"Spawn Time: `{fmt_delta_for_list(left)}` (almost up).")

    # Windows that opened since last tick
    opened = await db_fetchall("SELECT * FROM bosses WHERE next_spawn_ts <= ?", now)
    for b in opened:
        if not (prev < int(b["next_spawn_ts"]) <= now):
            continue
        guild = bot.get_guild(int(b["guild_id"]))
        if not guild: continue
        ch = await resolve_announce_channel(guild, b)
        if ch and can_send(ch):
            try: await ch.send(f"🕑 **{b['name']}** — **Spawn Window has opened!**")
            except Exception: pass
        await send_subscription_ping(guild, b, "Spawn Window has opened!")

@tasks.loop(minutes=1.0)
async def uptime_heartbeat():
    now_m = now_ts() // 60
    for g in bot.guilds:
        await upsert_guild_defaults(g.id)
        minutes = await get_config_int(g.id, "uptime_minutes")
        minutes = minutes if minutes is not None else DEFAULT_UPTIME_MINUTES
        if int(minutes) <= 0: continue
        if now_m % int(minutes) != 0: continue
        def_id = await get_config_int(g.id, "heartbeat_channel_id")
        ch = g.get_channel(def_id) if def_id else None
        if not ch:
            def_id = await get_config_int(g.id, "default_channel")
            ch = g.get_channel(def_id) if def_id else None
        if ch and can_send(ch):
            try: await ch.send("✅ Bot is online — timers active.")
            except Exception: pass

@tasks.loop(minutes=360)  # every 6 hours
async def market_digest_loop():
    for g in bot.guilds:
        ch_id = await get_config_int(g.id, "market_digest_channel_id")
        if not ch_id: continue
        ch = g.get_channel(ch_id)
        if not can_send(ch): continue
        rows = await market_list(g.id)
        if not rows: continue
        e = discord.Embed(title="Market Digest — last 6h", color=discord.Color.gold(), description=render_market_digest(rows))
        try: await ch.send(embed=e)
        except Exception: pass

@tasks.loop(minutes=360)  # every 6 hours
async def lixing_digest_loop():
    for g in bot.guilds:
        ch_id = await get_config_int(g.id, "lixing_digest_channel_id")
        if not ch_id: continue
        ch = g.get_channel(ch_id)
        if not can_send(ch): continue
        rows = await lixing_list(g.id)
        if not rows: continue
        e = discord.Embed(title="Lixing Digest — last 6h", color=discord.Color.teal(), description=render_lixing_digest(rows))
        try: await ch.send(embed=e)
        except Exception: pass

# -------------- EVENTS: READY / GUILD JOIN --------------
@bot.event
async def on_ready():
    # Ensure DB, defaults, and seeds
    await init_db()
    for g in bot.guilds:
        await upsert_guild_defaults(g.id)
        await ensure_seed_for_guild(g)

    # Start loops idempotently
    if not timers_tick.is_running(): timers_tick.start()
    if not uptime_heartbeat.is_running(): uptime_heartbeat.start()
    if not market_digest_loop.is_running(): market_digest_loop.start()
    if not lixing_digest_loop.is_running(): lixing_digest_loop.start()

    # Offline catch-up and panel refresh
    await boot_offline_processing()
    for g in bot.guilds:
        await refresh_all_sub_panels(g)

    # Sync slash commands (fixes "Unknown Command")
    try:
        await bot.tree.sync()
    except Exception as e:
        log.warning(f"App command sync failed: {e}")

    log.info(f"Logged in as {bot.user} (ID: {bot.user.id})")

@bot.event
async def on_guild_join(guild: discord.Guild):
    await init_db()
    await upsert_guild_defaults(guild.id)
    await ensure_seed_for_guild(guild)
    await refresh_all_sub_panels(guild)
    try:
        await bot.tree.sync(guild=guild)
    except Exception:
        pass

# -------------- REACTIONS: SUB PANELS + REACTION ROLES --------------
@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    if bot.user and payload.user_id == bot.user.id: return
    guild = bot.get_guild(payload.guild_id)
    if not guild: return
    emoji_str = str(payload.emoji)

    # Subscription panel?
    row = await db_fetchone("SELECT category, channel_id FROM subscription_panels WHERE guild_id=? AND message_id=?",
                            guild.id, payload.message_id)
    if row:
        bosses = await get_bosses_by_cat(guild.id, row["category"])
        # map emoji->boss_id
        for b in bosses:
            em = await get_or_assign_emoji(guild.id, b["id"])
            if em == emoji_str:
                await toggle_subscription(guild.id, payload.user_id, int(b["id"]), on=True)
                return

    # Reaction roles panel?
    rr = await db_fetchone("SELECT 1 FROM rr_panels WHERE message_id=?", payload.message_id)
    if rr:
        mapping = await db_fetchone("SELECT role_id FROM rr_map WHERE panel_message_id=? AND emoji=?",
                                    payload.message_id, emoji_str)
        if mapping:
            try:
                member = guild.get_member(payload.user_id) or await guild.fetch_member(payload.user_id)
                role = guild.get_role(int(mapping["role_id"]))
                if member and role:
                    await member.add_roles(role, reason="Reaction role opt-in")
            except Exception:
                pass

@bot.event
async def on_raw_reaction_remove(payload: discord.RawReactionActionEvent):
    guild = bot.get_guild(payload.guild_id)
    if not guild: return
    emoji_str = str(payload.emoji)

    # Subscription panel?
    row = await db_fetchone("SELECT category FROM subscription_panels WHERE guild_id=? AND message_id=?",
                            guild.id, payload.message_id)
    if row:
        bosses = await get_bosses_by_cat(guild.id, row["category"])
        for b in bosses:
            em = await get_or_assign_emoji(guild.id, b["id"])
            if em == emoji_str:
                await toggle_subscription(guild.id, payload.user_id, int(b["id"]), on=False)
                return

    # Reaction roles
    rr = await db_fetchone("SELECT 1 FROM rr_panels WHERE message_id=?", payload.message_id)
    if rr:
        mapping = await db_fetchone("SELECT role_id FROM rr_map WHERE panel_message_id=? AND emoji=?",
                                    payload.message_id, emoji_str)
        if mapping:
            try:
                member = guild.get_member(payload.user_id) or await guild.fetch_member(payload.user_id)
                role = guild.get_role(int(mapping["role_id"]))
                if member and role:
                    await member.remove_roles(role, reason="Reaction role opt-out")
            except Exception:
                pass

# -------------- QUICK SHORTHAND (prefix) --------------
RESERVED_TRIGGERS = {"help","boss","timers","market","lixing","config","sub","subs","admin"}

@bot.event
async def on_message(message: discord.Message):
    if not message.guild or message.author.bot:
        return
    # Global gate + blacklist
    if not await ensure_guild_auth(message.guild): 
        return
    if await is_blacklisted(message.guild.id, message.author.id):
        return

    content = (message.content or "").strip()
    if content.startswith(DEFAULT_PREFIX) and len(content) > len(DEFAULT_PREFIX):
        shorthand = content[len(DEFAULT_PREFIX):].strip()
        root = shorthand.split(" ", 1)[0].lower()
        if root not in RESERVED_TRIGGERS:
            ident = shorthand.strip().strip('"').strip("'")
            boss = await resolve_boss_by_name_or_alias(message.guild.id, ident)
            if boss:
                if await has_trusted(message.author, message.guild.id, int(boss["id"])):
                    next_ts = now_ts() + int(boss["spawn_minutes"])*60
                    await set_boss_next(message.guild.id, int(boss["id"]), next_ts)
                    if can_send(message.channel):
                        await message.channel.send(
                            f":crossed_swords: **{boss['name']}** killed. Next Spawn Time in `{boss['spawn_minutes']}m`."
                        )
                else:
                    if can_send(message.channel):
                        await message.channel.send(":no_entry: You lack permission to reset this boss.")
                return
    await bot.process_commands(message)

# -------------- STATUS / HEALTH / SYNC (slash) --------------
@bot.tree.command(name="status", description="Show server status/config snapshot")
async def status_cmd(inter: discord.Interaction):
    if not await _guild_gate(inter): return
    gid = inter.guild.id
    cfg = await db_fetchone("""SELECT COALESCE(prefix, ?) AS prefix,
                                      default_channel, sub_channel_id, sub_ping_channel_id,
                                      COALESCE(uptime_minutes, ?) AS hb,
                                      heartbeat_channel_id
                               FROM guild_config WHERE guild_id=?""", DEFAULT_PREFIX, DEFAULT_UPTIME_MINUTES, gid)
    count_row = await db_fetchone("SELECT COUNT(*) c FROM bosses WHERE guild_id=?", gid)
    boss_count = count_row["c"] if count_row else 0
    due_row = await db_fetchall("SELECT next_spawn_ts FROM bosses WHERE guild_id=?", gid)
    now_n = now_ts()
    due = sum(1 for r in due_row if int(r["next_spawn_ts"]) <= now_n)
    nada = sum(1 for r in due_row if (now_n - int(r["next_spawn_ts"])) > NADA_GRACE_SECONDS)
    def mention(cid): return f"<#{cid}>" if cid else "—"
    e = discord.Embed(title="Status", color=discord.Color.blurple())
    e.add_field(name="Channels",
                value=f"Default: {mention(cfg['default_channel'])}\n"
                      f"Sub Panels: {mention(cfg['sub_channel_id'])}\n"
                      f"Sub Pings: {mention(cfg['sub_ping_channel_id'])}\n"
                      f"Heartbeat: every {cfg['hb']}m → {mention(cfg['heartbeat_channel_id'])}",
                inline=False)
    e.add_field(name="Bosses", value=f"Total: {boss_count}\nDue now: {due}\n-Nada: {nada}", inline=False)
    await inter.response.send_message(embed=e, ephemeral=True)

@bot.tree.command(name="health", description="Admin: bot health snapshot")
async def health_cmd(inter: discord.Interaction):
    if not await _guild_gate(inter): return
    if not _admin_check(inter.user):
        await inter.response.send_message("Admin only.", ephemeral=True); return
    tick_age = now_ts() - _last_timer_tick_ts if _last_timer_tick_ts else None
    e = discord.Embed(title="Health", color=discord.Color.dark_gray())
    e.add_field(name="Timers loop", value="running" if timers_tick.is_running() else "stopped", inline=True)
    e.add_field(name="Heartbeat loop", value="running" if uptime_heartbeat.is_running() else "stopped", inline=True)
    e.add_field(name="Market digest", value="running" if market_digest_loop.is_running() else "stopped", inline=True)
    e.add_field(name="Lixing digest", value="running" if lixing_digest_loop.is_running() else "stopped", inline=True)
    e.add_field(name="Last timer tick", value=(ts_to_utc(_last_timer_tick_ts) if _last_timer_tick_ts else "—") + (f" ({human_ago(tick_age)})" if tick_age is not None else ""), inline=False)
    await inter.response.send_message(embed=e, ephemeral=True)

@bot.tree.command(name="sync", description="Admin: resync slash commands (use if commands show 'Unknown Command')")
async def sync_cmd(inter: discord.Interaction):
    if not await _guild_gate(inter): return
    if not _admin_check(inter.user):
        await inter.response.send_message("Admin only.", ephemeral=True); return
    await inter.response.defer(ephemeral=True)
    try:
        await bot.tree.sync(guild=inter.guild)
        await inter.followup.send("Slash commands resynced for this server.", ephemeral=True)
    except Exception as e:
        await inter.followup.send(f"Sync failed: {e}", ephemeral=True)

# -------------- REACTION ROLES SLASH --------------
@bot.tree.command(name="roles_panel", description="Admin: create a reaction-roles panel")
@app_commands.describe(channel="Target channel (or leave blank for current)", title="Panel title", pairs="Emoji + role mention pairs, comma-separated")
async def roles_panel(inter: discord.Interaction, title: str, pairs: str, channel: Optional[discord.TextChannel]=None):
    if not await _guild_gate(inter): return
    if not _admin_check(inter.user):
        await inter.response.send_message("Admin only.", ephemeral=True); return
    ch = channel or inter.channel
    if not can_send(ch):
        await inter.response.send_message("I can't post in that channel.", ephemeral=True); return
    entries = [e.strip() for e in pairs.split(",") if e.strip()]
    parsed: List[Tuple[str, int, str]] = []
    role_mention_re = re.compile(r"<@&(\d+)>")
    for entry in entries:
        parts = entry.split()
        if not parts: continue
        emoji = parts[0]
        m = role_mention_re.search(entry)
        if not m:
            await inter.response.send_message(f"Missing role mention in `{entry}`.", ephemeral=True); return
        role_id = int(m.group(1))
        role = inter.guild.get_role(role_id)
        if not role:
            await inter.response.send_message(f"Role not found in `{entry}`.", ephemeral=True); return
        parsed.append((emoji, role_id, role.name))
    if not parsed:
        await inter.response.send_message("No valid emoji/role pairs found.", ephemeral=True); return
    desc_lines = [f"{em} — <@&{rid}> ({rname})" for em, rid, rname in parsed]
    embed = discord.Embed(title=title, description="\n".join(desc_lines), color=discord.Color.blue())
    try:
        msg = await ch.send(embed=embed)
    except Exception as e:
        await inter.response.send_message(f"Couldn't post panel: {e}", ephemeral=True); return
    await db_execute("INSERT OR REPLACE INTO rr_panels (message_id,guild_id,channel_id,title) VALUES (?,?,?,?)",
                     msg.id, inter.guild_id, ch.id, title)
    for em, rid, _ in parsed:
        await db_execute("INSERT OR REPLACE INTO rr_map (panel_message_id,emoji,role_id) VALUES (?,?,?)",
                         msg.id, em, rid)
    for em, _, _ in parsed:
        try:
            await msg.add_reaction(em)
            await asyncio.sleep(0.2)
        except Exception:
            pass
    await inter.response.send_message(f"Reaction-roles panel posted in {ch.mention}.", ephemeral=True)

# -------------- POWERSHELL SLASH (guarded) --------------
@bot.tree.command(name="ps", description="Admin: run a PowerShell command on the host")
@app_commands.describe(command="Command to execute")
async def ps_run(inter: discord.Interaction, command: str):
    if not await _guild_gate(inter): return
    if not ALLOW_POWERSHELL:
        await inter.response.send_message("PowerShell execution is disabled. Set `ALLOW_POWERSHELL=1`.", ephemeral=True); return
    if not _admin_check(inter.user):
        await inter.response.send_message("Admins only.", ephemeral=True); return
    exe = shutil.which("pwsh") or shutil.which("powershell") or shutil.which("powershell.exe") or shutil.which("pwsh.exe")
    if not exe:
        await inter.response.send_message("No PowerShell executable found on host.", ephemeral=True); return
    await inter.response.defer(ephemeral=True)
    try:
        proc = await asyncio.create_subprocess_exec(
            exe, "-NoProfile", "-NonInteractive", "-Command", command,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=20)
        except asyncio.TimeoutError:
            proc.kill()
            await inter.followup.send("⏱️ Timed out after 20s.", ephemeral=True); return
        rc = proc.returncode
        out = (stdout or b"").decode("utf-8", errors="replace")
        err = (stderr or b"").decode("utf-8", errors="replace")
        blob = f"$ {command}\n\n[exit {rc}]\n\nSTDOUT:\n{out}\n\nSTDERR:\n{err}"
        if len(blob) <= 1900:
            await inter.followup.send(f"```text\n{blob}\n```", ephemeral=True)
        else:
            fp = io.BytesIO(blob.encode("utf-8"))
            fp.name = "ps_output.txt"
            await inter.followup.send(content="Output attached (truncated in chat).", file=discord.File(fp, filename="ps_output.txt"), ephemeral=True)
    except Exception as e:
        await inter.followup.send(f":warning: {e}", ephemeral=True)

# -------------- SHUTDOWN / MAIN --------------
async def graceful_shutdown(_sig=None):
    try: await meta_set("offline_since", str(now_ts()))
    finally:
        try:
            await bot.close()
        except Exception:
            pass

@atexit.register
def _persist_offline_since_on_exit():
    try:
        import sqlite3, time
        conn = sqlite3.connect(DB_PATH)
        conn.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)")
        conn.execute(
            "INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("offline_since", str(int(time.time())))
        )
        conn.commit(); conn.close()
    except Exception:
        pass

async def main():
    loop = asyncio.get_running_loop()
    for s in (getattr(signal, "SIGINT", None), getattr(signal, "SIGTERM", None)):
        if s:
            try: loop.add_signal_handler(s, lambda sig=s: asyncio.create_task(graceful_shutdown(sig)))
            except NotImplementedError: pass
    try:
        await bot.start(TOKEN)
    except KeyboardInterrupt:
        await graceful_shutdown()

if __name__ == "__main__":
    asyncio.run(main())

