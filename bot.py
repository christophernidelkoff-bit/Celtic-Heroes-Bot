# ============================================
# SECTION 1 / 4 — Core, Env, DB, Helpers, Seed
# ============================================
# Includes:
# - Imports, intents, logging, ENV
# - Global config & constants
# - Time, formatting & natural sort helpers
# - Category normalization, emojis, default colors
# - Bot + dynamic prefix
# - Preflight *sync* migrations (safe on cold start)
# - Async init (complete schema: legacy + Market + Lixing)
# - Guild defaults & meta helpers
# - Authorization gate: bot *disabled* if "@blunderbusstin" not in the guild
# - Seed data (fixed DL-180: 88m spawn / 3m window)
# - Panel/emoji mapping utilities shared by other sections
# - Subscription channel lookups (panels + ping)
# - Announce channel resolution & permission checks
# - Window label logic: minutes, "closed", "-Nada" with grace

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

ALLOW_POWERSHELL = os.getenv("ALLOW_POWERSHELL", "0") in {"1","true","True","yes","YES"}
DB_PATH = os.getenv("DB_PATH", "bosses.db")
DEFAULT_PREFIX = "!"
CHECK_INTERVAL_SECONDS = 15
DEFAULT_UPTIME_MINUTES = 60
NADA_GRACE_SECONDS = 1800  # after window closes, flip to -Nada only after grace
MARKET_DIGEST_HOURS = 6
LIXING_DIGEST_HOURS = 6

# authorization: bot only operates if a member named "@blunderbusstin" is present in the guild
REQUIRED_USERNAME = os.getenv("REQUIRED_USERNAME", "blunderbusstin")  # do not show in help

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ch-bossbot")

intents = discord.Intents.default()
intents.message_content = True
intents.reactions = True
intents.guilds = True
intents.members = True

# runtime tick markers
_last_timer_tick_ts: int = 0
_prev_timer_tick_ts: int = 0

# seed version (bump when changing SEED_DATA)
SEED_VERSION = "v2025-09-12-subping-window-ps-final-market-lixing-fix"

# Guilds allowed after presence check (populated on_ready & on_guild_join)
_allowed_guild_ids: Set[int] = set()

# -------------------- TIME HELPERS --------------------
def now_ts() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp())

def ts_to_utc(ts: int) -> str:
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return "—"

def ts_to_hm_utc(ts: int) -> str:
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%H:%M UTC")
    except Exception:
        return "—"

_nat_re = re.compile(r'(\d+|\D+)')
def natural_key(s: str) -> List[Any]:
    s = (s or "").strip().lower()
    return [int(p) if p.isdigit() else p for p in _nat_re.findall(s)]

def fmt_delta_for_list(delta_s: int) -> str:
    # Shows -Nada only after grace; otherwise negative minutes as "-Xm"
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

# -------------------- CATEGORIES / COLORS / EMOJIS --------------------
CATEGORY_ORDER = ["Warden","Meteoric","Frozen","DL","EDL","Midraids","Rings","EG","Default"]

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
        "Warden":"🛡️","Meteoric":"☄️","Frozen":"🧊","DL":"🐉","EDL":"🐲",
        "Midraids":"⚔️","Rings":"💍","EG":"🔱","Default":"📜",
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

# -------------------- PREFIX / BOT --------------------
async def get_guild_prefix(_bot, message: discord.Message):
    if not message or not message.guild: return DEFAULT_PREFIX
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            c = await db.execute(
                "SELECT COALESCE(prefix, ?) FROM guild_config WHERE guild_id=?",
                (DEFAULT_PREFIX, message.guild.id)
            )
            r = await c.fetchone()
            if r and r[0]: return r[0]
    except Exception:
        pass
    return DEFAULT_PREFIX

bot = commands.Bot(command_prefix=get_guild_prefix, intents=intents, help_command=None)

# commands that we won’t treat as “quick reset” tokens
RESERVED_TRIGGERS = {
    "help","boss","timers","setprefix","seed_import",
    "setsubchannel","setsubpingchannel","showsubscriptions","setuptime",
    "setheartbeatchannel","setannounce","seteta","status","health",
    "setcatcolor","intervals","setmarketchannel","setlixingchannel",
}

# -------------------- PRE-FLIGHT MIGRATIONS (sync) --------------------
def preflight_migrate_sync():
    import sqlite3
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Legacy tables
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
        sort_key TEXT DEFAULT ''
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS guild_config (
        guild_id INTEGER PRIMARY KEY,
        default_channel INTEGER DEFAULT NULL,
        prefix TEXT DEFAULT NULL
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)""")

    # Add columns if missing
    def col_exists(table, col):
        cur.execute(f"PRAGMA table_info({table})")
        return any(row[1] == col for row in cur.fetchall())

    if not col_exists("bosses","window_minutes"):
        cur.execute("ALTER TABLE bosses ADD COLUMN window_minutes INTEGER DEFAULT 0")
    if not col_exists("guild_config","sub_channel_id"):
        cur.execute("ALTER TABLE guild_config ADD COLUMN sub_channel_id INTEGER DEFAULT NULL")
    if not col_exists("guild_config","sub_message_id"):
        cur.execute("ALTER TABLE guild_config ADD COLUMN sub_message_id INTEGER DEFAULT NULL")
    if not col_exists("guild_config","uptime_minutes"):
        cur.execute("ALTER TABLE guild_config ADD COLUMN uptime_minutes INTEGER DEFAULT NULL")
    if not col_exists("guild_config","heartbeat_channel_id"):
        cur.execute("ALTER TABLE guild_config ADD COLUMN heartbeat_channel_id INTEGER DEFAULT NULL")
    if not col_exists("guild_config","show_eta"):
        cur.execute("ALTER TABLE guild_config ADD COLUMN show_eta INTEGER DEFAULT 0")
    if not col_exists("guild_config","sub_ping_channel_id"):
        cur.execute("ALTER TABLE guild_config ADD COLUMN sub_ping_channel_id INTEGER DEFAULT NULL")
    if not col_exists("guild_config","market_channel_id"):
        cur.execute("ALTER TABLE guild_config ADD COLUMN market_channel_id INTEGER DEFAULT NULL")
    if not col_exists("guild_config","lixing_channel_id"):
        cur.execute("ALTER TABLE guild_config ADD COLUMN lixing_channel_id INTEGER DEFAULT NULL")

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

    # New: Market tables
    cur.execute("""CREATE TABLE IF NOT EXISTS market_listings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        guild_id INTEGER NOT NULL,
        owner_user_id INTEGER NOT NULL,
        item_name TEXT NOT NULL,
        quantity INTEGER NOT NULL,
        price_text TEXT DEFAULT '',
        trades_ok INTEGER DEFAULT 0,
        offers_open INTEGER DEFAULT 1,
        notes TEXT DEFAULT '',
        status TEXT DEFAULT 'active',        -- active|sold|closed
        created_ts INTEGER DEFAULT 0,
        expires_ts INTEGER DEFAULT NULL,
        channel_id INTEGER DEFAULT NULL,
        message_id INTEGER DEFAULT NULL
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS market_offers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        guild_id INTEGER NOT NULL,
        listing_id INTEGER NOT NULL,
        offer_user_id INTEGER NOT NULL,
        offer_text TEXT NOT NULL,
        status TEXT DEFAULT 'open',          -- open|withdrawn|ignored|accepted
        created_ts INTEGER DEFAULT 0
    )""")

    # New: Lixing tables
    cur.execute("""CREATE TABLE IF NOT EXISTS lixing_posts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        guild_id INTEGER NOT NULL,
        owner_user_id INTEGER NOT NULL,
        character_name TEXT NOT NULL,
        char_class TEXT NOT NULL,
        level INTEGER NOT NULL,
        desired_lixes TEXT DEFAULT 'N/A',
        notes TEXT DEFAULT '',
        status TEXT DEFAULT 'active',        -- active|closed
        created_ts INTEGER DEFAULT 0,
        expires_ts INTEGER DEFAULT NULL,
        channel_id INTEGER DEFAULT NULL,
        message_id INTEGER DEFAULT NULL
    )""")

    conn.commit(); conn.close()

preflight_migrate_sync()

# -------------------- ASYNC DB INIT --------------------
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
            market_channel_id INTEGER DEFAULT NULL,
            lixing_channel_id INTEGER DEFAULT NULL
        )""")
        await db.execute("""CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS category_colors (
            guild_id INTEGER NOT NULL, category TEXT NOT NULL, color_hex TEXT NOT NULL,
            PRIMARY KEY (guild_id, category)
        )""")
        await db.execute("""CREATE TABLE IF NOT EXISTS subscription_emojis (
            guild_id INTEGER NOT NULL, boss_id INTEGER NOT NULL, emoji TEXT NOT NULL,
            PRIMARY KEY (guild_id, boss_id)
        )""")
        await db.execute("""CREATE TABLE IF NOT EXISTS subscription_members (
            guild_id INTEGER NOT NULL, boss_id INTEGER NOT NULL, user_id INTEGER NOT NULL,
            PRIMARY KEY (guild_id, boss_id, user_id)
        )""")
        await db.execute("""CREATE TABLE IF NOT EXISTS boss_aliases (
            guild_id INTEGER NOT NULL, boss_id INTEGER NOT NULL, alias TEXT NOT NULL,
            UNIQUE (guild_id, alias)
        )""")
        await db.execute("""CREATE TABLE IF NOT EXISTS category_channels (
            guild_id INTEGER NOT NULL, category TEXT NOT NULL, channel_id INTEGER NOT NULL,
            PRIMARY KEY (guild_id, category)
        )""")
        await db.execute("""CREATE TABLE IF NOT EXISTS user_timer_prefs (
            guild_id INTEGER NOT NULL, user_id INTEGER NOT NULL, categories TEXT NOT NULL,
            PRIMARY KEY (guild_id, user_id)
        )""")
        await db.execute("""CREATE TABLE IF NOT EXISTS subscription_panels (
            guild_id INTEGER NOT NULL, category TEXT NOT NULL, message_id INTEGER NOT NULL,
            channel_id INTEGER DEFAULT NULL,
            PRIMARY KEY (guild_id, category)
        )""")
        await db.execute("""CREATE TABLE IF NOT EXISTS rr_panels (
            message_id INTEGER PRIMARY KEY, guild_id INTEGER NOT NULL, channel_id INTEGER NOT NULL, title TEXT DEFAULT ''
        )""")
        await db.execute("""CREATE TABLE IF NOT EXISTS rr_map (
            panel_message_id INTEGER NOT NULL, emoji TEXT NOT NULL, role_id INTEGER NOT NULL,
            PRIMARY KEY (panel_message_id, emoji)
        )""")
        await db.execute("""CREATE TABLE IF NOT EXISTS blacklist (
            guild_id INTEGER NOT NULL, user_id INTEGER NOT NULL,
            PRIMARY KEY (guild_id, user_id)
        )""")
        # Market / Lixing
        await db.execute("""CREATE TABLE IF NOT EXISTS market_listings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id INTEGER NOT NULL,
            owner_user_id INTEGER NOT NULL,
            item_name TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            price_text TEXT DEFAULT '',
            trades_ok INTEGER DEFAULT 0,
            offers_open INTEGER DEFAULT 1,
            notes TEXT DEFAULT '',
            status TEXT DEFAULT 'active',
            created_ts INTEGER DEFAULT 0,
            expires_ts INTEGER DEFAULT NULL,
            channel_id INTEGER DEFAULT NULL,
            message_id INTEGER DEFAULT NULL
        )""")
        await db.execute("""CREATE TABLE IF NOT EXISTS market_offers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id INTEGER NOT NULL,
            listing_id INTEGER NOT NULL,
            offer_user_id INTEGER NOT NULL,
            offer_text TEXT NOT NULL,
            status TEXT DEFAULT 'open',
            created_ts INTEGER DEFAULT 0
        )""")
        await db.execute("""CREATE TABLE IF NOT EXISTS lixing_posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id INTEGER NOT NULL,
            owner_user_id INTEGER NOT NULL,
            character_name TEXT NOT NULL,
            char_class TEXT NOT NULL,
            level INTEGER NOT NULL,
            desired_lixes TEXT DEFAULT 'N/A',
            notes TEXT DEFAULT '',
            status TEXT DEFAULT 'active',
            created_ts INTEGER DEFAULT 0,
            expires_ts INTEGER DEFAULT NULL,
            channel_id INTEGER DEFAULT NULL,
            message_id INTEGER DEFAULT NULL
        )""")
        await db.commit()

async def upsert_guild_defaults(guild_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO guild_config (guild_id,prefix,uptime_minutes,show_eta) VALUES (?,?,?,?) "
            "ON CONFLICT(guild_id) DO NOTHING",
            (guild_id, DEFAULT_PREFIX, DEFAULT_UPTIME_MINUTES, 0)
        )
        await db.commit()

async def meta_set(key: str, value: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO meta(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value)
        )
        await db.commit()

async def meta_get(key: str) -> Optional[str]:
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT value FROM meta WHERE key=?", (key,))
        r = await c.fetchone()
        return r[0] if r else None

# -------------------- AUTHORIZATION GATE --------------------
async def guild_is_authorized(guild: discord.Guild) -> bool:
    """Enable bot features only if REQUIRED_USERNAME is in the guild (not a bot)."""
    if not guild or not REQUIRED_USERNAME:
        return True
    if guild.id in _allowed_guild_ids:
        return True
    # try cached first
    for m in guild.members:
        if (m and not m.bot and m.name.lower() == REQUIRED_USERNAME.lower()) or \
           (m and not m.bot and m.display_name.lower() == REQUIRED_USERNAME.lower()):
            _allowed_guild_ids.add(guild.id)
            return True
    # fetch just in case (requires intents.members)
    try:
        async for m in guild.fetch_members(limit=None):
            if (not m.bot) and (m.name.lower() == REQUIRED_USERNAME.lower() or m.display_name.lower() == REQUIRED_USERNAME.lower()):
                _allowed_guild_ids.add(guild.id)
                return True
    except Exception:
        pass
    return False

# -------------------- PERMISSIONS CHECKS --------------------
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

# -------------------- COLORS --------------------
async def get_category_color(guild_id: int, category: str) -> int:
    category = norm_cat(category)
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT color_hex FROM category_colors WHERE guild_id=? AND category=?", (guild_id, category))
        r = await c.fetchone()
    if r and r[0]:
        try: return int(r[0].lstrip("#"), 16)
        except Exception: pass
    return DEFAULT_COLORS.get(category, DEFAULT_COLORS["Default"])

# -------------------- ANNOUNCE CHANNEL RESOLUTION --------------------
async def resolve_announce_channel(guild_id: int, explicit_channel_id: Optional[int], category: Optional[str] = None) -> Optional[discord.TextChannel]:
    guild = bot.get_guild(guild_id)
    if not guild: return None
    if not await guild_is_authorized(guild):
        return None
    if explicit_channel_id:
        ch = guild.get_channel(explicit_channel_id)
        if can_send(ch): return ch
    if category:
        async with aiosqlite.connect(DB_PATH) as db:
            c = await db.execute("SELECT channel_id FROM category_channels WHERE guild_id=? AND category=?", (guild_id, norm_cat(category)))
            r = await c.fetchone()
        if r and r[0]:
            ch = guild.get_channel(r[0])
            if can_send(ch): return ch
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT default_channel FROM guild_config WHERE guild_id=?", (guild_id,))
        r = await c.fetchone()
    if r and r[0]:
        ch = guild.get_channel(r[0])
        if can_send(ch): return ch
    for ch in getattr(guild, "text_channels", []):
        if can_send(ch): return ch
    return None

async def resolve_heartbeat_channel(guild_id: int) -> Optional[discord.TextChannel]:
    guild = bot.get_guild(guild_id)
    if not guild or not await guild_is_authorized(guild): return None
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT heartbeat_channel_id, default_channel FROM guild_config WHERE guild_id=?", (guild_id,))
        r = await c.fetchone()
    hb_id, def_id = (r[0], r[1]) if r else (None, None)
    for cid in [hb_id, def_id]:
        if cid:
            ch = guild.get_channel(cid)
            if can_send(ch): return ch
    for ch in getattr(guild, "text_channels", []):
        if can_send(ch): return ch
    return None

# -------------------- SUBSCRIPTION PANELS / CHANNEL IDS --------------------
async def get_subchannel_id(guild_id: int) -> Optional[int]:
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT sub_channel_id FROM guild_config WHERE guild_id=?", (guild_id,))
        r = await c.fetchone()
        return r[0] if r else None

async def get_subping_channel_id(guild_id: int) -> Optional[int]:
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT sub_ping_channel_id FROM guild_config WHERE guild_id=?", (guild_id,))
        r = await c.fetchone()
        return r[0] if r else None

async def get_all_panel_records(guild_id: int) -> Dict[str, Tuple[int, Optional[int]]]:
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT category, message_id, channel_id FROM subscription_panels WHERE guild_id=?", (guild_id,))
        return {norm_cat(row[0]): (int(row[1]), (int(row[2]) if row[2] is not None else None)) for row in await c.fetchall()}

async def set_panel_record(guild_id: int, category: str, message_id: int, channel_id: Optional[int]):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO subscription_panels (guild_id,category,message_id,channel_id) VALUES (?,?,?,?) "
            "ON CONFLICT(guild_id,category) DO UPDATE SET message_id=excluded.message_id, channel_id=excluded.channel_id",
            (guild_id, norm_cat(category), int(message_id), (int(channel_id) if channel_id else None))
        )
        await db.commit()

async def clear_all_panel_records(guild_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM subscription_panels WHERE guild_id=?", (guild_id,))
        await db.commit()

# -------------------- EMOJI MAPPING (stable across edits) --------------------
async def ensure_emoji_mapping(guild_id: int, bosses: List[tuple]):
    """Assign unique emojis and keep them stable; reassign only when collisions occur."""
    palette = EMOJI_PALETTE + EXTRA_EMOJIS
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT boss_id, emoji FROM subscription_emojis WHERE guild_id=?", (guild_id,))
        rows = await c.fetchall()
        boss_to_emoji: Dict[int, str] = {int(b): str(e) for b, e in rows}
        emoji_to_bosses: Dict[str, List[int]] = {}
        for b, e in boss_to_emoji.items():
            emoji_to_bosses.setdefault(e, []).append(b)

        used_emojis = set()
        needs_reassign: List[int] = []
        for e, blist in emoji_to_bosses.items():
            if not blist: continue
            used_emojis.add(e)
            if len(blist) > 1:
                # keep the first, reassign the rest
                for b in sorted(blist)[1:]:
                    needs_reassign.append(b)

        available = [e for e in palette if e not in used_emojis]

        # Reassign collisions
        for boss_id in needs_reassign:
            if not available: break
            new_e = available.pop(0)
            await db.execute("UPDATE subscription_emojis SET emoji=? WHERE guild_id=? AND boss_id=?", (new_e, guild_id, boss_id))
            boss_to_emoji[boss_id] = new_e
            used_emojis.add(new_e)

        # Assign to bosses without an emoji
        have_ids = set(boss_to_emoji.keys())
        for boss_id, _name in bosses:
            if boss_id in have_ids: continue
            if not available:
                available = [e for e in palette if e not in used_emojis]
                if not available: break
            e = available.pop(0)
            await db.execute(
                "INSERT OR REPLACE INTO subscription_emojis (guild_id,boss_id,emoji) VALUES (?,?,?)",
                (guild_id, boss_id, e)
            )
            boss_to_emoji[boss_id] = e
            used_emojis.add(e)

        await db.commit()

# -------------------- WINDOW LABEL --------------------
def window_label(now: int, next_ts: int, window_m: int) -> str:
    """Return: '<Xm> (pending)' | '<Xm left> (open)' | 'closed' | '-Nada'"""
    delta = next_ts - now
    if delta >= 0:
        return f"{window_m}m (pending)"
    open_secs = -delta
    if open_secs <= window_m * 60:
        left_m = max(0, (window_m * 60 - open_secs) // 60)
        return f"{left_m}m left (open)"
    # past window, before -Nada grace:
    after_close = open_secs - window_m * 60
    if after_close <= NADA_GRACE_SECONDS:
        return "closed"
    return "-Nada"

# -------------------- SUBSCRIPTION PINGS --------------------
async def send_subscription_ping(guild_id: int, boss_id: int, phase: str, boss_name: str, when_left: Optional[int] = None):
    """Phase: 'pre' or 'window'. Posts in sub_ping_channel if set; else in sub panels channel; else skip."""
    # Authorization must be satisfied
    guild = bot.get_guild(guild_id)
    if not guild or not await guild_is_authorized(guild):
        return

    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT sub_ping_channel_id, sub_channel_id FROM guild_config WHERE guild_id=?", (guild_id,))
        r = await c.fetchone()
        sub_ping_id = (r[0] if r else None) or (r[1] if r else None)
        c = await db.execute("SELECT user_id FROM subscription_members WHERE guild_id=? AND boss_id=?", (guild_id, boss_id))
        subs = [row[0] for row in await c.fetchall()]
    if not sub_ping_id or not subs:
        return

    ch = guild.get_channel(sub_ping_id)
    if not can_send(ch):
        return
    mentions = " ".join(f"<@{uid}>" for uid in subs)
    if phase == "pre":
        left = max(0, when_left or 0)
        txt = f"⏳ {mentions} — **{boss_name}** Spawn Time: `{fmt_delta_for_list(left)}` (almost up)."
    else:
        txt = f"🕑 {mentions} — **{boss_name}** Spawn Window has opened!"
    try:
        await ch.send(txt)
    except Exception as e:
        log.warning(f"Sub ping failed: {e}")

# -------------------- SEED DATA --------------------
SEED_DATA = [
    # METEORIC
    ("Meteoric","Doomclaw",7,5,[]),
    ("Meteoric","Bonehad",15,5,[]),
    ("Meteoric","Rockbelly",15,5,[]),
    ("Meteoric","Redbane",20,5,[]),
    ("Meteoric","Coppinger",20,5,["copp"]),
    ("Meteoric","Goretusk",20,5,[]),
    ("Meteoric","Falgren",45,5,[]),
    # FROZEN
    ("Frozen","Redbane",20,5,[]),
    ("Frozen","Eye",28,3,[]),
    ("Frozen","Swampie",33,3,["swampy"]),
    ("Frozen","Woody",38,3,[]),
    ("Frozen","Chained",43,3,["chain"]),
    ("Frozen","Grom",48,3,[]),
    ("Frozen","Pyrus",58,3,["py"]),
    # DL (180 fixed to 88/3)
    ("DL","155",63,3,[]),
    ("DL","160",68,3,[]),
    ("DL","165",73,3,[]),
    ("DL","170",78,3,[]),
    ("DL","180",88,3,["snorri"]),  # <-- fixed per requirement
    # EDL
    ("EDL","185",72,3,[]),
    ("EDL","190",81,3,[]),
    ("EDL","195",89,4,[]),
    ("EDL","200",108,5,[]),
    ("EDL","205",117,4,[]),
    ("EDL","210",125,5,[]),
    ("EDL","215",134,5,["unox"]),
    # MIDRAIDS
    ("Midraids","Aggorath",1200,960,["aggy"]),
    ("Midraids","Mordris",1200,960,["mord","mordy"]),
    ("Midraids","Necromancer",1320,960,["necro"]),
    ("Midraids","Hrungnir",1320,960,["hrung","muk"]),
    # RINGS
    ("Rings","North Ring",215,50,["northring"]),
    ("Rings","Center Ring",215,50,["centre","centering"]),
    ("Rings","South Ring",215,50,["southring"]),
    ("Rings","East Ring",215,50,["eastring"]),
    # EG
    ("EG","Draig Liathphur",240,840,["draig","dragon","riverdragon"]),
    ("EG","Sciathan Leathair",240,300,["sciathan","bat","northbat"]),
    ("EG","Thymea Banebark",240,840,["thymea","tree","ancienttree"]),
    ("EG","Proteus",1080,15,["prot","base","prime"]),
    ("EG","Gelebron",1920,1680,["gele"]),
    ("EG","Dhiothu",2040,1680,["dino","dhio","d2"]),
    ("EG","Bloodthorn",2040,1680,["bt"]),
    ("EG","Crom’s Manikin",5760,1440,["manikin","crom","croms"]),
]

async def ensure_seed_for_guild(guild: discord.Guild):
    if not await guild_is_authorized(guild):
        log.warning(f"Guild {guild.id}: authorization failed (missing @{REQUIRED_USERNAME}). Seed skipped.")
        return
    key = f"seed:{SEED_VERSION}:g{guild.id}"
    already = await meta_get(key)
    if already == "done":
        return
    inserted = 0
    alias_ct = 0
    async with aiosqlite.connect(DB_PATH) as db:
        for cat, name, spawn_m, window_m, aliases in SEED_DATA:
            catn = norm_cat(cat)
            c = await db.execute("SELECT id FROM bosses WHERE guild_id=? AND name=? AND category=?", (guild.id, name, catn))
            r = await c.fetchone()
            if r:
                bid = r[0]
            else:
                next_spawn = now_ts() - 3601  # -Nada default
                await db.execute(
                    "INSERT INTO bosses (guild_id,channel_id,name,spawn_minutes,window_minutes,next_spawn_ts,pre_announce_min,created_by,category,sort_key) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (guild.id, None, name, int(spawn_m), int(window_m), next_spawn, 10, guild.owner_id if guild.owner_id else 0, catn, "")
                )
                inserted += 1
                c = await db.execute("SELECT id FROM bosses WHERE guild_id=? AND name=? AND category=?", (guild.id, name, catn))
                bid = (await c.fetchone())[0]
            seen = set()
            for al in aliases:
                al_l = str(al).strip().lower()
                if not al_l or al_l in seen: continue
                seen.add(al_l)
                try:
                    await db.execute("INSERT INTO boss_aliases (guild_id,boss_id,alias) VALUES (?,?,?)",
                                     (guild.id, bid, al_l))
                    alias_ct += 1
                except Exception:
                    pass
        await db.commit()
    await meta_set(key, "done")
    if inserted or alias_ct:
        log.info(f"Seeded {inserted} bosses, {alias_ct} aliases for guild {guild.id}")

# -------------------- HELP CONTENT (no auth configs displayed) --------------------
HELP_SECTIONS = {
    "basics": [
        "**Prefix**: `!`  • Change: `!setprefix <new>`",
        "Quick reset: `!<BossOrAlias>`  → sets next spawn for that boss (requires permissions).",
        "Timers view: `!timers`  • Intervals: `!intervals`",
    ],
    "boss ops": [
        "`!boss add \"Name\" <spawn_m> <window_m> [#chan] [pre_m] [category]`",
        "`!boss killed <name>` • `!boss increase <name> <m>` • `!boss reduce <name> <m>`",
        "`!boss nada <name>` • `!boss nadaall` • `!boss delete <name>`",
        "`!boss edit <name> <spawn_minutes|window_minutes|pre_announce_min|name|category|sort_key> <value>`",
        "`!boss setcategory <name> \"<Category>\"` • `!boss setsort <name> <sort_key>`",
        "`!boss alias add|remove|aliases \"Name\" \"Alias\"`",
    ],
    "announce & subs": [
        "`!setannounce category \"<Category>\" #chan` • `!setannounce categoryclear \"<Category>\"`",
        "`!setsubchannel #chan` (panels) • `!setsubpingchannel #chan` (subscriber pings)",
        "`!showsubscriptions` (refreshes panels without breaking emojis/reactions)",
        "`/timers` (per-user category toggles, remembered)",
    ],
    "display & status": [
        "`!setcatcolor <Category> <#hex>` • `!seteta on|off`",
        "`!setuptime <minutes>` • `!setheartbeatchannel #chan`",
        "`!status` • `!health`",
    ],
    "reaction roles": [
        "`/roles_panel` — create a reaction-roles message.",
    ],
    "market": [
        "`/market_add` • `/market_list [query]` • `/market_remove <id>`",
        "`!setmarketchannel #chan` (6h digest destination)",
        "Each listing has **Make Offer** button → modal → public offer + owner DM.",
    ],
    "lixing": [
        "`/lixing_add` • `/lixing_list` • `/lixing_remove <id>`",
        "`!setlixingchannel #chan` (6h digest destination)",
    ],
    "admin": [
        "`!blacklist add @user` • `!blacklist remove @user` • `!blacklist show`",
    ],
}

# -------------------- OFFLINE PERSISTENCE FOR '-Nada' SAFEGUARD --------------------
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
# ============================================
# SECTION 2 / 4 — Commands, Panels, Timers
# ============================================
# Depends on Section 1 symbols: bot, aiosqlite, discord, app_commands, commands,
# DB_PATH, CATEGORY_ORDER, norm_cat, category_emoji, DEFAULT_COLORS,
# natural_key, fmt_delta_for_list, window_label, get_category_color,
# get_subchannel_id, get_subping_channel_id, get_all_panel_records,
# set_panel_record, clear_all_panel_records, ensure_emoji_mapping,
# resolve_announce_channel, resolve_heartbeat_channel,
# now_ts, ts_to_utc, ts_to_hm_utc, human_ago, HELP_SECTIONS,
# RESERVED_TRIGGERS, can_send, can_react, NADA_GRACE_SECONDS,
# guild_is_authorized, upsert_guild_defaults, meta_get, meta_set,
# EMOJI_PALETTE, EXTRA_EMOJIS, category_emoji, DEFAULT_PREFIX

# -------------------- Utility: Guild auth gate (commands) --------------------
def guild_auth_check():
    async def predicate(ctx: commands.Context) -> bool:
        if not ctx.guild:
            return True
        if await guild_is_authorized(ctx.guild):
            return True
        try:
            await ctx.send(":no_entry: Bot is disabled in this server (authorization check failed).")
        except Exception:
            pass
        return False
    return commands.check(predicate)

# -------------------- BLACKLIST --------------------
async def is_blacklisted(guild_id: int, user_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT 1 FROM blacklist WHERE guild_id=? AND user_id=?", (guild_id, user_id))
        return (await c.fetchone()) is not None

def blacklist_check():
    async def predicate(ctx: commands.Context) -> bool:
        if not ctx.guild:
            return True
        if await is_blacklisted(ctx.guild.id, ctx.author.id):
            try:
                await ctx.send(":no_entry: You are blacklisted from using this bot.")
            except Exception:
                pass
            return False
        return True
    return commands.check(predicate)

@bot.group(name="blacklist", invoke_without_command=True)
@guild_auth_check()
@commands.has_permissions(manage_guild=True)
async def blacklist_group(ctx):
    await ctx.send("Use `!blacklist add @user` / `!blacklist remove @user` / `!blacklist show`")

@blacklist_group.command(name="add")
@guild_auth_check()
@commands.has_permissions(manage_guild=True)
async def blacklist_add(ctx, user: discord.Member):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR IGNORE INTO blacklist (guild_id,user_id) VALUES (?,?)", (ctx.guild.id, user.id))
        await db.commit()
    await ctx.send(f":no_entry: **{user.display_name}** is now blacklisted.")

@blacklist_group.command(name="remove")
@guild_auth_check()
@commands.has_permissions(manage_guild=True)
async def blacklist_remove(ctx, user: discord.Member):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM blacklist WHERE guild_id=? AND user_id=?", (ctx.guild.id, user.id))
        await db.commit()
    await ctx.send(f":white_check_mark: **{user.display_name}** removed from blacklist.")

@blacklist_group.command(name="show")
@guild_auth_check()
@commands.has_permissions(manage_guild=True)
async def blacklist_show(ctx):
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT user_id FROM blacklist WHERE guild_id=?", (ctx.guild.id,))
        rows = await c.fetchall()
    if not rows:
        return await ctx.send("No users blacklisted.")
    mentions = " ".join(f"<@{r[0]}>" for r in rows)
    await ctx.send(f"Blacklisted: {mentions}")

bot.add_check(blacklist_check())

# -------------------- USER PREFS (for /timers) --------------------
async def get_user_shown_categories(guild_id: int, user_id: int) -> List[str]:
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT categories FROM user_timer_prefs WHERE guild_id=? AND user_id=?", (guild_id, user_id))
        r = await c.fetchone()
    if not r or not r[0]:
        return []
    raw = [norm_cat(x.strip()) for x in r[0].split(",") if x.strip()]
    return [c for c in CATEGORY_ORDER if c in raw]

async def set_user_shown_categories(guild_id: int, user_id: int, cats: List[str]):
    cleaned = [norm_cat(c) for c in cats if c]
    ordered = [c for c in CATEGORY_ORDER if c in cleaned]
    joined = ",".join(ordered)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO user_timer_prefs (guild_id,user_id,categories) VALUES (?,?,?) "
            "ON CONFLICT(guild_id,user_id) DO UPDATE SET categories=excluded.categories",
            (guild_id, user_id, joined)
        )
        await db.commit()

# -------------------- PERMISSION CHECKS --------------------
async def has_trusted(member: discord.Member, guild_id: int, boss_id: Optional[int] = None) -> bool:
    if member.guild_permissions.administrator:
        return True
    async with aiosqlite.connect(DB_PATH) as db:
        if boss_id:
            c = await db.execute("SELECT trusted_role_id FROM bosses WHERE id=? AND guild_id=?", (boss_id, guild_id))
            r = await c.fetchone()
            if r and r[0]:
                return any(role.id == r[0] for role in member.roles)
    # Fallback: Manage Messages implies trusted
    return member.guild_permissions.manage_messages

# -------------------- HELP / STATUS / HEALTH --------------------
@bot.command(name="help")
@guild_auth_check()
async def help_cmd(ctx):
    pfx = ctx.prefix if isinstance(ctx.prefix, str) else "!"
    sections = []
    def add(title, lines):
        sections.append(f"**{title}**\n" + "\n".join(lines))
    add("Boss Tracker — Quick Help", [
        f"Prefix: `{pfx}`  • Change: `{pfx}setprefix <new>`",
        "Quick reset: `!<BossOrAlias>`  → sets next spawn for that boss (requires permission).",
    ])
    for title, lines in HELP_SECTIONS.items():
        add(title.title(), lines)
    text = "\n\n".join(sections)
    if len(text) > 1990:
        text = text[:1985] + "…"
    if can_send(ctx.channel):
        await ctx.send(text)

@bot.command(name="status")
@guild_auth_check()
async def status_cmd(ctx):
    gid = ctx.guild.id
    p = ctx.prefix if isinstance(ctx.prefix, str) else "!"
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute(
            "SELECT COALESCE(prefix, ?), default_channel, sub_channel_id, sub_ping_channel_id, "
            "COALESCE(uptime_minutes, ?), heartbeat_channel_id, COALESCE(show_eta,0), market_channel_id, lixing_channel_id "
            "FROM guild_config WHERE guild_id=?",
            (DEFAULT_PREFIX, 60, gid)
        )
        r = await c.fetchone()
        prefix, ann_id, sub_id, sub_ping_id, hb_min, hb_ch, show_eta, market_ch, lixing_ch = \
            (r if r else (DEFAULT_PREFIX, None, None, None, 60, None, 0, None, None))
        c = await db.execute("SELECT COUNT(*) FROM bosses WHERE guild_id=?", (gid,))
        boss_count = (await c.fetchone())[0]
        now_n = now_ts()
        c = await db.execute("SELECT next_spawn_ts FROM bosses WHERE guild_id=?", (gid,))
        times = [int(x[0]) for x in await c.fetchall()]
        due = sum(1 for t in times if t <= now_n)
        nada = sum(1 for t in times if (now_n - t) > NADA_GRACE_SECONDS)
        c = await db.execute("SELECT category,channel_id FROM category_channels WHERE guild_id=?", (gid,))
        cat_map = {row[0]: row[1] for row in await c.fetchall()}
        c = await db.execute("SELECT category FROM category_colors WHERE guild_id=?", (gid,))
        overridden = sorted({norm_cat(row[0]) for row in await c.fetchall()})
    last_start = await meta_get("last_startup_ts")
    hb_label = "off" if int(hb_min) <= 0 else f"every {int(hb_min)}m"
    def ch(idv): return f"<#{idv}>" if idv else "—"
    lines = [
        f"**Status**",
        f"Prefix: `{prefix}` (change: `{p}setprefix <new>`) ",
        f"Announce channel (global): {ch(ann_id)}",
        f"Category overrides: " + (", ".join(f"{k}→{ch(v)}" for k,v in cat_map.items()) if cat_map else "none"),
        f"Subscription panels: {ch(sub_id)}",
        f"Subscription **pings**: {ch(sub_ping_id)}",
        f"Market digest: {ch(market_ch)}  •  Lixing digest: {ch(lixing_ch)}",
        f"Heartbeat: {hb_label} • Channel: {ch(hb_ch)}",
        f"UTC ETA: {'on' if show_eta else 'off'}",
        f"Bosses: {boss_count} • Due now: {due} • -Nada: {nada}",
        f"Color overrides: {', '.join(overridden) if overridden else 'none'}",
        f"Last startup: {ts_to_utc(int(last_start)) if last_start and last_start.isdigit() else '—'}",
    ]
    await ctx.send("\n".join(lines))

@bot.command(name="health")
@guild_auth_check()
@commands.has_permissions(administrator=True)
async def health_cmd(ctx):
    required = {"bosses","guild_config","meta","category_colors","subscription_emojis","subscription_members",
                "boss_aliases","category_channels","user_timer_prefs","subscription_panels","rr_panels",
                "rr_map","blacklist","market_listings","market_offers","lixing_posts"}
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT name FROM sqlite_master WHERE type='table'")
        present = {row[0] for row in await c.fetchall()}
        c = await db.execute("SELECT COUNT(*) FROM guild_config WHERE guild_id=?", (ctx.guild.id,))
        cfg_rows = (await c.fetchone())[0]
    missing = sorted(list(required - present))
    tick_age = now_ts() - (await meta_get("last_tick_ts") and int(await meta_get("last_tick_ts")) or 0)
    lines = [
        "**Health**",
        f"DB: `{DB_PATH}`",
        f"Tables OK: {'yes' if not missing else 'no'}{'' if not missing else ' (missing: ' + ', '.join(missing) + ')'}",
        f"guild_config row present: {'yes' if cfg_rows > 0 else 'no'}",
    ]
    await ctx.send("\n".join(lines))

# -------------------- SETTINGS (prefix/announce/eta/heartbeat/subchannels) --------------------
@bot.command(name="setprefix")
@guild_auth_check()
@commands.has_permissions(manage_guild=True)
async def setprefix_cmd(ctx, new_prefix: str):
    if not new_prefix or len(new_prefix) > 5:
        return await ctx.send("Pick a prefix 1–5 characters.")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO guild_config (guild_id,prefix) VALUES (?,?) "
            "ON CONFLICT(guild_id) DO UPDATE SET prefix=excluded.prefix",
            (ctx.guild.id, new_prefix)
        )
        await db.commit()
    await ctx.send(f":white_check_mark: Prefix set to `{new_prefix}`.")

def _resolve_channel_id_from_arg(ctx, value: Optional[str]) -> Optional[int]:
    if not value: return None
    if isinstance(value, int): return value
    s = str(value)
    if s.startswith("<#") and s.endswith(">"): return int(s[2:-1])
    if s.isdigit(): return int(s)
    found = discord.utils.get(ctx.guild.channels, name=s.strip("#"))
    return found.id if found else None

@bot.command(name="setannounce")
@guild_auth_check()
@commands.has_permissions(manage_guild=True)
async def setannounce_cmd(ctx, *args):
    if not args:
        return await ctx.send("Usage: `!setannounce #chan` or `!setannounce category \"<Category>\" #chan`")
    first = args[0].lower()
    if first.startswith("<#") or first == "global" or (len(args) == 1 and args[0].isdigit()):
        channel_id = _resolve_channel_id_from_arg(ctx, args[-1])
        if not channel_id: return await ctx.send("Mention a channel, e.g., `#raids`.")
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT INTO guild_config (guild_id,default_channel) VALUES (?,?) "
                "ON CONFLICT(guild_id) DO UPDATE SET default_channel=excluded.default_channel",
                (ctx.guild.id, channel_id)
            )
            await db.commit()
        return await ctx.send(f":white_check_mark: Global announce channel set to <#{channel_id}>.")
    if first in {"category", "categoryclear"}:
        if first == "category":
            if len(args) < 3: return await ctx.send('Format: `!setannounce category "<Category>" #chan`')
            joined = " ".join(args[1:])
            if '"' in joined:
                cat = joined.split('"', 1)[1].split('"', 1)[0].strip()
                tail = joined.split('"', 2)[-1].strip().split()
                ch_id = _resolve_channel_id_from_arg(ctx, tail[-1]) if tail else None
            else:
                cat = " ".join(args[1:-1]).strip(); ch_id = _resolve_channel_id_from_arg(ctx, args[-1])
            if not cat or not ch_id: return await ctx.send('Format: `!setannounce category "<Category>" #chan`')
            catn = norm_cat(cat)
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute(
                    "INSERT INTO category_channels (guild_id,category,channel_id) VALUES (?,?,?) "
                    "ON CONFLICT(guild_id,category) DO UPDATE SET channel_id=excluded.channel_id",
                    (ctx.guild.id, catn, ch_id)
                )
                await db.commit()
            return await ctx.send(f":white_check_mark: **{catn}** reminders → <#{ch_id}>.")
        else:
            if len(args) < 2: return await ctx.send('Format: `!setannounce categoryclear "<Category>"`')
            cat = " ".join(args[1:]).strip().strip('"')
            catn = norm_cat(cat)
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute("DELETE FROM category_channels WHERE guild_id=? AND category=?", (ctx.guild.id, catn))
                await db.commit()
            return await ctx.send(f":white_check_mark: Cleared category channel for **{catn}**.")
    return await ctx.send("Usage: `!setannounce #chan` | `!setannounce category \"<Category>\" #chan` | `!setannounce categoryclear \"<Category>\"`")

@bot.command(name="seteta")
@guild_auth_check()
@commands.has_permissions(manage_guild=True)
async def seteta_cmd(ctx, state: str):
    val = state.strip().lower()
    if val not in {"on","off","true","false","1","0","yes","no"}:
        return await ctx.send("Use `!seteta on` or `!seteta off`.")
    on = val in {"on","true","1","yes"}
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO guild_config (guild_id,show_eta) VALUES (?,?) "
            "ON CONFLICT(guild_id) DO UPDATE SET show_eta=excluded.show_eta",
            (ctx.guild.id, 1 if on else 0)
        )
        await db.commit()
    await ctx.send(f":white_check_mark: UTC ETA display {'enabled' if on else 'disabled'}.")

@bot.command(name="setuptime")
@guild_auth_check()
@commands.has_permissions(manage_guild=True)
async def setuptime_cmd(ctx, minutes: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO guild_config (guild_id,uptime_minutes) VALUES (?,?) "
            "ON CONFLICT(guild_id) DO UPDATE SET uptime_minutes=excluded.uptime_minutes",
            (ctx.guild.id, max(-1, int(minutes)))
        )
        await db.commit()
    await ctx.send(":white_check_mark: Uptime heartbeat disabled." if minutes <= 0
                   else f":white_check_mark: Uptime heartbeat set to every {minutes} minutes.")

@bot.command(name="setheartbeatchannel")
@guild_auth_check()
@commands.has_permissions(manage_guild=True)
async def setheartbeatchannel_cmd(ctx, channel: discord.TextChannel):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO guild_config (guild_id,heartbeat_channel_id) VALUES (?,?) "
            "ON CONFLICT(guild_id) DO UPDATE SET heartbeat_channel_id=excluded.heartbeat_channel_id",
            (ctx.guild.id, channel.id)
        )
        await db.commit()
    await ctx.send(f":white_check_mark: Heartbeat channel set to {channel.mention}.")

@bot.command(name="setsubchannel")
@guild_auth_check()
@commands.has_permissions(manage_guild=True)
async def setsubchannel_cmd(ctx, channel: discord.TextChannel):
    # We will *not* delete old panel messages automatically to avoid losing reactions unless they live in another channel.
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO guild_config (guild_id,sub_channel_id) VALUES (?,?) "
            "ON CONFLICT(guild_id) DO UPDATE SET sub_channel_id=excluded.sub_channel_id",
            (ctx.guild.id, channel.id)
        )
        await db.commit()
    await ctx.send(f":white_check_mark: Subscription **panels** channel set to {channel.mention}. Rebuilding panels…")
    await refresh_subscription_messages(ctx.guild)
    await ctx.send(":white_check_mark: Subscription panels are ready (reactions preserved).")

@bot.command(name="setsubpingchannel")
@guild_auth_check()
@commands.has_permissions(manage_guild=True)
async def setsubpingchannel_cmd(ctx, channel: discord.TextChannel):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO guild_config (guild_id,sub_ping_channel_id) VALUES (?,?) "
            "ON CONFLICT(guild_id) DO UPDATE SET sub_ping_channel_id=excluded.sub_ping_channel_id",
            (ctx.guild.id, channel.id)
        )
        await db.commit()
    await ctx.send(f":white_check_mark: Subscription **ping** channel set to {channel.mention}.")

@bot.command(name="showsubscriptions")
@guild_auth_check()
async def showsubscriptions_cmd(ctx):
    await refresh_subscription_messages(ctx.guild)
    await ctx.send(":white_check_mark: Subscription panels refreshed (one per category, stable).")

# -------------------- SUBSCRIPTION PANELS (stable edit-in-place) --------------------
async def build_subscription_embed_for_category(guild_id: int, category: str) -> Tuple[str, Optional[discord.Embed], List[str]]:
    cat = norm_cat(category)
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT id,name,sort_key FROM bosses WHERE guild_id=? AND category=?", (guild_id, cat))
        rows = await c.fetchall()
    if not rows:
        return ("", None, [])
    rows.sort(key=lambda r: (natural_key(r[2] or ""), natural_key(r[1])))
    await ensure_emoji_mapping(guild_id, [(r[0], r[1]) for r in rows])
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT boss_id,emoji FROM subscription_emojis WHERE guild_id=?", (guild_id,))
        emoji_map = {row[0]: row[1] for row in await c.fetchall()}
    em = discord.Embed(
        title=f"{category_emoji(cat)} Subscriptions — {cat}",
        description="React with the emoji to subscribe/unsubscribe to alerts for these bosses.",
        color=await get_category_color(guild_id, cat)
    )
    lines = []
    per_message_emojis = []
    for bid, name, _sk in rows:
        e = emoji_map.get(bid, "⭐")
        if e in per_message_emojis:
            continue
        per_message_emojis.append(e)
        lines.append(f"{e} — **{name}**")
    bucket = ""; fields: List[str] = []
    for line in lines:
        if len(bucket) + len(line) + 1 > 1000:
            fields.append(bucket); bucket = line + "\n"
        else:
            bucket += line + "\n"
    if bucket: fields.append(bucket)
    for i, val in enumerate(fields, 1):
        em.add_field(name=f"{cat} ({i})" if len(fields) > 1 else cat, value=val, inline=False)
    content = "React to manage **per-boss pings** for this category."
    return content, em, per_message_emojis

async def refresh_subscription_messages(guild: discord.Guild):
    """Edit existing panel messages in-place if present; create only if missing or in wrong channel."""
    if not await guild_is_authorized(guild):
        return
    gid = guild.id
    sub_ch_id = await get_subchannel_id(gid)
    if not sub_ch_id:
        return
    channel = guild.get_channel(sub_ch_id)
    if not can_send(channel):
        return

    # ensure emoji mapping across *all* bosses (stable)
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT id,name FROM bosses WHERE guild_id=?", (gid,))
        all_bosses = await c.fetchall()
    await ensure_emoji_mapping(gid, all_bosses)

    panel_map = await get_all_panel_records(gid)
    for cat in CATEGORY_ORDER:
        # Skip empty categories
        async with aiosqlite.connect(DB_PATH) as db:
            c = await db.execute("SELECT COUNT(*) FROM bosses WHERE guild_id=? AND category=?", (gid, cat))
            count = (await c.fetchone())[0]
        if count == 0:
            continue

        content, embed, emojis = await build_subscription_embed_for_category(gid, cat)
        if not embed:
            continue

        message = None
        existing_id, existing_ch = panel_map.get(cat, (None, None))

        # If we have a panel but it's in another channel, try to edit there; if that fails, recreate in correct channel.
        if existing_id and existing_ch:
            old_ch = guild.get_channel(existing_ch)
            if old_ch and can_send(old_ch):
                try:
                    message = await old_ch.fetch_message(existing_id)
                    # If the channel changed, we will *copy* to new channel but keep the old one until success.
                    if existing_ch != sub_ch_id:
                        try:
                            copy = await channel.send(content=content, embed=embed)
                            await set_panel_record(gid, cat, copy.id, channel.id)
                            # add missing reactions on the **new** message only
                            if can_react(channel):
                                existing = set(str(r.emoji) for r in copy.reactions)
                                for e in [e for e in emojis if e not in existing]:
                                    await copy.add_reaction(e)
                                    await asyncio.sleep(0.2)
                            # optional: delete old after successful copy
                            try:
                                await message.delete()
                            except Exception:
                                pass
                            message = copy
                        except Exception as e:
                            log.warning(f"Failed to move panel ({cat}) to new channel: {e}")
                    else:
                        # Same channel — edit in place
                        try:
                            await message.edit(content=content, embed=embed)
                        except Exception as e:
                            log.warning(f"Edit panel failed ({cat}), will try recreate: {e}")
                            message = None
                except Exception:
                    message = None  # fetch failed; create below

        # Create if missing
        if message is None:
            try:
                message = await channel.send(content=content, embed=embed)
                await set_panel_record(gid, cat, message.id, channel.id)
            except Exception as e:
                log.warning(f"Subscription panel ({cat}) create failed: {e}")
                continue

        # Add any missing reactions on the current message without touching existing ones
        if can_react(message.channel):
            try:
                existing = set(str(r.emoji) for r in message.reactions)
                for e in [e for e in emojis if e not in existing]:
                    await message.add_reaction(e)
                    await asyncio.sleep(0.2)
            except Exception as e:
                log.warning(f"Adding reactions failed for {cat}: {e}")

# Reactions: subscriptions + reaction-role panels
@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    # guard: ignore bot self
    if bot.user and payload.user_id == bot.user.id:
        return
    guild = bot.get_guild(payload.guild_id)
    if not guild or not await guild_is_authorized(guild):
        return
    emoji_str = str(payload.emoji)

    # Subscription panels
    panels = await get_all_panel_records(guild.id)
    if payload.message_id in [mid for (mid, _chid) in panels.values()]:
        async with aiosqlite.connect(DB_PATH) as db:
            c = await db.execute("SELECT boss_id FROM subscription_emojis WHERE guild_id=? AND emoji=?", (guild.id, emoji_str))
            r = await c.fetchone()
            if r:
                boss_id = r[0]
                await db.execute(
                    "INSERT OR IGNORE INTO subscription_members (guild_id,boss_id,user_id) VALUES (?,?,?)",
                    (guild.id, boss_id, payload.user_id)
                )
                await db.commit()
        return

    # Reaction roles panel
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT 1 FROM rr_panels WHERE message_id=?", (payload.message_id,))
        panel_present = (await c.fetchone()) is not None
    if panel_present:
        try:
            member = guild.get_member(payload.user_id) or await guild.fetch_member(payload.user_id)
            async with aiosqlite.connect(DB_PATH) as db:
                c = await db.execute("SELECT role_id FROM rr_map WHERE panel_message_id=? AND emoji=?", (payload.message_id, emoji_str))
                row = await c.fetchone()
            if not row:
                return
            role = guild.get_role(int(row[0]))
            if role:
                await member.add_roles(role, reason="Reaction role opt-in")
        except Exception as e:
            log.warning(f"Add reaction-role failed: {e}")

@bot.event
async def on_raw_reaction_remove(payload: discord.RawReactionActionEvent):
    guild = bot.get_guild(payload.guild_id)
    if not guild or not await guild_is_authorized(guild):
        return
    emoji_str = str(payload.emoji)

    # Subscription panels
    panels = await get_all_panel_records(guild.id)
    if payload.message_id in [mid for (mid, _chid) in panels.values()]:
        async with aiosqlite.connect(DB_PATH) as db:
            c = await db.execute("SELECT boss_id FROM subscription_emojis WHERE guild_id=? AND emoji=?", (guild.id, emoji_str))
            r = await c.fetchone()
            if r:
                boss_id = r[0]
                await db.execute(
                    "DELETE FROM subscription_members WHERE guild_id=? AND boss_id=? AND user_id=?",
                    (guild.id, boss_id, payload.user_id)
                )
                await db.commit()
        return

    # Reaction roles panel
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT 1 FROM rr_panels WHERE message_id=?", (payload.message_id,))
        panel_present = (await c.fetchone()) is not None
    if panel_present:
        try:
            member = guild.get_member(payload.user_id) or await guild.fetch_member(payload.user_id)
            async with aiosqlite.connect(DB_PATH) as db:
                c = await db.execute("SELECT role_id FROM rr_map WHERE panel_message_id=? AND emoji=?", (payload.message_id, str(payload.emoji)))
                row = await c.fetchone()
            if not row:
                return
            role = guild.get_role(int(row[0]))
            if role:
                await member.remove_roles(role, reason="Reaction role opt-out")
        except Exception as e:
            log.warning(f"Remove reaction-role failed: {e}")

# -------------------- QUICK RESET VIA MESSAGE --------------------
@bot.event
async def on_message(message: discord.Message):
    if message.author.bot or not message.guild:
        return
    if not await guild_is_authorized(message.guild):
        return
    if await is_blacklisted(message.guild.id, message.author.id):
        return

    # dynamic prefix
    try:
        prefix = await bot.get_prefix(message)  # may return list/str
        p = prefix if isinstance(prefix, str) else (prefix[0] if prefix else "!")
    except Exception:
        p = "!"

    content = (message.content or "").strip()
    if content.startswith(p) and len(content) > len(p):
        shorthand = content[len(p):].strip()
        root = shorthand.split(" ", 1)[0].lower()
        if root not in RESERVED_TRIGGERS:
            ident = shorthand.strip().strip('"').strip("'")
            result, err = await resolve_boss(message, ident)
            if result and not err:
                bid, nm, mins = result
                if await has_trusted(message.author, message.guild.id, bid):
                    async with aiosqlite.connect(DB_PATH) as db:
                        await db.execute("UPDATE bosses SET next_spawn_ts=? WHERE id=?", (now_ts() + int(mins)*60, bid))
                        await db.commit()
                    if can_send(message.channel):
                        await message.channel.send(f":crossed_swords: **{nm}** killed. Next **Spawn Time** in `{mins}m`.")
                    await refresh_subscription_messages(message.guild)
                    return
                else:
                    if can_send(message.channel):
                        await message.channel.send(":no_entry: You lack permission to reset this boss.")
                    return
    await bot.process_commands(message)

# -------------------- RESOLVE HELPERS (boss) --------------------
async def resolve_boss(ctx_or_msg, identifier: str) -> Tuple[Optional[tuple], Optional[str]]:
    gid = ctx_or_msg.guild.id
    ident = (identifier or "").strip()
    ident_lc = ident.lower()
    async with aiosqlite.connect(DB_PATH) as db:
        for q, param in [
            ("SELECT id,name,spawn_minutes FROM bosses WHERE guild_id=? AND LOWER(name)=?", ident_lc),
            ("SELECT id,name,spawn_minutes FROM bosses WHERE guild_id=? AND LOWER(name) LIKE ?", f"{ident_lc}%"),
            ("SELECT id,name,spawn_minutes FROM bosses WHERE guild_id=? AND LOWER(name) LIKE ?", f"%{ident_lc}%"),
        ]:
            c = await db.execute(q, (gid, param))
            rows = await c.fetchall()
            if len(rows) == 1: return rows[0], None
            if len(rows) > 1:  return None, f"Multiple matches for '{identifier}'. Use the exact name (quotes OK)."
        for q, param in [
            ("""SELECT b.id,b.name,b.spawn_minutes
                FROM boss_aliases a JOIN bosses b ON b.id=a.boss_id
                WHERE a.guild_id=? AND LOWER(a.alias)=?""", ident_lc),
            ("""SELECT b.id,b.name,b.spawn_minutes
                FROM boss_aliases a JOIN bosses b ON b.id=a.boss_id
                WHERE a.guild_id=? AND LOWER(a.alias) LIKE ?""", f"{ident_lc}%"),
            ("""SELECT b.id,b.name,b.spawn_minutes
                FROM boss_aliases a JOIN bosses b ON b.id=a.boss_id
                WHERE a.guild_id=? AND LOWER(a.alias) LIKE ?""", f"%{ident_lc}%"),
        ]:
            c = await db.execute(q, (gid, param))
            rows = await c.fetchall()
            if len(rows) == 1: return rows[0], None
            if len(rows) > 1:  return None, f"Multiple alias matches for '{identifier}'. Use exact alias."
    return None, f"No boss found for '{identifier}'."

# -------------------- BOSS COMMANDS --------------------
@bot.group(name="boss", invoke_without_command=True)
@guild_auth_check()
async def boss_group(ctx):
    p = ctx.prefix if isinstance(ctx.prefix, str) else "!"
    await ctx.send(f"Use `{p}help` for commands.")

@boss_group.command(name="add")
@guild_auth_check()
async def boss_add(ctx, *args):
    """
    !boss add "Name" <spawn_m> <window_m> [#channel] [pre_m] [category]
    """
    def _smart_parse_add(args: List[str], ctx: commands.Context) -> Tuple[str,int,int,Optional[int],int,str]:
        text = " ".join(args).strip()
        name = None
        if '"' in text:
            first = text.split('"', 1)[1]
            name = first.split('"', 1)[0].strip()
            remainder = (text.split('"', 1)[1]).split('"', 1)[1].strip()
            tokens = [t for t in remainder.split() if t]
        else:
            tokens = args[:]
        if name is None and tokens:
            name = tokens.pop(0)
        spawn_m = None; window_m = 0; ch_id: Optional[int] = None; pre_m = 10; cat = "Default"
        if tokens and tokens[0].lstrip("-").isdigit(): spawn_m = int(tokens.pop(0))
        if tokens and tokens[0].lstrip("-").isdigit(): window_m = int(tokens.pop(0))
        if tokens:
            maybe_ch = _resolve_channel_id_from_arg(ctx, tokens[0])
            if maybe_ch: ch_id = maybe_ch; tokens.pop(0)
        if tokens and tokens[0].lstrip("-").isdigit(): pre_m = int(tokens.pop(0))
        if tokens: cat = " ".join(tokens).strip()
        if spawn_m is None: raise ValueError("Missing spawn_minutes.")
        return name, int(spawn_m), int(max(0, window_m)), ch_id, int(max(0, pre_m)), norm_cat(cat)

    try:
        name, spawn_minutes, window_minutes, ch_id, pre_min, category = _smart_parse_add(list(args), ctx)
    except Exception:
        return await ctx.send('Format: `!boss add "Name" <spawn_m> <window_m> [#channel] [pre_m] [category]`')
    next_spawn = now_ts() - 3601  # -Nada
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO bosses (guild_id,channel_id,name,spawn_minutes,window_minutes,next_spawn_ts,pre_announce_min,created_by,category) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (ctx.guild.id, ch_id, name, int(spawn_minutes), int(window_minutes), next_spawn, int(pre_min), ctx.author.id, category)
        )
        await db.commit()
    await ctx.send(f":white_check_mark: Added **{name}** — every {spawn_minutes}m, window {window_minutes}m, pre {pre_min}m, cat {category}.")
    await refresh_subscription_messages(ctx.guild)

@boss_group.command(name="idleall")
@guild_auth_check()
@commands.has_permissions(manage_guild=True)
async def boss_idleall(ctx):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE bosses SET next_spawn_ts=? WHERE guild_id=?", (now_ts() - 3601, ctx.guild.id))
        await db.commit()
    await ctx.send(":white_check_mark: All timers set to **-Nada**.")
    await refresh_subscription_messages(ctx.guild)

@boss_group.command(name="nada")
@guild_auth_check()
@commands.has_permissions(manage_guild=True)
async def boss_nada(ctx, *, name: str):
    res, err = await resolve_boss(ctx, name)
    if err: return await ctx.send(f":no_entry: {err}")
    bid, nm, _ = res
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE bosses SET next_spawn_ts=? WHERE id=? AND guild_id=?", (now_ts() - 3601, bid, ctx.guild.id))
        await db.commit()
    await ctx.send(f":pause_button: **{nm}** set to **-Nada**.")
    await refresh_subscription_messages(ctx.guild)

@boss_group.command(name="nadaall")
@guild_auth_check()
@commands.has_permissions(manage_guild=True)
async def boss_nadaall(ctx):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE bosses SET next_spawn_ts=? WHERE guild_id=?", (now_ts() - 3601, ctx.guild.id))
        await db.commit()
    await ctx.send(":pause_button: **All bosses** set to **-Nada**.")
    await refresh_subscription_messages(ctx.guild)

@boss_group.command(name="info")
@guild_auth_check()
async def boss_info(ctx, *, name: str):
    res, err = await resolve_boss(ctx, name)
    if err: return await ctx.send(f":no_entry: {err}")
    bid, nm, _ = res
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute(
            "SELECT name,spawn_minutes,window_minutes,next_spawn_ts,channel_id,pre_announce_min,trusted_role_id,category,sort_key "
            "FROM bosses WHERE id=? AND guild_id=?", (bid, ctx.guild.id)
        )
        r = await c.fetchone()
    if not r: return await ctx.send("Boss not found.")
    name, spawn_m, window_m, ts, ch_id, pre, role_id, cat, sort_key = r
    left = int(ts) - now_ts()
    line1 = f"**{name}**\nCategory: {cat} | Sort: {sort_key or '(none)'}\n"
    line2 = f"Respawn: {spawn_m}m | Window: {window_m}m\n"
    line3 = f"Spawn Time: `{fmt_delta_for_list(left)}`"
    line3b = f"\n> *ETA {ts_to_hm_utc(ts)}*" if left > 0 else ""
    line4 = f"\nPre: {pre}m | Channel: {f'<#{ch_id}>' if ch_id else 'Default/Category'} | Role: {f'<@&{role_id}>' if role_id else 'None'}"
    await ctx.send(line1 + line2 + line3 + line3b + line4)

@boss_group.command(name="killed")
@guild_auth_check()
async def boss_killed(ctx, *, name: str):
    res, err = await resolve_boss(ctx, name)
    if err: return await ctx.send(f":no_entry: {err}")
    bid, nm, mins = res
    if not await has_trusted(ctx.author, ctx.guild.id, bid):
        return await ctx.send(":no_entry: You don't have permission for this boss.")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE bosses SET next_spawn_ts=? WHERE id=?", (now_ts() + int(mins) * 60, bid))
        await db.commit()
    await ctx.send(f":crossed_swords: **{nm}** killed. Next **Spawn Time** in `{mins}m`.")
    await refresh_subscription_messages(ctx.guild)

@boss_group.command(name="increase")
@guild_auth_check()
async def boss_increase(ctx, name: str, minutes: int):
    res, err = await resolve_boss(ctx, name)
    if err: return await ctx.send(f":no_entry: {err}")
    bid, nm, _ = res
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE bosses SET next_spawn_ts=next_spawn_ts+(?*60) WHERE id=? AND guild_id=?", (int(minutes), bid, ctx.guild.id))
        await db.commit()
        c = await db.execute("SELECT next_spawn_ts FROM bosses WHERE id=? AND guild_id=?", (bid, ctx.guild.id))
        ts = (await c.fetchone())[0]
    await ctx.send(f":arrow_up: Increased **{nm}** by {minutes}m. Spawn Time: `{fmt_delta_for_list(int(ts) - now_ts())}`.")
    await refresh_subscription_messages(ctx.guild)

@boss_group.command(name="reduce")
@guild_auth_check()
async def boss_reduce(ctx, name: str, minutes: int):
    res, err = await resolve_boss(ctx, name)
    if err: return await ctx.send(f":no_entry: {err}")
    bid, nm, _ = res
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT next_spawn_ts FROM bosses WHERE id=? AND guild_id=?", (bid, ctx.guild.id))
        ts_row = await c.fetchone()
        if not ts_row: return await ctx.send("Boss not found.")
        current_ts = int(ts_row[0]); new_ts = max(now_ts(), current_ts - int(minutes) * 60)
        await db.execute("UPDATE bosses SET next_spawn_ts=? WHERE id=? AND guild_id=?", (new_ts, bid, ctx.guild.id))
        await db.commit()
    await ctx.send(f":arrow_down: Reduced **{nm}** by {minutes}m. Spawn Time: `{fmt_delta_for_list(new_ts - now_ts())}`.")
    await refresh_subscription_messages(ctx.guild)

@boss_group.command(name="edit")
@guild_auth_check()
@commands.has_permissions(manage_guild=True)
async def boss_edit(ctx, name: str, field: str, value: str):
    allowed = {"spawn_minutes", "window_minutes", "pre_announce_min", "name", "category", "sort_key"}
    if field not in allowed: return await ctx.send(f"Editable: {', '.join(sorted(allowed))}")
    res, err = await resolve_boss(ctx, name)
    if err: return await ctx.send(f":no_entry: {err}")
    bid, _, _ = res
    async with aiosqlite.connect(DB_PATH) as db:
        if field in {"spawn_minutes", "window_minutes", "pre_announce_min"}:
            try: v = int(value)
            except ValueError: return await ctx.send("Value must be an integer.")
            if field == "spawn_minutes" and v < 1: return await ctx.send(":no_entry: spawn_minutes must be >= 1.")
            await db.execute(f"UPDATE bosses SET {field}=? WHERE id=?", (v, bid))
        elif field == "category":
            await db.execute("UPDATE bosses SET category=? WHERE id=?", (norm_cat(value), bid))
        else:
            await db.execute(f"UPDATE bosses SET {field}=? WHERE id=?", (value, bid))
        await db.commit()
    await ctx.send(":white_check_mark: Updated.")
    await refresh_subscription_messages(ctx.guild)

@boss_group.command(name="delete")
@guild_auth_check()
@commands.has_permissions(manage_guild=True)
async def boss_delete(ctx, *, name: str):
    res, err = await resolve_boss(ctx, name)
    if err: return await ctx.send(f":no_entry: {err}")
    bid, nm, _ = res
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM bosses WHERE id=? AND guild_id=?", (bid, ctx.guild.id))
        await db.execute("DELETE FROM subscription_emojis WHERE guild_id=? AND boss_id=?", (ctx.guild.id, bid))
        await db.execute("DELETE FROM subscription_members WHERE guild_id=? AND boss_id=?", (ctx.guild.id, bid))
        await db.execute("DELETE FROM boss_aliases WHERE guild_id=? AND boss_id=?", (ctx.guild.id, bid))
        await db.commit()
    await ctx.send(f":wastebasket: Deleted **{nm}**.")
    await refresh_subscription_messages(ctx.guild)

@boss_group.command(name="setcategory")
@guild_auth_check()
async def boss_setcategory(ctx, *, args: str):
    ident = None; category = None
    if '"' in args:
        a, b = args.split('"', 1); ident = a.strip()
        category = b.split('"', 1)[0].strip()
    if not ident or not category:
        parts = args.rsplit(" ", 1)
        if len(parts) == 2: ident, category = parts[0].strip(), parts[1].strip()
    if not ident or not category:
        return await ctx.send('Format: `!boss setcategory <name> "<Category>"`')
    res, err = await resolve_boss(ctx, ident)
    if err: return await ctx.send(f":no_entry: {err}")
    bid, nm, _ = res
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE bosses SET category=? WHERE id=? AND guild_id=?", (norm_cat(category), bid, ctx.guild.id))
        await db.commit()
    await ctx.send(f":label: **{nm}** → **{norm_cat(category)}**.")
    await refresh_subscription_messages(ctx.guild)

@boss_group.command(name="setsort")
@guild_auth_check()
async def boss_setsort(ctx, name: str, sort_key: str):
    res, err = await resolve_boss(ctx, name)
    if err: return await ctx.send(f":no_entry: {err}")
    bid, nm, _ = res
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE bosses SET sort_key=? WHERE id=? AND guild_id=?", (sort_key, bid, ctx.guild.id))
        await db.commit()
    await ctx.send(f":1234: Sort key for **{nm}** set to `{sort_key}`.")
    await refresh_subscription_messages(ctx.guild)

@boss_group.command(name="setchannel")
@guild_auth_check()
async def boss_setchannel(ctx, name: str, channel: discord.TextChannel):
    if name.lower() in {"all"}:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE bosses SET channel_id=? WHERE guild_id=?", (channel.id, ctx.guild.id))
            await db.commit()
        return await ctx.send(f":satellite: All boss reminders → {channel.mention}.")
    elif name.lower() in {"category","cat"}:
        return await ctx.send('Use `!boss setchannelcat "<Category>" #chan`.')
    res, err = await resolve_boss(ctx, name)
    if err: return await ctx.send(f":no_entry: {err}")
    bid, nm, _ = res
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE bosses SET channel_id=? WHERE id=? AND guild_id=?", (channel.id, bid, ctx.guild.id))
        await db.commit()
    await ctx.send(f":satellite: **{nm}** reminders → {channel.mention}.")

@boss_group.command(name="setchannelall")
@guild_auth_check()
@commands.has_permissions(manage_guild=True)
async def boss_setchannelall(ctx, channel: discord.TextChannel):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE bosses SET channel_id=? WHERE guild_id=?", (channel.id, ctx.guild.id))
        await db.commit()
    await ctx.send(f":satellite: All boss reminders → {channel.mention}.")

@boss_group.command(name="setchannelcat")
@guild_auth_check()
@commands.has_permissions(manage_guild=True)
async def boss_setchannelcat(ctx, *, args: str):
    if '"' in args:
        cat = args.split('"',1)[1].split('"',1)[0].strip()
        tail = args.split('"',2)[-1].strip()
        ch_id = _resolve_channel_id_from_arg(ctx, tail.split()[-1]) if tail else None
    else:
        parts = args.rsplit(" ", 1)
        if len(parts) != 2: return await ctx.send('Format: `!boss setchannelcat "<Category>" #chan`')
        cat, ch_token = parts[0], parts[1]
        ch_id = _resolve_channel_id_from_arg(ctx, ch_token)
    if not cat or not ch_id: return await ctx.send('Format: `!boss setchannelcat "<Category>" #chan`')
    catn = norm_cat(cat)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE bosses SET channel_id=? WHERE guild_id=? AND category=?", (ch_id, ctx.guild.id, catn))
        await db.commit()
    await ctx.send(f":satellite: **{catn}** boss reminders → <#{ch_id}>.")

@boss_group.command(name="setrole")
@guild_auth_check()
@commands.has_permissions(manage_guild=True)
async def boss_setrole(ctx, *args):
    if not args:
        return await ctx.send("Use `!boss setrole @Role` or `!boss setrole \"Name\" @Role` (or `none`).")
    text = " ".join(args).strip()
    if text.count('"') >= 2:
        boss_name = text.split('"', 1)[1].split('"', 1)[0].strip()
        remainder = text.split('"', 2)[-1].strip()
        if not remainder: return await ctx.send("Provide a role or `none` after the boss name.")
        role_arg = remainder
        res, err = await resolve_boss(ctx, boss_name)
        if err: return await ctx.send(f":no_entry: {err}")
        bid, nm, _ = res
        async with aiosqlite.connect(DB_PATH) as db:
            if role_arg.lower() in ("none","clear"):
                await db.execute("UPDATE bosses SET trusted_role_id=NULL WHERE id=? AND guild_id=?", (bid, ctx.guild.id))
                await db.commit()
                return await ctx.send(f":white_check_mark: Cleared reset role for **{nm}**.")
            role_obj = None
            if role_arg.startswith("<@&") and role_arg.endswith(">"):
                try: role_obj = ctx.guild.get_role(int(role_arg[3:-1]))
                except Exception: role_obj = None
            if not role_obj: role_obj = discord.utils.get(ctx.guild.roles, name=role_arg)
            if not role_obj: return await ctx.send("Role not found. Mention it or use exact name.")
            await db.execute("UPDATE bosses SET trusted_role_id=? WHERE id=? AND guild_id=?", (role_obj.id, bid, ctx.guild.id))
            await db.commit()
        return await ctx.send(f":white_check_mark: **{nm}** now requires **{role_obj.name}** to reset.")
    role_arg = text
    if role_arg.lower() in ("none","clear"):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE bosses SET trusted_role_id=NULL WHERE guild_id=?", (ctx.guild.id,))
            await db.commit()
        return await ctx.send(":white_check_mark: Cleared reset role on all bosses.")
    role_obj = None
    if role_arg.startswith("<@&") and role_arg.endswith(">"):
        try: role_obj = ctx.guild.get_role(int(role_arg[3:-1]))
        except Exception: role_obj = None
    if not role_obj: role_obj = discord.utils.get(ctx.guild.roles, name=role_arg)
    if not role_obj: return await ctx.send("Role not found. Mention it or use exact name.")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE bosses SET trusted_role_id=? WHERE guild_id=?", (role_obj.id, ctx.guild.id))
        await db.commit()
    await ctx.send(f":white_check_mark: All bosses now require **{role_obj.name}** to reset.")

@boss_group.command(name="alias")
@guild_auth_check()
@commands.has_permissions(manage_guild=True)
async def boss_alias(ctx, action: str = None, *, args: str = ""):
    action = (action or "").lower()
    if action not in {"add","remove","list","aliases"}:
        return await ctx.send('Use: `!boss alias add "Name" "Alias"`, `!boss alias remove "Name" "Alias"`, or `!boss aliases "Name"`')

    def parse_two_quoted(s: str) -> Optional[Tuple[str,str]]:
        s = s.strip()
        if s.count('"') < 4: return None
        first = s.split('"',1)[1].split('"',1)[0].strip()
        rest = s.split('"',2)[-1].strip()
        second = rest.split('"',1)[1].split('"',1)[0].strip() if rest.count('"')>=2 else None
        return (first, second) if second else None

    if action in {"add","remove"}:
        parsed = parse_two_quoted(args)
        if not parsed: return await ctx.send('Format: `!boss alias add "Name" "Alias"`')
        boss_name, alias = parsed
        res, err = await resolve_boss(ctx, boss_name)
        if err: return await ctx.send(f":no_entry: {err}")
        bid, nm, _ = res
        async with aiosqlite.connect(DB_PATH) as db:
            if action == "add":
                try:
                    await db.execute("INSERT INTO boss_aliases (guild_id,boss_id,alias) VALUES (?,?,?)",
                                     (ctx.guild.id, bid, alias.lower()))
                    await db.commit()
                    await ctx.send(f":white_check_mark: Added alias **{alias}** → **{nm}**.")
                except Exception:
                    await ctx.send(f":warning: Could not add alias (maybe already used?)")
            else:
                await db.execute("DELETE FROM boss_aliases WHERE guild_id=? AND boss_id=? AND alias=?",
                                 (ctx.guild.id, bid, alias.lower()))
                await db.commit()
                await ctx.send(f":white_check_mark: Removed alias **{alias}** from **{nm}**.")
        return

    name = args.strip().strip('"')
    if not name:
        return await ctx.send('Format: `!boss aliases "Boss Name"`')
    res, err = await resolve_boss(ctx, name)
    if err: return await ctx.send(f":no_entry: {err}")
    bid, nm, _ = res
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT alias FROM boss_aliases WHERE guild_id=? AND boss_id=? ORDER BY alias", (ctx.guild.id, bid))
        rows = [r[0] for r in await c.fetchall()]
    await ctx.send(f"**Aliases for {nm}:** " + (", ".join(rows) if rows else "*none*"))

@boss_group.command(name="find")
@guild_auth_check()
async def boss_find(ctx, *, ident: str):
    res, err = await resolve_boss(ctx, ident)
    if err: return await ctx.send(f":no_entry: {err}")
    bid, nm, _ = res
    await ctx.send(f"Matched: **{nm}**")

# -------------------- TIMERS VIEW (with window states) --------------------
async def get_show_eta(guild_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT COALESCE(show_eta,0) FROM guild_config WHERE guild_id=?", (guild_id,))
        r = await c.fetchone()
        return bool(r and int(r[0]) == 1)

@bot.command(name="timers")
@guild_auth_check()
async def timers_cmd(ctx):
    gid = ctx.guild.id; show_eta = await get_show_eta(gid)
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT name,next_spawn_ts,category,sort_key,window_minutes FROM bosses WHERE guild_id=?", (gid,))
        rows = await c.fetchall()
    if not rows:
        return await ctx.send("No timers. Add with `boss add \"Name\" <spawn_m> <window_m> [#chan] [pre_m] [cat]`.")
    now = now_ts()
    grouped: Dict[str, List[tuple]] = {k: [] for k in CATEGORY_ORDER}
    for name, ts, cat, sk, win in rows:
        grouped.setdefault(norm_cat(cat), []).append((sk or "", name, int(ts), int(win)))
    for cat in CATEGORY_ORDER:
        items = grouped.get(cat, [])
        if not items: continue
        normal: List[tuple] = []; nada_list: List[tuple] = []
        for sk, nm, ts, win in items:
            delta = ts - now
            t = fmt_delta_for_list(delta)
            if t == "-Nada":
                nada_list.append((sk, nm, t, ts, win))
            else:
                normal.append((sk, nm, t, ts, win))
        normal.sort(key=lambda x: (natural_key(x[0]), natural_key(x[1])))
        nada_list.sort(key=lambda x: natural_key(x[1]))
        blocks: List[str] = []
        for sk, nm, t, ts, win_m in normal:
            win_status = window_label(now, ts, win_m)
            line1 = f"〔 **{nm}** • Spawn: `{t}` • Window: `{win_status}` 〕"
            eta_line = f"\n> *ETA {ts_to_hm_utc(ts)}*" if show_eta and (ts - now) > 0 else ""
            blocks.append(line1 + (eta_line if eta_line else ""))
        if nada_list:
            blocks.append("*Lost (-Nada):*")
            for sk, nm, t, ts, win_m in nada_list:
                blocks.append(f"• **{nm}** — `{t}`")
        description = "\n\n".join(blocks) if blocks else "No timers."
        em = discord.Embed(
            title=f"{category_emoji(cat)} {cat}",
            description=description,
            color=await get_category_color(gid, cat)
        )
        await ctx.send(embed=em)

# ---------- intervals list ----------
async def send_intervals_list(ctx):
    gid = ctx.guild.id
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT name,category,spawn_minutes,window_minutes,pre_announce_min,sort_key FROM bosses WHERE guild_id=?", (gid,))
        rows = await c.fetchall()
    if not rows:
        return await ctx.send("No bosses configured.")

    grouped: Dict[str, List[tuple]] = {k: [] for k in CATEGORY_ORDER}
    for name, cat, spawn_m, window_m, pre_m, sk in rows:
        grouped.setdefault(norm_cat(cat), []).append((sk or "", name, int(spawn_m), int(window_m), int(pre_m)))

    for cat in CATEGORY_ORDER:
        items = grouped.get(cat, [])
        if not items: continue
        items.sort(key=lambda x: (natural_key(x[0]), natural_key(x[1])))
        lines: List[str] = []
        for sk, nm, sp, win, pre in items:
            lines.append(f"• **{nm}** — Respawn: {sp}m • Window: {win}m • Pre: {pre}m")
        em = discord.Embed(
            title=f"{category_emoji(cat)} {cat} — Intervals",
            description="",
            color=await get_category_color(gid, cat)
        )
        bucket = ""; chunks: List[str] = []
        for line in lines:
            if len(bucket) + len(line) + 1 > 1000:
                chunks.append(bucket); bucket = line + "\n"
            else:
                bucket += line + "\n"
        if bucket: chunks.append(bucket)
        for i, ch in enumerate(chunks, 1):
            em.add_field(name=f"{cat} ({i})" if len(chunks) > 1 else cat, value=ch, inline=False)
        try:
            await ctx.send(embed=em)
        except Exception:
            text_fallback = f"**{cat} — Intervals**\n" + "\n".join(lines)
            if len(text_fallback) > 1990: text_fallback = text_fallback[:1985] + "…"
            await ctx.send(text_fallback)

@bot.command(name="intervals")
@guild_auth_check()
async def intervals_cmd(ctx):
    await send_intervals_list(ctx)

@boss_group.command(name="intervals")
@guild_auth_check()
async def boss_intervals_cmd(ctx):
    await send_intervals_list(ctx)

# -------------------- Command error handler --------------------
@bot.event
async def on_command_error(ctx, error):
    from discord.ext import commands as ext
    if isinstance(error, ext.CommandNotFound):
        return
    try:
        await ctx.send(f":warning: {error}")
    except Exception:
        pass
# ============================================
# SECTION 3 / 4 — Market & Lixing Systems
# ============================================
# Depends on Section 1+2 symbols:
# bot, aiosqlite, discord, app_commands, commands, atexit, asyncio, logging
# DB_PATH, now_ts, ts_to_utc, human_ago, can_send, guild_is_authorized
# guild_auth_check, CATEGORY_ORDER, norm_cat, category_emoji
# meta_get, meta_set

log = logging.getLogger("ch-bossbot.market")

# ---------- Small shared utils ----------
def _yn(val: bool) -> str:
    return "Yes" if val else "No"

def _short(s: str, n: int = 200) -> str:
    s = s or ""
    return s if len(s) <= n else (s[: n - 1] + "…")

# ==================================================
# MARKET: tables (created in migrations in Section 1)
# Schema recap:
#  market_listings(
#    id INTEGER PK AUTOINCREMENT, guild_id, user_id, type TEXT('BUY'|'SELL'),
#    item_name TEXT, quantity INTEGER, price TEXT, accepts_trades INTEGER,
#    taking_offers INTEGER, notes TEXT, created_ts INTEGER, is_active INTEGER,
#    message_channel_id INTEGER, message_id INTEGER
#  )
#  market_offers(
#    id INTEGER PK AUTOINCREMENT, guild_id, listing_id, user_id,
#    amount TEXT, message TEXT, created_ts INTEGER
#  )
# ==================================================

# ---------- Market Embeds ----------
async def market_listing_embed(guild: discord.Guild, row: dict, offers: list[dict] | None = None) -> discord.Embed:
    typ = row["type"]
    title = f"🛒 Market — {'Buying' if typ=='BUY' else 'Selling'}: {row['item_name']}"
    color = 0x1abc9c if typ == "BUY" else 0xe67e22
    em = discord.Embed(title=title, color=color, timestamp=discord.utils.utcnow())
    em.add_field(name="Listing ID", value=str(row["id"]))
    em.add_field(name="Quantity", value=str(row["quantity"]), inline=True)
    em.add_field(name="Price/Range", value=row["price"] or "—", inline=True)
    em.add_field(name="Accepts Trades", value=_yn(bool(row["accepts_trades"])), inline=True)
    em.add_field(name="Taking Offers", value=_yn(bool(row["taking_offers"])), inline=True)
    em.add_field(name="Owner", value=f"<@{row['user_id']}>", inline=True)
    if row.get("notes"):
        em.add_field(name="Notes", value=_short(row["notes"], 800), inline=False)
    em.set_footer(text=f"Created {human_ago(max(1, now_ts()-int(row['created_ts'])))}")
    if offers:
        # Show up to 5 most recent offers
        snippet = []
        for off in offers[:5]:
            snippet.append(f"• <@{off['user_id']}> offered **{off['amount'] or '—'}** — {_short(off['message'] or '', 120)}")
        if snippet:
            em.add_field(name="Recent Offers", value="\n".join(snippet), inline=False)
    return em

class OfferModal(discord.ui.Modal, title="Make an Offer"):
    def __init__(self, guild_id: int, listing_id: int, owner_id: int):
        super().__init__(timeout=180)
        self.guild_id = guild_id
        self.listing_id = listing_id
        self.owner_id = owner_id
        self.amount = discord.ui.TextInput(label="Offer (amount / terms)", required=False, max_length=120, placeholder="e.g., 1.2m or 250k + items")
        self.message = discord.ui.TextInput(label="Message", style=discord.TextStyle.paragraph, required=True, max_length=500, placeholder="Add details, availability, contact pref…")
        self.add_item(self.amount); self.add_item(self.message)

    async def on_submit(self, interaction: discord.Interaction):
        # Persist offer
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute(
                    "INSERT INTO market_offers (guild_id, listing_id, user_id, amount, message, created_ts) VALUES (?,?,?,?,?,?)",
                    (self.guild_id, self.listing_id, interaction.user.id, str(self.amount.value or "").strip(), str(self.message.value or "").strip(), now_ts())
                )
                await db.commit()
        except Exception as e:
            log.warning(f"Offer insert failed: {e}")
            return await interaction.response.send_message(":warning: Could not save your offer.", ephemeral=True)

        # Notify owner (mention in listing channel if we have one; also ephemeral confirm)
        await interaction.response.send_message(":white_check_mark: Offer submitted!", ephemeral=True)

        # Try to update the public listing message with newest offers preview
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                c = await db.execute("SELECT message_channel_id, message_id FROM market_listings WHERE id=? AND guild_id=?", (self.listing_id, self.guild_id))
                row = await c.fetchone()
                if not row: return
                ch_id, msg_id = row
                ch = interaction.client.get_channel(int(ch_id)) if ch_id else None
                if not ch or not hasattr(ch, "fetch_message"):
                    return
                msg = await ch.fetch_message(int(msg_id))
                # Rebuild embed with latest offers (top 5)
                c = await db.execute("SELECT user_id, amount, message FROM market_offers WHERE guild_id=? AND listing_id=? ORDER BY id DESC LIMIT 5",
                                     (self.guild_id, self.listing_id))
                offs = [{"user_id": u, "amount": a, "message": m} for (u, a, m) in await c.fetchall()]
                # Build listing dict
                c = await db.execute("""SELECT id, user_id, type, item_name, quantity, price, accepts_trades,
                                               taking_offers, notes, created_ts
                                        FROM market_listings WHERE id=? AND guild_id=?""", (self.listing_id, self.guild_id))
                lr = await c.fetchone()
            if lr:
                keys = ["id","user_id","type","item_name","quantity","price","accepts_trades","taking_offers","notes","created_ts"]
                d = dict(zip(keys, lr))
                emb = await market_listing_embed(interaction.guild, d, offs)
                # Keep existing view / buttons intact (edit embed only)
                await msg.edit(embed=emb)
        except Exception as e:
            log.info(f"Could not refresh listing message after offer: {e}")

class ListingView(discord.ui.View):
    def __init__(self, guild_id: int, listing_id: int, owner_id: int, timeout: int = 600):
        super().__init__(timeout=timeout)
        self.guild_id = guild_id
        self.listing_id = listing_id
        self.owner_id = owner_id

    @discord.ui.button(label="Make Offer", style=discord.ButtonStyle.primary, emoji="💬")
    async def make_offer(self, interaction: discord.Interaction, button: discord.ui.Button):
        # open modal
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                c = await db.execute("SELECT is_active FROM market_listings WHERE id=? AND guild_id=?", (self.listing_id, self.guild_id))
                r = await c.fetchone()
            if not r or int(r[0]) != 1:
                return await interaction.response.send_message(":no_entry: This listing is no longer active.", ephemeral=True)
        except Exception:
            return await interaction.response.send_message(":warning: Unable to verify listing state.", ephemeral=True)

        await interaction.response.send_modal(OfferModal(self.guild_id, self.listing_id, self.owner_id))

# ---------- Market Commands ----------
@bot.group(name="market", invoke_without_command=True)
@guild_auth_check()
async def market_group(ctx):
    await ctx.send("Use `!market help` for commands.")

@market_group.command(name="help")
@guild_auth_check()
async def market_help(ctx):
    lines = [
        "**Market Commands**",
        "• `!market add buy|sell \"Item\" <qty> <price_or_range> <acceptTrades:yes|no> <takingOffers:yes|no> [notes...]`",
        "• `!market list [buy|sell] [search...]` — show active listings",
        "• `!market mine` — your listings",
        "• `!market view <id>` — open listing with offer button",
        "• `!market remove <id>` — deactivate your listing",
        "• `!market edit <id> field value` — fields: item, qty, price, trades, offers, notes, type",
        "• Admin: `!setmarketchannel #chan` — 6h digest output",
    ]
    await ctx.send("\n".join(lines))

@bot.command(name="setmarketchannel")
@guild_auth_check()
@commands.has_permissions(manage_guild=True)
async def setmarketchannel_cmd(ctx, channel: discord.TextChannel):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE guild_config SET market_channel_id=? WHERE guild_id=?", (channel.id, ctx.guild.id))
        await db.commit()
    await ctx.send(f":white_check_mark: Market digest channel set to {channel.mention}.")

@market_group.command(name="add")
@guild_auth_check()
async def market_add(ctx, typ: str, item_or_quoted: str, qty: int, price: str, accept_trades: str, taking_offers: str, *, notes: str = ""):
    typu = typ.strip().upper()
    if typu not in {"BUY","SELL"}:
        return await ctx.send(":no_entry: Type must be `buy` or `sell`.")
    # Allow quoted item name without quotes param if user typed normal; discord already splits tokens unless quoted.
    item_name = item_or_quoted.strip().strip('"')
    acc = accept_trades.strip().lower() in {"y","yes","true","1"}
    tak = taking_offers.strip().lower() in {"y","yes","true","1"}
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                """INSERT INTO market_listings (guild_id,user_id,type,item_name,quantity,price,accepts_trades,taking_offers,notes,created_ts,is_active)
                   VALUES (?,?,?,?,?,?,?,?,?,?,1)""",
                (ctx.guild.id, ctx.author.id, typu, item_name, int(qty), price, 1 if acc else 0, 1 if tak else 0, notes or "", now_ts())
            )
            await db.commit()
            c = await db.execute("SELECT last_insert_rowid()")
            lid = (await c.fetchone())[0]
    except Exception as e:
        log.warning(f"market_add insert failed: {e}")
        return await ctx.send(":warning: Could not create listing.")
    await ctx.send(f":white_check_mark: Listing **#{lid}** created.")
    # Post a standalone listing message (optional): use current channel
    try:
        listing = {
            "id": lid, "user_id": ctx.author.id, "type": typu, "item_name": item_name,
            "quantity": int(qty), "price": price, "accepts_trades": 1 if acc else 0,
            "taking_offers": 1 if tak else 0, "notes": notes or "", "created_ts": now_ts()
        }
        emb = await market_listing_embed(ctx.guild, listing, [])
        view = ListingView(ctx.guild.id, lid, ctx.author.id)
        msg = await ctx.send(embed=emb, view=view)
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE market_listings SET message_channel_id=?, message_id=? WHERE id=? AND guild_id=?",
                             (msg.channel.id, msg.id, lid, ctx.guild.id))
            await db.commit()
    except Exception as e:
        log.info(f"Failed to publish listing message: {e}")

@market_group.command(name="list")
@guild_auth_check()
async def market_list_cmd(ctx, typ: str | None = None, *, search: str = ""):
    where = ["guild_id=?", "is_active=1"]
    args: list = [ctx.guild.id]
    if typ:
        t = typ.strip().upper()
        if t not in {"BUY","SELL"}:
            return await ctx.send(":no_entry: Use `buy` or `sell` (or omit).")
        where.append("type=?"); args.append(t)
    if search:
        where.append("LOWER(item_name) LIKE ?"); args.append(f"%{search.strip().lower()}%")
    q = f"""SELECT id,user_id,type,item_name,quantity,price,accepts_trades,taking_offers,notes,created_ts
            FROM market_listings WHERE {" AND ".join(where)} ORDER BY id DESC LIMIT 20"""
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute(q, tuple(args))
        rows = await c.fetchall()
    if not rows:
        return await ctx.send("No matching listings.")
    out = []
    for r in rows:
        lid, uid, t, item, qty, price, trd, off, notes, cts = r
        out.append(f"• **#{lid}** [{ 'BUY' if t=='BUY' else 'SELL' }] **{item}** ×{qty} — {price} • Trades:{_yn(trd)} • Offers:{_yn(off)} • <@{uid}>")
    text = "\n".join(out)
    if len(text) > 1900:
        text = text[:1895] + "…"
    await ctx.send(text)

@market_group.command(name="mine")
@guild_auth_check()
async def market_mine_cmd(ctx):
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("""SELECT id,type,item_name,quantity,price,accepts_trades,taking_offers,is_active
                                FROM market_listings
                                WHERE guild_id=? AND user_id=? ORDER BY id DESC LIMIT 20""",
                             (ctx.guild.id, ctx.author.id))
        rows = await c.fetchall()
    if not rows:
        return await ctx.send("You have no listings.")
    lines = []
    for lid, t, item, qty, price, trd, off, active in rows:
        lines.append(f"• **#{lid}** [{'BUY' if t=='BUY' else 'SELL'}] {item} ×{qty} — {price} • Trades:{_yn(trd)} • Offers:{_yn(off)} • {'Active' if active else 'Closed'}")
    await ctx.send("\n".join(lines))

@market_group.command(name="view")
@guild_auth_check()
async def market_view_cmd(ctx, lid: int):
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("""SELECT id,user_id,type,item_name,quantity,price,accepts_trades,taking_offers,notes,created_ts,is_active
                                FROM market_listings WHERE guild_id=? AND id=?""", (ctx.guild.id, lid))
        r = await c.fetchone()
    if not r:
        return await ctx.send(":no_entry: Listing not found.")
    keys = ["id","user_id","type","item_name","quantity","price","accepts_trades","taking_offers","notes","created_ts","is_active"]
    d = dict(zip(keys, r))
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT user_id, amount, message FROM market_offers WHERE guild_id=? AND listing_id=? ORDER BY id DESC LIMIT 5", (ctx.guild.id, lid))
        offs = [{"user_id": u, "amount": a, "message": m} for (u, a, m) in await c.fetchall()]
    em = await market_listing_embed(ctx.guild, d, offs)
    view = ListingView(ctx.guild.id, lid, d["user_id"])
    await ctx.send(embed=em, view=view)

@market_group.command(name="remove")
@guild_auth_check()
async def market_remove_cmd(ctx, lid: int):
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT user_id FROM market_listings WHERE id=? AND guild_id=?", (lid, ctx.guild.id))
        r = await c.fetchone()
        if not r:
            return await ctx.send(":no_entry: Listing not found.")
        if r[0] != ctx.author.id and not ctx.author.guild_permissions.manage_messages:
            return await ctx.send(":no_entry: Only the owner or staff can remove this.")
        await db.execute("UPDATE market_listings SET is_active=0 WHERE id=? AND guild_id=?", (lid, ctx.guild.id))
        await db.commit()
    await ctx.send(f":white_check_mark: Listing **#{lid}** closed.")

@market_group.command(name="edit")
@guild_auth_check()
async def market_edit_cmd(ctx, lid: int, field: str, *, value: str):
    field = field.lower()
    mapping = {
        "item": "item_name",
        "qty": "quantity",
        "price": "price",
        "trades": "accepts_trades",
        "offers": "taking_offers",
        "notes": "notes",
        "type": "type"
    }
    if field not in mapping:
        return await ctx.send("Editable fields: item, qty, price, trades, offers, notes, type")
    dbf = mapping[field]
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT user_id FROM market_listings WHERE id=? AND guild_id=?", (lid, ctx.guild.id))
        r = await c.fetchone()
        if not r:
            return await ctx.send(":no_entry: Listing not found.")
        if r[0] != ctx.author.id and not ctx.author.guild_permissions.manage_messages:
            return await ctx.send(":no_entry: Only the owner or staff can edit this.")
        if dbf in {"accepts_trades","taking_offers"}:
            v = 1 if value.strip().lower() in {"y","yes","true","1"} else 0
            await db.execute(f"UPDATE market_listings SET {dbf}=? WHERE id=? AND guild_id=?", (v, lid, ctx.guild.id))
        elif dbf == "quantity":
            try:
                v = int(value)
            except ValueError:
                return await ctx.send(":no_entry: qty must be an integer.")
            await db.execute(f"UPDATE market_listings SET {dbf}=? WHERE id=? AND guild_id=?", (v, lid, ctx.guild.id))
        elif dbf == "type":
            t = value.strip().upper()
            if t not in {"BUY","SELL"}:
                return await ctx.send(":no_entry: type must be BUY or SELL.")
            await db.execute("UPDATE market_listings SET type=? WHERE id=? AND guild_id=?", (t, lid, ctx.guild.id))
        else:
            await db.execute(f"UPDATE market_listings SET {dbf}=? WHERE id=? AND guild_id=?", (value, lid, ctx.guild.id))
        await db.commit()
    await ctx.send(":white_check_mark: Listing updated.")

# ---------- Market Digest (every 6 hours) ----------
@tasks.loop(hours=6)
async def market_digest_loop():
    try:
        for g in bot.guilds:
            if not await guild_is_authorized(g):
                continue
            async with aiosqlite.connect(DB_PATH) as db:
                c = await db.execute("SELECT market_channel_id FROM guild_config WHERE guild_id=?", (g.id,))
                r = await c.fetchone()
                ch_id = r[0] if r else None
            if not ch_id:
                continue
            ch = g.get_channel(int(ch_id))
            if not can_send(ch):
                continue
            # Fetch active listings
            async with aiosqlite.connect(DB_PATH) as db:
                c = await db.execute("""SELECT id,type,item_name,quantity,price,accepts_trades,taking_offers,user_id
                                        FROM market_listings WHERE guild_id=? AND is_active=1 ORDER BY id DESC LIMIT 30""", (g.id,))
                rows = await c.fetchall()
            if not rows:
                continue
            # Build two sections
            buys = []
            sells = []
            for lid, typ, item, qty, price, trd, off, uid in rows:
                line = f"• **#{lid}** {item} ×{qty} — {price} • Trades:{_yn(trd)} • Offers:{_yn(off)} • <@{uid}>"
                (buys if typ == "BUY" else sells).append(line)
            em = discord.Embed(title="🛒 Market Digest — Active Listings", color=0x7289da, timestamp=discord.utils.utcnow())
            if buys:
                em.add_field(name="Buying", value=_short("\n".join(buys), 1000), inline=False)
            if sells:
                em.add_field(name="Selling", value=_short("\n".join(sells), 1000), inline=False)
            em.set_footer(text="Use !market list or !market view <id> to interact.")
            try:
                await ch.send(embed=em)
            except Exception as e:
                log.info(f"Market digest post failed in {g.id}: {e}")
    except Exception as e:
        log.warning(f"market_digest_loop error: {e}")

# ==================================================
# LIXING: tables (created in migrations in Section 1)
# Schema recap:
#  lixing_posts(
#    id INTEGER PK AUTOINCREMENT, guild_id, user_id, char_name TEXT,
#    char_class TEXT, level INTEGER, desired_lixes TEXT, notes TEXT,
#    created_ts INTEGER, is_active INTEGER, message_channel_id INTEGER, message_id INTEGER
#  )
# ==================================================

async def lixing_embed(row: dict) -> discord.Embed:
    em = discord.Embed(title=f"⚔️ Lixing Group — {row['char_name']}", color=0x4aa3ff, timestamp=discord.utils.utcnow())
    em.add_field(name="Class", value=row["char_class"] or "—", inline=True)
    em.add_field(name="Level", value=str(row["level"]) if row["level"] is not None else "—", inline=True)
    em.add_field(name="Desired Lixes", value=row["desired_lixes"] or "N/A", inline=True)
    if row.get("notes"):
        em.add_field(name="Notes", value=_short(row["notes"], 800), inline=False)
    em.add_field(name="Contact", value=f"<@{row['user_id']}>", inline=False)
    em.set_footer(text=f"Post #{row['id']} • Created {human_ago(max(1, now_ts()-int(row['created_ts'])))}")
    return em

@bot.group(name="lixing", invoke_without_command=True)
@guild_auth_check()
async def lixing_group(ctx):
    await ctx.send("Use `!lixing help` for commands.")

@lixing_group.command(name="help")
@guild_auth_check()
async def lixing_help(ctx):
    lines = [
        "**Lixing Commands**",
        "• `!lixing post \"CharName\" <Class> <Level|NA> <DesiredLixes|NA> [notes...]`",
        "• `!lixing list [class] [min-max] [search...]`",
        "• `!lixing mine` — your posts",
        "• `!lixing view <id>` — view card",
        "• `!lixing remove <id>` — close your post",
        "• Admin: `!setlixingchannel #chan` — 6h digest output",
    ]
    await ctx.send("\n".join(lines))

@bot.command(name="setlixingchannel")
@guild_auth_check()
@commands.has_permissions(manage_guild=True)
async def setlixingchannel_cmd(ctx, channel: discord.TextChannel):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE guild_config SET lixing_channel_id=? WHERE guild_id=?", (channel.id, ctx.guild.id))
        await db.commit()
    await ctx.send(f":white_check_mark: Lixing digest channel set to {channel.mention}.")

@lixing_group.command(name="post")
@guild_auth_check()
async def lixing_post(ctx, char_name: str, char_class: str, level: str, desired_lixes: str, *, notes: str = ""):
    lvl_val: int | None
    if level.strip().lower() in {"na","n/a","-"}:
        lvl_val = None
    else:
        try:
            lvl_val = int(level)
        except ValueError:
            return await ctx.send(":no_entry: Level must be a number or NA.")
    desired = desired_lixes if desired_lixes.lower() not in {"na","n/a"} else "N/A"
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("""INSERT INTO lixing_posts
                                (guild_id,user_id,char_name,char_class,level,desired_lixes,notes,created_ts,is_active)
                                VALUES (?,?,?,?,?,?,?,?,1)""",
                             (ctx.guild.id, ctx.author.id, char_name, char_class, lvl_val, desired, notes or "", now_ts()))
            await db.commit()
            c = await db.execute("SELECT last_insert_rowid()")
            pid = (await c.fetchone())[0]
    except Exception as e:
        log.warning(f"lixing_post insert failed: {e}")
        return await ctx.send(":warning: Could not create lixing post.")
    await ctx.send(f":white_check_mark: Lixing post **#{pid}** created.")
    # Publish a card
    try:
        row = {"id": pid, "user_id": ctx.author.id, "char_name": char_name, "char_class": char_class,
               "level": lvl_val, "desired_lixes": desired, "notes": notes or "", "created_ts": now_ts()}
        emb = await lixing_embed(row)
        msg = await ctx.send(embed=emb)
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE lixing_posts SET message_channel_id=?, message_id=? WHERE id=? AND guild_id=?",
                             (msg.channel.id, msg.id, pid, ctx.guild.id))
            await db.commit()
    except Exception as e:
        log.info(f"Failed to publish lixing message: {e}")

@lixing_group.command(name="list")
@guild_auth_check()
async def lixing_list_cmd(ctx, class_filter: str | None = None, level_range: str | None = None, *, search: str = ""):
    where = ["guild_id=?", "is_active=1"]
    args: list = [ctx.guild.id]
    if class_filter:
        where.append("LOWER(char_class)=?"); args.append(class_filter.lower())
    if level_range and "-" in level_range:
        try:
            lo, hi = level_range.split("-", 1)
            if lo.strip(): where.append("(level IS NULL OR level>=?)"); args.append(int(lo))
            if hi.strip(): where.append("(level IS NULL OR level<=?)"); args.append(int(hi))
        except Exception:
            return await ctx.send(":no_entry: Level range must be like `120-180` (or `-180`, `150-`).")
    if search:
        where.append("LOWER(char_name) LIKE ?"); args.append(f"%{search.strip().lower()}%")
    q = f"""SELECT id,char_name,char_class,COALESCE(level,-1),desired_lixes,user_id
            FROM lixing_posts WHERE {" AND ".join(where)} ORDER BY id DESC LIMIT 20"""
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute(q, tuple(args))
        rows = await c.fetchall()
    if not rows:
        return await ctx.send("No matching lixing posts.")
    lines = []
    for pid, name, cl, lvl, des, uid in rows:
        lvl_txt = "N/A" if int(lvl) < 0 else str(lvl)
        lines.append(f"• **#{pid}** {name} ({cl} {lvl_txt}) — Desired: {des} • <@{uid}>")
    await ctx.send("\n".join(lines))

@lixing_group.command(name="mine")
@guild_auth_check()
async def lixing_mine_cmd(ctx):
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("""SELECT id,char_name,char_class,COALESCE(level,-1),desired_lixes,is_active
                                FROM lixing_posts WHERE guild_id=? AND user_id=? ORDER BY id DESC LIMIT 20""",
                             (ctx.guild.id, ctx.author.id))
        rows = await c.fetchall()
    if not rows:
        return await ctx.send("You have no lixing posts.")
    lines = []
    for pid, name, cl, lvl, des, active in rows:
        lvl_txt = "N/A" if int(lvl) < 0 else str(lvl)
        lines.append(f"• **#{pid}** {name} ({cl} {lvl_txt}) — Desired: {des} • {'Active' if active else 'Closed'}")
    await ctx.send("\n".join(lines))

@lixing_group.command(name="view")
@guild_auth_check()
async def lixing_view_cmd(ctx, pid: int):
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("""SELECT id,user_id,char_name,char_class,COALESCE(level,-1),desired_lixes,notes,created_ts,is_active
                                FROM lixing_posts WHERE guild_id=? AND id=?""", (ctx.guild.id, pid))
        r = await c.fetchone()
    if not r:
        return await ctx.send(":no_entry: Post not found.")
    keys = ["id","user_id","char_name","char_class","level","desired_lixes","notes","created_ts","is_active"]
    d = dict(zip(keys, r))
    d["level"] = None if int(d["level"]) < 0 else int(d["level"])
    emb = await lixing_embed(d)
    await ctx.send(embed=emb)

@lixing_group.command(name="remove")
@guild_auth_check()
async def lixing_remove_cmd(ctx, pid: int):
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT user_id FROM lixing_posts WHERE id=? AND guild_id=?", (pid, ctx.guild.id))
        r = await c.fetchone()
        if not r:
            return await ctx.send(":no_entry: Post not found.")
        if r[0] != ctx.author.id and not ctx.author.guild_permissions.manage_messages:
            return await ctx.send(":no_entry: Only the owner or staff can remove this.")
        await db.execute("UPDATE lixing_posts SET is_active=0 WHERE id=? AND guild_id=?", (pid, ctx.guild.id))
        await db.commit()
    await ctx.send(f":white_check_mark: Lixing post **#{pid}** closed.")

# ---------- Lixing Digest (every 6 hours) ----------
@tasks.loop(hours=6)
async def lixing_digest_loop():
    try:
        for g in bot.guilds:
            if not await guild_is_authorized(g):
                continue
            async with aiosqlite.connect(DB_PATH) as db:
                c = await db.execute("SELECT lixing_channel_id FROM guild_config WHERE guild_id=?", (g.id,))
                r = await c.fetchone()
                ch_id = r[0] if r else None
            if not ch_id:
                continue
            ch = g.get_channel(int(ch_id))
            if not can_send(ch):
                continue
            async with aiosqlite.connect(DB_PATH) as db:
                c = await db.execute("""SELECT id,char_name,char_class,COALESCE(level,-1),desired_lixes,user_id
                                        FROM lixing_posts WHERE guild_id=? AND is_active=1 ORDER BY id DESC LIMIT 30""", (g.id,))
                rows = await c.fetchall()
            if not rows:
                continue
            lines = []
            for pid, name, cl, lvl, des, uid in rows:
                lvl_txt = "N/A" if int(lvl) < 0 else str(lvl)
                lines.append(f"• **#{pid}** {name} ({cl} {lvl_txt}) — Desired: {des} • <@{uid}>")
            em = discord.Embed(title="⚔️ Lixing Digest — Active Posts", color=0x4aa3ff, timestamp=discord.utils.utcnow())
            em.add_field(name="Open Groups / Players", value=_short("\n".join(lines), 1900), inline=False)
            em.set_footer(text="Use !lixing list or !lixing view <id> to contact.")
            try:
                await ch.send(embed=em)
            except Exception as e:
                log.info(f"Lixing digest post failed in {g.id}: {e}")
    except Exception as e:
        log.warning(f"lixing_digest_loop error: {e}")

# ---------- Startup hooks to ensure loops are running ----------
@bot.event
async def on_ready():
    # Start loops if not running
    if not market_digest_loop.is_running():
        market_digest_loop.start()
    if not lixing_digest_loop.is_running():
        lixing_digest_loop.start()
# ============================================
# SECTION 4 / 4 — Final Glue (Help, Errors, Ready Hooks)
# ============================================
# Depends on Sections 1–3 symbols:
# bot, discord, app_commands, commands, asyncio, aiosqlite, logging
# DB_PATH, now_ts, human_ago, ts_to_utc, can_send, guild_is_authorized, guild_auth_check
# market_digest_loop, lixing_digest_loop  (from Section 3, if present)
# timers_tick, uptime_heartbeat           (from Section 1/2)
# refresh_subscription_messages           (from Section 2)
# SEED_VERSION, ensure_seed_for_guild     (from Section 1)
# meta_get, meta_set

log = logging.getLogger("ch-bossbot.glue")

# -------------------- Unified Help --------------------
@bot.command(name="help")
@guild_auth_check()
async def help_cmd(ctx):
    p = ctx.prefix if isinstance(ctx.prefix, str) else "!"
    lines = [
        "### Boss Tracker — Quick Help",
        f"**Prefix**: `{p}`",
        "",
        "**Use**",
        f"• Reset (quick): `{p}<BossOrAlias>` → sets next **Spawn Time**.",
        f"• Timers: `{p}timers`  • Slash `/timers` (ephemeral, with category toggles).",
        f"• Intervals: `{p}intervals` (per-boss Respawn / Window / Pre).",
        "",
        "**Boss Ops**",
        f"• Add: `{p}boss add \"Name\" <spawn_m> <window_m> [#chan] [pre_m] [category]` (starts at `-Nada`)",
        f"• Killed: `{p}boss killed \"Name\"`",
        f"• Adjust: `{p}boss increase|reduce \"Name\" <m>`",
        f"• Idle: `{p}boss nada \"Name\"` • All idle: `{p}boss nadaall`",
        f"• Edit: `{p}boss edit \"Name\" <spawn_minutes|window_minutes|pre_announce_min|name|category|sort_key> <value>`",
        f"• Aliases: `{p}boss alias add|remove \"Name\" \"Alias\"` • Show: `{p}boss aliases \"Name\"`",
        "",
        "**Announcements**",
        f"• Global channel: `{p}setannounce #chan`",
        f"• Category route: `{p}setannounce category \"<Category>\" #chan`",
        f"• Per-boss: `{p}boss setchannel \"Name\" #chan` • All: `{p}boss setchannelall #chan` • By category: `{p}boss setchannelcat \"<Category>\" #chan`",
        "",
        "**Subscriptions**",
        f"• Panels channel: `{p}setsubchannel #chan` • Refresh: `{p}showsubscriptions`",
        f"• **Ping channel**: `{p}setsubpingchannel #chan`",
        "",
        "**Display & Health**",
        f"• UTC ETA on/off: `{p}seteta on|off`",
        f"• Category colors: `{p}setcatcolor <Category> <#hex>`",
        f"• Heartbeat interval: `{p}setuptime <minutes>` • Heartbeat channel: `{p}setheartbeatchannel #chan`",
        f"• Status: `{p}status` • Health: `{p}health`",
        "",
        "**Reaction Roles**",
        "• Slash: `/roles_panel` — create a panel where reacting grants/removes roles.",
        "",
        "**Market**",
        f"• Add: `{p}market add buy|sell \"Item\" <qty> <price_or_range> <acceptTrades:yes|no> <takingOffers:yes|no> [notes...]`",
        f"• Browse: `{p}market list [buy|sell] [search...]` • Mine: `{p}market mine`",
        f"• View: `{p}market view <id>` → interactive **Make Offer** modal",
        f"• Edit/Close: `{p}market edit <id> field value` • `{p}market remove <id>`",
        f"• Digest channel (admin): `{p}setmarketchannel #chan`",
        "",
        "**Lixing (leveling groups)**",
        f"• Post: `{p}lixing post \"CharName\" <Class> <Level|NA> <DesiredLixes|NA> [notes...]`",
        f"• Browse: `{p}lixing list [class] [min-max] [search...]` • Mine: `{p}lixing mine`",
        f"• View/Close: `{p}lixing view <id>` • `{p}lixing remove <id>`",
        f"• Digest channel (admin): `{p}setlixingchannel #chan`",
    ]
    text = "\n".join(lines)
    if len(text) > 1990:
        text = text[:1985] + "…"
    await ctx.send(text)

# -------------------- Slash /help (mirror of !help) --------------------
@bot.tree.command(name="help", description="Show bot help")
@app_commands.guild_only()
async def slash_help(interaction: discord.Interaction):
    p = "!"
    try:
        # Try to peek resolved prefix for this guild (optional)
        async with aiosqlite.connect(DB_PATH) as db:
            c = await db.execute("SELECT COALESCE(prefix, ?) FROM guild_config WHERE guild_id=?", (p, interaction.guild_id))
            r = await c.fetchone()
            if r and r[0]:
                p = r[0]
    except Exception:
        pass

    content = (
        f"**Boss**: `{p}<Boss>` reset • `{p}timers` • `{p}intervals`\n"
        f"**Ops**: `{p}boss add|killed|increase|reduce|nada|nadaall|edit|alias|setsort|setchannel*`\n"
        f"**Announce**: `{p}setannounce ...` • `{p}setsubchannel` • `{p}setsubpingchannel`\n"
        f"**Display**: `{p}seteta` • `{p}setcatcolor` • `{p}setuptime` • `{p}setheartbeatchannel`\n"
        f"**Info**: `{p}status` • `{p}health`\n\n"
        f"**Market**: `{p}market add|list|mine|view|edit|remove` • `{p}setmarketchannel`\n"
        f"**Lixing**: `{p}lixing post|list|mine|view|remove` • `{p}setlixingchannel`\n"
        f"**Roles**: `/roles_panel`"
    )
    await interaction.response.send_message(content, ephemeral=True)

# -------------------- Error Handling --------------------
from discord.ext import commands as ext

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, ext.CommandNotFound):
        return
    try:
        await ctx.send(f":warning: {error}")
    except Exception:
        pass
    # Log trace to server logs
    log.exception("Command error:", exc_info=error)

@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    try:
        if hasattr(interaction.response, "is_done") and not interaction.response.is_done():
            await interaction.response.send_message(f":warning: {error}", ephemeral=True)
        else:
            await interaction.followup.send(f":warning: {error}", ephemeral=True)
    except Exception:
        pass
    log.exception("App command error:", exc_info=error)

# -------------------- Safe Ready Hooks (non-destructive) --------------------
# We avoid overwriting any existing @bot.event on_ready declared in previous sections.
# Instead, we (1) add a lightweight listener, and (2) leverage setup_hook to ensure loops are running.
async def _aux_on_ready():
    try:
        # Ensure background loops are running (idempotent checks)
        if "timers_tick" in globals():
            if not timers_tick.is_running():
                timers_tick.start()
        if "uptime_heartbeat" in globals():
            if not uptime_heartbeat.is_running():
                uptime_heartbeat.start()
        if "market_digest_loop" in globals():
            if not market_digest_loop.is_running():
                market_digest_loop.start()
        if "lixing_digest_loop" in globals():
            if not lixing_digest_loop.is_running():
                lixing_digest_loop.start()
        # Optionally refresh sub panels on boot (non-intrusive)
        for g in bot.guilds:
            try:
                if await guild_is_authorized(g):
                    # Light touch: only rebuild if panels table has entries (avoids spam)
                    async with aiosqlite.connect(DB_PATH) as db:
                        c = await db.execute("SELECT COUNT(*) FROM subscription_panels WHERE guild_id=?", (g.id,))
                        cnt = (await c.fetchone())[0]
                    if cnt:
                        await refresh_subscription_messages(g)
            except Exception as e:
                log.info(f"Aux refresh for guild {g.id} skipped: {e}")
        log.info("Aux on_ready completed.")
    except Exception as e:
        log.warning(f"Aux on_ready error: {e}")

# Register the auxiliary listener without replacing the main on_ready
bot.add_listener(_aux_on_ready, "on_ready")

# Also use setup_hook to make sure app commands are synced once when the bot connects.
@bot.event
async def setup_hook():
    # This runs before on_ready and is safe to exist alongside other on_ready definitions.
    try:
        # Global sync — okay for single-shard; if you targeted a guild earlier, this is still safe.
        await bot.tree.sync()
    except Exception as e:
        log.warning(f"App command sync in setup_hook failed: {e}")

# -------------------- Optional: Lightweight ping command --------------------
@bot.command(name="ping")
@guild_auth_check()
async def ping_cmd(ctx):
    await ctx.send("Pong!")

# -------------------- (No run/main block here) --------------------
# Run/entrypoint is defined in Section 1 to avoid duplication.

