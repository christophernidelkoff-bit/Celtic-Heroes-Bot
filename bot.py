# ======================================================
# SECTION 1 / 4 — Core bootstrap, helpers, DB, blacklist,
#                  and robust guild-auth gate (fixed)
# ======================================================

from __future__ import annotations

import os
import re
import atexit
import signal
import asyncio
import logging
import shutil
import io
from typing import Optional, Tuple, List, Dict, Any, Set
from datetime import datetime, timezone, timedelta

import aiosqlite
import discord
from discord.ext import commands, tasks
from discord import app_commands
from dotenv import load_dotenv

# -------------------- ENV / GLOBALS --------------------
load_dotenv()

TOKEN = os.getenv("DISCORD_TOKEN")
if not TOKEN:
    raise SystemExit("DISCORD_TOKEN missing in environment")

# Optional, for /ps
ALLOW_POWERSHELL = os.getenv("ALLOW_POWERSHELL", "0").lower() in {"1", "true", "yes"}

# Persistent DB path (Render disk is /data)
DB_PATH = os.getenv("DB_PATH", "/data/bosses.db" if os.path.isdir("/data") else "bosses.db")

# Auth patches: strongly prefer a fixed user ID; name fallback
AUTH_USER_ID_ENV = os.getenv("AUTH_USER_ID")  # numeric Discord user id (string)
AUTH_USER_NAME_ENV = os.getenv("AUTH_USER_NAME", "blunderbusstin").strip()  # fallback search string

DEFAULT_PREFIX = "!"
CHECK_INTERVAL_SECONDS = 15
DEFAULT_UPTIME_MINUTES = 60
NADA_GRACE_SECONDS = 1800  # after window closes, only flip to -Nada once this grace passes

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ch-bossbot")

intents = discord.Intents.default()
# Required for prefix commands & timers UI
intents.message_content = True
intents.reactions = True
intents.guilds = True
# We use MEMBERS safely via fetch, but enabling improves reliability:
intents.members = True

# Timer ticks (for offline catch-up)
_last_timer_tick_ts: int = 0
_prev_timer_tick_ts: int = 0

SEED_VERSION = "v2025-09-12-subping-window-ps-final"

# -------------------- TIME / FORMAT HELPERS --------------------
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

def window_label(now: int, next_ts: int, window_m: int) -> str:
    """
    Minute-based window status:
      - future: '{window_m}m (pending)'
      - during window: '{X}m left (open)'  (X minutes remaining)
      - after window but before grace: 'closed'
      - after grace: '-Nada'
    """
    delta = next_ts - now
    if delta >= 0:
        return f"{max(0, int(window_m))}m (pending)"
    open_secs = -delta
    if open_secs <= window_m * 60:
        left_m = max(0, (window_m * 60 - open_secs) // 60)
        return f"{left_m}m left (open)"
    after_close = open_secs - window_m * 60
    if after_close <= NADA_GRACE_SECONDS:
        return "closed"
    return "-Nada"

# -------------------- CATEGORIES / COLORS / EMOJIS --------------------
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

# -------------------- BOT / PREFIX --------------------
async def get_guild_prefix(_bot, message: discord.Message):
    if not message or not message.guild: return DEFAULT_PREFIX
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            c = await db.execute("SELECT COALESCE(prefix, ?) FROM guild_config WHERE guild_id=?",
                                 (DEFAULT_PREFIX, message.guild.id))
            r = await c.fetchone()
            if r and r[0]: return r[0]
    except Exception:
        pass
    return DEFAULT_PREFIX

bot = commands.Bot(command_prefix=get_guild_prefix, intents=intents, help_command=None)

# Triggers we reserve for our own commands (so quick-kill shorthand doesn't grab them)
RESERVED_TRIGGERS = {
    "help","boss","timers","setprefix","seed_import",
    "setsubchannel","setsubpingchannel","showsubscriptions","setuptime",
    "setheartbeatchannel","setannounce","seteta","status","health",
    "setcatcolor","intervals","market","lixing","reslash"
}

muted_due_on_boot: Set[int] = set()
bot._seen_keys = set()

# -------------------- PREFLIGHT MIGRATIONS (sync) --------------------
def preflight_migrate_sync():
    import sqlite3
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
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
        auth_user_id INTEGER DEFAULT NULL         -- for robust guild auth gate
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)""")
    # Additional tables used elsewhere:
    cur.execute("""CREATE TABLE IF NOT EXISTS category_colors (
        guild_id INTEGER NOT NULL, category TEXT NOT NULL, color_hex TEXT NOT NULL,
        PRIMARY KEY (guild_id, category)
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS subscription_emojis (
        guild_id INTEGER NOT NULL, boss_id INTEGER NOT NULL, emoji TEXT NOT NULL,
        PRIMARY KEY (guild_id, boss_id)
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS subscription_members (
        guild_id INTEGER NOT NULL, boss_id INTEGER NOT NULL, user_id INTEGER NOT NULL,
        PRIMARY KEY (guild_id, boss_id, user_id)
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS boss_aliases (
        guild_id INTEGER NOT NULL, boss_id INTEGER NOT NULL, alias TEXT NOT NULL,
        UNIQUE (guild_id, alias)
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS category_channels (
        guild_id INTEGER NOT NULL, category TEXT NOT NULL, channel_id INTEGER NOT NULL,
        PRIMARY KEY (guild_id, category)
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS user_timer_prefs (
        guild_id INTEGER NOT NULL, user_id INTEGER NOT NULL, categories TEXT NOT NULL,
        PRIMARY KEY (guild_id, user_id)
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS subscription_panels (
        guild_id INTEGER NOT NULL, category TEXT NOT NULL, message_id INTEGER NOT NULL,
        channel_id INTEGER DEFAULT NULL,
        PRIMARY KEY (guild_id, category)
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS rr_panels (
        message_id INTEGER PRIMARY KEY, guild_id INTEGER NOT NULL, channel_id INTEGER NOT NULL, title TEXT DEFAULT ''
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS rr_map (
        panel_message_id INTEGER NOT NULL, emoji TEXT NOT NULL, role_id INTEGER NOT NULL,
        PRIMARY KEY (panel_message_id, emoji)
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS blacklist (
        guild_id INTEGER NOT NULL, user_id INTEGER NOT NULL,
        PRIMARY KEY (guild_id, user_id)
    )""")
    # Market / Lixing (created again in async too)
    cur.execute("""CREATE TABLE IF NOT EXISTS market_channels (
        guild_id INTEGER PRIMARY KEY,
        market_channel_id INTEGER,  -- optional digest/announce channel
        lixing_channel_id INTEGER   -- optional digest/announce channel
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS market_listings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        guild_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        type TEXT NOT NULL CHECK (type IN ('buy','sell')),
        item_name TEXT NOT NULL,
        quantity INTEGER NOT NULL,
        price TEXT,
        accepts_trades INTEGER NOT NULL DEFAULT 0,
        taking_offers INTEGER NOT NULL DEFAULT 1,
        is_active INTEGER NOT NULL DEFAULT 1,
        created_ts INTEGER NOT NULL
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS market_offers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        listing_id INTEGER NOT NULL,
        guild_id INTEGER NOT NULL,
        bidder_id INTEGER NOT NULL,
        text TEXT NOT NULL,
        created_ts INTEGER NOT NULL
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS lixing_posts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        guild_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        char_name TEXT NOT NULL,
        char_class TEXT NOT NULL,
        level INTEGER NOT NULL,
        desired_lixes TEXT,
        is_active INTEGER NOT NULL DEFAULT 1,
        created_ts INTEGER NOT NULL
    )""")
    conn.commit()
    conn.close()

preflight_migrate_sync()

# -------------------- ASYNC MIGRATIONS --------------------
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
            auth_user_id INTEGER DEFAULT NULL
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
        # Market/Lixing
        await db.execute("""CREATE TABLE IF NOT EXISTS market_channels (
            guild_id INTEGER PRIMARY KEY,
            market_channel_id INTEGER,
            lixing_channel_id INTEGER
        )""")
        await db.execute("""CREATE TABLE IF NOT EXISTS market_listings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            type TEXT NOT NULL CHECK (type IN ('buy','sell')),
            item_name TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            price TEXT,
            accepts_trades INTEGER NOT NULL DEFAULT 0,
            taking_offers INTEGER NOT NULL DEFAULT 1,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_ts INTEGER NOT NULL
        )""")
        await db.execute("""CREATE TABLE IF NOT EXISTS market_offers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            listing_id INTEGER NOT NULL,
            guild_id INTEGER NOT NULL,
            bidder_id INTEGER NOT NULL,
            text TEXT NOT NULL,
            created_ts INTEGER NOT NULL
        )""")
        await db.execute("""CREATE TABLE IF NOT EXISTS lixing_posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            char_name TEXT NOT NULL,
            char_class TEXT NOT NULL,
            level INTEGER NOT NULL,
            desired_lixes TEXT,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_ts INTEGER NOT NULL
        )""")
        await db.commit()

async def upsert_guild_defaults(guild_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO guild_config (guild_id, prefix, uptime_minutes, show_eta) VALUES (?,?,?,?) "
            "ON CONFLICT(guild_id) DO NOTHING",
            (guild_id, DEFAULT_PREFIX, DEFAULT_UPTIME_MINUTES, 0)
        )
        await db.commit()

async def meta_set(key: str, value: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value)
        )
        await db.commit()

async def meta_get(key: str) -> Optional[str]:
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT value FROM meta WHERE key=?", (key,))
        r = await c.fetchone()
        return r[0] if r else None

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

bot.add_check(blacklist_check())

# -------------------- ROBUST GUILD AUTH GATE --------------------
# Prevent false negatives in production (after redeploy) and keep commands responsive.
# Strategy:
#  1) Prefer guild_config.auth_user_id if set -> fetch_member(id).
#  2) Else use AUTH_USER_ID env -> fetch_member(id).
#  3) Else try name search (AUTH_USER_NAME env, default 'blunderbusstin').
#  4) Cache a positive result for 15 minutes per guild.
#  5) If we cannot verify and Intents are missing, LOG a warning and ALLOW (to avoid total outage).
# Use this gate at start of each command.

_auth_cache: Dict[int, Tuple[bool, float]] = {}  # guild_id -> (allowed, expires_at_ts)

async def _get_auth_user_id_from_db(guild_id: int) -> Optional[int]:
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT auth_user_id FROM guild_config WHERE guild_id=?", (guild_id,))
        r = await c.fetchone()
        if r and r[0]:
            try: return int(r[0])
            except Exception: return None
    return None

async def ensure_guild_auth(guild: Optional[discord.Guild]) -> bool:
    if guild is None:
        return False
    now = now_ts()
    cached = _auth_cache.get(guild.id)
    if cached and cached[1] > now:
        return cached[0]

    # 1) DB override
    db_uid = await _get_auth_user_id_from_db(guild.id)
    # 2) ENV fallback id
    env_uid = None
    if AUTH_USER_ID_ENV:
        try: env_uid = int(AUTH_USER_ID_ENV)
        except Exception: env_uid = None

    target_uid = db_uid or env_uid

    async def has_member_by_id(uid: int) -> bool:
        try:
            # fetch_member works even if not cached
            m = await guild.fetch_member(uid)
            return m is not None
        except discord.NotFound:
            return False
        except discord.Forbidden:
            # No permission to fetch members (rare); try cache
            m = guild.get_member(uid)
            return m is not None
        except Exception:
            return False

    allowed = False
    if target_uid:
        allowed = await has_member_by_id(target_uid)
    else:
        # 3) Name search fallback
        name_q = (AUTH_USER_NAME_ENV or "blunderbusstin").strip()
        if name_q:
            try:
                # search_members requires Members Intent; gracefully handle if not enabled
                matches = await guild.search_members(query=name_q, limit=1)  # type: ignore
                allowed = bool(matches)
            except Exception:
                # As a last resort, check cached members
                for m in guild.members:
                    nm = (m.nick or m.name or "").lower()
                    if name_q.lower() in nm:
                        allowed = True
                        break
                # If we still can't confirm, allow to avoid outages
                if not allowed:
                    log.warning(f"[auth] Could not verify '{name_q}' in guild {guild.id}; allowing due to limited intents.")
                    allowed = True

    # cache decision for 15 minutes
    _auth_cache[guild.id] = (allowed, now + 15 * 60)
    return allowed

# (Admin commands to set/clear auth user live in Section 2; not shown in help.)
# ======================================================
# SECTION 2 / 4 — Resolvers, subscription panels (stable),
#                 sub-ping channel, settings, boss/admin cmds
# ======================================================

# -------------------- RESOLVE HELPERS --------------------
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

# -------------------- CHANNEL RESOLUTION --------------------
def can_send(channel: Optional[discord.abc.GuildChannel]) -> bool:
    if not channel or not isinstance(channel, (discord.TextChannel, discord.Thread)): return False
    me = channel.guild.me
    perms = channel.permissions_for(me)
    return perms.view_channel and perms.send_messages and perms.embed_links and perms.read_message_history

def can_react(channel: Optional[discord.abc.GuildChannel]) -> bool:
    if not channel or not isinstance(channel, (discord.TextChannel, discord.Thread)): return False
    me = channel.guild.me
    perms = channel.permissions_for(me)
    return perms.add_reactions and perms.view_channel and perms.read_message_history

async def resolve_announce_channel(guild_id: int, explicit_channel_id: Optional[int], category: Optional[str] = None) -> Optional[discord.TextChannel]:
    guild = bot.get_guild(guild_id)
    if not guild: return None
    # per-boss override
    if explicit_channel_id:
        ch = guild.get_channel(explicit_channel_id)
        if can_send(ch): return ch
    # per-category override
    if category:
        async with aiosqlite.connect(DB_PATH) as db:
            c = await db.execute("SELECT channel_id FROM category_channels WHERE guild_id=? AND category=?",
                                 (guild_id, norm_cat(category)))
            r = await c.fetchone()
        if r and r[0]:
            ch = guild.get_channel(r[0])
            if can_send(ch): return ch
    # global default
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT default_channel FROM guild_config WHERE guild_id=?", (guild_id,))
        r = await c.fetchone()
        if r and r[0]:
            ch = guild.get_channel(r[0])
            if can_send(ch): return ch
    # first sendable channel fallback
    for ch in guild.text_channels:
        if can_send(ch): return ch
    return None

async def resolve_heartbeat_channel(guild_id: int) -> Optional[discord.TextChannel]:
    guild = bot.get_guild(guild_id)
    if not guild: return None
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT heartbeat_channel_id, default_channel FROM guild_config WHERE guild_id=?", (guild_id,))
        r = await c.fetchone()
    hb_id, def_id = (r[0], r[1]) if r else (None, None)
    for cid in [hb_id, def_id]:
        if cid:
            ch = guild.get_channel(cid)
            if can_send(ch): return ch
    for ch in guild.text_channels:
        if can_send(ch): return ch
    return None

# -------------------- SUBSCRIPTION PANELS: LOOKUP --------------------
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

# -------------------- SUBSCRIPTION PANELS: EMOJI MAP (STABLE) --------------------
async def ensure_emoji_mapping(guild_id: int, bosses: List[tuple]):
    """
    Creates/repairs a 1:1 mapping of boss_id -> emoji, preserving existing pairs.
    If duplicates are found, they are reassigned deterministically to free emojis.
    """
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
                for b in sorted(blist)[1:]:
                    needs_reassign.append(b)
        available = [e for e in palette if e not in used_emojis]
        for boss_id in needs_reassign:
            if not available: break
            new_e = available.pop(0)
            await db.execute("UPDATE subscription_emojis SET emoji=? WHERE guild_id=? AND boss_id=?", (new_e, guild_id, boss_id))
            boss_to_emoji[boss_id] = new_e
            used_emojis.add(new_e)
        have_ids = set(boss_to_emoji.keys())
        for boss_id, _name in bosses:
            if boss_id in have_ids: continue
            if not available:
                available = [e for e in palette if e not in used_emojis]
                if not available: break
            e = available.pop(0)
            await db.execute("INSERT OR REPLACE INTO subscription_emojis (guild_id,boss_id,emoji) VALUES (?,?,?)",
                             (guild_id, boss_id, e))
            boss_to_emoji[boss_id] = e
            used_emojis.add(e)
        await db.commit()

# -------------------- SUBSCRIPTION PANELS: BUILD + REFRESH (EDIT-IN-PLACE) --------------------
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
        # prevent duplicate reaction requirement within the same message
        if e in per_message_emojis: 
            continue
        per_message_emojis.append(e)
        lines.append(f"{e} — **{name}**")

    # field-chunking so we never exceed embed limits
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
    """
    Stable behavior:
      - One panel per category, ID recorded in DB.
      - If a message exists, we EDIT it in place (no fresh post), preserving reactions.
      - If the recorded message no longer exists, we post once and store new ID.
      - We never reorder/reshuffle categories; we iterate CATEGORY_ORDER.
    """
    gid = guild.id
    sub_ch_id = await get_subchannel_id(gid)
    if not sub_ch_id:
        return
    channel = guild.get_channel(sub_ch_id)
    if not can_send(channel):
        return

    # ensure emojis exist for all bosses
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT id,name FROM bosses WHERE guild_id=?", (gid,))
        all_bosses = await c.fetchall()
    await ensure_emoji_mapping(gid, all_bosses)

    panel_map = await get_all_panel_records(gid)

    for cat in CATEGORY_ORDER:
        # skip empty categories
        async with aiosqlite.connect(DB_PATH) as db:
            c = await db.execute("SELECT COUNT(*) FROM bosses WHERE guild_id=? AND category=?", (gid, cat))
            count = (await c.fetchone())[0]
        if count == 0:
            continue

        content, embed, emojis = await build_subscription_embed_for_category(gid, cat)
        if not embed:
            continue

        existing_id, existing_ch = panel_map.get(cat, (None, None))
        # If recorded channel differs from current sub channel, try to delete old to keep tidiness
        if existing_id and existing_ch and existing_ch != sub_ch_id:
            old_ch = guild.get_channel(existing_ch)
            if old_ch and can_send(old_ch):
                try:
                    old_msg = await old_ch.fetch_message(existing_id)
                    await old_msg.delete()
                except Exception:
                    pass
            existing_id = None

        message = None
        if existing_id:
            # Edit in place
            try:
                message = await channel.fetch_message(existing_id)
                await message.edit(content=content, embed=embed)
            except discord.NotFound:
                message = None  # will re-create below
            except Exception as e:
                log.warning(f"Panel edit failed for {cat}: {e}")
                message = None

        if message is None:
            try:
                message = await channel.send(content=content, embed=embed)
                await set_panel_record(gid, cat, message.id, channel.id)
            except Exception as e:
                log.warning(f"Panel create failed for {cat}: {e}")
                continue

        # Ensure required reactions are present (add any missing; do NOT clear)
        if can_react(channel):
            try:
                existing = set(str(r.emoji) for r in message.reactions)
                for e in [e for e in emojis if e not in existing]:
                    await message.add_reaction(e)
                    await asyncio.sleep(0.2)
            except Exception as e:
                log.warning(f"Adding reactions failed for {cat}: {e}")

# -------------------- Pings to subscribers (separate channel supported) --------------------
async def send_subscription_ping(guild_id: int, boss_id: int, phase: str, boss_name: str, when_left: Optional[int] = None):
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT sub_ping_channel_id, sub_channel_id FROM guild_config WHERE guild_id=?", (guild_id,))
        r = await c.fetchone()
        sub_ping_id = (r[0] if r else None) or (r[1] if r else None)  # fallback to sub panels channel
        c = await db.execute("SELECT user_id FROM subscription_members WHERE guild_id=? AND boss_id=?", (guild_id, boss_id))
        subs = [row[0] for row in await c.fetchall()]
    if not sub_ping_id or not subs: return
    guild = bot.get_guild(guild_id);  ch = guild.get_channel(sub_ping_id) if guild else None
    if not can_send(ch): return
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

# -------------------- PERMISSIONS --------------------
async def has_trusted(member: discord.Member, guild_id: int, boss_id: Optional[int] = None) -> bool:
    if member.guild_permissions.administrator: return True
    async with aiosqlite.connect(DB_PATH) as db:
        if boss_id:
            c = await db.execute("SELECT trusted_role_id FROM bosses WHERE id=? AND guild_id=?", (boss_id, guild_id))
            r = await c.fetchone()
            if r and r[0]: return any(role.id == r[0] for role in member.roles)
    return member.guild_permissions.manage_messages

# -------------------- HELP (tidy, no auth/blacklist shown) --------------------
@bot.command(name="help")
async def help_cmd(ctx):
    if not await ensure_guild_auth(ctx.guild):
        return
    p = await get_guild_prefix(bot, ctx.message)
    lines = [
        f"**Boss Tracker — Quick Help**",
        f"Prefix: `{p}`",
        "",
        f"**Use**",
        f"• Reset (quick): `{p}<BossOrAlias>` → sets next **Spawn Time**.",
        f"• Timers: `{p}timers`  • Slash `/timers` has per-user category toggles.",
        f"• Intervals: `{p}intervals`  (Respawn/Window/Pre per boss).",
        "",
        f"**Boss Ops**",
        f"• Add: `{p}boss add \"Name\" <spawn_m> <window_m> [#chan] [pre_m] [category]` (starts at `-Nada`)",
        f"• Killed: `{p}boss killed <name>` • Adjust: `{p}boss increase|reduce <name> <m>`",
        f"• Idle: `{p}boss nada <name>` • All idle: `{p}boss nadaall`",
        f"• Edit: `{p}boss edit <name> <spawn_minutes|window_minutes|pre_announce_min|name|category|sort_key> <value>`",
        f"• Aliases: `{p}boss alias add|remove \"Name\" \"Alias\"` • Show: `{p}boss aliases \"Name\"`",
        "",
        f"**Announce Channels**",
        f"• Global: `{p}setannounce #chan`  • Category: `{p}setannounce category \"<Category>\" #chan`",
        f"• Per-boss: `{p}boss setchannel \"<Name>\" #chan` • All: `{p}boss setchannelall #chan` • By category: `{p}boss setchannelcat \"<Category>\" #chan`",
        "",
        f"**Subscriptions**",
        f"• Panels channel: `{p}setsubchannel #chan` • Refresh: `{p}showsubscriptions`",
        f"• **Ping channel**: `{p}setsubpingchannel #chan` (where @subs are pinged)",
        "",
        f"**Misc**",
        f"• UTC ETA: `{p}seteta on|off` • Colors: `{p}setcatcolor <Category> <#hex>`",
        f"• Heartbeat: `{p}setuptime <N>` • HB channel: `{p}setheartbeatchannel #chan`",
        f"• Status: `{p}status` • Health: `{p}health`",
        "",
        f"**Reaction Roles (slash)**",
        f"• `/roles_panel` — create a message where reacting grants/removes roles.",
        "",
        f"**Market & Lixing (slash)**",
        f"• `/market` — manage buy/sell listings & offers.  • `/lixing` — post/join lix groups.",
        "",
        f"**Host PowerShell (slash)**",
        f"• `/ps command:<...>` — runs on the bot host (Admins only; set `ALLOW_POWERSHELL=1`).",
    ]
    text = "\n".join(lines)
    if len(text) > 1990: text = text[:1985] + "…"
    if can_send(ctx.channel): await ctx.send(text)

# -------------------- STATUS / HEALTH --------------------
@bot.command(name="status")
async def status_cmd(ctx):
    if not await ensure_guild_auth(ctx.guild):
        return
    gid = ctx.guild.id; p = await get_guild_prefix(bot, ctx.message)
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute(
            "SELECT COALESCE(prefix, ?), default_channel, sub_channel_id, sub_ping_channel_id, COALESCE(uptime_minutes, ?), heartbeat_channel_id, COALESCE(show_eta,0) "
            "FROM guild_config WHERE guild_id=?",
            (DEFAULT_PREFIX, DEFAULT_UPTIME_MINUTES, gid)
        )
        r = await c.fetchone()
        prefix, ann_id, sub_id, sub_ping_id, hb_min, hb_ch, show_eta = (r if r else (DEFAULT_PREFIX, None, None, None, DEFAULT_UPTIME_MINUTES, None, 0))
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
        f"Heartbeat: {hb_label} • Channel: {ch(hb_ch)}",
        f"UTC ETA: {'on' if show_eta else 'off'}",
        f"Bosses: {boss_count} • Due now: {due} • -Nada: {nada}",
        f"Color overrides: {', '.join(overridden) if overridden else 'none'}",
        f"Last startup: {ts_to_utc(int(last_start)) if last_start and last_start.isdigit() else '—'}",
    ]
    await ctx.send("\n".join(lines))

@bot.command(name="health")
@commands.has_permissions(administrator=True)
async def health_cmd(ctx):
    if not await ensure_guild_auth(ctx.guild):
        return
    required = {"bosses","guild_config","meta","category_colors","subscription_emojis","subscription_members","boss_aliases","category_channels","user_timer_prefs","subscription_panels","rr_panels","rr_map","blacklist","market_channels","market_listings","market_offers","lixing_posts"}
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT name FROM sqlite_master WHERE type='table'")
        present = {row[0] for row in await c.fetchall()}
        c = await db.execute("SELECT COUNT(*) FROM guild_config WHERE guild_id=?", (ctx.guild.id,))
        cfg_rows = (await c.fetchone())[0]
    missing = sorted(list(required - present))
    tick_age = now_ts() - _last_timer_tick_ts if _last_timer_tick_ts else None
    lines = [
        "**Health**",
        f"DB: `{DB_PATH}`",
        f"Tables OK: {'yes' if not missing else 'no'}{'' if not missing else ' (missing: ' + ', '.join(missing) + ')'}",
        f"Timers loop: {'running' if 'timers_tick' in globals() and timers_tick.is_running() else 'stopped'}",
        f"Heartbeat loop: {'running' if 'uptime_heartbeat' in globals() and uptime_heartbeat.is_running() else 'stopped'}",
        f"Last timer tick: {ts_to_utc(_last_timer_tick_ts) if _last_timer_tick_ts else '—'}"
        + (f" ({human_ago(tick_age)})" if tick_age is not None else ""),
        f"guild_config row present: {'yes' if cfg_rows > 0 else 'no'}",
    ]
    await ctx.send("\n".join(lines))

# -------------------- SETTINGS (prefix/announce/eta/heartbeat/subchannels) --------------------
@bot.command(name="setprefix")
@commands.has_permissions(manage_guild=True)
async def setprefix_cmd(ctx, new_prefix: str):
    if not await ensure_guild_auth(ctx.guild):
        return
    if not new_prefix or len(new_prefix) > 5:
        return await ctx.send("Pick a prefix 1–5 characters.")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO guild_config (guild_id,prefix) VALUES (?,?) "
            "ON CONFLICT(guild_id) DO UPDATE SET prefix=excluded.prefix",
            (ctx.guild.id, new_prefix)
        ); await db.commit()
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
@commands.has_permissions(manage_guild=True)
async def setannounce_cmd(ctx, *args):
    if not await ensure_guild_auth(ctx.guild):
        return
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
            ); await db.commit()
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
                ); await db.commit()
            return await ctx.send(f":white_check_mark: **{catn}** reminders → <#{ch_id}>.")
        else:
            if len(args) < 2: return await ctx.send('Format: `!setannounce categoryclear "<Category>"`')
            cat = " ".join(args[1:]).strip().strip('"')
            catn = norm_cat(cat)
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute("DELETE FROM category_channels WHERE guild_id=? AND category=?", (ctx.guild.id, catn))
                await db.commit()
            return await ctx.send(f":white_check_mark: Cleared category channel for **{catn}**.")
    return await ctx.send("Usage: `!setannounce #chan` | `!setannounce global #chan` | `!setannounce category \"<Category>\" #chan` | `!setannounce categoryclear \"<Category>\"`")

@bot.command(name="seteta")
@commands.has_permissions(manage_guild=True)
async def seteta_cmd(ctx, state: str):
    if not await ensure_guild_auth(ctx.guild):
        return
    val = state.strip().lower()
    if val not in {"on","off","true","false","1","0","yes","no"}:
        return await ctx.send("Use `!seteta on` or `!seteta off`.")
    on = val in {"on","true","1","yes"}
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO guild_config (guild_id,show_eta) VALUES (?,?) "
            "ON CONFLICT(guild_id) DO UPDATE SET show_eta=excluded.show_eta",
            (ctx.guild.id, 1 if on else 0)
        ); await db.commit()
    await ctx.send(f":white_check_mark: UTC ETA display {'enabled' if on else 'disabled'}.")

@bot.command(name="setuptime")
@commands.has_permissions(manage_guild=True)
async def setuptime_cmd(ctx, minutes: int):
    if not await ensure_guild_auth(ctx.guild):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO guild_config (guild_id,uptime_minutes) VALUES (?,?) "
            "ON CONFLICT(guild_id) DO UPDATE SET uptime_minutes=excluded.uptime_minutes",
            (ctx.guild.id, max(-1, int(minutes)))
        ); await db.commit()
    await ctx.send(":white_check_mark: Uptime heartbeat disabled." if minutes <= 0
                   else f":white_check_mark: Uptime heartbeat set to every {minutes} minutes.")

@bot.command(name="setheartbeatchannel")
@commands.has_permissions(manage_guild=True)
async def setheartbeatchannel_cmd(ctx, channel: discord.TextChannel):
    if not await ensure_guild_auth(ctx.guild):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO guild_config (guild_id,heartbeat_channel_id) VALUES (?,?) "
            "ON CONFLICT(guild_id) DO UPDATE SET heartbeat_channel_id=excluded.heartbeat_channel_id",
            (ctx.guild.id, channel.id)
        ); await db.commit()
    await ctx.send(f":white_check_mark: Heartbeat channel set to {channel.mention}.")

@bot.command(name="setsubchannel")
@commands.has_permissions(manage_guild=True)
async def setsubchannel_cmd(ctx, channel: discord.TextChannel):
    if not await ensure_guild_auth(ctx.guild):
        return
    # We do NOT delete existing panels anymore globally to avoid churn; we will rebuild in-place in the new channel.
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO guild_config (guild_id,sub_channel_id) VALUES (?,?) "
            "ON CONFLICT(guild_id) DO UPDATE SET sub_channel_id=excluded.sub_channel_id",
            (ctx.guild.id, channel.id)
        ); await db.commit()
    await ctx.send(f":white_check_mark: Subscription **panels** channel set to {channel.mention}. Rebuilding panels…")
    await refresh_subscription_messages(ctx.guild)
    await ctx.send(":white_check_mark: Subscription panels are ready.")

@bot.command(name="setsubpingchannel")
@commands.has_permissions(manage_guild=True)
async def setsubpingchannel_cmd(ctx, channel: discord.TextChannel):
    if not await ensure_guild_auth(ctx.guild):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO guild_config (guild_id,sub_ping_channel_id) VALUES (?,?) "
            "ON CONFLICT(guild_id) DO UPDATE SET sub_ping_channel_id=excluded.sub_ping_channel_id",
            (ctx.guild.id, channel.id)
        ); await db.commit()
    await ctx.send(f":white_check_mark: Subscription **ping** channel set to {channel.mention}.")

@bot.command(name="showsubscriptions")
async def showsubscriptions_cmd(ctx):
    if not await ensure_guild_auth(ctx.guild):
        return
    await refresh_subscription_messages(ctx.guild)
    await ctx.send(":white_check_mark: Subscription panels refreshed (one per category).")

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

@bot.command(name="setcatcolor")
@commands.has_permissions(manage_guild=True)
async def setcatcolor_cmd(ctx, category: str, hexcolor: str):
    if not await ensure_guild_auth(ctx.guild):
        return
    cat = norm_cat(category)
    h = hexcolor.strip().lstrip("#")
    try:
        int(h, 16)
    except Exception:
        return await ctx.send("Provide a valid hex color like `#1abc9c`.")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO category_colors (guild_id,category,color_hex) VALUES (?,?,?) "
            "ON CONFLICT(guild_id,category) DO UPDATE SET color_hex=excluded.color_hex",
            (ctx.guild.id, cat, f"#{h}")
        ); await db.commit()
    await ctx.send(f":white_check_mark: Color for **{cat}** set to `#{h}`.")

# -------------------- BOSS COMMAND GROUP --------------------
@bot.group(name="boss", invoke_without_command=True)
async def boss_group(ctx):
    if not await ensure_guild_auth(ctx.guild):
        return
    p = await get_guild_prefix(bot, ctx.message)
    await ctx.send(f"Use `{p}help` for commands.")

@boss_group.command(name="add")
async def boss_add(ctx, *args):
    if not await ensure_guild_auth(ctx.guild):
        return
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
        if name is None and tokens: name = tokens.pop(0)
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
        ); await db.commit()
    await ctx.send(f":white_check_mark: Added **{name}** — every {spawn_minutes}m, window {window_minutes}m, pre {pre_min}m, cat {category}.")
    await refresh_subscription_messages(ctx.guild)

@boss_group.command(name="idleall")
@commands.has_permissions(manage_guild=True)
async def boss_idleall(ctx):
    if not await ensure_guild_auth(ctx.guild):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE bosses SET next_spawn_ts=? WHERE guild_id=?", (now_ts() - 3601, ctx.guild.id))
        await db.commit()
    await ctx.send(":white_check_mark: All timers set to **-Nada**.")
    await refresh_subscription_messages(ctx.guild)

@boss_group.command(name="nada")
@commands.has_permissions(manage_guild=True)
async def boss_nada(ctx, *, name: str):
    if not await ensure_guild_auth(ctx.guild):
        return
    res, err = await resolve_boss(ctx, name)
    if err: return await ctx.send(f":no_entry: {err}")
    bid, nm, _ = res
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE bosses SET next_spawn_ts=? WHERE id=? AND guild_id=?", (now_ts() - 3601, bid, ctx.guild.id))
        await db.commit()
    await ctx.send(f":pause_button: **{nm}** set to **-Nada**.")
    await refresh_subscription_messages(ctx.guild)

@boss_group.command(name="nadaall")
@commands.has_permissions(manage_guild=True)
async def boss_nadaall(ctx):
    if not await ensure_guild_auth(ctx.guild):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE bosses SET next_spawn_ts=? WHERE guild_id=?", (now_ts() - 3601, ctx.guild.id))
        await db.commit()
    await ctx.send(":pause_button: **All bosses** set to **-Nada**.")
    await refresh_subscription_messages(ctx.guild)

@boss_group.command(name="info")
async def boss_info(ctx, *, name: str):
    if not await ensure_guild_auth(ctx.guild):
        return
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
    when_small = datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime('%H:%M UTC')
    line1 = f"**{name}**\nCategory: {cat} | Sort: {sort_key or '(none)'}\n"
    line2 = f"Respawn: {spawn_m}m | Window: {window_m}m\n"
    line3 = f"Spawn Time: `{fmt_delta_for_list(left)}`"
    line3b = f"\n> *ETA {when_small}*" if left > 0 else ""
    line4 = f"\nPre: {pre}m | Channel: {f'<#{ch_id}>' if ch_id else 'Default/Category'} | Role: {f'<@&{role_id}>' if role_id else 'None'}"
    await ctx.send(line1 + line2 + line3 + line3b + line4)

@boss_group.command(name="killed")
async def boss_killed(ctx, *, name: str):
    if not await ensure_guild_auth(ctx.guild):
        return
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
async def boss_increase(ctx, name: str, minutes: int):
    if not await ensure_guild_auth(ctx.guild):
        return
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
async def boss_reduce(ctx, name: str, minutes: int):
    if not await ensure_guild_auth(ctx.guild):
        return
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
@commands.has_permissions(manage_guild=True)
async def boss_edit(ctx, name: str, field: str, value: str):
    if not await ensure_guild_auth(ctx.guild):
        return
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
@commands.has_permissions(manage_guild=True)
async def boss_delete(ctx, *, name: str):
    if not await ensure_guild_auth(ctx.guild):
        return
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
async def boss_setcategory(ctx, *, args: str):
    if not await ensure_guild_auth(ctx.guild):
        return
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
async def boss_setsort(ctx, name: str, sort_key: str):
    if not await ensure_guild_auth(ctx.guild):
        return
    res, err = await resolve_boss(ctx, name)
    if err: return await ctx.send(f":no_entry: {err}")
    bid, nm, _ = res
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE bosses SET sort_key=? WHERE id=? AND guild_id=?", (sort_key, bid, ctx.guild.id))
        await db.commit()
    await ctx.send(f":1234: Sort key for **{nm}** set to `{sort_key}`.")
    await refresh_subscription_messages(ctx.guild)

@boss_group.command(name="setchannel")
async def boss_setchannel(ctx, name: str, channel: discord.TextChannel):
    if not await ensure_guild_auth(ctx.guild):
        return
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
@commands.has_permissions(manage_guild=True)
async def boss_setchannelall(ctx, channel: discord.TextChannel):
    if not await ensure_guild_auth(ctx.guild):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE bosses SET channel_id=? WHERE guild_id=?", (channel.id, ctx.guild.id))
        await db.commit()
    await ctx.send(f":satellite: All boss reminders → {channel.mention}.")

@boss_group.command(name="setchannelcat")
@commands.has_permissions(manage_guild=True)
async def boss_setchannelcat(ctx, *, args: str):
    if not await ensure_guild_auth(ctx.guild):
        return
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
@commands.has_permissions(manage_guild=True)
async def boss_setrole(ctx, *args):
    if not await ensure_guild_auth(ctx.guild):
        return
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
@commands.has_permissions(manage_guild=True)
async def boss_alias(ctx, action: str = None, *, args: str = ""):
    if not await ensure_guild_auth(ctx.guild):
        return
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
async def boss_find(ctx, *, ident: str):
    if not await ensure_guild_auth(ctx.guild):
        return
    res, err = await resolve_boss(ctx, ident)
    if err: return await ctx.send(f":no_entry: {err}")
    bid, nm, _ = res
    await ctx.send(f"Matched: **{nm}**")

# -------------------- BLACKLIST COMMANDS (hidden from help) --------------------
@bot.group(name="blacklist", invoke_without_command=True)
@commands.has_permissions(manage_guild=True)
async def blacklist_group(ctx):
    if not await ensure_guild_auth(ctx.guild):
        return
    await ctx.send("Use `!blacklist add @user` / `!blacklist remove @user` / `!blacklist show`")

@blacklist_group.command(name="add")
@commands.has_permissions(manage_guild=True)
async def blacklist_add(ctx, user: discord.Member):
    if not await ensure_guild_auth(ctx.guild):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR IGNORE INTO blacklist (guild_id,user_id) VALUES (?,?)", (ctx.guild.id, user.id))
        await db.commit()
    await ctx.send(f":no_entry: **{user.display_name}** is now blacklisted.")

@blacklist_group.command(name="remove")
@commands.has_permissions(manage_guild=True)
async def blacklist_remove(ctx, user: discord.Member):
    if not await ensure_guild_auth(ctx.guild):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM blacklist WHERE guild_id=? AND user_id=?", (ctx.guild.id, user.id))
        await db.commit()
    await ctx.send(f":white_check_mark: **{user.display_name}** removed from blacklist.")

@blacklist_group.command(name="show")
@commands.has_permissions(manage_guild=True)
async def blacklist_show(ctx):
    if not await ensure_guild_auth(ctx.guild):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT user_id FROM blacklist WHERE guild_id=?", (ctx.guild.id,))
        rows = await c.fetchall()
    if not rows: return await ctx.send("No users blacklisted.")
    mentions = " ".join(f"<@{r[0]}>" for r in rows)
    await ctx.send(f"Blacklisted: {mentions}")

# -------------------- AUTH ADMIN (hidden, not in help) --------------------
@bot.command(name="setauthuser")
@commands.has_permissions(administrator=True)
async def setauthuser_cmd(ctx, user: discord.Member):
    # intentionally not in help; admin safeguard
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO guild_config (guild_id, auth_user_id) VALUES (?, ?) "
            "ON CONFLICT(guild_id) DO UPDATE SET auth_user_id=excluded.auth_user_id",
            (ctx.guild.id, int(user.id))
        )
        await db.commit()
    # clear cache for this guild
    _auth_cache.pop(ctx.guild.id, None)
    await ctx.send(f":white_check_mark: Auth anchor set to **{user.mention}** for this guild.")

@bot.command(name="clearauthuser")
@commands.has_permissions(administrator=True)
async def clearauthuser_cmd(ctx):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE guild_config SET auth_user_id=NULL WHERE guild_id=?", (ctx.guild.id,))
        await db.commit()
    _auth_cache.pop(ctx.guild.id, None)
    await ctx.send(":white_check_mark: Cleared guild auth anchor (env/user search fallback will be used).")
# ======================================================
# SECTION 3 / 4 — Timers UI (cmd + slash), Market & Lixing
# ======================================================

# -------------------- TIMERS VIEW (with minute window states) --------------------
async def get_show_eta(guild_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT COALESCE(show_eta,0) FROM guild_config WHERE guild_id=?", (guild_id,))
        r = await c.fetchone()
        return bool(r and int(r[0]) == 1)

def window_label(now: int, next_ts: int, window_m: int) -> str:
    """
    States:
      - future: "<window_m>m (pending)"
      - open:   "<Xm> left (open)"  [remaining window minutes]
      - closed: "closed"            [between window close and NADA_GRACE_SECONDS]
      - lost:   "-Nada"             [after grace expires]
    """
    delta = next_ts - now
    if delta >= 0:
        return f"{window_m}m (pending)"
    open_secs = -delta
    if open_secs <= window_m * 60:
        left_m = max(0, (window_m * 60 - open_secs) // 60)
        return f"{left_m}m left (open)"
    # past window:
    after_close = open_secs - window_m * 60
    if after_close <= NADA_GRACE_SECONDS:
        return "closed"
    return "-Nada"

@bot.command(name="timers")
async def timers_cmd(ctx):
    if not await ensure_guild_auth(ctx.guild):
        return
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
            eta_line = f"\n> *ETA {datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%H:%M UTC')}*" if show_eta and (ts - now) > 0 else ""
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

# ---------- /timers (per-user, remembers categories) ----------
async def build_timer_embeds_for_categories(guild: discord.Guild, categories: List[str]) -> List[discord.Embed]:
    gid = guild.id
    show_eta = await get_show_eta(gid)
    if not categories:
        return []
    async with aiosqlite.connect(DB_PATH) as db:
        q_marks = ",".join("?" for _ in categories)
        c = await db.execute(
            f"SELECT name,next_spawn_ts,category,sort_key,window_minutes FROM bosses WHERE guild_id=? AND category IN ({q_marks})",
            (gid, *[norm_cat(c) for c in categories])
        )
        rows = await c.fetchall()
    now = now_ts()
    grouped: Dict[str, List[tuple]] = {k: [] for k in categories}
    for name, ts, cat, sk, win in rows:
        nc = norm_cat(cat)
        if nc in grouped:
            grouped[nc].append((sk or "", name, int(ts), int(win)))
    embeds: List[discord.Embed] = []
    for cat in categories:
        items = grouped.get(cat, [])
        items.sort(key=lambda x: (natural_key(x[0]), natural_key(x[1])))
        normal: List[tuple] = []; nada_list: List[tuple] = []
        for sk, nm, ts, win in items:
            delta = ts - now; t = fmt_delta_for_list(delta)
            (nada_list if t == "-Nada" else normal).append((sk, nm, t, ts, win))
        blocks: List[str] = []
        for sk, nm, t, ts, win_m in normal:
            win_status = window_label(now, ts, win_m)
            line1 = f"〔 **{nm}** • Spawn: `{t}` • Window: `{win_status}` 〕"
            eta_line = f"\n> *ETA {datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%H:%M UTC')}*" if show_eta and (ts - now) > 0 else ""
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
        embeds.append(em)
    return embeds[:10]

class TimerToggleView(discord.ui.View):
    def __init__(self, guild: discord.Guild, user_id: int, init_show: List[str]):
        super().__init__(timeout=300)
        self.guild = guild
        self.user_id = user_id
        self.shown = [c for c in CATEGORY_ORDER if c in init_show] or CATEGORY_ORDER[:]  # default to all
        for idx, cat in enumerate(CATEGORY_ORDER):
            self.add_item(self._make_toggle_button(cat, idx))
        self.add_item(self._make_all_button())
        self.add_item(self._make_none_button())
        self.message = None

    def _make_toggle_button(self, cat: str, idx: int):
        return ToggleButton(label=cat, style=discord.ButtonStyle.primary, cat=cat, row=min(4, idx // 3))

    def _make_all_button(self):
        return ControlButton(label="Show All", style=discord.ButtonStyle.success, action="all", row=4)

    def _make_none_button(self):
        return ControlButton(label="Hide All", style=discord.ButtonStyle.danger, action="none", row=4)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("This panel isn't yours — run `/timers` to get your own.", ephemeral=True)
            return False
        return True

    async def persist(self):
        await set_user_shown_categories(self.guild.id, self.user_id, self.shown)

    async def refresh(self, interaction: discord.Interaction):
        embeds = await build_timer_embeds_for_categories(self.guild, self.shown)
        content = f"**Categories shown:** {', '.join(self.shown) if self.shown else '(none)'}"
        await self.persist()
        if interaction.response.is_done():
            await interaction.edit_original_response(content=content, embeds=embeds, view=self)
        else:
            await interaction.response.edit_message(content=content, embeds=embeds, view=self)

class ToggleButton(discord.ui.Button):
    def __init__(self, label: str, style: discord.ButtonStyle, cat: str, row: int):
        super().__init__(label=label, style=style, row=row)
        self.cat = cat

    async def callback(self, interaction: discord.Interaction):
        view: TimerToggleView = self.view  # type: ignore
        if self.cat in view.shown:
            view.shown.remove(self.cat)
        else:
            ordered = [c for c in CATEGORY_ORDER if c in (view.shown + [self.cat])]
            view.shown = ordered
        await view.refresh(interaction)

class ControlButton(discord.ui.Button):
    def __init__(self, label: str, style: discord.ButtonStyle, action: str, row: int):
        super().__init__(label=label, style=style, row=row)
        self.action = action

    async def callback(self, interaction: discord.Interaction):
        view: TimerToggleView = self.view  # type: ignore
        if self.action == "all":
            view.shown = [c for c in CATEGORY_ORDER]
        else:
            view.shown = []
        await view.refresh(interaction)

@app_commands.guild_only()
@bot.tree.command(name="timers", description="Show timers with per-category toggles (ephemeral, remembers your selection)")
async def slash_timers(interaction: discord.Interaction):
    if not interaction.guild or not await ensure_guild_auth(interaction.guild):
        return await interaction.response.send_message("Use this in an authorized server.", ephemeral=True)
    saved = await get_user_shown_categories(interaction.guild.id, interaction.user.id)
    view = TimerToggleView(guild=interaction.guild, user_id=interaction.user.id, init_show=saved)
    embeds = await build_timer_embeds_for_categories(interaction.guild, view.shown)
    await interaction.response.send_message(
        content=f"**Categories shown:** {', '.join(view.shown) if view.shown else '(none)'}",
        embeds=embeds,
        view=view,
        ephemeral=True
    )
    view.message = await interaction.original_response()

# ---------------------------------------------------------
# MARKET: Listings + Offers (slash UI + buttons/modals)
# Tables (created in migrations):
#   market_channels(guild_id INTEGER PRIMARY KEY, market_channel_id INTEGER, lixing_channel_id INTEGER, digest_channel_id INTEGER)
#   market_listings(id INTEGER PK, guild_id, user_id, type TEXT('buy'|'sell'),
#                   item_name TEXT, qty INTEGER, price TEXT, trades_ok INTEGER, taking_offers INTEGER,
#                   notes TEXT, active INTEGER, created_ts INTEGER, updated_ts INTEGER, message_id INTEGER)
#   market_offers(id INTEGER PK, listing_id, offer_user_id, offer_text TEXT, created_ts INTEGER)
#   lixing_posts(id INTEGER PK, guild_id, user_id, char_name TEXT, class_name TEXT, level TEXT,
#                desired_lixes TEXT, active INTEGER, created_ts INTEGER, updated_ts INTEGER, message_id INTEGER)
# ---------------------------------------------------------

# ----------- Utilities for market/lixing embeds -----------
def _yn(v: int) -> str:
    return "Yes" if int(v) else "No"

async def market_get_channels(guild_id: int) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT market_channel_id, lixing_channel_id, digest_channel_id FROM market_channels WHERE guild_id=?", (guild_id,))
        r = await c.fetchone()
        if not r:
            return (None, None, None)
        return (r[0], r[1], r[2])

async def market_set_channel(guild_id: int, field: str, channel_id: int):
    valid = {"market_channel_id","lixing_channel_id","digest_channel_id"}
    if field not in valid: return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO market_channels (guild_id, market_channel_id, lixing_channel_id, digest_channel_id) "
            "VALUES (?, NULL, NULL, NULL) ON CONFLICT(guild_id) DO NOTHING", (guild_id,)
        )
        await db.execute(f"UPDATE market_channels SET {field}=? WHERE guild_id=?", (channel_id, guild_id))
        await db.commit()

def embed_market_listing(l: dict, offers: List[dict] = None) -> discord.Embed:
    offers = offers or []
    title = f"{'🛒 BUY' if l['type']=='buy' else '🏷️ SELL'} — {l['item_name']}"
    em = discord.Embed(
        title=title,
        description=(l.get("notes") or "").strip()[:4000],
        color=0x1abc9c if l['type']=='buy' else 0xf39c12
    )
    em.add_field(name="Quantity", value=str(l['qty']), inline=True)
    em.add_field(name="Price/Range", value=l['price'] or "—", inline=True)
    em.add_field(name="Trades Accepted", value=_yn(l['trades_ok']), inline=True)
    em.add_field(name="Taking Offers", value=_yn(l['taking_offers']), inline=True)
    em.add_field(name="Active", value=_yn(l['active']), inline=True)
    em.add_field(name="Listing ID", value=str(l['id']), inline=True)
    em.set_footer(text=f"By {l.get('author_tag','Unknown')} • Updated {ts_to_utc(l['updated_ts'])}")
    if offers:
        # summary line of most recent offers
        latest = "\n".join([f"• <@{o['offer_user_id']}>: {o['offer_text'][:120]}" for o in offers[-5:]])
        em.add_field(name=f"Offers ({len(offers)})", value=latest[:1024] or "—", inline=False)
    return em

def embed_lixing_post(x: dict) -> discord.Embed:
    em = discord.Embed(
        title=f"⚔️ Lixing — {x['char_name']}",
        description=(f"Class: **{x['class_name']}** • Level: **{x['level']}**\n"
                     f"Desired lixes: **{x['desired_lixes'] or 'N/A'}**"),
        color=0x7289DA
    )
    em.add_field(name="Active", value=_yn(x['active']), inline=True)
    em.add_field(name="Post ID", value=str(x['id']), inline=True)
    em.set_footer(text=f"By {x.get('author_tag','Unknown')} • Updated {ts_to_utc(x['updated_ts'])}")
    return em

# ----------- Market Offer modal & button -----------
class OfferModal(discord.ui.Modal, title="Make an Offer"):
    offer_text = discord.ui.TextInput(label="Your offer", style=discord.TextStyle.paragraph, max_length=500)

    def __init__(self, listing_id: int, owner_id: int, channel_id: int):
        super().__init__()
        self.listing_id = listing_id
        self.owner_id = owner_id
        self.channel_id = channel_id

    async def on_submit(self, interaction: discord.Interaction):
        if not interaction.guild or not await ensure_guild_auth(interaction.guild):
            return await interaction.response.send_message("Unauthorized server.", ephemeral=True)
        txt = str(self.offer_text.value).strip()
        if not txt:
            return await interaction.response.send_message("Offer cannot be empty.", ephemeral=True)
        nowi = now_ts()
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT INTO market_offers (listing_id, offer_user_id, offer_text, created_ts) VALUES (?,?,?,?)",
                (self.listing_id, interaction.user.id, txt, nowi)
            )
            await db.commit()

        # Notify owner & publish offer underneath listing channel
        ch = interaction.guild.get_channel(self.channel_id)
        mention_owner = f"<@{self.owner_id}>"
        if ch and can_send(ch):
            try:
                await ch.send(f"💬 New offer on listing **#{self.listing_id}** from {interaction.user.mention} to {mention_owner}:\n> {txt[:1800]}")
            except Exception:
                pass
        try:
            await interaction.response.send_message("✅ Offer submitted!", ephemeral=True)
        except Exception:
            pass

class OfferButton(discord.ui.Button):
    def __init__(self, listing_id: int, owner_id: int, channel_id: int, disabled: bool):
        super().__init__(label="Make Offer", style=discord.ButtonStyle.primary, disabled=disabled)
        self.listing_id = listing_id
        self.owner_id = owner_id
        self.channel_id = channel_id

    async def callback(self, interaction: discord.Interaction):
        # Anyone can offer if listing is active
        async with aiosqlite.connect(DB_PATH) as db:
            c = await db.execute("SELECT taking_offers, active FROM market_listings WHERE id=?", (self.listing_id,))
            row = await c.fetchone()
        if not row:
            return await interaction.response.send_message("Listing not found.", ephemeral=True)
        taking_offers, active = int(row[0]), int(row[1])
        if not active:
            return await interaction.response.send_message("This listing is closed.", ephemeral=True)
        if not taking_offers:
            return await interaction.response.send_message("This listing is not taking offers.", ephemeral=True)
        await interaction.response.send_modal(OfferModal(self.listing_id, self.owner_id, self.channel_id))

class ListingView(discord.ui.View):
    def __init__(self, listing_id: int, owner_id: int, channel_id: int, taking_offers: int, active: int):
        super().__init__(timeout=None)
        self.add_item(OfferButton(listing_id, owner_id, channel_id, disabled=(not taking_offers or not active)))

# -------------------- /market slash group --------------------
market_group = app_commands.Group(name="market", description="Buy/Sell listings & offers")

@market_group.command(name="add", description="Add a buy/sell listing")
@app_commands.describe(
    type="buy or sell",
    item="Item name",
    quantity="Quantity (number)",
    price="Price or range (text)",
    trades_ok="Accept trades? true/false",
    taking_offers="Taking offers/bids? true/false",
    notes="Optional notes"
)
async def market_add(interaction: discord.Interaction,
                     type: str,
                     item: str,
                     quantity: int,
                     price: str,
                     trades_ok: bool,
                     taking_offers: bool,
                     notes: Optional[str] = None):
    if not interaction.guild or not await ensure_guild_auth(interaction.guild):
        return await interaction.response.send_message("Unauthorized server.", ephemeral=True)
    t = type.strip().lower()
    if t not in {"buy","sell"}:
        return await interaction.response.send_message("Type must be `buy` or `sell`.", ephemeral=True)
    qty = max(1, int(quantity))
    nowi = now_ts()
    (market_ch_id, _lx, _dg) = await market_get_channels(interaction.guild.id)
    ch = interaction.guild.get_channel(market_ch_id) if market_ch_id else interaction.channel
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO market_listings (guild_id,user_id,type,item_name,qty,price,trades_ok,taking_offers,notes,active,created_ts,updated_ts,message_id) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?, ?, NULL)",
            (interaction.guild.id, interaction.user.id, t, item.strip(), qty, price.strip(), 1 if trades_ok else 0,
             1 if taking_offers else 0, (notes or "").strip(), 1, nowi, nowi)
        )
        await db.commit()
        c = await db.execute("SELECT last_insert_rowid()")
        listing_id = int((await c.fetchone())[0])

    # Compose and send listing
    listing = {
        "id": listing_id, "type": t, "item_name": item.strip(), "qty": qty, "price": price.strip(),
        "trades_ok": 1 if trades_ok else 0, "taking_offers": 1 if taking_offers else 0,
        "notes": (notes or "").strip(), "active": 1, "updated_ts": nowi, "author_tag": str(interaction.user)
    }
    em = embed_market_listing(listing, [])
    target_ch = ch if can_send(ch) else interaction.channel
    msg = await target_ch.send(embed=em, view=ListingView(listing_id, interaction.user.id, target_ch.id, listing["taking_offers"], listing["active"]))
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE market_listings SET message_id=? WHERE id=?", (msg.id, listing_id))
        await db.commit()
    await interaction.response.send_message(f"✅ Listing **#{listing_id}** posted in {target_ch.mention}.", ephemeral=True)

@market_group.command(name="browse", description="Browse listings")
@app_commands.describe(
    type="Filter to buy or sell",
    query="Search item names (optional)"
)
async def market_browse(interaction: discord.Interaction, type: Optional[str] = None, query: Optional[str] = None):
    if not interaction.guild or not await ensure_guild_auth(interaction.guild):
        return await interaction.response.send_message("Unauthorized server.", ephemeral=True)
    t = (type or "").strip().lower()
    if t and t not in {"buy","sell"}:
        return await interaction.response.send_message("Type must be `buy`, `sell`, or blank.", ephemeral=True)
    q = (query or "").strip().lower()
    async with aiosqlite.connect(DB_PATH) as db:
        if t and q:
            c = await db.execute(
                "SELECT id,user_id,type,item_name,qty,price,trades_ok,taking_offers,notes,active,updated_ts FROM market_listings "
                "WHERE guild_id=? AND active=1 AND type=? AND LOWER(item_name) LIKE ? "
                "ORDER BY updated_ts DESC LIMIT 10",
                (interaction.guild.id, t, f"%{q}%")
            )
        elif t:
            c = await db.execute(
                "SELECT id,user_id,type,item_name,qty,price,trades_ok,taking_offers,notes,active,updated_ts FROM market_listings "
                "WHERE guild_id=? AND active=1 AND type=? "
                "ORDER BY updated_ts DESC LIMIT 10",
                (interaction.guild.id, t)
            )
        elif q:
            c = await db.execute(
                "SELECT id,user_id,type,item_name,qty,price,trades_ok,taking_offers,notes,active,updated_ts FROM market_listings "
                "WHERE guild_id=? AND active=1 AND LOWER(item_name) LIKE ? "
                "ORDER BY updated_ts DESC LIMIT 10",
                (interaction.guild.id, f"%{q}%")
            )
        else:
            c = await db.execute(
                "SELECT id,user_id,type,item_name,qty,price,trades_ok,taking_offers,notes,active,updated_ts FROM market_listings "
                "WHERE guild_id=? AND active=1 ORDER BY updated_ts DESC LIMIT 10",
                (interaction.guild.id,)
            )
        rows = await c.fetchall()
    if not rows:
        return await interaction.response.send_message("No listings found.", ephemeral=True)

    for r in rows:
        l = {
            "id": r[0], "author_tag": "", "type": r[2], "item_name": r[3], "qty": r[4], "price": r[5],
            "trades_ok": r[6], "taking_offers": r[7], "notes": r[8], "active": r[9], "updated_ts": r[10]
        }
        # fetch latest offers
        async with aiosqlite.connect(DB_PATH) as db:
            c = await db.execute("SELECT offer_user_id, offer_text FROM market_offers WHERE listing_id=? ORDER BY created_ts ASC", (l["id"],))
            offer_rows = await c.fetchall()
        offers = [{"offer_user_id": u, "offer_text": t} for (u, t) in offer_rows]
        await interaction.channel.send(embed=embed_market_listing(l, offers),
                                       view=ListingView(l["id"], 0, interaction.channel.id, l["taking_offers"], l["active"]))
    await interaction.response.send_message("📋 Sent top results below.", ephemeral=True)

@market_group.command(name="view", description="Show a single listing by ID")
async def market_view(interaction: discord.Interaction, listing_id: int):
    if not interaction.guild or not await ensure_guild_auth(interaction.guild):
        return await interaction.response.send_message("Unauthorized server.", ephemeral=True)
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT id,user_id,type,item_name,qty,price,trades_ok,taking_offers,notes,active,updated_ts,message_id "
                             "FROM market_listings WHERE guild_id=? AND id=?", (interaction.guild.id, listing_id))
        r = await c.fetchone()
    if not r:
        return await interaction.response.send_message("Listing not found.", ephemeral=True)
    listing = {
        "id": r[0], "owner_id": r[1], "type": r[2], "item_name": r[3], "qty": r[4], "price": r[5],
        "trades_ok": r[6], "taking_offers": r[7], "notes": r[8], "active": r[9], "updated_ts": r[10]
    }
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT offer_user_id, offer_text FROM market_offers WHERE listing_id=? ORDER BY created_ts ASC", (listing_id,))
        offer_rows = await c.fetchall()
    offers = [{"offer_user_id": u, "offer_text": t} for (u, t) in offer_rows]
    em = embed_market_listing({**listing, "author_tag": str(interaction.user)}, offers)
    await interaction.response.send_message(embed=em,
        view=ListingView(listing_id, listing["owner_id"], interaction.channel.id, listing["taking_offers"], listing["active"]))

@market_group.command(name="close", description="Close your listing")
async def market_close(interaction: discord.Interaction, listing_id: int):
    if not interaction.guild or not await ensure_guild_auth(interaction.guild):
        return await interaction.response.send_message("Unauthorized server.", ephemeral=True)
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT user_id FROM market_listings WHERE guild_id=? AND id=?", (interaction.guild.id, listing_id))
        r = await c.fetchone()
        if not r:
            return await interaction.response.send_message("Listing not found.", ephemeral=True)
        if r[0] != interaction.user.id and not interaction.user.guild_permissions.manage_messages:
            return await interaction.response.send_message("You don't own this listing.", ephemeral=True)
        await db.execute("UPDATE market_listings SET active=0, updated_ts=? WHERE id=?", (now_ts(), listing_id))
        await db.commit()
    await interaction.response.send_message(f"✅ Listing **#{listing_id}** closed.", ephemeral=True)

@market_group.command(name="setchannel", description="Set channels for market/lixing/digest (Admin)")
@app_commands.describe(
    which="market | lixing | digest",
    channel="Target channel"
)
@commands.has_permissions(manage_guild=True)
async def market_setchannel(interaction: discord.Interaction, which: str, channel: discord.TextChannel):
    if not interaction.guild or not await ensure_guild_auth(interaction.guild):
        return await interaction.response.send_message("Unauthorized server.", ephemeral=True)
    w = which.strip().lower()
    field_map = {"market":"market_channel_id", "lixing":"lixing_channel_id", "digest":"digest_channel_id"}
    if w not in field_map:
        return await interaction.response.send_message("`which` must be one of: market, lixing, digest", ephemeral=True)
    await market_set_channel(interaction.guild.id, field_map[w], channel.id)
    await interaction.response.send_message(f"✅ Set **{w}** channel to {channel.mention}.", ephemeral=True)

# Register group
bot.tree.add_command(market_group)

# -------------------- /lixing slash group --------------------
lixing_group = app_commands.Group(name="lixing", description="Lixing/leveling group finder")

@lixing_group.command(name="post", description="Post your lixing/leveling request")
@app_commands.describe(
    character="Character name",
    klass="Class",
    level="Level (text or number)",
    desired="Desired lixes (number or N/A)"
)
async def lixing_post(interaction: discord.Interaction,
                      character: str,
                      klass: str,
                      level: str,
                      desired: Optional[str] = None):
    if not interaction.guild or not await ensure_guild_auth(interaction.guild):
        return await interaction.response.send_message("Unauthorized server.", ephemeral=True)
    nowi = now_ts()
    (_m, lx_ch_id, _d) = await market_get_channels(interaction.guild.id)
    ch = interaction.guild.get_channel(lx_ch_id) if lx_ch_id else interaction.channel
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO lixing_posts (guild_id,user_id,char_name,class_name,level,desired_lixes,active,created_ts,updated_ts,message_id) "
            "VALUES (?,?,?,?,?,?,1,?,?, NULL)",
            (interaction.guild.id, interaction.user.id, character.strip(), klass.strip(), level.strip(), (desired or "N/A").strip(), nowi, nowi)
        )
        await db.commit()
        c = await db.execute("SELECT last_insert_rowid()")
        x_id = int((await c.fetchone())[0])

    post = {"id": x_id, "char_name": character.strip(), "class_name": klass.strip(), "level": level.strip(),
            "desired_lixes": (desired or "N/A").strip(), "active": 1, "updated_ts": nowi, "author_tag": str(interaction.user)}
    em = embed_lixing_post(post)
    target_ch = ch if can_send(ch) else interaction.channel
    msg = await target_ch.send(embed=em)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE lixing_posts SET message_id=? WHERE id=?", (msg.id, x_id))
        await db.commit()
    await interaction.response.send_message(f"✅ Lixing post **#{x_id}** published in {target_ch.mention}.", ephemeral=True)

@lixing_group.command(name="browse", description="Browse lixing posts")
@app_commands.describe(
    klass="Filter by class (optional)",
    level_contains="Filter by level substring (optional)"
)
async def lixing_browse(interaction: discord.Interaction,
                        klass: Optional[str] = None,
                        level_contains: Optional[str] = None):
    if not interaction.guild or not await ensure_guild_auth(interaction.guild):
        return await interaction.response.send_message("Unauthorized server.", ephemeral=True)
    kc = (klass or "").strip().lower()
    lc = (level_contains or "").strip().lower()
    async with aiosqlite.connect(DB_PATH) as db:
        if kc and lc:
            c = await db.execute(
                "SELECT id,user_id,char_name,class_name,level,desired_lixes,active,updated_ts FROM lixing_posts "
                "WHERE guild_id=? AND active=1 AND LOWER(class_name)=? AND LOWER(level) LIKE ? "
                "ORDER BY updated_ts DESC LIMIT 10",
                (interaction.guild.id, kc, f"%{lc}%")
            )
        elif kc:
            c = await db.execute(
                "SELECT id,user_id,char_name,class_name,level,desired_lixes,active,updated_ts FROM lixing_posts "
                "WHERE guild_id=? AND active=1 AND LOWER(class_name)=? "
                "ORDER BY updated_ts DESC LIMIT 10",
                (interaction.guild.id, kc)
            )
        elif lc:
            c = await db.execute(
                "SELECT id,user_id,char_name,class_name,level,desired_lixes,active,updated_ts FROM lixing_posts "
                "WHERE guild_id=? AND active=1 AND LOWER(level) LIKE ? "
                "ORDER BY updated_ts DESC LIMIT 10",
                (interaction.guild.id, f"%{lc}%")
            )
        else:
            c = await db.execute(
                "SELECT id,user_id,char_name,class_name,level,desired_lixes,active,updated_ts FROM lixing_posts "
                "WHERE guild_id=? AND active=1 ORDER BY updated_ts DESC LIMIT 10",
                (interaction.guild.id,)
            )
        rows = await c.fetchall()
    if not rows:
        return await interaction.response.send_message("No lixing posts found.", ephemeral=True)
    for r in rows:
        post = {"id": r[0], "char_name": r[2], "class_name": r[3], "level": r[4],
                "desired_lixes": r[5], "active": r[6], "updated_ts": r[7]}
        await interaction.channel.send(embed=embed_lixing_post(post))
    await interaction.response.send_message("📋 Sent top results below.", ephemeral=True)

@lixing_group.command(name="close", description="Close your lixing post")
async def lixing_close(interaction: discord.Interaction, post_id: int):
    if not interaction.guild or not await ensure_guild_auth(interaction.guild):
        return await interaction.response.send_message("Unauthorized server.", ephemeral=True)
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT user_id FROM lixing_posts WHERE guild_id=? AND id=?", (interaction.guild.id, post_id))
        r = await c.fetchone()
        if not r:
            return await interaction.response.send_message("Post not found.", ephemeral=True)
        if r[0] != interaction.user.id and not interaction.user.guild_permissions.manage_messages:
            return await interaction.response.send_message("You don't own this post.", ephemeral=True)
        await db.execute("UPDATE lixing_posts SET active=0, updated_ts=? WHERE id=?", (now_ts(), post_id))
        await db.commit()
    await interaction.response.send_message(f"✅ Lixing post **#{post_id}** closed.", ephemeral=True)

# Register group
bot.tree.add_command(lixing_group)
# ======================================================
# SECTION 4 / 4 — Loops, Events, Digests, Shutdown, Main
# ======================================================

# -------------------- RUNTIME LOOPS --------------------
@tasks.loop(seconds=CHECK_INTERVAL_SECONDS)
async def timers_tick():
    global _last_timer_tick_ts, _prev_timer_tick_ts
    now = now_ts()
    prev = _last_timer_tick_ts or (now - CHECK_INTERVAL_SECONDS)
    _prev_timer_tick_ts = prev
    _last_timer_tick_ts = now
    try: await meta_set("last_tick_ts", str(_last_timer_tick_ts))
    except Exception: pass

    # Pre announces
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute(
            "SELECT id,guild_id,channel_id,name,next_spawn_ts,pre_announce_min,category FROM bosses WHERE next_spawn_ts > ?",
            (now,)
        )
        for bid, gid, ch_id, name, next_ts, pre, cat in await c.fetchall():
            if not pre or pre <= 0: continue
            pre_ts = int(next_ts) - int(pre) * 60
            if prev < pre_ts <= now:
                key = f"{gid}:{bid}:PRE:{next_ts}"
                if key in bot._seen_keys: continue
                bot._seen_keys.add(key)
                ch = await resolve_announce_channel(gid, ch_id, cat)
                if ch and can_send(ch):
                    left = max(0, int(next_ts) - now)
                    try: await ch.send(f"⏳ **{name}** — **Spawn Time**: `{fmt_delta_for_list(left)}` (almost up).")
                    except Exception as e: log.warning(f"Pre announce failed: {e}")
                await send_subscription_ping(gid, bid, phase="pre", boss_name=name, when_left=max(0, int(next_ts) - now))

        # Window opens
        c = await db.execute(
            "SELECT id,guild_id,channel_id,name,next_spawn_ts,category FROM bosses WHERE next_spawn_ts <= ?",
            (now,)
        )
        for bid, gid, ch_id, name, next_ts, cat in await c.fetchall():
            if not (prev < int(next_ts) <= now): continue
            key = f"{gid}:{bid}:WINDOW:{next_ts}"
            if key in bot._seen_keys: continue
            bot._seen_keys.add(key)
            ch = await resolve_announce_channel(gid, ch_id, cat)
            if ch and can_send(ch):
                try: await ch.send(f"🕑 **{name}** — **Spawn Window has opened!**")
                except Exception as e: log.warning(f"Window announce failed: {e}")
            await send_subscription_ping(gid, bid, phase="window", boss_name=name)

@tasks.loop(minutes=1.0)
async def uptime_heartbeat():
    now_m = now_ts() // 60
    for g in bot.guilds:
        if not await ensure_guild_auth(g):  # silently skip unauthorized
            continue
        await upsert_guild_defaults(g.id)
        async with aiosqlite.connect(DB_PATH) as db:
            c = await db.execute("SELECT COALESCE(uptime_minutes, ?) FROM guild_config WHERE guild_id=?", (DEFAULT_UPTIME_MINUTES, g.id))
            r = await c.fetchone()
        minutes = int(r[0]) if r else DEFAULT_UPTIME_MINUTES
        if minutes <= 0 or now_m % minutes != 0: continue
        ch = await resolve_heartbeat_channel(g.id)
        if ch and can_send(ch):
            try: await ch.send("✅ Bot is online — timers active.")
            except Exception as e: log.warning(f"Heartbeat failed: {e}")

# -------------------- 6-hour DIGESTS (Market & Lixing) --------------------
def _digest_header(title: str) -> str:
    return f"**{title} — {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}**"

async def _send_market_digest(guild: discord.Guild):
    (market_ch_id, lixing_ch_id, digest_ch_id) = await market_get_channels(guild.id)
    target = guild.get_channel(digest_ch_id) if digest_ch_id else (guild.get_channel(market_ch_id) if market_ch_id else None)
    if not can_send(target): return
    # Collect active listings
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute(
            "SELECT type,item_name,qty,price,trades_ok,taking_offers,id FROM market_listings "
            "WHERE guild_id=? AND active=1 ORDER BY updated_ts DESC LIMIT 50",
            (guild.id,)
        )
        rows = await c.fetchall()
    if not rows:
        try: await target.send(_digest_header("Market Digest") + "\n_No active listings._")
        except Exception: pass
        return
    # Build lines grouped by type
    buys = []; sells = []
    for t,name,qty,price,trd,tk,idv in rows:
        line = f"• **{name}** ×{qty} — {price or '—'} • Trades:{'Y' if trd else 'N'} • Offers:{'Y' if tk else 'N'} • `#${idv}`"
        (buys if t=='buy' else sells).append(line)
    blob = [_digest_header("Market Digest")]
    if buys:
        blob.append("\n__Buying:__"); blob += buys[:20]
    if sells:
        blob.append("\n__Selling:__"); blob += sells[:20]
    text = "\n".join(blob)
    if len(text) > 1950: text = text[:1945] + "…"
    try: await target.send(text)
    except Exception: pass

async def _send_lixing_digest(guild: discord.Guild):
    (market_ch_id, lixing_ch_id, digest_ch_id) = await market_get_channels(guild.id)
    target = guild.get_channel(digest_ch_id) if digest_ch_id else (guild.get_channel(lixing_ch_id) if lixing_ch_id else None)
    if not can_send(target): return
    # Collect active posts
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute(
            "SELECT char_name,class_name,level,desired_lixes,id FROM lixing_posts "
            "WHERE guild_id=? AND active=1 ORDER BY updated_ts DESC LIMIT 50",
            (guild.id,)
        )
        rows = await c.fetchall()
    if not rows:
        try: await target.send(_digest_header("Lixing Digest") + "\n_No active posts._")
        except Exception: pass
        return
    lines = [f"• **{cn}** — {cl} • Lvl {lv} • Desired: {dx or 'N/A'} • `#${pid}`"
             for (cn,cl,lv,dx,pid) in rows][:40]
    text = _digest_header("Lixing Digest") + "\n" + "\n".join(lines)
    if len(text) > 1950: text = text[:1945] + "…"
    try: await target.send(text)
    except Exception: pass

@tasks.loop(hours=6.0)
async def digest_six_hour_tick():
    for g in bot.guilds:
        if not await ensure_guild_auth(g):  # only in allowed servers
            continue
        try:
            await _send_market_digest(g)
            await asyncio.sleep(0.5)
            await _send_lixing_digest(g)
        except Exception as e:
            log.warning(f"Digest failed for guild {g.id}: {e}")

# -------------------- BOOT OFFLINE PROCESSING --------------------
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

    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT id,guild_id,channel_id,name,next_spawn_ts,category FROM bosses")
        rows = await c.fetchall()

    due_at_boot = [(bid, gid, ch, nm, ts, cat) for bid, gid, ch, nm, ts, cat in rows if int(ts) <= boot]
    for bid, *_ in due_at_boot:
        muted_due_on_boot.add(int(bid))

    if off_since:
        just_due = [(bid, gid, ch, nm, ts, cat) for (bid, gid, ch, nm, ts, cat) in due_at_boot if off_since <= int(ts) <= boot]
        for bid, gid, ch_id, name, ts, cat in just_due:
            guild = bot.get_guild(gid)
            if guild and await ensure_guild_auth(guild):
                ch = await resolve_announce_channel(gid, ch_id, cat)
                if ch and can_send(ch):
                    try:
                        ago = human_ago(boot - int(ts))
                        await ch.send(f":zzz: While I was offline, **{name}** spawned ({ago}).")
                    except Exception as e:
                        log.warning(f"Offline notice failed: {e}")
                await send_subscription_ping(gid, bid, phase="window", boss_name=name)

# -------------------- EVENTS --------------------
@bot.event
async def on_ready():
    await init_db()
    for g in bot.guilds:
        if not await ensure_guild_auth(g):
            continue
        await upsert_guild_defaults(g.id)
    await meta_set("last_startup_ts", str(now_ts()))
    await boot_offline_processing()
    # Seed & panels after DB is ready
    for g in bot.guilds:
        if not await ensure_guild_auth(g):
            continue
        await ensure_seed_for_guild(g)
        await refresh_subscription_messages(g)
    # Start loops
    if not timers_tick.is_running(): timers_tick.start()
    if not uptime_heartbeat.is_running(): uptime_heartbeat.start()
    if not digest_six_hour_tick.is_running(): digest_six_hour_tick.start()
    # Slash sync
    try:
        await bot.tree.sync()
    except Exception as e:
        log.warning(f"App command sync failed: {e}")
    log.info(f"Logged in as {bot.user} (ID: {bot.user.id})")

@bot.event
async def on_guild_join(guild):
    await init_db()
    if not await ensure_guild_auth(guild):
        log.info(f"Joined unauthorized guild {guild.id}; bot will stay inert here.")
        return
    await upsert_guild_defaults(guild.id)
    await ensure_seed_for_guild(guild)
    await refresh_subscription_messages(guild)
    try: await bot.tree.sync(guild=guild)
    except Exception: pass

@bot.event
async def on_message(message: discord.Message):
    if message.author.bot or not message.guild: return
    guild = message.guild
    if not await ensure_guild_auth(guild):
        return
    if await is_blacklisted(message.guild.id, message.author.id):
        return
    prefix = await get_guild_prefix(bot, message)
    content = (message.content or "").strip()

    # Quick reset shorthand: "!<BossOrAlias>"
    if content.startswith(prefix) and len(content) > len(prefix):
        shorthand = content[len(prefix):].strip()
        root = shorthand.split(" ", 1)[0].lower()
        if root not in RESERVED_TRIGGERS:
            ident = shorthand.strip().strip('"').strip("'")
            result, err = await resolve_boss(message, ident)
            if result and not err:
                bid, nm, mins = result
                if await has_trusted(message.author, message.guild.id, bid):
                    async with aiosqlite.connect(DB_PATH) as db:
                        await db.execute("UPDATE bosses SET next_spawn_ts=? WHERE id=?", (now_ts() + int(mins) * 60, bid))
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

# Reactions: subscriptions + reaction-role panels
@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    if bot.user and payload.user_id == bot.user.id: return
    guild = bot.get_guild(payload.guild_id)
    if not guild or not await ensure_guild_auth(guild): return
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

    # Reaction roles
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT 1 FROM rr_panels WHERE message_id=?", (payload.message_id,))
        panel_present = (await c.fetchone()) is not None
    if panel_present:
        try:
            member = guild.get_member(payload.user_id) or await guild.fetch_member(payload.user_id)
            async with aiosqlite.connect(DB_PATH) as db:
                c = await db.execute("SELECT role_id FROM rr_map WHERE panel_message_id=? AND emoji=?", (payload.message_id, emoji_str))
                row = await c.fetchone()
            if not row: return
            role = guild.get_role(int(row[0]))
            if role:
                await member.add_roles(role, reason="Reaction role opt-in")
        except Exception as e:
            log.warning(f"Add reaction-role failed: {e}")

@bot.event
async def on_raw_reaction_remove(payload: discord.RawReactionActionEvent):
    guild = bot.get_guild(payload.guild_id)
    if not guild or not await ensure_guild_auth(guild): return
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

    # Reaction roles
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT 1 FROM rr_panels WHERE message_id=?", (payload.message_id,))
        panel_present = (await c.fetchone()) is not None
    if panel_present:
        try:
            member = guild.get_member(payload.user_id) or await guild.fetch_member(payload.user_id)
            async with aiosqlite.connect(DB_PATH) as db:
                c = await db.execute("SELECT role_id FROM rr_map WHERE panel_message_id=? AND emoji=?", (payload.message_id, emoji_str))
                row = await c.fetchone()
            if not row: return
            role = guild.get_role(int(row[0]))
            if role:
                await member.remove_roles(role, reason="Reaction role opt-out")
        except Exception as e:
            log.warning(f"Remove reaction-role failed: {e}")

# -------------------- ERRORS --------------------
@bot.event
async def on_command_error(ctx, error):
    from discord.ext import commands as ext
    if isinstance(error, ext.CommandNotFound): return
    try: await ctx.send(f":warning: {error}")
    except Exception: pass

# -------------------- SHUTDOWN --------------------
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

# -------------------- RUN --------------------
async def main():
    # Signal handlers
    loop = asyncio.get_running_loop()
    for s in (getattr(signal, "SIGINT", None), getattr(signal, "SIGTERM", None)):
        if s:
            try: loop.add_signal_handler(s, lambda sig=s: asyncio.create_task(graceful_shutdown(sig)))
            except NotImplementedError: pass
    # Start
    try: await bot.start(TOKEN)
    except KeyboardInterrupt: await graceful_shutdown()

if __name__ == "__main__":
    asyncio.run(main())

