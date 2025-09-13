# ============================================
# SECTION 1 / 4 — Foundations & Migrations
# ============================================

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
from discord.ext import commands
from discord import app_commands
from dotenv import load_dotenv

# -------------------- ENV / GLOBALS --------------------
load_dotenv()

TOKEN = os.getenv("DISCORD_TOKEN")
if not TOKEN:
    raise SystemExit("DISCORD_TOKEN missing in .env")

# Optional host PowerShell (used by Section 2 command; stays harmless here)
ALLOW_POWERSHELL = os.getenv("ALLOW_POWERSHELL", "0") in {"1", "true", "True", "yes", "YES"}

# Persistent DB path (Render: set DB_PATH=/data/bosses.db)
DB_PATH = os.getenv("DB_PATH", "bosses.db")

# Required presence guard: bot should operate only if this user is present in guild
REQUIRED_USER_TAG = os.getenv("REQUIRED_USER_TAG", "@blunderbusstin").strip()

DEFAULT_PREFIX = "!"
CHECK_INTERVAL_SECONDS = 15
DEFAULT_UPTIME_MINUTES = 60
# After a window fully closes, only flip to -Nada once this grace expires
NADA_GRACE_SECONDS = 1800  # 30 minutes

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ch-bossbot")

intents = discord.Intents.default()
intents.message_content = True
intents.reactions = True
intents.guilds = True
intents.members = True

# Create bot with no built-in help (we provide our own later)
async def get_guild_prefix(_bot, message: discord.Message):
    if not message or not message.guild:
        return DEFAULT_PREFIX
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            c = await db.execute(
                "SELECT COALESCE(prefix, ?) FROM guild_config WHERE guild_id=?",
                (DEFAULT_PREFIX, message.guild.id)
            )
            r = await c.fetchone()
            if r and r[0]:
                return r[0]
    except Exception:
        pass
    return DEFAULT_PREFIX

bot = commands.Bot(command_prefix=get_guild_prefix, intents=intents, help_command=None)

# Global tick tracking (set by Section 2 timers)
_last_timer_tick_ts: int = 0
_prev_timer_tick_ts: int = 0

SEED_VERSION = "v2025-09-12-subping-window-ps-final"

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
    # Positive deltas in h/m/s; negatives show "-Xm" until lost grace passes, then "-Nada"
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

# -------------------- DB: PREFLIGHT (sync) --------------------
def preflight_migrate_sync():
    import sqlite3
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Core tables
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

    # Column existence helper
    def col_exists(table, col):
        cur.execute(f"PRAGMA table_info({table})")
        return any(row[1] == col for row in cur.fetchall())

    # Bosses additional columns
    if not col_exists("bosses","window_minutes"):
        cur.execute("ALTER TABLE bosses ADD COLUMN window_minutes INTEGER DEFAULT 0")

    # Guild config additional columns
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

    # Feature tables
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

    # Market & Lixing tables (new features)
    cur.execute("""CREATE TABLE IF NOT EXISTS market_listings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        guild_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        type TEXT NOT NULL,                     -- BUY | SELL
        item_name TEXT NOT NULL,
        quantity INTEGER NOT NULL,
        price TEXT DEFAULT '',
        accepts_trades INTEGER DEFAULT 0,
        taking_offers INTEGER DEFAULT 0,
        notes TEXT DEFAULT '',
        created_ts INTEGER NOT NULL,
        is_active INTEGER DEFAULT 1,
        message_channel_id INTEGER DEFAULT NULL,
        message_id INTEGER DEFAULT NULL
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS market_offers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        guild_id INTEGER NOT NULL,
        listing_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        amount TEXT DEFAULT '',
        message TEXT DEFAULT '',
        created_ts INTEGER NOT NULL
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS lixing_posts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        guild_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        char_name TEXT NOT NULL,
        char_class TEXT NOT NULL,
        level INTEGER,                          -- NULL = N/A
        desired_lixes TEXT DEFAULT 'N/A',
        notes TEXT DEFAULT '',
        created_ts INTEGER NOT NULL,
        is_active INTEGER DEFAULT 1,
        message_channel_id INTEGER DEFAULT NULL,
        message_id INTEGER DEFAULT NULL
    )""")

    conn.commit()
    conn.close()

preflight_migrate_sync()

# -------------------- ASYNC MIGRATIONS (idempotent) --------------------
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
        # Market / Lixing
        await db.execute("""CREATE TABLE IF NOT EXISTS market_listings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            type TEXT NOT NULL,
            item_name TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            price TEXT DEFAULT '',
            accepts_trades INTEGER DEFAULT 0,
            taking_offers INTEGER DEFAULT 0,
            notes TEXT DEFAULT '',
            created_ts INTEGER NOT NULL,
            is_active INTEGER DEFAULT 1,
            message_channel_id INTEGER DEFAULT NULL,
            message_id INTEGER DEFAULT NULL
        )""")
        await db.execute("""CREATE TABLE IF NOT EXISTS market_offers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id INTEGER NOT NULL,
            listing_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            amount TEXT DEFAULT '',
            message TEXT DEFAULT '',
            created_ts INTEGER NOT NULL
        )""")
        await db.execute("""CREATE TABLE IF NOT EXISTS lixing_posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            char_name TEXT NOT NULL,
            char_class TEXT NOT NULL,
            level INTEGER,
            desired_lixes TEXT DEFAULT 'N/A',
            notes TEXT DEFAULT '',
            created_ts INTEGER NOT NULL,
            is_active INTEGER DEFAULT 1,
            message_channel_id INTEGER DEFAULT NULL,
            message_id INTEGER DEFAULT NULL
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

# -------------------- AUTH GUARD (required user present) --------------------
async def guild_is_authorized(guild: discord.Guild) -> bool:
    """
    Returns True if REQUIRED_USER_TAG is empty or the user is present in the guild.
    REQUIRED_USER_TAG may be a raw mention like @blunderbusstin or a numeric ID.
    """
    tag = REQUIRED_USER_TAG
    if not tag:
        return True
    # Try ID
    uid: Optional[int] = None
    s = tag.strip()
    if s.startswith("<@") and s.endswith(">"):
        s = s.strip("<@!>")
    if s.isdigit():
        try:
            uid = int(s)
        except Exception:
            uid = None
    if uid:
        member = guild.get_member(uid) or (await guild.fetch_member(uid) if guild.me.guild_permissions.view_guild_insights else None)
        return member is not None
    # Fallback: search by name (best-effort)
    name = s.lstrip("@").lower()
    for m in guild.members:
        if m.name.lower() == name or (m.display_name and m.display_name.lower() == name):
            return True
    return False

def guild_auth_check():
    """Decorator to block commands when the guild is not authorized."""
    def wrapper():
        async def predicate(ctx: commands.Context) -> bool:
            if not ctx.guild:
                return False
            try:
                ok = await guild_is_authorized(ctx.guild)
                if not ok:
                    await ctx.send(":no_entry: Bot is disabled in this server (required user not present).")
                return ok
            except Exception:
                return False
        return commands.check(predicate)
    return wrapper()

# -------------------- PERMISSION CHECKS --------------------
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

def can_send(channel: Optional[discord.abc.GuildChannel]) -> bool:
    if not channel or not isinstance(channel, (discord.TextChannel, discord.Thread)):
        return False
    me = channel.guild.me
    perms = channel.permissions_for(me)
    return perms.view_channel and perms.send_messages and perms.embed_links and perms.read_message_history

def can_react(channel: Optional[discord.abc.GuildChannel]) -> bool:
    if not channel or not isinstance(channel, (discord.TextChannel, discord.Thread)):
        return False
    me = channel.guild.me
    perms = channel.permissions_for(me)
    return perms.add_reactions and perms.view_channel and perms.read_message_history

async def has_trusted(member: discord.Member, guild_id: int, boss_id: Optional[int] = None) -> bool:
    if member.guild_permissions.administrator:
        return True
    async with aiosqlite.connect(DB_PATH) as db:
        if boss_id:
            c = await db.execute("SELECT trusted_role_id FROM bosses WHERE id=? AND guild_id=?", (boss_id, guild_id))
            r = await c.fetchone()
            if r and r[0]:
                return any(role.id == r[0] for role in member.roles)
    return member.guild_permissions.manage_messages

# -------------------- CHANNEL RESOLUTION HELPERS --------------------
async def resolve_announce_channel(guild_id: int, explicit_channel_id: Optional[int], category: Optional[str] = None) -> Optional[discord.TextChannel]:
    guild = bot.get_guild(guild_id)
    if not guild:
        return None
    # Per-boss override
    if explicit_channel_id:
        ch = guild.get_channel(explicit_channel_id)
        if can_send(ch):
            return ch
    # Per-category route
    if category:
        async with aiosqlite.connect(DB_PATH) as db:
            c = await db.execute("SELECT channel_id FROM category_channels WHERE guild_id=? AND category=?",
                                 (guild_id, norm_cat(category)))
            r = await c.fetchone()
        if r and r[0]:
            ch = guild.get_channel(r[0])
            if can_send(ch):
                return ch
    # Global default
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT default_channel FROM guild_config WHERE guild_id=?", (guild_id,))
        r = await c.fetchone()
        if r and r[0]:
            ch = guild.get_channel(r[0])
            if can_send(ch):
                return ch
    # Fallback: first channel we can talk in
    for ch in guild.text_channels:
        if can_send(ch):
            return ch
    return None

async def resolve_heartbeat_channel(guild_id: int) -> Optional[discord.TextChannel]:
    guild = bot.get_guild(guild_id)
    if not guild:
        return None
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT heartbeat_channel_id, default_channel FROM guild_config WHERE guild_id=?", (guild_id,))
        r = await c.fetchone()
    hb_id, def_id = (r[0], r[1]) if r else (None, None)
    for cid in [hb_id, def_id]:
        if cid:
            ch = guild.get_channel(cid)
            if can_send(ch):
                return ch
    for ch in guild.text_channels:
        if can_send(ch):
            return ch
    return None

# -------------------- SEED DATA (includes DL 180 fix) --------------------
SEED_DATA = [
    # METEORIC
    ("Meteoric", "Doomclaw", 7, 5, []),
    ("Meteoric", "Bonehad", 15, 5, []),
    ("Meteoric", "Rockbelly", 15, 5, []),
    ("Meteoric", "Redbane", 20, 5, []),
    ("Meteoric", "Coppinger", 20, 5, ["copp"]),
    ("Meteoric", "Goretusk", 20, 5, []),
    ("Meteoric", "Falgren", 45, 5, []),
    # FROZEN
    ("Frozen", "Redbane", 20, 5, []),
    ("Frozen", "Eye", 28, 3, []),
    ("Frozen", "Swampie", 33, 3, ["swampy"]),
    ("Frozen", "Woody", 38, 3, []),
    ("Frozen", "Chained", 43, 3, ["chain"]),
    ("Frozen", "Grom", 48, 3, []),
    ("Frozen", "Pyrus", 58, 3, ["py"]),
    # DL (180 fixed to 88/3)
    ("DL", "155", 63, 3, []),
    ("DL", "160", 68, 3, []),
    ("DL", "165", 73, 3, []),
    ("DL", "170", 78, 3, []),
    ("DL", "180", 88, 3, ["snorri"]),
    # EDL
    ("EDL", "185", 72, 3, []),
    ("EDL", "190", 81, 3, []),
    ("EDL", "195", 89, 4, []),
    ("EDL", "200", 108, 5, []),
    ("EDL", "205", 117, 4, []),
    ("EDL", "210", 125, 5, []),
    ("EDL", "215", 134, 5, ["unox"]),
    # MIDRAIDS
    ("Midraids", "Aggorath", 1200, 960, ["aggy"]),
    ("Midraids", "Mordris", 1200, 960, ["mord","mordy"]),
    ("Midraids", "Necromancer", 1320, 960, ["necro"]),
    ("Midraids", "Hrungnir", 1320, 960, ["hrung","muk"]),
    # RINGS
    ("Rings", "North Ring", 215, 50, ["northring"]),
    ("Rings", "Center Ring", 215, 50, ["centre","centering"]),
    ("Rings", "South Ring", 215, 50, ["southring"]),
    ("Rings", "East Ring", 215, 50, ["eastring"]),
    # EG
    ("EG", "Draig Liathphur", 240, 840, ["draig","dragon","riverdragon"]),
    ("EG", "Sciathan Leathair", 240, 300, ["sciathan","bat","northbat"]),
    ("EG", "Thymea Banebark", 240, 840, ["thymea","tree","ancienttree"]),
    ("EG", "Proteus", 1080, 15, ["prot","base","prime"]),
    ("EG", "Gelebron", 1920, 1680, ["gele"]),
    ("EG", "Dhiothu", 2040, 1680, ["dino","dhio","d2"]),
    ("EG", "Bloodthorn", 2040, 1680, ["bt"]),
    ("EG", "Crom’s Manikin", 5760, 1440, ["manikin","crom","croms"]),
]

async def ensure_seed_for_guild(guild: discord.Guild):
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
                # Optional: ensure DL 180 fix applied if an old value lingers
                if name == "180":
                    await db.execute("UPDATE bosses SET spawn_minutes=?, window_minutes=? WHERE id=?", (int(spawn_m), int(window_m), bid))
            else:
                next_spawn = now_ts() - 3601  # default to -Nada
                await db.execute(
                    "INSERT INTO bosses (guild_id,channel_id,name,spawn_minutes,window_minutes,next_spawn_ts,pre_announce_min,created_by,category,sort_key) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (guild.id, None, name, int(spawn_m), int(window_m), next_spawn, 10, guild.owner_id if guild.owner_id else 0, catn, "")
                )
                c = await db.execute("SELECT id FROM bosses WHERE guild_id=? AND name=? AND category=?", (guild.id, name, catn))
                bid = (await c.fetchone())[0]
                inserted += 1
            seen = set()
            for al in aliases:
                al_l = str(al).strip().lower()
                if not al_l or al_l in seen:
                    continue
                seen.add(al_l)
                try:
                    await db.execute("INSERT INTO boss_aliases (guild_id,boss_id,alias) VALUES (?,?,?)", (guild.id, bid, al_l))
                    alias_ct += 1
                except Exception:
                    pass
        await db.commit()
    await meta_set(key, "done")
    if inserted or alias_ct:
        log.info(f"Seeded {inserted} bosses, {alias_ct} aliases for guild {guild.id}")

# -------------------- OFFLINE CATCH-UP --------------------
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

    # Mark due at boot (muted announcements handled by Section 2 logic)
    # (We still send catch-up messages here, non-intrusively)
    if off_since:
        just_due = [(bid, gid, ch, nm, ts, cat) for (bid, gid, ch, nm, ts, cat) in rows if off_since <= int(ts) <= boot]
        for _bid, gid, ch_id, name, ts, cat in just_due:
            # Only send if we can resolve a channel; errors swallowed
            guild = bot.get_guild(gid)
            if not guild:
                continue
            try:
                # Use announce resolution helper
                ch = await resolve_announce_channel(gid, ch_id, cat)
                if ch and can_send(ch):
                    ago = human_ago(boot - int(ts))
                    await ch.send(f":zzz: While I was offline, **{name}** spawned ({ago}).")
            except Exception as e:
                log.warning(f"Offline notice failed: {e}")

# -------------------- READY / JOIN EVENTS --------------------
@bot.event
async def on_ready():
    # Core boot (no loop starts here; Section 4 ensures loops without clobbering)
    await init_db()
    for g in bot.guilds:
        await upsert_guild_defaults(g.id)

    await meta_set("last_startup_ts", str(now_ts()))
    await boot_offline_processing()

    # Ensure seed + (if available) refresh panels
    for g in bot.guilds:
        try:
            if await guild_is_authorized(g):
                await ensure_seed_for_guild(g)
                # Call refresh_subscription_messages if defined later (Section 2)
                if "refresh_subscription_messages" in globals():
                    try:
                        await refresh_subscription_messages(g)  # type: ignore[name-defined]
                    except Exception as e:
                        log.info(f"Initial panel refresh skipped for guild {g.id}: {e}")
        except Exception as e:
            log.info(f"Seed/setup skipped for guild {g.id}: {e}")

    # Sync app commands (safe to try here; Section 4 will also attempt a sync via setup_hook)
    try:
        await bot.tree.sync()
    except Exception as e:
        log.warning(f"App command sync failed: {e}")

    log.info(f"Logged in as {bot.user} (ID: {bot.user.id})")

@bot.event
async def on_guild_join(guild: discord.Guild):
    await init_db()
    await upsert_guild_defaults(guild.id)
    try:
        if await guild_is_authorized(guild):
            await ensure_seed_for_guild(guild)
            if "refresh_subscription_messages" in globals():
                try:
                    await refresh_subscription_messages(guild)  # type: ignore[name-defined]
                except Exception:
                    pass
    except Exception:
        pass
    try:
        await bot.tree.sync(guild=guild)
    except Exception:
        pass

# -------------------- SHUTDOWN (persist offline_since) --------------------
async def graceful_shutdown(_sig=None):
    try:
        await meta_set("offline_since", str(now_ts()))
    finally:
        await bot.close()

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

# -------------------- ENTRYPOINT --------------------
async def main():
    loop = asyncio.get_running_loop()
    for s in (getattr(signal, "SIGINT", None), getattr(signal, "SIGTERM", None)):
        if s:
            try:
                loop.add_signal_handler(s, lambda sig=s: asyncio.create_task(graceful_shutdown(sig)))
            except NotImplementedError:
                pass
    try:
        await bot.start(TOKEN)
    except KeyboardInterrupt:
        await graceful_shutdown()

if __name__ == "__main__":
    asyncio.run(main())
# ============================================
# SECTION 2 / 4 — Commands & Panels
# ============================================

from discord.ext import tasks
from discord import ui, ButtonStyle

# -------------------- HELP COMMAND --------------------
@bot.command(name="help")
@blacklist_check()
async def help_cmd(ctx: commands.Context):
    """Show tidy help with categories."""
    prefix = await get_guild_prefix(bot, ctx.message)

    embed = discord.Embed(
        title="Bot Commands",
        description=f"Use prefix `{prefix}` before commands.",
        color=0x3498db
    )

    embed.add_field(
        name="Boss Timers",
        value=(
            f"`{prefix}add <name> <spawn_m> <window_m>` - Add boss\n"
            f"`{prefix}del <name>` - Remove boss\n"
            f"`{prefix}next <name>` - Set next spawn to now\n"
            f"`{prefix}timers` - Show all timers\n"
            f"`{prefix}setchannel [#ch]` - Set announce channel\n"
        ),
        inline=False
    )

    embed.add_field(
        name="Subscriptions",
        value=(
            f"`{prefix}setsubchannel [#ch]` - Set subs panel channel\n"
            f"`{prefix}setsubpingchannel [#ch]` - Set ping-out channel\n"
        ),
        inline=False
    )

    embed.add_field(
        name="Market",
        value=(
            f"`{prefix}market add <BUY|SELL> <item> <qty> [price] [trades?] [offers?] [notes]`\n"
            f"`{prefix}market remove <id>`\n"
            f"`{prefix}market list`"
        ),
        inline=False
    )

    embed.add_field(
        name="Lixing",
        value=(
            f"`{prefix}lixing add <char> <class> <lvl|N/A> [lixes|N/A] [notes]`\n"
            f"`{prefix}lixing remove <id>`\n"
            f"`{prefix}lixing list`"
        ),
        inline=False
    )

    embed.add_field(
        name="Roles",
        value=(
            f"`{prefix}rrcreate <title>` - Create panel\n"
            f"`{prefix}rradd <msg_id> <emoji> <@role>`\n"
            f"`{prefix}rrdel <msg_id> <emoji>`"
        ),
        inline=False
    )

    embed.add_field(
        name="Config/Admin",
        value=(
            f"`{prefix}setprefix <p>`\n"
            f"`{prefix}setdefault [#ch]`\n"
            f"`{prefix}setheartbeat [#ch]`\n"
            f"`{prefix}blacklist <@u>` / `{prefix}unblacklist <@u>`"
        ),
        inline=False
    )

    await ctx.send(embed=embed)

# -------------------- BOSS MANAGEMENT --------------------
@bot.command(name="add")
@guild_auth_check()
async def add_boss(ctx, name: str, spawn_m: int, window_m: int):
    """Add boss timer"""
    gid = ctx.guild.id
    await init_db()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO bosses (guild_id,name,spawn_minutes,window_minutes,next_spawn_ts,pre_announce_min,created_by,category) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (gid, name, spawn_m, window_m, now_ts()-3601, 10, ctx.author.id, norm_cat(name))
        )
        await db.commit()
    await ctx.send(f"✅ Added boss {name} ({spawn_m}m/{window_m}m).")

@bot.command(name="del")
@guild_auth_check()
async def del_boss(ctx, name: str):
    gid = ctx.guild.id
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM bosses WHERE guild_id=? AND name LIKE ?", (gid, name))
        await db.commit()
    await ctx.send(f"🗑️ Deleted boss {name}.")

@bot.command(name="next")
@guild_auth_check()
async def next_spawn(ctx, name: str):
    gid = ctx.guild.id
    ts = now_ts()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE bosses SET next_spawn_ts=? WHERE guild_id=? AND name LIKE ?", (ts, gid, name))
        await db.commit()
    await ctx.send(f"⏩ Next spawn for {name} set to now.")

@bot.command(name="timers")
@blacklist_check()
async def list_timers(ctx):
    gid = ctx.guild.id
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT name,spawn_minutes,window_minutes,next_spawn_ts,category FROM bosses WHERE guild_id=?",(gid,))
        rows = await c.fetchall()
    if not rows:
        await ctx.send("No bosses.")
        return
    embed = discord.Embed(title="Boss Timers", color=0x2ecc71)
    for name,spawn,window,ts,cat in sorted(rows, key=lambda r: natural_key(r[0])):
        delta = (ts+spawn*60)-now_ts()
        embed.add_field(name=f"{category_emoji(cat)} {name}", value=fmt_delta_for_list(delta), inline=True)
    await ctx.send(embed=embed)

# -------------------- SUBSCRIPTION PANELS --------------------
async def refresh_subscription_messages(guild: discord.Guild):
    """Ensure one persistent panel per category; reuse messages; keep reactions."""
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT sub_channel_id FROM guild_config WHERE guild_id=?", (guild.id,))
        r = await c.fetchone()
    if not r or not r[0]:
        return
    ch = guild.get_channel(r[0])
    if not can_send(ch): return

    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT DISTINCT category FROM bosses WHERE guild_id=?", (guild.id,))
        cats = [row[0] for row in await c.fetchall()]

    for cat in cats:
        # Try to fetch existing
        async with aiosqlite.connect(DB_PATH) as db:
            c = await db.execute("SELECT message_id FROM subscription_panels WHERE guild_id=? AND category=?", (guild.id,cat))
            r = await c.fetchone()
        msg: Optional[discord.Message] = None
        if r and r[0]:
            try:
                msg = await ch.fetch_message(r[0])
            except Exception:
                msg = None
        embed = discord.Embed(title=f"{category_emoji(cat)} Subscriptions: {cat}", color=DEFAULT_COLORS.get(cat,0x95a5a6))
        embed.description = "React to subscribe/unsubscribe."
        if msg:
            await msg.edit(embed=embed)
        else:
            msg = await ch.send(embed=embed)
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute("INSERT OR REPLACE INTO subscription_panels (guild_id,category,message_id,channel_id) VALUES (?,?,?,?)",(guild.id,cat,msg.id,ch.id))
                await db.commit()
        # Always ensure base reactions
        try:
            await msg.add_reaction("⭐")
        except Exception: pass

# ... (Section 2 continues with Market/Lixing, rr, config, blacklist, etc.)
# =============================
# SECTION 2 / 4 — Commands & Panels (chunk 2/2)
# =============================

# ---------- SUBSCRIPTION EMOJI MAPPING & PANELS (robust, sticky) ----------

async def get_subchannel_id(guild_id: int) -> Optional[int]:
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT sub_channel_id FROM guild_config WHERE guild_id=?", (guild_id,))
        r = await c.fetchone()
        return (int(r[0]) if r and r[0] else None)

async def get_subping_channel_id(guild_id: int) -> Optional[int]:
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT sub_ping_channel_id FROM guild_config WHERE guild_id=?", (guild_id,))
        r = await c.fetchone()
        return (int(r[0]) if r and r[0] else None)

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

async def ensure_emoji_mapping(guild_id: int, bosses: List[tuple]):
    """Assign unique emojis to each boss; keep stable; resolve duplicates."""
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
            if not blist: 
                continue
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
            if boss_id in have_ids: 
                continue
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
        color=DEFAULT_COLORS.get(cat, DEFAULT_COLORS["Default"])
    )
    lines = []
    per_message_emojis = []
    for bid, name, _sk in rows:
        e = emoji_map.get(bid, "⭐")
        if e in per_message_emojis:  # avoid dups per message
            continue
        per_message_emojis.append(e)
        lines.append(f"{e} — **{name}**")

    # Split into fields if long
    bucket = ""; fields: List[str] = []
    for line in lines:
        if len(bucket) + len(line) + 1 > 1000:
            fields.append(bucket); bucket = line + "\n"
        else:
            bucket += line + "\n"
    if bucket: 
        fields.append(bucket)
    for i, val in enumerate(fields, 1):
        em.add_field(name=f"{cat} ({i})" if len(fields) > 1 else cat, value=val, inline=False)

    content = "React to manage **per-boss pings** for this category."
    return content, em, per_message_emojis

async def delete_old_subscription_messages(guild: discord.Guild):
    gid = guild.id
    records = await get_all_panel_records(gid)
    for _cat, (msg_id, ch_id) in records.items():
        if not ch_id: 
            continue
        ch = guild.get_channel(ch_id)
        if not ch: 
            continue
        try:
            msg = await ch.fetch_message(msg_id)
            await msg.delete()
            await asyncio.sleep(0.2)
        except Exception:
            pass
    await clear_all_panel_records(gid)

async def refresh_subscription_messages(guild: discord.Guild):
    """Rebuild/refresh all panels without creating duplicates; preserve reactions."""
    gid = guild.id
    sub_ch_id = await get_subchannel_id(gid)
    if not sub_ch_id:
        return
    channel = guild.get_channel(sub_ch_id)
    if not can_send(channel):
        return

    # Ensure emoji mapping has entries
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT id,name FROM bosses WHERE guild_id=?", (gid,))
        all_bosses = await c.fetchall()
    await ensure_emoji_mapping(gid, all_bosses)

    panel_map = await get_all_panel_records(gid)
    for cat in CATEGORY_ORDER:
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

        # If existing in another channel, delete & recreate in the configured one
        if existing_id and existing_ch and existing_ch != sub_ch_id:
            old_ch = guild.get_channel(existing_ch)
            if old_ch and can_send(old_ch):
                try:
                    old_msg = await old_ch.fetch_message(existing_id)
                    await old_msg.delete()
                except Exception:
                    pass
            existing_id = None

        if existing_id:
            try:
                message = await channel.fetch_message(existing_id)
                await message.edit(content=content, embed=embed)
            except Exception:
                # Could not fetch/edit; recreate
                try:
                    message = await channel.send(content=content, embed=embed)
                    await set_panel_record(gid, cat, message.id, channel.id)
                except Exception as e:
                    log.warning(f"Subscription panel ({cat}) recreate failed: {e}")
                    continue
        else:
            try:
                message = await channel.send(content=content, embed=embed)
                await set_panel_record(gid, cat, message.id, channel.id)
            except Exception as e:
                log.warning(f"Subscription panel ({cat}) create failed: {e}")
                continue

        # Ensure reactions: add only those missing on this message
        if can_react(channel) and message:
            try:
                existing = set(str(r.emoji) for r in message.reactions)
                for e in [e for e in emojis if e not in existing]:
                    await message.add_reaction(e)
                    await asyncio.sleep(0.2)
            except Exception as e:
                log.warning(f"Adding reactions failed for {cat}: {e}")

# Pings to subscribers (separate channel)
async def send_subscription_ping(guild_id: int, boss_id: int, phase: str, boss_name: str, when_left: Optional[int] = None):
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT sub_ping_channel_id, sub_channel_id FROM guild_config WHERE guild_id=?", (guild_id,))
        r = await c.fetchone()
        sub_ping_id = (r[0] if r else None) or (r[1] if r else None)  # fallback to panel channel if ping channel unset
        c = await db.execute("SELECT user_id FROM subscription_members WHERE guild_id=? AND boss_id=?", (guild_id, boss_id))
        subs = [row[0] for row in await c.fetchall()]
    if not sub_ping_id or not subs:
        return
    guild = bot.get_guild(guild_id); ch = guild.get_channel(sub_ping_id) if guild else None
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

# Reaction handling: subscriptions + reaction roles
@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    if bot.user and payload.user_id == bot.user.id:
        return
    guild = bot.get_guild(payload.guild_id)
    if not guild:
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
    if not guild:
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
            if not row:
                return
            role = guild.get_role(int(row[0]))
            if role:
                await member.remove_roles(role, reason="Reaction role opt-out")
        except Exception as e:
            log.warning(f"Remove reaction-role failed: {e}")

# ---------- CONFIG / ADMIN COMMANDS ----------

@bot.command(name="setprefix")
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
    await ctx.send(f"✅ Prefix set to `{new_prefix}`.")

def _resolve_channel_id_from_arg(ctx, value: Optional[str]) -> Optional[int]:
    if not value:
        return None
    if isinstance(value, int):
        return value
    s = str(value)
    if s.startswith("<#") and s.endswith(">"):
        return int(s[2:-1])
    if s.isdigit():
        return int(s)
    found = discord.utils.get(ctx.guild.channels, name=s.strip("#"))
    return found.id if found else None

@bot.command(name="setannounce")
@commands.has_permissions(manage_guild=True)
async def setannounce_cmd(ctx, *args):
    if not args:
        return await ctx.send("Usage: `!setannounce #chan` or `!setannounce category \"<Category>\" #chan`")
    first = args[0].lower()
    if first.startswith("<#") or first == "global" or (len(args) == 1 and args[0].isdigit()):
        channel_id = _resolve_channel_id_from_arg(ctx, args[-1])
        if not channel_id:
            return await ctx.send("Mention a channel, e.g., `#raids`.")
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT INTO guild_config (guild_id,default_channel) VALUES (?,?) "
                "ON CONFLICT(guild_id) DO UPDATE SET default_channel=excluded.default_channel",
                (ctx.guild.id, channel_id)
            )
            await db.commit()
        return await ctx.send(f"✅ Global announce channel set to <#{channel_id}>.")
    if first in {"category", "categoryclear"}:
        if first == "category":
            if len(args) < 3: 
                return await ctx.send('Format: `!setannounce category "<Category>" #chan`')
            joined = " ".join(args[1:])
            if '"' in joined:
                cat = joined.split('"', 1)[1].split('"', 1)[0].strip()
                tail = joined.split('"', 2)[-1].strip().split()
                ch_id = _resolve_channel_id_from_arg(ctx, tail[-1]) if tail else None
            else:
                cat = " ".join(args[1:-1]).strip(); ch_id = _resolve_channel_id_from_arg(ctx, args[-1])
            if not cat or not ch_id: 
                return await ctx.send('Format: `!setannounce category "<Category>" #chan`')
            catn = norm_cat(cat)
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute(
                    "INSERT INTO category_channels (guild_id,category,channel_id) VALUES (?,?,?) "
                    "ON CONFLICT(guild_id,category) DO UPDATE SET channel_id=excluded.channel_id",
                    (ctx.guild.id, catn, ch_id)
                )
                await db.commit()
            return await ctx.send(f"✅ **{catn}** reminders → <#{ch_id}>.")
        else:
            if len(args) < 2: 
                return await ctx.send('Format: `!setannounce categoryclear "<Category>"`')
            cat = " ".join(args[1:]).strip().strip('"')
            catn = norm_cat(cat)
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute("DELETE FROM category_channels WHERE guild_id=? AND category=?", (ctx.guild.id, catn))
                await db.commit()
            return await ctx.send(f"✅ Cleared category channel for **{catn}**.")
    return await ctx.send("Usage: `!setannounce #chan` | `!setannounce global #chan` | `!setannounce category \"<Category>\" #chan` | `!setannounce categoryclear \"<Category>\"`")

@bot.command(name="seteta")
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
    await ctx.send(f"✅ UTC ETA display {'enabled' if on else 'disabled'}.")

@bot.command(name="setuptime")
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
                   else f"✅ Uptime heartbeat set to every {minutes} minutes.")

@bot.command(name="setheartbeatchannel")
@commands.has_permissions(manage_guild=True)
async def setheartbeatchannel_cmd(ctx, channel: discord.TextChannel):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO guild_config (guild_id,heartbeat_channel_id) VALUES (?,?) "
            "ON CONFLICT(guild_id) DO UPDATE SET heartbeat_channel_id=excluded.heartbeat_channel_id",
            (ctx.guild.id, channel.id)
        )
        await db.commit()
    await ctx.send(f"✅ Heartbeat channel set to {channel.mention}.")

@bot.command(name="setsubchannel")
@commands.has_permissions(manage_guild=True)
async def setsubchannel_cmd(ctx, channel: discord.TextChannel):
    await delete_old_subscription_messages(ctx.guild)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO guild_config (guild_id,sub_channel_id) VALUES (?,?) "
            "ON CONFLICT(guild_id) DO UPDATE SET sub_channel_id=excluded.sub_channel_id",
            (ctx.guild.id, channel.id)
        )
        await db.commit()
    await ctx.send(f"✅ Subscription **panels** channel set to {channel.mention}. Rebuilding panels…")
    await refresh_subscription_messages(ctx.guild)
    await ctx.send("✅ Subscription panels are ready.")

@bot.command(name="setsubpingchannel")
@commands.has_permissions(manage_guild=True)
async def setsubpingchannel_cmd(ctx, channel: discord.TextChannel):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO guild_config (guild_id,sub_ping_channel_id) VALUES (?,?) "
            "ON CONFLICT(guild_id) DO UPDATE SET sub_ping_channel_id=excluded.sub_ping_channel_id",
            (ctx.guild.id, channel.id)
        )
        await db.commit()
    await ctx.send(f"✅ Subscription **ping** channel set to {channel.mention}.")

@bot.command(name="showsubscriptions")
async def showsubscriptions_cmd(ctx):
    await refresh_subscription_messages(ctx.guild)
    await ctx.send("✅ Subscription panels refreshed (one per category).")

# ---------- BLACKLIST ----------
@bot.group(name="blacklist", invoke_without_command=True)
@commands.has_permissions(manage_guild=True)
async def blacklist_group(ctx):
    await ctx.send("Use `!blacklist add @user` / `!blacklist remove @user` / `!blacklist show`")

@blacklist_group.command(name="add")
@commands.has_permissions(manage_guild=True)
async def blacklist_add(ctx, user: discord.Member):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR IGNORE INTO blacklist (guild_id,user_id) VALUES (?,?)", (ctx.guild.id, user.id))
        await db.commit()
    await ctx.send(f"⛔ **{user.display_name}** is now blacklisted.")

@blacklist_group.command(name="remove")
@commands.has_permissions(manage_guild=True)
async def blacklist_remove(ctx, user: discord.Member):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM blacklist WHERE guild_id=? AND user_id=?", (ctx.guild.id, user.id))
        await db.commit()
    await ctx.send(f"✅ **{user.display_name}** removed from blacklist.")

@blacklist_group.command(name="show")
@commands.has_permissions(manage_guild=True)
async def blacklist_show(ctx):
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT user_id FROM blacklist WHERE guild_id=?", (ctx.guild.id,))
        rows = await c.fetchall()
    if not rows:
        return await ctx.send("No users blacklisted.")
    mentions = " ".join(f"<@{r[0]}>" for r in rows)
    await ctx.send(f"Blacklisted: {mentions}")

# ---------- REACTION ROLES (slash) ----------
@app_commands.guild_only()
@app_commands.default_permissions(manage_roles=True)
@bot.tree.command(name="roles_panel", description="Create a reaction-roles message (react to get/remove roles).")
async def roles_panel(interaction: discord.Interaction,
                      channel: Optional[discord.TextChannel],
                      title: str,
                      pairs: str):
    if not interaction.user.guild_permissions.manage_roles:
        return await interaction.response.send_message("You need Manage Roles permission.", ephemeral=True)
    ch = channel or interaction.channel
    if not can_send(ch):
        return await interaction.response.send_message("I can't post in that channel.", ephemeral=True)
    entries = [e.strip() for e in pairs.split(",") if e.strip()]
    parsed: List[Tuple[str, int, str]] = []
    role_mention_re = re.compile(r"<@&(\d+)>")
    for entry in entries:
        parts = entry.split()
        if not parts:
            continue
        emoji = parts[0]
        m = role_mention_re.search(entry)
        if not m:
            return await interaction.response.send_message(f"Missing role mention in `{entry}`.", ephemeral=True)
        role_id = int(m.group(1))
        role = interaction.guild.get_role(role_id)
        if not role:
            return await interaction.response.send_message(f"Role not found in `{entry}`.", ephemeral=True)
        parsed.append((emoji, role_id, role.name))
    if not parsed:
        return await interaction.response.send_message("No valid emoji/role pairs found.", ephemeral=True)
    desc_lines = [f"{em} — <@&{rid}> ({rname})" for em, rid, rname in parsed]
    embed = discord.Embed(title=title, description="\n".join(desc_lines), color=0x4aa3ff)
    try:
        msg = await ch.send(embed=embed)
    except Exception as e:
        return await interaction.response.send_message(f"Couldn't post panel: {e}", ephemeral=True)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR REPLACE INTO rr_panels (message_id,guild_id,channel_id,title) VALUES (?,?,?,?)",
                         (msg.id, interaction.guild_id, ch.id, title))
        for em, rid, _ in parsed:
            await db.execute("INSERT OR REPLACE INTO rr_map (panel_message_id,emoji,role_id) VALUES (?,?,?)",
                             (msg.id, em, rid))
        await db.commit()
    for em, _, _ in parsed:
        try:
            await msg.add_reaction(em)
            await asyncio.sleep(0.2)
        except Exception:
            pass
    await interaction.response.send_message(f"Reaction-roles panel posted in {ch.mention}.", ephemeral=True)

# ---------- MARKET COMMANDS (core) ----------
@bot.command(name="setmarketchannel")
@commands.has_permissions(manage_guild=True)
async def setmarketchannel_cmd(ctx, channel: discord.TextChannel):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO guild_config (guild_id,market_channel_id) VALUES (?,?) "
            "ON CONFLICT(guild_id) DO UPDATE SET market_channel_id=excluded.market_channel_id",
            (ctx.guild.id, channel.id)
        )
        await db.commit()
    await ctx.send(f"✅ Market digest channel set to {channel.mention}.")

@bot.group(name="market", invoke_without_command=True)
async def market_group(ctx):
    await ctx.send("Use `!market add|list|mine|view|edit|remove`")

@market_group.command(name="add")
async def market_add(ctx, type: str, item: str, qty: int, price: str = "", accepts_trades: str = "no", taking_offers: str = "no", *, notes: str = ""):
    t = type.strip().upper()
    if t not in {"BUY","SELL"}:
        return await ctx.send("Type must be BUY or SELL.")
    accepts = 1 if accepts_trades.lower() in {"yes","y","true","1"} else 0
    offers  = 1 if taking_offers.lower() in {"yes","y","true","1"} else 0
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO market_listings (guild_id,user_id,type,item_name,quantity,price,accepts_trades,taking_offers,notes,created_ts,is_active) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,1)",
            (ctx.guild.id, ctx.author.id, t, item, int(qty), price, accepts, offers, notes, now_ts())
        )
        await db.commit()
        c = await db.execute("SELECT last_insert_rowid()")
        listing_id = (await c.fetchone())[0]
    await ctx.send(f"✅ Listing **#{listing_id}** created.")
    # Optionally render a detail view (UI/embeds live in Section 3)
    if 'build_market_listing_embed' in globals():
        try:
            emb, view = await build_market_listing_embed(ctx.guild.id, listing_id, ctx.author)
            await ctx.send(embed=emb, view=view)
        except Exception:
            pass

@market_group.command(name="list")
async def market_list(ctx, t: Optional[str] = None, *, search: str = ""):
    cond = []; params: List[Any] = [ctx.guild.id]
    sql = "SELECT id,type,item_name,quantity,price,accepts_trades,taking_offers,notes,user_id,is_active,created_ts FROM market_listings WHERE guild_id=?"
    if t:
        tt = t.strip().upper()
        if tt in {"BUY","SELL"}:
            sql += " AND type=?"; params.append(tt)
    if search:
        sql += " AND item_name LIKE ?"; params.append(f"%{search}%")
    sql += " AND is_active=1 ORDER BY created_ts DESC LIMIT 20"
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute(sql, tuple(params))
        rows = await c.fetchall()
    if not rows:
        return await ctx.send("No listings.")
    lines = []
    for rid, typ, name, qty, price, acc, off, notes, uid, active, ts in rows:
        lines.append(f"• **#{rid}** [{typ}] {name} ×{qty} — {price or '—'} {'(trades ok)' if acc else ''} {'(taking offers)' if off else ''}")
    msg = "\n".join(lines)
    if len(msg) > 1900: 
        msg = msg[:1895] + "…"
    await ctx.send(msg)

@market_group.command(name="mine")
async def market_mine(ctx):
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute(
            "SELECT id,type,item_name,quantity,price,is_active FROM market_listings WHERE guild_id=? AND user_id=? ORDER BY created_ts DESC",
            (ctx.guild.id, ctx.author.id)
        )
        rows = await c.fetchall()
    if not rows:
        return await ctx.send("You have no listings.")
    lines = []
    for rid, typ, name, qty, price, active in rows:
        lines.append(f"• **#{rid}** [{typ}] {name} ×{qty} — {price or '—'} {'(active)' if active else '(closed)'}")
    await ctx.send("\n".join(lines))

@market_group.command(name="view")
async def market_view(ctx, listing_id: int):
    if 'build_market_listing_embed' in globals():
        try:
            emb, view = await build_market_listing_embed(ctx.guild.id, int(listing_id), ctx.author)
            return await ctx.send(embed=emb, view=view)
        except Exception as e:
            return await ctx.send(f"Could not render listing: {e}")
    await ctx.send("Listing view UI is not loaded yet.")

@market_group.command(name="edit")
async def market_edit(ctx, listing_id: int, field: str, *, value: str):
    allowed = {"item_name","quantity","price","accepts_trades","taking_offers","notes","is_active"}
    if field not in allowed:
        return await ctx.send(f"Editable fields: {', '.join(sorted(allowed))}")
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT user_id FROM market_listings WHERE guild_id=? AND id=?", (ctx.guild.id, int(listing_id)))
        r = await c.fetchone()
        if not r:
            return await ctx.send("Listing not found.")
        if r[0] != ctx.author.id and not ctx.author.guild_permissions.manage_messages:
            return await ctx.send("You do not own this listing.")
        if field in {"quantity","is_active"}:
            try:
                ival = int(value)
            except ValueError:
                return await ctx.send("Value must be an integer.")
            await db.execute(f"UPDATE market_listings SET {field}=? WHERE guild_id=? AND id=?", (ival, ctx.guild.id, int(listing_id)))
        elif field in {"accepts_trades","taking_offers"}:
            ival = 1 if value.lower() in {"yes","y","true","1"} else 0
            await db.execute(f"UPDATE market_listings SET {field}=? WHERE guild_id=? AND id=?", (ival, ctx.guild.id, int(listing_id)))
        else:
            await db.execute(f"UPDATE market_listings SET {field}=? WHERE guild_id=? AND id=?", (value, ctx.guild.id, int(listing_id)))
        await db.commit()
    await ctx.send("✅ Updated.")
    # refresh UI if present
    if 'update_market_listing_message' in globals():
        try:
            await update_market_listing_message(ctx.guild.id, int(listing_id))
        except Exception:
            pass

@market_group.command(name="remove")
async def market_remove(ctx, listing_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT user_id FROM market_listings WHERE guild_id=? AND id=?", (ctx.guild.id, int(listing_id)))
        r = await c.fetchone()
        if not r:
            return await ctx.send("Listing not found.")
        if r[0] != ctx.author.id and not ctx.author.guild_permissions.manage_messages:
            return await ctx.send("You do not own this listing.")
        await db.execute("UPDATE market_listings SET is_active=0 WHERE guild_id=? AND id=?", (ctx.guild.id, int(listing_id)))
        await db.commit()
    await ctx.send("🗑️ Listing closed.")
    if 'update_market_listing_message' in globals():
        try:
            await update_market_listing_message(ctx.guild.id, int(listing_id))
        except Exception:
            pass

# ---------- LIXING COMMANDS (core) ----------
@bot.command(name="setlixingchannel")
@commands.has_permissions(manage_guild=True)
async def setlixingchannel_cmd(ctx, channel: discord.TextChannel):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO guild_config (guild_id,lixing_channel_id) VALUES (?,?) "
            "ON CONFLICT(guild_id) DO UPDATE SET lixing_channel_id=excluded.lixing_channel_id",
            (ctx.guild.id, channel.id)
        )
        await db.commit()
    await ctx.send(f"✅ Lixing digest channel set to {channel.mention}.")

@bot.group(name="lixing", invoke_without_command=True)
async def lixing_group(ctx):
    await ctx.send("Use `!lixing post|list|mine|view|remove`")

@lixing_group.command(name="post")
async def lixing_post(ctx, char_name: str, char_class: str, level: str, desired_lixes: str = "N/A", *, notes: str = ""):
    level_int: Optional[int] = None
    if level.isdigit():
        level_int = int(level)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO lixing_posts (guild_id,user_id,char_name,char_class,level,desired_lixes,notes,created_ts,is_active) "
            "VALUES (?,?,?,?,?,?,?,?,1)",
            (ctx.guild.id, ctx.author.id, char_name, char_class, level_int, desired_lixes, notes, now_ts())
        )
        await db.commit()
        c = await db.execute("SELECT last_insert_rowid()")
        post_id = (await c.fetchone())[0]
    await ctx.send(f"✅ Lixing post **#{post_id}** created.")
    if 'build_lixing_post_embed' in globals():
        try:
            emb = await build_lixing_post_embed(ctx.guild.id, post_id)
            await ctx.send(embed=emb)
        except Exception:
            pass

@lixing_group.command(name="list")
async def lixing_list(ctx, class_filter: str = "", level_range: str = "", *, search: str = ""):
    sql = "SELECT id,char_name,char_class,COALESCE(level,-1),desired_lixes,notes,user_id,created_ts FROM lixing_posts WHERE guild_id=? AND is_active=1"
    params: List[Any] = [ctx.guild.id]
    if class_filter:
        sql += " AND LOWER(char_class)=?"
        params.append(class_filter.lower())
    if level_range and "-" in level_range:
        try:
            lo, hi = level_range.split("-", 1)
            lo_i = int(lo.strip()); hi_i = int(hi.strip())
            sql += " AND (level IS NULL OR (level>=? AND level<=?))"
            params.extend([lo_i, hi_i])
        except Exception:
            pass
    if search:
        sql += " AND (char_name LIKE ? OR notes LIKE ?)"
        like = f"%{search}%"
        params.extend([like, like])
    sql += " ORDER BY created_ts DESC LIMIT 30"
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute(sql, tuple(params))
        rows = await c.fetchall()
    if not rows:
        return await ctx.send("No lixing posts.")
    lines = []
    for pid, name, cls, lvl, dlx, notes, uid, ts in rows:
        lvl_txt = "N/A" if int(lvl) < 0 else str(lvl)
        lines.append(f"• **#{pid}** {name} ({cls} {lvl_txt}) — Lixes: {dlx} {'— ' + notes if notes else ''}")
    msg = "\n".join(lines)
    if len(msg) > 1900:
        msg = msg[:1895] + "…"
    await ctx.send(msg)

@lixing_group.command(name="mine")
async def lixing_mine(ctx):
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute(
            "SELECT id,char_name,char_class,COALESCE(level,-1),is_active FROM lixing_posts WHERE guild_id=? AND user_id=? ORDER BY created_ts DESC",
            (ctx.guild.id, ctx.author.id)
        )
        rows = await c.fetchall()
    if not rows:
        return await ctx.send("You have no lixing posts.")
    lines = []
    for pid, name, cls, lvl, active in rows:
        lvl_txt = "N/A" if int(lvl) < 0 else str(lvl)
        lines.append(f"• **#{pid}** {name} ({cls} {lvl_txt}) {'(active)' if active else '(closed)'}")
    await ctx.send("\n".join(lines))

@lixing_group.command(name="view")
async def lixing_view(ctx, post_id: int):
    if 'build_lixing_post_embed' in globals():
        try:
            emb = await build_lixing_post_embed(ctx.guild.id, int(post_id))
            return await ctx.send(embed=emb)
        except Exception as e:
            return await ctx.send(f"Could not render post: {e}")
    await ctx.send("Lixing view UI is not loaded yet.")

@lixing_group.command(name="remove")
async def lixing_remove(ctx, post_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT user_id FROM lixing_posts WHERE guild_id=? AND id=?", (ctx.guild.id, int(post_id)))
        r = await c.fetchone()
        if not r:
            return await ctx.send("Post not found.")
        if r[0] != ctx.author.id and not ctx.author.guild_permissions.manage_messages:
            return await ctx.send("You do not own this post.")
        await db.execute("UPDATE lixing_posts SET is_active=0 WHERE guild_id=? AND id=?", (ctx.guild.id, int(post_id)))
        await db.commit()
    await ctx.send("🗑️ Lixing post closed.")

# ---------- NOTE ----------
# The interactive UI for Market (Make Offer button & modal), the digest loops
# for Market/Lixing, advanced timers (/timers view with toggles), and the
# improved timers loop belong to Sections 3 and 4. They reference the DB and
# helpers defined here and in Section 1.
# ============================================
# SECTION 3 / 4 — UI, Embeds, Modals, Interactive Views
# ============================================

from discord import ui, ButtonStyle

# --------------------------------------------
# Utilities
# --------------------------------------------

def _short_user(u: Optional[discord.abc.User]) -> str:
    if not u:
        return "Unknown"
    tag = f"{u.name}#{u.discriminator}" if getattr(u, "discriminator", None) else u.name
    return f"{u.display_name} ({tag})"

async def _fetch_member(guild: discord.Guild, user_id: int) -> Optional[discord.Member]:
    if not guild:
        return None
    m = guild.get_member(user_id)
    if m:
        return m
    try:
        return await guild.fetch_member(user_id)
    except Exception:
        return None

# --------------------------------------------
# MARKET — Embeds, View, Modal, and Message Updaters
# --------------------------------------------
# DB expectations:
#   market_listings(
#     id, guild_id, user_id, type, item_name, quantity, price,
#     accepts_trades, taking_offers, notes, created_ts, is_active,
#     message_channel_id NULL, message_id NULL
#   )
#   market_offers(id, guild_id, listing_id, offerer_id, offer_text, note, created_ts)

async def _get_market_listing(guild_id: int, listing_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("""
            SELECT id,guild_id,user_id,type,item_name,quantity,price,accepts_trades,taking_offers,notes,created_ts,is_active,
                   COALESCE(message_channel_id,0),COALESCE(message_id,0)
            FROM market_listings
            WHERE guild_id=? AND id=?
        """, (guild_id, listing_id))
        return await c.fetchone()

async def _get_market_offers(guild_id: int, listing_id: int, limit: int = 10):
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("""
            SELECT id,offerer_id,offer_text,COALESCE(note,''),created_ts
            FROM market_offers
            WHERE guild_id=? AND listing_id=?
            ORDER BY created_ts DESC
            LIMIT ?
        """, (guild_id, listing_id, limit))
        return await c.fetchall()

def _fmt_price(price: Optional[str]) -> str:
    price = (price or "").strip()
    return price if price else "—"

def _yn(flag: Any) -> str:
    try:
        return "Yes" if int(flag) else "No"
    except Exception:
        return "No"

async def build_market_listing_embed(guild_id: int, listing_id: int, viewer: Optional[discord.Member] = None):
    """Return (Embed, View) for a listing with most recent offers attached."""
    row = await _get_market_listing(guild_id, listing_id)
    if not row:
        em = discord.Embed(title=f"Listing #{listing_id}", description="(not found)", color=0xe74c3c)
        return em, None

    (rid, gid, owner_id, typ, item, qty, price, acc_trades, taking_offers,
     notes, cts, active, msg_ch_id, msg_id) = row

    guild = bot.get_guild(gid)
    owner = await _fetch_member(guild, owner_id)

    em = discord.Embed(
        title=f"Market Listing #{rid} — {typ}",
        color=0x1abc9c if typ == "SELL" else 0xf1c40f
    )
    em.add_field(name="Item", value=f"**{item}** ×{qty}", inline=True)
    em.add_field(name="Price", value=_fmt_price(price), inline=True)
    em.add_field(name="Active", value="Yes" if int(active) else "No", inline=True)

    em.add_field(name="Accepts Trades", value=_yn(acc_trades), inline=True)
    em.add_field(name="Taking Offers", value=_yn(taking_offers), inline=True)
    em.add_field(name="Owner", value=(owner.mention if owner else f"<@{owner_id}>"), inline=True)

    if notes:
        em.add_field(name="Notes", value=notes[:1024], inline=False)

    # Offers preview
    offers = await _get_market_offers(gid, rid, limit=10)
    if offers:
        lines = []
        for oid, offerer_id, offer_text, onote, ots in offers:
            offerer = await _fetch_member(guild, offerer_id)
            offerer_disp = offerer.mention if offerer else f"<@{offerer_id}>"
            line = f"• **#{oid}** {offerer_disp}: {offer_text}"
            if onote:
                line += f" — _{onote[:100]}_"
            lines.append(line)
        joined = "\n".join(lines)
        if len(joined) > 1024:
            joined = joined[:1019] + "…"
        em.add_field(name="Recent Offers", value=joined, inline=False)
    else:
        em.add_field(name="Recent Offers", value="No offers yet.", inline=False)

    # Footer/meta
    em.set_footer(text=f"Listing owner: {_short_user(owner)} • ID #{rid}")

    # Build view
    view = MarketListingView(listing_id=rid, guild_id=gid, owner_id=owner_id, active=bool(active), taking_offers=bool(int(taking_offers)))
    return em, view

class OfferModal(ui.Modal, title="Make an Offer"):
    def __init__(self, guild_id: int, listing_id: int, owner_id: int):
        super().__init__(timeout=120)
        self.guild_id = guild_id
        self.listing_id = listing_id
        self.owner_id = owner_id

        self.offer = ui.TextInput(label="Your offer (amount / item)", placeholder="e.g. 3m gold or 150k + items", max_length=150, required=True)
        self.note  = ui.TextInput(label="Message (optional)", style=discord.TextStyle.paragraph, required=False, max_length=300)
        self.add_item(self.offer)
        self.add_item(self.note)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        # Persist offer
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("""
                INSERT INTO market_offers (guild_id, listing_id, offerer_id, offer_text, note, created_ts)
                VALUES (?,?,?,?,?,?)
            """, (self.guild_id, self.listing_id, interaction.user.id, str(self.offer.value).strip(), str(self.note.value).strip(), now_ts()))
            await db.commit()

        # Update embedded listing if we can locate a message to edit
        try:
            await update_market_listing_message(self.guild_id, self.listing_id)
        except Exception:
            pass

        # Notify the owner (public ping in the same channel of the interaction, if possible)
        try:
            await interaction.response.send_message(
                content=f"✅ Offer posted on listing #{self.listing_id}. <@{self.owner_id}> you have a new offer.",
                ephemeral=False
            )
        except Exception:
            # As a fallback, try ephemeral confirmation
            try:
                await interaction.followup.send("✅ Offer saved.", ephemeral=True)
            except Exception:
                pass

class MarketListingView(ui.View):
    def __init__(self, listing_id: int, guild_id: int, owner_id: int, active: bool, taking_offers: bool):
        super().__init__(timeout=600)
        self.listing_id = listing_id
        self.guild_id = guild_id
        self.owner_id = owner_id
        self._active = active
        self._taking_offers = taking_offers

        # Buttons
        self.add_item(ui.Button(label="Make Offer", style=ButtonStyle.success, custom_id=f"mkoffer:{guild_id}:{listing_id}"))
        if taking_offers:
            self.children[-1].callback = self._cb_make_offer  # type: ignore

        # Owner controls
        self.add_item(ui.Button(label=("Close" if active else "Open"), style=ButtonStyle.secondary, custom_id=f"mkstate:{guild_id}:{listing_id}"))
        self.children[-1].callback = self._cb_toggle_active  # type: ignore

        self.add_item(ui.Button(label="Refresh", style=ButtonStyle.primary, custom_id=f"mkrefresh:{guild_id}:{listing_id}"))
        self.children[-1].callback = self._cb_refresh  # type: ignore

    async def _cb_make_offer(self, interaction: discord.Interaction):
        # Check if listing accepts offers
        row = await _get_market_listing(self.guild_id, self.listing_id)
        if not row:
            return await interaction.response.send_message("Listing not found.", ephemeral=True)
        taking_offers = bool(int(row[9]))
        if not taking_offers:
            return await interaction.response.send_message("This listing is not taking offers.", ephemeral=True)

        modal = OfferModal(self.guild_id, self.listing_id, owner_id=row[2])
        await interaction.response.send_modal(modal)

    async def _cb_toggle_active(self, interaction: discord.Interaction):
        # Only owner or moderator can toggle
        row = await _get_market_listing(self.guild_id, self.listing_id)
        if not row:
            return await interaction.response.send_message("Listing not found.", ephemeral=True)
        owner_id = int(row[2])
        if interaction.user.id != owner_id and not interaction.user.guild_permissions.manage_messages:
            return await interaction.response.send_message("Only the owner or a moderator can change status.", ephemeral=True)

        new_active = 0 if int(row[11]) else 1
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE market_listings SET is_active=? WHERE guild_id=? AND id=?", (new_active, self.guild_id, self.listing_id))
            await db.commit()
        await update_market_listing_message(self.guild_id, self.listing_id)
        try:
            await interaction.response.send_message(f"✅ Listing #{self.listing_id} is now {'active' if new_active else 'closed'}.", ephemeral=True)
        except Exception:
            pass

    async def _cb_refresh(self, interaction: discord.Interaction):
        try:
            await update_market_listing_message(self.guild_id, self.listing_id)
            await interaction.response.send_message("🔄 Refreshed.", ephemeral=True)
        except Exception as e:
            await interaction.response.send_message(f"Could not refresh: {e}", ephemeral=True)

async def update_market_listing_message(guild_id: int, listing_id: int):
    """If the listing has a recorded message, refresh it in-place. (Used after edits/offers.)"""
    row = await _get_market_listing(guild_id, listing_id)
    if not row:
        return
    (rid, gid, owner_id, typ, item, qty, price, acc_trades, taking_offers,
     notes, cts, active, msg_ch_id, msg_id) = row
    if not msg_ch_id or not msg_id:
        return  # nothing to update

    guild = bot.get_guild(gid)
    ch = guild.get_channel(int(msg_ch_id)) if guild else None
    if not ch or not can_send(ch):
        return
    try:
        msg = await ch.fetch_message(int(msg_id))
    except Exception:
        return

    emb, view = await build_market_listing_embed(gid, listing_id)
    try:
        await msg.edit(embed=emb, view=view)
    except Exception:
        pass

# --------------------------------------------
# LIXING — Embeds (simple, readable)
# --------------------------------------------

async def build_lixing_post_embed(guild_id: int, post_id: int) -> discord.Embed:
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("""
            SELECT id,guild_id,user_id,char_name,char_class,COALESCE(level,-1),desired_lixes,COALESCE(notes,''),created_ts,is_active
            FROM lixing_posts
            WHERE guild_id=? AND id=?
        """, (guild_id, post_id))
        row = await c.fetchone()
    if not row:
        return discord.Embed(title=f"Lixing Post #{post_id}", description="(not found)", color=0xe74c3c)

    (pid, gid, uid, cname, cclass, lvl, dlx, notes, cts, active) = row
    guild = bot.get_guild(gid)
    owner = await _fetch_member(guild, uid)

    em = discord.Embed(title=f"Lixing Post #{pid}", color=0x9b59b6 if int(active) else 0x95a5a6)
    em.add_field(name="Character", value=cname, inline=True)
    em.add_field(name="Class", value=cclass, inline=True)
    em.add_field(name="Level", value=("N/A" if int(lvl) < 0 else str(lvl)), inline=True)
    em.add_field(name="Desired Lixes", value=(dlx or "N/A"), inline=True)
    em.add_field(name="Active", value=("Yes" if int(active) else "No"), inline=True)
    em.add_field(name="Owner", value=(owner.mention if owner else f"<@{uid}>"), inline=True)
    if notes:
        em.add_field(name="Notes", value=notes[:1024], inline=False)
    em.set_footer(text=f"Owned by {_short_user(owner)} • ID #{pid}")
    return em

# --------------------------------------------
# /timers — per-user category toggles (ephemeral)
# --------------------------------------------

async def get_user_shown_categories(guild_id: int, user_id: int) -> List[str]:
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT categories FROM user_timer_prefs WHERE guild_id=? AND user_id=?", (guild_id, user_id))
        r = await c.fetchone()
    if not r or not r[0]:
        return []
    raw = [norm_cat(x.strip()) for x in r[0].split(",") if x.strip()]
    # preserve category order
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

async def get_show_eta(guild_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT COALESCE(show_eta,0) FROM guild_config WHERE guild_id=?", (guild_id,))
        r = await c.fetchone()
        return bool(r and int(r[0]) == 1)

def window_label(now: int, next_ts: int, window_m: int) -> str:
    """
    Return window state text:
      - "{window_m}m (pending)" if window not opened yet
      - "{left_m}m left (open)" during the window
      - "closed" after window closes, until NADA_GRACE_SECONDS
      - "-Nada" once grace is exceeded
    """
    delta = next_ts - now
    if delta >= 0:
        return f"{window_m}m (pending)"
    open_secs = -delta
    if open_secs <= window_m * 60:
        left_m = max(0, (window_m * 60 - open_secs) // 60)
        return f"{left_m}m left (open)"
    after_close = open_secs - window_m * 60
    if after_close <= NADA_GRACE_SECONDS:
        return "closed"
    return "-Nada"

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
        normal: List[tuple] = []
        nada_list: List[tuple] = []
        for sk, nm, ts, win in items:
            delta = ts - now
            t = fmt_delta_for_list(delta)
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
    guild = interaction.guild
    if not guild:
        return await interaction.response.send_message("Use this in a server.", ephemeral=True)
    saved = await get_user_shown_categories(guild.id, interaction.user.id)
    view = TimerToggleView(guild=guild, user_id=interaction.user.id, init_show=saved)
    embeds = await build_timer_embeds_for_categories(guild, view.shown)
    await interaction.response.send_message(
        content=f"**Categories shown:** {', '.join(view.shown) if view.shown else '(none)'}",
        embeds=embeds,
        view=view,
        ephemeral=True
    )
    view.message = await interaction.original_response()
# ============================================
# SECTION 4 / 4 — Runtime Loops, Digests, Startup/Shutdown
# ============================================

from discord.ext import tasks

CHECK_INTERVAL_SECONDS = 15
DEFAULT_UPTIME_MINUTES = 60
NADA_GRACE_SECONDS = 1800  # 30m after window closes -> becomes -Nada

_last_timer_tick_ts: int = 0
_prev_timer_tick_ts: int = 0

# -------------------- TIMER LOOPS --------------------

@tasks.loop(seconds=CHECK_INTERVAL_SECONDS)
async def timers_tick():
    """Drives pre-announces and window-open announces; sends sub pings to configured channel."""
    global _last_timer_tick_ts, _prev_timer_tick_ts
    now = now_ts()
    prev = _last_timer_tick_ts or (now - CHECK_INTERVAL_SECONDS)
    _prev_timer_tick_ts = prev
    _last_timer_tick_ts = now
    try:
        await meta_set("last_tick_ts", str(_last_timer_tick_ts))
    except Exception:
        pass

    # Pre-announces
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute(
            "SELECT id,guild_id,channel_id,name,next_spawn_ts,pre_announce_min,category "
            "FROM bosses WHERE next_spawn_ts > ?",
            (now,)
        )
        rows = await c.fetchall()

    for bid, gid, ch_id, name, next_ts, pre, cat in rows:
        if not pre or pre <= 0:
            continue
        pre_ts = int(next_ts) - int(pre) * 60
        if prev < pre_ts <= now:
            key = f"{gid}:{bid}:PRE:{next_ts}"
            if key in bot._seen_keys:
                continue
            bot._seen_keys.add(key)
            ch = await resolve_announce_channel(gid, ch_id, cat)
            if ch and can_send(ch):
                left = max(0, int(next_ts) - now)
                try:
                    await ch.send(f"⏳ **{name}** — **Spawn Time**: `{fmt_delta_for_list(left)}` (almost up).")
                except Exception as e:
                    log.warning(f"Pre announce failed: {e}")
            await send_subscription_ping(gid, bid, phase="pre", boss_name=name, when_left=max(0, int(next_ts) - now))

    # Window opens
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute(
            "SELECT id,guild_id,channel_id,name,next_spawn_ts,category FROM bosses WHERE next_spawn_ts <= ?",
            (now,)
        )
        due_rows = await c.fetchall()

    for bid, gid, ch_id, name, next_ts, cat in due_rows:
        if not (prev < int(next_ts) <= now):
            continue
        key = f"{gid}:{bid}:WINDOW:{next_ts}"
        if key in bot._seen_keys:
            continue
        bot._seen_keys.add(key)
        ch = await resolve_announce_channel(gid, ch_id, cat)
        if ch and can_send(ch):
            try:
                await ch.send(f"🕑 **{name}** — **Spawn Window has opened!**")
            except Exception as e:
                log.warning(f"Window announce failed: {e}")
        await send_subscription_ping(gid, bid, phase="window", boss_name=name)

@tasks.loop(minutes=1.0)
async def uptime_heartbeat():
    """Simple heartbeat so guilds know the bot is alive."""
    now_m = now_ts() // 60
    for g in bot.guilds:
        await upsert_guild_defaults(g.id)
        async with aiosqlite.connect(DB_PATH) as db:
            c = await db.execute("SELECT COALESCE(uptime_minutes, ?), heartbeat_channel_id FROM guild_config WHERE guild_id=?",
                                 (DEFAULT_UPTIME_MINUTES, g.id))
            r = await c.fetchone()
        if not r:
            continue
        minutes, hb_ch_id = int(r[0]), (int(r[1]) if r[1] else None)
        if minutes <= 0 or now_m % minutes != 0:
            continue
        ch = g.get_channel(hb_ch_id) if hb_ch_id else await resolve_heartbeat_channel(g.id)
        if ch and can_send(ch):
            try:
                await ch.send("✅ Bot is online — timers active.")
            except Exception as e:
                log.warning(f"Heartbeat failed: {e}")

# -------------------- OFFLINE CATCH-UP --------------------

async def boot_offline_processing():
    """When coming online after downtime, post a catch-up note for bosses that became due while offline."""
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
            ch = await resolve_announce_channel(gid, ch_id, cat)
            if ch and can_send(ch):
                try:
                    ago = human_ago(boot - int(ts))
                    await ch.send(f":zzz: While I was offline, **{name}** spawned ({ago}).")
                except Exception as e:
                    log.warning(f"Offline notice failed: {e}")
            await send_subscription_ping(gid, bid, phase="window", boss_name=name)

# -------------------- MARKET & LIXING — 6H DIGESTS --------------------

def _chunk_lines(lines: List[str], hard_limit: int = 1900) -> List[str]:
    chunks: List[str] = []
    cur = ""
    for ln in lines:
        if len(cur) + len(ln) + 1 > hard_limit:
            chunks.append(cur)
            cur = ln + "\n"
        else:
            cur += ln + "\n"
    if cur:
        chunks.append(cur)
    return chunks

@tasks.loop(hours=6)
async def market_digest_loop():
    for g in bot.guilds:
        async with aiosqlite.connect(DB_PATH) as db:
            c = await db.execute("SELECT market_channel_id FROM guild_config WHERE guild_id=?", (g.id,))
            r = await c.fetchone()
        ch_id = int(r[0]) if r and r[0] else None
        if not ch_id:
            continue
        ch = g.get_channel(ch_id)
        if not can_send(ch):
            continue

        # Gather active listings
        async with aiosqlite.connect(DB_PATH) as db:
            c = await db.execute("""
                SELECT id,type,item_name,quantity,COALESCE(price,''),accepts_trades,taking_offers,COALESCE(notes,''),user_id,created_ts
                FROM market_listings
                WHERE guild_id=? AND is_active=1
                ORDER BY created_ts DESC
                LIMIT 100
            """, (g.id,))
            rows = await c.fetchall()

        if not rows:
            try:
                await ch.send("🛒 Market Digest — No active listings.")
            except Exception:
                pass
            continue

        lines = []
        for rid, typ, name, qty, price, acc, off, notes, uid, cts in rows:
            user = g.get_member(uid)
            user_disp = (user.mention if user else f"<@{uid}>")
            price_txt = price if price else "—"
            flags = []
            if int(acc): flags.append("trades ok")
            if int(off): flags.append("offers open")
            flag_txt = f" ({', '.join(flags)})" if flags else ""
            note_txt = f" — {notes}" if notes else ""
            lines.append(f"• **#{rid}** [{typ}] {name} ×{qty} — {price_txt}{flag_txt} — by {user_disp}{note_txt}")

        for chunk in _chunk_lines(lines):
            try:
                await ch.send(f"🛒 **Market Digest** (last 6h)\n{chunk}")
            except Exception:
                break

@tasks.loop(hours=6)
async def lixing_digest_loop():
    for g in bot.guilds:
        async with aiosqlite.connect(DB_PATH) as db:
            c = await db.execute("SELECT lixing_channel_id FROM guild_config WHERE guild_id=?", (g.id,))
            r = await c.fetchone()
        ch_id = int(r[0]) if r and r[0] else None
        if not ch_id:
            continue
        ch = g.get_channel(ch_id)
        if not can_send(ch):
            continue

        async with aiosqlite.connect(DB_PATH) as db:
            c = await db.execute("""
                SELECT id,char_name,char_class,COALESCE(level,-1),COALESCE(desired_lixes,'N/A'),COALESCE(notes,''),user_id,created_ts
                FROM lixing_posts
                WHERE guild_id=? AND is_active=1
                ORDER BY created_ts DESC
                LIMIT 100
            """, (g.id,))
            rows = await c.fetchall()

        if not rows:
            try:
                await ch.send("⚔️ Lixing Digest — No active posts.")
            except Exception:
                pass
            continue

        lines = []
        for pid, name, cls, lvl, dlx, notes, uid, cts in rows:
            user = g.get_member(uid)
            user_disp = (user.mention if user else f"<@{uid}>")
            lvl_txt = "N/A" if int(lvl) < 0 else str(lvl)
            note_txt = f" — {notes}" if notes else ""
            lines.append(f"• **#{pid}** {name} ({cls} {lvl_txt}) — Lixes: {dlx} — by {user_disp}{note_txt}")

        for chunk in _chunk_lines(lines):
            try:
                await ch.send(f"⚔️ **Lixing Digest** (last 6h)\n{chunk}")
            except Exception:
                break

# -------------------- SEEDING & MIGRATIONS (LIGHT) --------------------

SEED_VERSION = "v2025-09-12-subping-window-ps-final"

SEED_DATA = [
    # (Category, Name, spawn_m, window_m, aliases)
    # --- Meteoric
    ("Meteoric","Doomclaw",7,5,[]),
    ("Meteoric","Bonehad",15,5,[]),
    ("Meteoric","Rockbelly",15,5,[]),
    ("Meteoric","Redbane",20,5,[]),
    ("Meteoric","Coppinger",20,5,["copp"]),
    ("Meteoric","Goretusk",20,5,[]),
    ("Meteoric","Falgren",45,5,[]),
    # --- Frozen
    ("Frozen","Redbane",20,5,[]),
    ("Frozen","Eye",28,3,[]),
    ("Frozen","Swampie",33,3,["swampy"]),
    ("Frozen","Woody",38,3,[]),
    ("Frozen","Chained",43,3,["chain"]),
    ("Frozen","Grom",48,3,[]),
    ("Frozen","Pyrus",58,3,["py"]),
    # --- DL  (ensure 180 = 88 spawn, 3 window)
    ("DL","155",63,3,[]),
    ("DL","160",68,3,[]),
    ("DL","165",73,3,[]),
    ("DL","170",78,3,[]),
    ("DL","180",88,3,["snorri"]),
    # --- EDL
    ("EDL","185",72,3,[]),
    ("EDL","190",81,3,[]),
    ("EDL","195",89,4,[]),
    ("EDL","200",108,5,[]),
    ("EDL","205",117,4,[]),
    ("EDL","210",125,5,[]),
    ("EDL","215",134,5,["unox"]),
    # --- Midraids
    ("Midraids","Aggorath",1200,960,["aggy"]),
    ("Midraids","Mordris",1200,960,["mord","mordy"]),
    ("Midraids","Necromancer",1320,960,["necro"]),
    ("Midraids","Hrungnir",1320,960,["hrung","muk"]),
    # --- Rings
    ("Rings","North Ring",215,50,["northring"]),
    ("Rings","Center Ring",215,50,["centre","centering"]),
    ("Rings","South Ring",215,50,["southring"]),
    ("Rings","East Ring",215,50,["eastring"]),
    # --- EG
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
    key = f"seed:{SEED_VERSION}:g{guild.id}"
    already = await meta_get(key)
    if already == "done":
        # migration: enforce DL 180 correction if an older row exists
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute(
                    "UPDATE bosses SET spawn_minutes=88, window_minutes=3 "
                    "WHERE guild_id=? AND category='DL' AND name='180' AND (spawn_minutes<>88 OR window_minutes<>3)",
                    (guild.id,)
                )
                await db.commit()
        except Exception:
            pass
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
                # Correct DL 180 if necessary
                if catn == "DL" and name == "180":
                    await db.execute("UPDATE bosses SET spawn_minutes=88, window_minutes=3 WHERE id=?", (bid,))
            else:
                next_spawn = now_ts() - 3601  # default to -Nada
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
                if not al_l or al_l in seen:
                    continue
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
    await refresh_subscription_messages(guild)

# -------------------- EVENTS & STARTUP --------------------

@bot.event
async def on_ready():
    await init_db()
    # ensure tables for market/lixing offer records exist (idempotent safety)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS market_offers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                listing_id INTEGER NOT NULL,
                offerer_id INTEGER NOT NULL,
                offer_text TEXT NOT NULL,
                note TEXT DEFAULT '',
                created_ts INTEGER NOT NULL
            )
        """)
        await db.commit()

    for g in bot.guilds:
        await upsert_guild_defaults(g.id)
    await meta_set("last_startup_ts", str(now_ts()))
    await boot_offline_processing()
    for g in bot.guilds:
        await ensure_seed_for_guild(g)
    if not timers_tick.is_running():
        timers_tick.start()
    if not uptime_heartbeat.is_running():
        uptime_heartbeat.start()
    if not market_digest_loop.is_running():
        market_digest_loop.start()
    if not lixing_digest_loop.is_running():
        lixing_digest_loop.start()
    for g in bot.guilds:
        await refresh_subscription_messages(g)
    try:
        await bot.tree.sync()
    except Exception as e:
        log.warning(f"App command sync failed: {e}")
    log.info(f"Logged in as {bot.user} (ID: {bot.user.id})")

@bot.event
async def on_guild_join(guild):
    await init_db()
    await upsert_guild_defaults(guild.id)
    await ensure_seed_for_guild(guild)
    await refresh_subscription_messages(guild)
    try:
        await bot.tree.sync(guild=guild)
    except Exception:
        pass

# Quick shorthand kill/reset via prefix (only if not reserved)
@bot.event
async def on_message(message: discord.Message):
    if message.author.bot or not message.guild:
        return
    if await is_blacklisted(message.guild.id, message.author.id):
        return
    prefix = await get_guild_prefix(bot, message)
    content = (message.content or "").strip()
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

# -------------------- SHUTDOWN --------------------

async def graceful_shutdown(_sig=None):
    try:
        await meta_set("offline_since", str(now_ts()))
    finally:
        try:
            if timers_tick.is_running():
                timers_tick.cancel()
            if uptime_heartbeat.is_running():
                uptime_heartbeat.cancel()
            if market_digest_loop.is_running():
                market_digest_loop.cancel()
            if lixing_digest_loop.is_running():
                lixing_digest_loop.cancel()
        except Exception:
            pass
        await bot.close()

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
    loop = asyncio.get_running_loop()
    for s in (getattr(signal, "SIGINT", None), getattr(signal, "SIGTERM", None)):
        if s:
            try:
                loop.add_signal_handler(s, lambda sig=s: asyncio.create_task(graceful_shutdown(sig)))
            except NotImplementedError:
                pass
    try:
        await bot.start(TOKEN)
    except KeyboardInterrupt:
        await graceful_shutdown()

if __name__ == "__main__":
    asyncio.run(main())

