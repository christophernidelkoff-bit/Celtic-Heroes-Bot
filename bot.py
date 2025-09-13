# ============================================
# SECTION 1 / 4 — Imports, ENV, Logging, Intents, Core Helpers,
#                 Auth Guard, DB Boot (sync+async), Blacklist, Colors
# ============================================

from __future__ import annotations

import os
import re
import io
import atexit
import signal
import asyncio
import logging
import shutil
from typing import Optional, Tuple, List, Dict, Any, Set
from datetime import datetime, timezone

import aiosqlite
import discord
from discord.ext import commands
from discord import app_commands
from dotenv import load_dotenv

# -------------------- ENV / GLOBALS --------------------
load_dotenv()

TOKEN = os.getenv("DISCORD_TOKEN") or os.getenv("TOKEN")
if not TOKEN:
    raise SystemExit("DISCORD_TOKEN (or TOKEN) missing in environment")

ALLOW_POWERSHELL = os.getenv("ALLOW_POWERSHELL", "0") in {"1", "true", "True", "yes", "YES"}
DB_PATH = os.getenv("DB_PATH", "/data/bosses.db")  # use /data on Render persistent disk by default

DEFAULT_PREFIX = "!"
CHECK_INTERVAL_SECONDS = 15
DEFAULT_UPTIME_MINUTES = 60
NADA_GRACE_SECONDS = 1800  # 30 minutes after window close before showing -Nada

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ch-bossbot")

# -------------------- DISCORD INTENTS / BOT --------------------
intents = discord.Intents.default()
intents.guilds = True
intents.members = True
intents.reactions = True
intents.message_content = True

async def get_guild_prefix(_bot, message: discord.Message):
    if not message or not message.guild:
        return DEFAULT_PREFIX
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            c = await db.execute(
                "SELECT COALESCE(prefix, ?) FROM guild_config WHERE guild_id=?",
                (DEFAULT_PREFIX, message.guild.id),
            )
            r = await c.fetchone()
            if r and r[0]:
                return r[0]
    except Exception:
        pass
    return DEFAULT_PREFIX

bot = commands.Bot(command_prefix=get_guild_prefix, intents=intents, help_command=None)

# Slash tree is accessed via bot.tree
RESERVED_TRIGGERS = {
    "help", "boss", "timers", "setprefix", "seed_import",
    "setsubchannel", "setsubpingchannel", "showsubscriptions", "setuptime",
    "setheartbeatchannel", "setannounce", "seteta", "status", "health",
    "setcatcolor", "intervals", "market", "lixing", "reslash",
}

# Seen keys for announce de-dupe
bot._seen_keys = set()

# -------------------- TIME / TEXT HELPERS --------------------
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

def human_ago(seconds: int) -> str:
    if seconds < 60:
        return "just now"
    m, _ = divmod(seconds, 60)
    h, m = divmod(m, 60)
    return f"{h}h {m}m ago" if h else f"{m}m ago"

def fmt_delta_for_list(delta_s: int) -> str:
    """
    Display remaining/overdue time. While within NADA_GRACE_SECONDS after window closes,
    show a negative minute count (e.g., "-12m"). Only after grace, show "-Nada".
    """
    if delta_s <= 0:
        overdue = -delta_s
        return "-Nada" if overdue > NADA_GRACE_SECONDS else f"-{overdue // 60}m"
    m, s = divmod(delta_s, 60)
    h, m = divmod(m, 60)
    parts = []
    if h:
        parts.append(f"{h}h")
    if m:
        parts.append(f"{m}m")
    if not parts:
        parts.append(f"{s}s")
    return " ".join(parts)

def window_label(now: int, next_ts: int, window_m: int) -> str:
    """
    Window status string:
    - if next_ts in future -> "<window_m>m (pending)"
    - if during window     -> "<X>m left (open)" where X is minutes left in window
    - if just closed       -> "closed" (until -Nada grace elapses)
    - if beyond grace      -> "-Nada"
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

# -------------------- CATEGORY / COLORS / EMOJI --------------------
CATEGORY_ORDER = ["Warden", "Meteoric", "Frozen", "DL", "EDL", "Midraids", "Rings", "EG", "Default"]

def norm_cat(c: Optional[str]) -> str:
    c = (c or "Default").strip()
    cl = c.lower()
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

# -------------------- PERMISSIONS / CHANNEL HELPERS --------------------
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

# -------------------- AUTH GUARD (bot only works if @blunderbusstin is present) --------------------
REQUIRED_USER_TAG = os.getenv("REQUIRED_USER_TAG", "blunderbusstin")  # case-insensitive substring of username/nickname

async def ensure_guild_auth(guild: Optional[discord.Guild]) -> bool:
    """
    Returns True if the required user is found in the guild (by username/nick contains REQUIRED_USER_TAG).
    If not found, the bot should avoid executing commands/features for that guild.
    """
    if not guild:
        return False
    tag = (REQUIRED_USER_TAG or "").lower().strip()
    if not tag:
        return True  # no restriction configured
    try:
        for m in guild.members:
            name_blend = f"{m.name} {m.display_name}".lower()
            if tag in name_blend:
                return True
    except Exception:
        pass
    return False

# -------------------- DB BOOT (SYNC MIGRATIONS) --------------------
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
        sort_key TEXT DEFAULT ''
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS guild_config (
        guild_id INTEGER PRIMARY KEY,
        default_channel INTEGER DEFAULT NULL,
        prefix TEXT DEFAULT NULL
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)""")
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
    # Market / Lixing tables (created here so Section 2 commands can assume presence)
    cur.execute("""CREATE TABLE IF NOT EXISTS market_listings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        guild_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        type TEXT NOT NULL,              -- 'buy' or 'sell'
        item_name TEXT NOT NULL,
        quantity INTEGER NOT NULL,
        price TEXT DEFAULT '',
        accepts_trades INTEGER DEFAULT 0,
        taking_offers INTEGER DEFAULT 0,
        notes TEXT DEFAULT '',
        is_active INTEGER DEFAULT 1,
        message_id INTEGER DEFAULT NULL,  -- listing message (for buttons)
        channel_id INTEGER DEFAULT NULL,  -- channel where listing is posted
        created_ts INTEGER NOT NULL
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS market_offers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        guild_id INTEGER NOT NULL,
        listing_id INTEGER NOT NULL,
        offerer_id INTEGER NOT NULL,
        offer_text TEXT NOT NULL,
        note TEXT DEFAULT '',
        created_ts INTEGER NOT NULL
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS lixing_posts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        guild_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        char_name TEXT NOT NULL,
        char_class TEXT NOT NULL,
        level INTEGER DEFAULT -1,
        desired_lixes TEXT DEFAULT 'N/A',
        notes TEXT DEFAULT '',
        is_active INTEGER DEFAULT 1,
        message_id INTEGER DEFAULT NULL,
        channel_id INTEGER DEFAULT NULL,
        created_ts INTEGER NOT NULL
    )""")
    conn.commit()
    conn.close()

preflight_migrate_sync()

# -------------------- ASYNC DB HELPERS --------------------
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
            guild_id INTEGER NOT NULL,
            category TEXT NOT NULL,
            color_hex TEXT NOT NULL,
            PRIMARY KEY (guild_id, category)
        )""")
        await db.execute("""CREATE TABLE IF NOT EXISTS subscription_emojis (
            guild_id INTEGER NOT NULL,
            boss_id INTEGER NOT NULL,
            emoji TEXT NOT NULL,
            PRIMARY KEY (guild_id, boss_id)
        )""")
        await db.execute("""CREATE TABLE IF NOT EXISTS subscription_members (
            guild_id INTEGER NOT NULL,
            boss_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            PRIMARY KEY (guild_id, boss_id, user_id)
        )""")
        await db.execute("""CREATE TABLE IF NOT EXISTS boss_aliases (
            guild_id INTEGER NOT NULL,
            boss_id INTEGER NOT NULL,
            alias TEXT NOT NULL,
            UNIQUE (guild_id, alias)
        )""")
        await db.execute("""CREATE TABLE IF NOT EXISTS category_channels (
            guild_id INTEGER NOT NULL,
            category TEXT NOT NULL,
            channel_id INTEGER NOT NULL,
            PRIMARY KEY (guild_id, category)
        )""")
        await db.execute("""CREATE TABLE IF NOT EXISTS user_timer_prefs (
            guild_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            categories TEXT NOT NULL,
            PRIMARY KEY (guild_id, user_id)
        )""")
        await db.execute("""CREATE TABLE IF NOT EXISTS subscription_panels (
            guild_id INTEGER NOT NULL,
            category TEXT NOT NULL,
            message_id INTEGER NOT NULL,
            channel_id INTEGER DEFAULT NULL,
            PRIMARY KEY (guild_id, category)
        )""")
        await db.execute("""CREATE TABLE IF NOT EXISTS rr_panels (
            message_id INTEGER PRIMARY KEY,
            guild_id INTEGER NOT NULL,
            channel_id INTEGER NOT NULL,
            title TEXT DEFAULT ''
        )""")
        await db.execute("""CREATE TABLE IF NOT EXISTS rr_map (
            panel_message_id INTEGER NOT NULL,
            emoji TEXT NOT NULL,
            role_id INTEGER NOT NULL,
            PRIMARY KEY (panel_message_id, emoji)
        )""")
        await db.execute("""CREATE TABLE IF NOT EXISTS blacklist (
            guild_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            PRIMARY KEY (guild_id, user_id)
        )""")
        # market/lixing tables already created by preflight; ensure existence here too
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
            is_active INTEGER DEFAULT 1,
            message_id INTEGER DEFAULT NULL,
            channel_id INTEGER DEFAULT NULL,
            created_ts INTEGER NOT NULL
        )""")
        await db.execute("""CREATE TABLE IF NOT EXISTS market_offers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id INTEGER NOT NULL,
            listing_id INTEGER NOT NULL,
            offerer_id INTEGER NOT NULL,
            offer_text TEXT NOT NULL,
            note TEXT DEFAULT '',
            created_ts INTEGER NOT NULL
        )""")
        await db.execute("""CREATE TABLE IF NOT EXISTS lixing_posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            char_name TEXT NOT NULL,
            char_class TEXT NOT NULL,
            level INTEGER DEFAULT -1,
            desired_lixes TEXT DEFAULT 'N/A',
            notes TEXT DEFAULT '',
            is_active INTEGER DEFAULT 1,
            message_id INTEGER DEFAULT NULL,
            channel_id INTEGER DEFAULT NULL,
            created_ts INTEGER NOT NULL
        )""")
        await db.commit()

async def upsert_guild_defaults(guild_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO guild_config (guild_id, prefix, uptime_minutes, show_eta) VALUES (?,?,?,?) "
            "ON CONFLICT(guild_id) DO NOTHING",
            (guild_id, DEFAULT_PREFIX, DEFAULT_UPTIME_MINUTES, 0),
        )
        await db.commit()

async def meta_set(key: str, value: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO meta(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
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
        # auth guard: require REQUIRED_USER_TAG to be in guild
        if not await ensure_guild_auth(ctx.guild):
            try:
                await ctx.send(":lock: This bot is disabled in this server.")
            except Exception:
                pass
            return False
        return True
    return commands.check(predicate)

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
    await ctx.send(f":no_entry: **{user.display_name}** is now blacklisted.")

@blacklist_group.command(name="remove")
@commands.has_permissions(manage_guild=True)
async def blacklist_remove(ctx, user: discord.Member):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM blacklist WHERE guild_id=? AND user_id=?", (ctx.guild.id, user.id))
        await db.commit()
    await ctx.send(f":white_check_mark: **{user.display_name}** removed from blacklist.")

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

bot.add_check(blacklist_check())

# -------------------- COLORS --------------------
async def get_category_color(guild_id: int, category: str) -> int:
    category = norm_cat(category)
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT color_hex FROM category_colors WHERE guild_id=? AND category=?", (guild_id, category))
        r = await c.fetchone()
    if r and r[0]:
        try:
            return int(r[0].lstrip("#"), 16)
        except Exception:
            pass
    return DEFAULT_COLORS.get(category, DEFAULT_COLORS["Default"])

@bot.command(name="setcatcolor")
@commands.has_permissions(manage_guild=True)
async def setcatcolor_cmd(ctx, category: str, hexcolor: str):
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
            (ctx.guild.id, cat, f"#{h}"),
        )
        await db.commit()
    await ctx.send(f":white_check_mark: Color for **{cat}** set to `#{h}`.")
# ======================================================
# SECTION 2 / 4 — Boss resolve • Channels • Sub Panels
#                  Timers (cmd + slash) • Slash Sync
#                  Status/Health/Help • Settings
#                  Market & Lixing (UI + buttons/modals)
# ======================================================

# -------------------- RESOLVE HELPERS --------------------
async def resolve_boss(ctx_or_msg, identifier: str) -> Tuple[Optional[tuple], Optional[str]]:
    gid = ctx_or_msg.guild.id
    ident_lc = (identifier or "").strip().lower()
    async with aiosqlite.connect(DB_PATH) as db:
        for q, param in [
            ("SELECT id,name,spawn_minutes FROM bosses WHERE guild_id=? AND LOWER(name)=?", ident_lc),
            ("SELECT id,name,spawn_minutes FROM bosses WHERE guild_id=? AND LOWER(name) LIKE ?", f"{ident_lc}%"),
            ("SELECT id,name,spawn_minutes FROM bosses WHERE guild_id=? AND LOWER(name) LIKE ?", f"%{ident_lc}%"),
        ]:
            c = await db.execute(q, (gid, param))
            rows = await c.fetchall()
            if len(rows) == 1: return rows[0], None
            if len(rows) > 1:  return None, f"Multiple matches for '{identifier}'. Use the exact name."
        # aliases
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
async def resolve_announce_channel(guild_id: int, explicit_channel_id: Optional[int], category: Optional[str] = None) -> Optional[discord.TextChannel]:
    guild = bot.get_guild(guild_id)
    if not guild: return None
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

# -------------------- SUBSCRIPTION PANELS (stable / carry-over) --------------------
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

async def ensure_emoji_mapping(guild_id: int, bosses: List[tuple]):
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
        if e in per_message_emojis:  # avoid dup reactions in one message
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
    """
    Stable updater:
    - Creates exactly ONE message per category in the configured sub channel.
    - If message exists, it is EDITED in-place (never creates duplicates).
    - Reaction set is reconciled (adds missing only; never removes users' reactions).
    """
    gid = guild.id
    sub_ch_id = await get_subchannel_id(gid)
    if not sub_ch_id:
        return
    channel = guild.get_channel(sub_ch_id)
    if not can_send(channel):
        return

    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT id,name,category FROM bosses WHERE guild_id=?", (gid,))
        all_bosses = await c.fetchall()

    await ensure_emoji_mapping(gid, [(bid, nm) for (bid, nm, _cat) in all_bosses])

    panel_map = await get_all_panel_records(gid)
    for cat in CATEGORY_ORDER:
        # only if category has bosses
        cat_has = any(norm_cat(c) == cat for (_bid, _nm, c) in all_bosses)
        if not cat_has:
            continue

        content, embed, emojis = await build_subscription_embed_for_category(gid, cat)
        if not embed:
            continue

        current_id, current_ch = panel_map.get(cat, (None, None))
        msg = None

        # If exists, try to edit in place
        if current_id and current_ch == sub_ch_id:
            try:
                msg = await channel.fetch_message(current_id)
                await msg.edit(content=content, embed=embed)
            except Exception:
                msg = None

        # If not found or channel changed or fetch failed, (re)create
        if msg is None:
            try:
                msg = await channel.send(content=content, embed=embed)
                await set_panel_record(gid, cat, msg.id, channel.id)
            except Exception as e:
                log.warning(f"Subscription panel create failed in {guild.id} {cat}: {e}")
                continue

        # Reconcile reactions: add missing (never remove existing)
        if can_react(channel):
            try:
                existing = set(str(r.emoji) for r in msg.reactions)
                for e in [e for e in emojis if e not in existing]:
                    await msg.add_reaction(e)
                    await asyncio.sleep(0.15)
            except Exception as e:
                log.warning(f"Adding reactions failed for {cat}: {e}")

# Pings to subscribers (separate channel)
async def send_subscription_ping(guild_id: int, boss_id: int, phase: str, boss_name: str, when_left: Optional[int] = None):
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT sub_ping_channel_id, sub_channel_id FROM guild_config WHERE guild_id=?", (guild_id,))
        r = await c.fetchone()
        sub_ping_id = (r[0] if r else None) or (r[1] if r else None)  # fallback to sub panels channel if unset
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
    try: await ch.send(txt)
    except Exception as e: log.warning(f"Sub ping failed: {e}")

# ---------- Reaction handlers (subscriptions + reaction roles) ----------
@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    if bot.user and payload.user_id == bot.user.id: return
    guild = bot.get_guild(payload.guild_id)
    if not guild: return
    emoji_str = str(payload.emoji)

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
                ); await db.commit()
        return

    # reaction roles
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
    if not guild: return
    emoji_str = str(payload.emoji)

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
                ); await db.commit()
        return

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

# -------------------- USER PREFS (for /timers) --------------------
async def get_user_shown_categories(guild_id: int, user_id: int) -> List[str]:
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT categories FROM user_timer_prefs WHERE guild_id=? AND user_id=?", (guild_id, user_id))
        r = await c.fetchone()
    if not r or not r[0]: return []
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
        ); await db.commit()

# -------------------- HELP / STATUS / HEALTH --------------------
@bot.command(name="help")
async def help_cmd(ctx):
    if not await ensure_guild_auth(ctx.guild):
        return await ctx.send(":lock: This bot is disabled in this server.")
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
        f"• Add: `{p}boss add \"Name\" <spawn_m> <window_m> [#chan] [pre_m] [category]`",
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
        f"**Market**",
        f"• `{p}market` to open menu • `{p}market add` / `{p}market list` / `{p}market mine` / `{p}market close <id>`",
        "",
        f"**Lixing**",
        f"• `{p}lixing` to open menu • `{p}lixing add` / `{p}lixing list` / `{p}lixing mine` / `{p}lixing close <id>`",
        "",
        f"**Misc**",
        f"• UTC ETA: `{p}seteta on|off` • Colors: `{p}setcatcolor <Category> <#hex>`",
        f"• Heartbeat: `{p}setuptime <N>` • HB channel: `{p}setheartbeatchannel #chan`",
        f"• Status: `{p}status` • Health: `{p}health` • Reslash: `{p}reslash`",
    ]
    text = "\n".join(lines)
    if len(text) > 1990: text = text[:1985] + "…"
    await ctx.send(text)

@bot.command(name="status")
async def status_cmd(ctx):
    if not await ensure_guild_auth(ctx.guild):
        return await ctx.send(":lock: This bot is disabled in this server.")
    gid = ctx.guild.id; p = await get_guild_prefix(bot, ctx.message)
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute(
            "SELECT COALESCE(prefix, ?), default_channel, sub_channel_id, sub_ping_channel_id, COALESCE(uptime_minutes, ?), heartbeat_channel_id, COALESCE(show_eta,0), market_channel_id, lixing_channel_id "
            "FROM guild_config WHERE guild_id=?",
            (DEFAULT_PREFIX, DEFAULT_UPTIME_MINUTES, gid)
        )
        r = await c.fetchone()
        prefix, ann_id, sub_id, sub_ping_id, hb_min, hb_ch, show_eta, mkt_ch, lix_ch = (r if r else (DEFAULT_PREFIX, None, None, None, DEFAULT_UPTIME_MINUTES, None, 0, None, None))
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
        f"Market digest: {ch(mkt_ch)} • Lixing digest: {ch(lix_ch)}",
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
    required = {"bosses","guild_config","meta","category_colors","subscription_emojis","subscription_members","boss_aliases","category_channels","user_timer_prefs","subscription_panels","rr_panels","rr_map","blacklist","market_listings","market_offers","lixing_posts"}
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT name FROM sqlite_master WHERE type='table'")
        present = {row[0] for row in await c.fetchall()}
        c = await db.execute("SELECT COUNT(*) FROM guild_config WHERE guild_id=?", (ctx.guild.id,))
        cfg_rows = (await c.fetchone())[0]
    missing = sorted(list(required - present))
    lines = [
        "**Health**",
        f"DB: `{DB_PATH}`",
        f"Tables OK: {'yes' if not missing else 'no'}{'' if not missing else ' (missing: ' + ', '.join(missing) + ')'}",
        f"guild_config row present: {'yes' if cfg_rows > 0 else 'no'}",
    ]
    await ctx.send("\n".join(lines))

# -------------------- SETTINGS (prefix/announce/eta/heartbeat/subchannels) --------------------
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
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO guild_config (guild_id,sub_ping_channel_id) VALUES (?,?) "
            "ON CONFLICT(guild_id) DO UPDATE SET sub_ping_channel_id=excluded.sub_ping_channel_id",
            (ctx.guild.id, channel.id)
        ); await db.commit()
    await ctx.send(f":white_check_mark: Subscription **ping** channel set to {channel.mention}.")

@bot.command(name="showsubscriptions")
async def showsubscriptions_cmd(ctx):
    await refresh_subscription_messages(ctx.guild)
    await ctx.send(":white_check_mark: Subscription panels refreshed (one per category).")

# -------------------- TIMERS VIEW (message command) --------------------
async def get_show_eta(guild_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT COALESCE(show_eta,0) FROM guild_config WHERE guild_id=?", (guild_id,))
        r = await c.fetchone()
        return bool(r and int(r[0]) == 1)

@bot.command(name="timers")
async def timers_cmd(ctx):
    if not await ensure_guild_auth(ctx.guild):
        return await ctx.send(":lock: This bot is disabled in this server.")
    gid = ctx.guild.id; show_eta = await get_show_eta(gid)
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT name,next_spawn_ts,category,sort_key,window_minutes FROM bosses WHERE guild_id=?", (gid,))
        rows = await c.fetchall()
    if not rows: return await ctx.send("No timers. Add with `boss add \"Name\" <spawn_m> <window_m> [#chan] [pre_m] [cat]`.")
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
            (nada_list if t == "-Nada" else normal).append((sk, nm, t, ts, win))
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

# ---------- /timers (per-user) ----------
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
        view.shown = [c for c in CATEGORY_ORDER] if self.action == "all" else []
        await view.refresh(interaction)

async def build_timer_embeds_for_categories(guild: discord.Guild, categories: List[str]) -> List[discord.Embed]:
    gid = guild.id
    show_eta = await get_show_eta(gid)
    if not categories:
        return []
    async with aiosqlite.connect(DB_PATH) as db:
        q_marks = ",".join("?" for _ in categories)
        c = await db.execute(f"SELECT name,next_spawn_ts,category,sort_key,window_minutes FROM bosses WHERE guild_id=? AND category IN ({q_marks})",
                             (gid, *[norm_cat(c) for c in categories]))
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

@app_commands.guild_only()
@bot.tree.command(name="timers", description="Show timers with per-category toggles (ephemeral, remembers your selection)")
async def slash_timers(interaction: discord.Interaction):
    guild = interaction.guild
    if not guild or not await ensure_guild_auth(guild):
        return await interaction.response.send_message(":lock: This bot is disabled in this server.", ephemeral=True)
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

# -------------------- PATCH A — Slash Command Sync Helpers --------------------
async def _sync_all_slash_commands():
    """
    Force-resync application commands globally and per-guild.
    Fixes 'Unknown interaction/Unknown command' issues after deploys.
    """
    try:
        synced_global = await bot.tree.sync()
        log.info(f"Slash sync: global {len(synced_global)} commands")
    except Exception as e:
        log.warning(f"Global slash sync failed: {e}")

    ok = 0
    for g in bot.guilds:
        try:
            synced_g = await bot.tree.sync(guild=g)
            ok += 1
            log.info(f"Slash sync: {g.name} ({g.id}) -> {len(synced_g)} commands")
        except Exception as e:
            log.warning(f"Guild slash sync failed for {g.id}: {e}")
    log.info(f"Slash sync complete for {ok}/{len(bot.guilds)} guilds.")

# Manual resync commands
@bot.command(name="reslash")
@commands.has_permissions(administrator=True)
async def reslash_cmd(ctx):
    await ctx.send("Resyncing slash commands…")
    await _sync_all_slash_commands()
    await ctx.send("Slash command resync complete.")

@app_commands.guild_only()
@app_commands.default_permissions(administrator=True)
@bot.tree.command(name="reslash", description="Force-resync slash commands (Admins only)")
async def reslash_slash(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    await _sync_all_slash_commands()
    await interaction.followup.send("Slash command resync complete.", ephemeral=True)

# Global app command error handler
@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    try:
        msg = f":warning: {type(error).__name__}: {error}"
        if not interaction.response.is_done():
            await interaction.response.send_message(msg, ephemeral=True)
        else:
            await interaction.followup.send(msg, ephemeral=True)
    except Exception:
        log.exception("Failed sending app command error response")

# -------------------- MARKET (commands + UI) --------------------
# (Compact functional set: add/list/mine/close + button 'Make Offer')
class OfferModal(discord.ui.Modal, title="Make an Offer"):
    offer = discord.ui.TextInput(label="Your offer", style=discord.TextStyle.paragraph, max_length=300)

    def __init__(self, listing_id: int):
        super().__init__()
        self.listing_id = listing_id

    async def on_submit(self, interaction: discord.Interaction):
        gid = interaction.guild_id
        uid = interaction.user.id
        txt = str(self.offer.value).strip()
        if not txt:
            return await interaction.response.send_message("Empty offer.", ephemeral=True)
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT INTO market_offers (guild_id, listing_id, offerer_id, offer_text, created_ts) VALUES (?,?,?,?,?)",
                (gid, self.listing_id, uid, txt, now_ts())
            )
            await db.commit()
            # fetch listing owner
            c = await db.execute("SELECT user_id, channel_id, message_id FROM market_listings WHERE id=? AND guild_id=?", (self.listing_id, gid))
            r = await c.fetchone()
        if not r:
            return await interaction.response.send_message("Listing not found (maybe closed).", ephemeral=True)
        owner_id, ch_id, msg_id = int(r[0]), r[1], r[2]
        # Notify owner
        try:
            await interaction.guild.get_member(owner_id).send(f"📨 New offer on your listing #{self.listing_id} from <@{uid}>:\n> {txt}")
        except Exception:
            pass
        # Publicly append under the listing message if we can
        if ch_id and msg_id:
            ch = interaction.guild.get_channel(ch_id)
            if can_send(ch):
                try:
                    await ch.send(f"💬 **Offer on #{self.listing_id}** by <@{uid}>: {txt}")
                except Exception:
                    pass
        await interaction.response.send_message("Offer submitted.", ephemeral=True)

class OfferButton(discord.ui.View):
    def __init__(self, listing_id: int):
        super().__init__(timeout=None)
        self.listing_id = listing_id

    @discord.ui.button(label="Make Offer", style=discord.ButtonStyle.primary)
    async def make_offer(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(OfferModal(self.listing_id))

async def _post_listing_embed(ctx, row) -> int:
    (lid, _gid, user_id, typ, item_name, qty, price, acc_trades, taking_offers, notes, is_active, msg_id, ch_id, created_ts) = row
    e = discord.Embed(
        title=f"{'BUY' if typ=='buy' else 'SELL'} — {item_name}",
        description=(notes or "—"),
        color=0x2ecc71 if typ == "buy" else 0xe74c3c,
        timestamp=datetime.fromtimestamp(created_ts, tz=timezone.utc)
    )
    e.add_field(name="Quantity", value=str(qty), inline=True)
    e.add_field(name="Price/Range", value=(price or "N/A"), inline=True)
    e.add_field(name="Accepts Trades", value="Yes" if acc_trades else "No", inline=True)
    e.add_field(name="Taking Offers", value="Yes" if taking_offers else "No", inline=True)
    e.set_footer(text=f"Listing #{lid} • by {ctx.guild.get_member(user_id).display_name if ctx.guild.get_member(user_id) else user_id}")

    ch = ctx.channel
    msg = await ch.send(embed=e, view=OfferButton(lid))
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE market_listings SET message_id=?, channel_id=? WHERE id=?", (msg.id, ch.id, lid))
        await db.commit()
    return msg.id

@bot.group(name="market", invoke_without_command=True)
async def market_group(ctx):
    await ctx.send("`!market add` • `!market list` • `!market mine` • `!market close <id>`")

@market_group.command(name="add")
async def market_add(ctx, typ: str, item_name: str, quantity: int, *, rest: str = ""):
    typ = typ.lower().strip()
    if typ not in {"buy","sell"}:
        return await ctx.send("Type must be `buy` or `sell`.")
    # parse inline flags: price=..., trades=yes/no, offers=yes/no, notes="..."
    price = ""; acc_trades = 0; taking_offers = 0; notes = ""
    m = re.search(r'price=("[^"]+"|\S+)', rest);  price = (m.group(1).strip('"') if m else "")
    m = re.search(r'trades=(yes|no)', rest, re.I); acc_trades = 1 if m and m.group(1).lower()=="yes" else 0
    m = re.search(r'offers=(yes|no)', rest, re.I); taking_offers = 1 if m and m.group(1).lower()=="yes" else 0
    m = re.search(r'notes="([^"]+)"', rest); notes = m.group(1) if m else ""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""INSERT INTO market_listings
            (guild_id,user_id,type,item_name,quantity,price,accepts_trades,taking_offers,notes,is_active,created_ts)
            VALUES (?,?,?,?,?,?,?,?,?,1,?)""",
            (ctx.guild.id, ctx.author.id, typ, item_name, int(quantity), price, acc_trades, taking_offers, notes, now_ts()))
        await db.commit()
        c = await db.execute("SELECT * FROM market_listings WHERE rowid=last_insert_rowid()")
        row = await c.fetchone()
    await _post_listing_embed(ctx, row)
    await ctx.send(":white_check_mark: Listing created.")

@market_group.command(name="list")
async def market_list(ctx, typ: Optional[str] = None):
    where = ""
    args: List[Any] = [ctx.guild.id]
    if typ and typ.lower() in {"buy","sell"}:
        where = "AND type=?"; args.append(typ.lower())
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute(f"SELECT id,type,item_name,quantity,price,accepts_trades,taking_offers,user_id FROM market_listings WHERE guild_id=? AND is_active=1 {where} ORDER BY created_ts DESC", args)
        rows = await c.fetchall()
    if not rows: return await ctx.send("No active listings.")
    lines = []
    for (lid, typ, item, qty, price, trades, offers, uid) in rows[:50]:
        lines.append(f"• `#{lid}` **{typ.upper()}** {item} x{qty} — {price or 'N/A'} • trades:{'Y' if trades else 'N'} offers:{'Y' if offers else 'N'} • by <@{uid}>")
    await ctx.send("\n".join(lines))

@market_group.command(name="mine")
async def market_mine(ctx):
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT id,type,item_name,quantity,price,accepts_trades,taking_offers FROM market_listings WHERE guild_id=? AND user_id=? AND is_active=1 ORDER BY created_ts DESC", (ctx.guild.id, ctx.author.id))
        rows = await c.fetchall()
    if not rows: return await ctx.send("You have no active listings.")
    await ctx.send("\n".join([f"• `#{lid}` **{typ.upper()}** {item} x{qty} — {price or 'N/A'} • trades:{'Y' if tr else 'N'} offers:{'Y' if off else 'N'}" for (lid, typ, item, qty, price, tr, off) in rows]))

@market_group.command(name="close")
async def market_close(ctx, listing_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT user_id FROM market_listings WHERE id=? AND guild_id=?", (listing_id, ctx.guild.id))
        r = await c.fetchone()
        if not r: return await ctx.send("Listing not found.")
        if int(r[0]) != ctx.author.id and not ctx.author.guild_permissions.manage_messages:
            return await ctx.send("Only the owner (or a mod) can close this listing.")
        await db.execute("UPDATE market_listings SET is_active=0 WHERE id=? AND guild_id=?", (listing_id, ctx.guild.id))
        await db.commit()
    await ctx.send(f":white_check_mark: Closed listing #{listing_id}.")

# -------------------- LIXING (commands + UI) --------------------
@bot.group(name="lixing", invoke_without_command=True)
async def lixing_group(ctx):
    await ctx.send("`!lixing add` • `!lixing list` • `!lixing mine` • `!lixing close <id>`")

@lixing_group.command(name="add")
async def lixing_add(ctx, char_name: str, char_class: str, level: int = -1, *, desired_lixes: str = "N/A"):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""INSERT INTO lixing_posts
            (guild_id,user_id,char_name,char_class,level,desired_lixes,created_ts)
            VALUES (?,?,?,?,?,?,?)""",
            (ctx.guild.id, ctx.author.id, char_name, char_class, int(level), desired_lixes, now_ts()))
        await db.commit()
        c = await db.execute("SELECT * FROM lixing_posts WHERE rowid=last_insert_rowid()")
        row = await c.fetchone()
    (lid, _gid, uid, cname, cclass, lvl, dlx, notes, is_active, msg_id, ch_id, cts) = row
    e = discord.Embed(
        title=f"Lixing Request — {cname} ({cclass})",
        description=(notes or "—"),
        color=0x4aa3ff,
        timestamp=datetime.fromtimestamp(cts, tz=timezone.utc)
    )
    e.add_field(name="Level", value=("N/A" if lvl < 0 else str(lvl)), inline=True)
    e.add_field(name="Desired Lixes", value=(dlx or "N/A"), inline=True)
    e.set_footer(text=f"Post #{lid} • by {ctx.guild.get_member(uid).display_name if ctx.guild.get_member(uid) else uid}")
    msg = await ctx.channel.send(embed=e)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE lixing_posts SET message_id=?, channel_id=? WHERE id=?", (msg.id, ctx.channel.id, lid))
        await db.commit()
    await ctx.send(":white_check_mark: Lixing post created.")

@lixing_group.command(name="list")
async def lixing_list(ctx):
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT id,char_name,char_class,level,desired_lixes,user_id FROM lixing_posts WHERE guild_id=? AND is_active=1 ORDER BY created_ts DESC", (ctx.guild.id,))
        rows = await c.fetchall()
    if not rows: return await ctx.send("No active lixing posts.")
    lines = []
    for (lid, cname, cclass, lvl, dlx, uid) in rows[:50]:
        lines.append(f"• `#{lid}` **{cname}** ({cclass}) lvl {('N/A' if lvl<0 else lvl)} — wants {dlx} • by <@{uid}>")
    await ctx.send("\n".join(lines))

@lixing_group.command(name="mine")
async def lixing_mine(ctx):
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT id,char_name,char_class,level,desired_lixes FROM lixing_posts WHERE guild_id=? AND user_id=? AND is_active=1 ORDER BY created_ts DESC", (ctx.guild.id, ctx.author.id))
        rows = await c.fetchall()
    if not rows: return await ctx.send("You have no active lixing posts.")
    await ctx.send("\n".join([f"• `#{lid}` **{n}** ({cls}) lvl {('N/A' if lvl<0 else lvl)} — wants {dlx}" for (lid,n,cls,lvl,dlx) in rows]))

@lixing_group.command(name="close")
async def lixing_close(ctx, post_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT user_id FROM lixing_posts WHERE id=? AND guild_id=?", (post_id, ctx.guild.id))
        r = await c.fetchone()
        if not r: return await ctx.send("Post not found.")
        if int(r[0]) != ctx.author.id and not ctx.author.guild_permissions.manage_messages:
            return await ctx.send("Only the owner (or a mod) can close this post.")
        await db.execute("UPDATE lixing_posts SET is_active=0 WHERE id=? AND guild_id=?", (post_id, ctx.guild.id))
        await db.commit()
    await ctx.send(f":white_check_mark: Closed lixing post #{post_id}.")
# ======================================================
# SECTION 3 / 4 — Permissions • Boss commands
#                  Intervals list • Reaction Roles
#                  PowerShell (/ps) • Digest helpers
# ======================================================

# -------------------- PERMISSIONS / AUTH --------------------
async def has_trusted(member: discord.Member, guild_id: int, boss_id: Optional[int] = None) -> bool:
    # Admin bypass; otherwise Manage Messages or boss-specific trusted role
    if member.guild_permissions.administrator:
        return True
    async with aiosqlite.connect(DB_PATH) as db:
        if boss_id:
            c = await db.execute("SELECT trusted_role_id FROM bosses WHERE id=? AND guild_id=?", (boss_id, guild_id))
            r = await c.fetchone()
            if r and r[0]:
                return any(role.id == int(r[0]) for role in member.roles)
    return member.guild_permissions.manage_messages

async def ensure_guild_auth(guild: Optional[discord.Guild]) -> bool:
    """
    Bot should only function if user @blunderbusstin is in the same server.
    Returns True if allowed, False to disable commands.
    """
    if not guild:
        return False
    # Try to find by name first; if no match, allow (so it doesn't hard-lock by nickname variance)
    target = discord.utils.find(lambda m: m.name == "blunderbusstin" or (m.nick and "blunderbusstin" in m.nick.lower()), guild.members)
    return target is not None

# -------------------- INTERVALS VIEW (shared helper) --------------------
async def send_intervals_list(ctx):
    if not await ensure_guild_auth(ctx.guild):
        return await ctx.send(":lock: This bot is disabled in this server.")
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
        if not items:
            continue
        items.sort(key=lambda x: (natural_key(x[0]), natural_key(x[1])))
        lines: List[str] = []
        for sk, nm, sp, win, pre in items:
            lines.append(f"• **{nm}** — Respawn: {sp}m • Window: {win}m • Pre: {pre}m")
        em = discord.Embed(
            title=f"{category_emoji(cat)} {cat} — Intervals",
            description="",
            color=await get_category_color(gid, cat)
        )
        bucket = ""
        chunks: List[str] = []
        for line in lines:
            if len(bucket) + len(line) + 1 > 1000:
                chunks.append(bucket)
                bucket = line + "\n"
            else:
                bucket += line + "\n"
        if bucket:
            chunks.append(bucket)
        for i, ch in enumerate(chunks, 1):
            em.add_field(name=f"{cat} ({i})" if len(chunks) > 1 else cat, value=ch, inline=False)
        try:
            await ctx.send(embed=em)
        except Exception:
            text_fallback = f"**{cat} — Intervals**\n" + "\n".join(lines)
            if len(text_fallback) > 1990:
                text_fallback = text_fallback[:1985] + "…"
            await ctx.send(text_fallback)

@bot.command(name="intervals")
async def intervals_cmd(ctx):
    await send_intervals_list(ctx)

@bot.group(name="boss", invoke_without_command=True)
async def boss_group(ctx):
    p = await get_guild_prefix(bot, ctx.message)
    await ctx.send(f"Use `{p}help` for commands.")

# -------------------- BOSS COMMANDS --------------------
@boss_group.command(name="add")
async def boss_add(ctx, *args):
    """
    !boss add "Name" <spawn_m> <window_m> [#channel] [pre_m] [category]
    """
    if not await ensure_guild_auth(ctx.guild):
        return await ctx.send(":lock: This bot is disabled in this server.")

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
        spawn_m = None
        window_m = 0
        ch_id: Optional[int] = None
        pre_m = 10
        cat = "Default"
        if tokens and tokens[0].lstrip("-").isdigit():
            spawn_m = int(tokens.pop(0))
        if tokens and tokens[0].lstrip("-").isdigit():
            window_m = int(tokens.pop(0))
        if tokens:
            maybe_ch = _resolve_channel_id_from_arg(ctx, tokens[0])
            if maybe_ch:
                ch_id = maybe_ch
                tokens.pop(0)
        if tokens and tokens[0].lstrip("-").isdigit():
            pre_m = int(tokens.pop(0))
        if tokens:
            cat = " ".join(tokens).strip()
        if spawn_m is None:
            raise ValueError("Missing spawn_minutes.")
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
@commands.has_permissions(manage_guild=True)
async def boss_idleall(ctx):
    if not await ensure_guild_auth(ctx.guild):
        return await ctx.send(":lock: This bot is disabled in this server.")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE bosses SET next_spawn_ts=? WHERE guild_id=?", (now_ts() - 3601, ctx.guild.id))
        await db.commit()
    await ctx.send(":white_check_mark: All timers set to **-Nada**.")
    await refresh_subscription_messages(ctx.guild)

@boss_group.command(name="nada")
@commands.has_permissions(manage_guild=True)
async def boss_nada(ctx, *, name: str):
    if not await ensure_guild_auth(ctx.guild):
        return await ctx.send(":lock: This bot is disabled in this server.")
    res, err = await resolve_boss(ctx, name)
    if err:
        return await ctx.send(f":no_entry: {err}")
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
        return await ctx.send(":lock: This bot is disabled in this server.")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE bosses SET next_spawn_ts=? WHERE guild_id=?", (now_ts() - 3601, ctx.guild.id))
        await db.commit()
    await ctx.send(":pause_button: **All bosses** set to **-Nada**.")
    await refresh_subscription_messages(ctx.guild)

@boss_group.command(name="info")
async def boss_info(ctx, *, name: str):
    if not await ensure_guild_auth(ctx.guild):
        return await ctx.send(":lock: This bot is disabled in this server.")
    res, err = await resolve_boss(ctx, name)
    if err:
        return await ctx.send(f":no_entry: {err}")
    bid, nm, _ = res
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute(
            "SELECT name,spawn_minutes,window_minutes,next_spawn_ts,channel_id,pre_announce_min,trusted_role_id,category,sort_key "
            "FROM bosses WHERE id=? AND guild_id=?", (bid, ctx.guild.id)
        )
        r = await c.fetchone()
    if not r:
        return await ctx.send("Boss not found.")
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
        return await ctx.send(":lock: This bot is disabled in this server.")
    res, err = await resolve_boss(ctx, name)
    if err:
        return await ctx.send(f":no_entry: {err}")
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
        return await ctx.send(":lock: This bot is disabled in this server.")
    res, err = await resolve_boss(ctx, name)
    if err:
        return await ctx.send(f":no_entry: {err}")
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
        return await ctx.send(":lock: This bot is disabled in this server.")
    res, err = await resolve_boss(ctx, name)
    if err:
        return await ctx.send(f":no_entry: {err}")
    bid, nm, _ = res
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT next_spawn_ts FROM bosses WHERE id=? AND guild_id=?", (bid, ctx.guild.id))
        ts_row = await c.fetchone()
        if not ts_row:
            return await ctx.send("Boss not found.")
        current_ts = int(ts_row[0])
        new_ts = max(now_ts(), current_ts - int(minutes) * 60)
        await db.execute("UPDATE bosses SET next_spawn_ts=? WHERE id=? AND guild_id=?", (new_ts, bid, ctx.guild.id))
        await db.commit()
    await ctx.send(f":arrow_down: Reduced **{nm}** by {minutes}m. Spawn Time: `{fmt_delta_for_list(new_ts - now_ts())}`.")
    await refresh_subscription_messages(ctx.guild)

@boss_group.command(name="edit")
@commands.has_permissions(manage_guild=True)
async def boss_edit(ctx, name: str, field: str, value: str):
    if not await ensure_guild_auth(ctx.guild):
        return await ctx.send(":lock: This bot is disabled in this server.")
    allowed = {"spawn_minutes", "window_minutes", "pre_announce_min", "name", "category", "sort_key"}
    if field not in allowed:
        return await ctx.send(f"Editable: {', '.join(sorted(allowed))}")
    res, err = await resolve_boss(ctx, name)
    if err:
        return await ctx.send(f":no_entry: {err}")
    bid, _, _ = res
    async with aiosqlite.connect(DB_PATH) as db:
        if field in {"spawn_minutes", "window_minutes", "pre_announce_min"}:
            try:
                v = int(value)
            except ValueError:
                return await ctx.send("Value must be an integer.")
            if field == "spawn_minutes" and v < 1:
                return await ctx.send(":no_entry: spawn_minutes must be >= 1.")
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
        return await ctx.send(":lock: This bot is disabled in this server.")
    res, err = await resolve_boss(ctx, name)
    if err:
        return await ctx.send(f":no_entry: {err}")
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
        return await ctx.send(":lock: This bot is disabled in this server.")
    ident = None
    category = None
    if '"' in args:
        a, b = args.split('"', 1)
        ident = a.strip()
        category = b.split('"', 1)[0].strip()
    if not ident or not category:
        parts = args.rsplit(" ", 1)
        if len(parts) == 2:
            ident, category = parts[0].strip(), parts[1].strip()
    if not ident or not category:
        return await ctx.send('Format: `!boss setcategory <name> "<Category>"`')
    res, err = await resolve_boss(ctx, ident)
    if err:
        return await ctx.send(f":no_entry: {err}")
    bid, nm, _ = res
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE bosses SET category=? WHERE id=? AND guild_id=?", (norm_cat(category), bid, ctx.guild.id))
        await db.commit()
    await ctx.send(f":label: **{nm}** → **{norm_cat(category)}**.")
    await refresh_subscription_messages(ctx.guild)

@boss_group.command(name="setsort")
async def boss_setsort(ctx, name: str, sort_key: str):
    if not await ensure_guild_auth(ctx.guild):
        return await ctx.send(":lock: This bot is disabled in this server.")
    res, err = await resolve_boss(ctx, name)
    if err:
        return await ctx.send(f":no_entry: {err}")
    bid, nm, _ = res
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE bosses SET sort_key=? WHERE id=? AND guild_id=?", (sort_key, bid, ctx.guild.id))
        await db.commit()
    await ctx.send(f":1234: Sort key for **{nm}** set to `{sort_key}`.")
    await refresh_subscription_messages(ctx.guild)

@boss_group.command(name="setchannel")
async def boss_setchannel(ctx, name: str, channel: discord.TextChannel):
    if not await ensure_guild_auth(ctx.guild):
        return await ctx.send(":lock: This bot is disabled in this server.")
    if name.lower() in {"all"}:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE bosses SET channel_id=? WHERE guild_id=?", (channel.id, ctx.guild.id))
            await db.commit()
        return await ctx.send(f":satellite: All boss reminders → {channel.mention}.")
    elif name.lower() in {"category", "cat"}:
        return await ctx.send('Use `!boss setchannelcat "<Category>" #chan`.')
    res, err = await resolve_boss(ctx, name)
    if err:
        return await ctx.send(f":no_entry: {err}")
    bid, nm, _ = res
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE bosses SET channel_id=? WHERE id=? AND guild_id=?", (channel.id, bid, ctx.guild.id))
        await db.commit()
    await ctx.send(f":satellite: **{nm}** reminders → {channel.mention}.")

@boss_group.command(name="setchannelall")
@commands.has_permissions(manage_guild=True)
async def boss_setchannelall(ctx, channel: discord.TextChannel):
    if not await ensure_guild_auth(ctx.guild):
        return await ctx.send(":lock: This bot is disabled in this server.")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE bosses SET channel_id=? WHERE guild_id=?", (channel.id, ctx.guild.id))
        await db.commit()
    await ctx.send(f":satellite: All boss reminders → {channel.mention}.")

@boss_group.command(name="setchannelcat")
@commands.has_permissions(manage_guild=True)
async def boss_setchannelcat(ctx, *, args: str):
    if not await ensure_guild_auth(ctx.guild):
        return await ctx.send(":lock: This bot is disabled in this server.")
    if '"' in args:
        cat = args.split('"', 1)[1].split('"', 1)[0].strip()
        tail = args.split('"', 2)[-1].strip()
        ch_id = _resolve_channel_id_from_arg(ctx, tail.split()[-1]) if tail else None
    else:
        parts = args.rsplit(" ", 1)
        if len(parts) != 2:
            return await ctx.send('Format: `!boss setchannelcat "<Category>" #chan`')
        cat, ch_token = parts[0], parts[1]
        ch_id = _resolve_channel_id_from_arg(ctx, ch_token)
    if not cat or not ch_id:
        return await ctx.send('Format: `!boss setchannelcat "<Category>" #chan`')
    catn = norm_cat(cat)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE bosses SET channel_id=? WHERE guild_id=? AND category=?", (ch_id, ctx.guild.id, catn))
        await db.commit()
    await ctx.send(f":satellite: **{catn}** boss reminders → <#{ch_id}>.")

@boss_group.command(name="setrole")
@commands.has_permissions(manage_guild=True)
async def boss_setrole(ctx, *args):
    if not await ensure_guild_auth(ctx.guild):
        return await ctx.send(":lock: This bot is disabled in this server.")
    if not args:
        return await ctx.send("Use `!boss setrole @Role` or `!boss setrole \"Name\" @Role` (or `none`).")
    text = " ".join(args).strip()
    if text.count('"') >= 2:
        boss_name = text.split('"', 1)[1].split('"', 1)[0].strip()
        remainder = text.split('"', 2)[-1].strip()
        if not remainder:
            return await ctx.send("Provide a role or `none` after the boss name.")
        role_arg = remainder
        res, err = await resolve_boss(ctx, boss_name)
        if err:
            return await ctx.send(f":no_entry: {err}")
        bid, nm, _ = res
        async with aiosqlite.connect(DB_PATH) as db:
            if role_arg.lower() in ("none", "clear"):
                await db.execute("UPDATE bosses SET trusted_role_id=NULL WHERE id=? AND guild_id=?", (bid, ctx.guild.id))
                await db.commit()
                return await ctx.send(f":white_check_mark: Cleared reset role for **{nm}**.")
            role_obj = None
            if role_arg.startswith("<@&") and role_arg.endswith(">"):
                try:
                    role_obj = ctx.guild.get_role(int(role_arg[3:-1]))
                except Exception:
                    role_obj = None
            if not role_obj:
                role_obj = discord.utils.get(ctx.guild.roles, name=role_arg)
            if not role_obj:
                return await ctx.send("Role not found. Mention it or use exact name.")
            await db.execute("UPDATE bosses SET trusted_role_id=? WHERE id=? AND guild_id=?", (role_obj.id, bid, ctx.guild.id))
            await db.commit()
        return await ctx.send(f":white_check_mark: **{nm}** now requires **{role_obj.name}** to reset.")
    role_arg = text
    if role_arg.lower() in ("none", "clear"):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE bosses SET trusted_role_id=NULL WHERE guild_id=?", (ctx.guild.id,))
            await db.commit()
        return await ctx.send(":white_check_mark: Cleared reset role on all bosses.")
    role_obj = None
    if role_arg.startswith("<@&") and role_arg.endswith(">"):
        try:
            role_obj = ctx.guild.get_role(int(role_arg[3:-1]))
        except Exception:
            role_obj = None
    if not role_obj:
        role_obj = discord.utils.get(ctx.guild.roles, name=role_arg)
    if not role_obj:
        return await ctx.send("Role not found. Mention it or use exact name.")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE bosses SET trusted_role_id=? WHERE guild_id=?", (role_obj.id, ctx.guild.id))
        await db.commit()
    await ctx.send(f":white_check_mark: All bosses now require **{role_obj.name}** to reset.")

@boss_group.command(name="alias")
@commands.has_permissions(manage_guild=True)
async def boss_alias(ctx, action: str = None, *, args: str = ""):
    if not await ensure_guild_auth(ctx.guild):
        return await ctx.send(":lock: This bot is disabled in this server.")
    action = (action or "").lower()
    if action not in {"add", "remove", "list", "aliases"}:
        return await ctx.send('Use: `!boss alias add "Name" "Alias"`, `!boss alias remove "Name" "Alias"`, or `!boss aliases "Name"`')

    def parse_two_quoted(s: str) -> Optional[Tuple[str, str]]:
        s = s.strip()
        if s.count('"') < 4:
            return None
        first = s.split('"', 1)[1].split('"', 1)[0].strip()
        rest = s.split('"', 2)[-1].strip()
        second = rest.split('"', 1)[1].split('"', 1)[0].strip() if rest.count('"') >= 2 else None
        return (first, second) if second else None

    if action in {"add", "remove"}:
        parsed = parse_two_quoted(args)
        if not parsed:
            return await ctx.send('Format: `!boss alias add "Name" "Alias"`')
        boss_name, alias = parsed
        res, err = await resolve_boss(ctx, boss_name)
        if err:
            return await ctx.send(f":no_entry: {err}")
        bid, nm, _ = res
        async with aiosqlite.connect(DB_PATH) as db:
            if action == "add":
                try:
                    await db.execute("INSERT INTO boss_aliases (guild_id,boss_id,alias) VALUES (?,?,?)",
                                     (ctx.guild.id, bid, alias.lower()))
                    await db.commit()
                    await ctx.send(f":white_check_mark: Added alias **{alias}** → **{nm}**.")
                except Exception:
                    await ctx.send(":warning: Could not add alias (maybe already used?)")
            else:
                await db.execute("DELETE FROM boss_aliases WHERE guild_id=? AND boss_id=? AND alias=?",
                                 (ctx.guild.id, bid, alias.lower()))
                await db.commit()
                await ctx.send(f":white_check_mark: Removed alias **{alias}** from **{nm}**.")
        return

    # list
    name = args.strip().strip('"')
    if not name:
        return await ctx.send('Format: `!boss aliases "Boss Name"`')
    res, err = await resolve_boss(ctx, name)
    if err:
        return await ctx.send(f":no_entry: {err}")
    bid, nm, _ = res
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT alias FROM boss_aliases WHERE guild_id=? AND boss_id=? ORDER BY alias", (ctx.guild.id, bid))
        rows = [r[0] for r in await c.fetchall()]
    await ctx.send(f"**Aliases for {nm}:** " + (", ".join(rows) if rows else "*none*"))

@boss_group.command(name="find")
async def boss_find(ctx, *, ident: str):
    if not await ensure_guild_auth(ctx.guild):
        return await ctx.send(":lock: This bot is disabled in this server.")
    res, err = await resolve_boss(ctx, ident)
    if err:
        return await ctx.send(f":no_entry: {err}")
    _bid, nm, _ = res
    await ctx.send(f"Matched: **{nm}**")

# -------------------- REACTION ROLES (slash) --------------------
@app_commands.guild_only()
@app_commands.default_permissions(manage_roles=True)
@bot.tree.command(name="roles_panel", description="Create a reaction-roles message (react to get/remove roles).")
async def roles_panel(interaction: discord.Interaction,
                      channel: Optional[discord.TextChannel],
                      title: str,
                      pairs: str):
    if not await ensure_guild_auth(interaction.guild):
        return await interaction.response.send_message(":lock: This bot is disabled in this server.", ephemeral=True)
    if not interaction.user.guild_permissions.manage_roles:
        return await interaction.response.send_message("You need Manage Roles permission.", ephemeral=True)
    ch = channel or interaction.channel
    if not can_send(ch):
        return await interaction.response.send_message("I can't post in that channel.", ephemeral=True)
    entries = [e.strip() for e in pairs.split(",") if e.strip()]
    parsed: List[Tuple[str, int, str]] = []
    role_mention_re = re.compile(r"<@&(\d+)>")
    for entry in entries:
        m = role_mention_re.search(entry)
        if not m:
            return await interaction.response.send_message(f"Missing role mention in `{entry}`.", ephemeral=True)
        role_id = int(m.group(1))
        role = interaction.guild.get_role(role_id)
        if not role:
            return await interaction.response.send_message(f"Role not found in `{entry}`.", ephemeral=True)
        emoji = entry.split()[0]
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

# -------------------- POWERSHELL (slash /ps) --------------------
def _find_pwsh_exe() -> Optional[str]:
    for exe in ("pwsh", "powershell", "powershell.exe", "pwsh.exe"):
        path = shutil.which(exe)
        if path:
            return path
    return None

@app_commands.guild_only()
@bot.tree.command(name="ps", description="Run a PowerShell command on the bot host (Admins only; requires ALLOW_POWERSHELL=1)")
async def ps_run(interaction: discord.Interaction, command: str):
    if not await ensure_guild_auth(interaction.guild):
        return await interaction.response.send_message(":lock: This bot is disabled in this server.", ephemeral=True)
    if not ALLOW_POWERSHELL:
        return await interaction.response.send_message("PowerShell execution is disabled. Set `ALLOW_POWERSHELL=1` to enable.", ephemeral=True)
    if not interaction.user.guild_permissions.administrator:
        return await interaction.response.send_message("Admins only.", ephemeral=True)
    exe = _find_pwsh_exe()
    if not exe:
        return await interaction.response.send_message("No PowerShell executable found on host.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    try:
        proc = await asyncio.create_subprocess_exec(
            exe, "-NoProfile", "-NonInteractive", "-Command", command,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=20)
        except asyncio.TimeoutError:
            proc.kill()
            return await interaction.followup.send("⏱️ Timed out after 20s.", ephemeral=True)
        rc = proc.returncode
        out = (stdout or b"").decode("utf-8", errors="replace")
        err = (stderr or b"").decode("utf-8", errors="replace")
        blob = f"$ {command}\n\n[exit {rc}]\n\nSTDOUT:\n{out}\n\nSTDERR:\n{err}"
        if len(blob) <= 1900:
            await interaction.followup.send(f"```text\n{blob}\n```", ephemeral=True)
        else:
            fp = io.BytesIO(blob.encode("utf-8"))
            fp.name = "ps_output.txt"
            await interaction.followup.send(content="Output attached (truncated in chat).", file=discord.File(fp, filename="ps_output.txt"), ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f":warning: {e}", ephemeral=True)

# -------------------- MARKET/LIXING DIGEST HELPERS --------------------
async def _post_market_digest(guild: discord.Guild):
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT market_channel_id FROM guild_config WHERE guild_id=?", (guild.id,))
        r = await c.fetchone()
        ch_id = r[0] if r else None
        if not ch_id:
            return
        c = await db.execute("""SELECT id,type,item_name,quantity,price,accepts_trades,taking_offers,user_id
                                FROM market_listings
                                WHERE guild_id=? AND is_active=1
                                ORDER BY created_ts DESC LIMIT 100""", (guild.id,))
        rows = await c.fetchall()
    ch = guild.get_channel(ch_id)
    if not can_send(ch):
        return
    if not rows:
        try:
            await ch.send("📦 **Market Digest** — No active listings.")
        except Exception:
            pass
        return
    # Build grouped digest
    buys = [r for r in rows if r[1] == "buy"]
    sells = [r for r in rows if r[1] == "sell"]
    def lines_for(lst):
        out = []
        for (lid, typ, item, qty, price, trades, offers, uid) in lst:
            out.append(f"`#{lid}` **{item}** x{qty} — {price or 'N/A'} • trades:{'Y' if trades else 'N'} • offers:{'Y' if offers else 'N'} • by <@{uid}>")
        return out[:50]
    e = discord.Embed(title="🛒 Market Digest (last 6h)", color=0x2ecc71, timestamp=datetime.now(timezone.utc))
    buy_lines = lines_for(buys)
    sell_lines = lines_for(sells)
    if buy_lines:
        e.add_field(name="BUY", value="\n".join(buy_lines)[:1000], inline=False)
    if sell_lines:
        e.add_field(name="SELL", value="\n".join(sell_lines)[:1000], inline=False)
    try:
        await ch.send(embed=e)
    except Exception:
        pass

async def _post_lixing_digest(guild: discord.Guild):
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT lixing_channel_id FROM guild_config WHERE guild_id=?", (guild.id,))
        r = await c.fetchone()
        ch_id = r[0] if r else None
        if not ch_id:
            return
        c = await db.execute("""SELECT id,char_name,char_class,level,desired_lixes,user_id
                                FROM lixing_posts
                                WHERE guild_id=? AND is_active=1
                                ORDER BY created_ts DESC LIMIT 100""", (guild.id,))
        rows = await c.fetchall()
    ch = guild.get_channel(ch_id)
    if not can_send(ch):
        return
    if not rows:
        try:
            await ch.send("⚔️ **Lixing Digest** — No active requests.")
        except Exception:
            pass
        return
    lines = []
    for (lid, cname, cclass, lvl, dlx, uid) in rows:
        lines.append(f"`#{lid}` **{cname}** ({cclass}) lvl {('N/A' if int(lvl) < 0 else lvl)} — wants {dlx or 'N/A'} • by <@{uid}>")
    e = discord.Embed(title="⚔️ Lixing Digest (last 6h)", color=0x4aa3ff, timestamp=datetime.now(timezone.utc))
    # split if too long
    chunk = ""
    buckets = []
    for ln in lines:
        if len(chunk) + len(ln) + 1 > 1000:
            buckets.append(chunk); chunk = ln + "\n"
        else:
            chunk += ln + "\n"
    if chunk:
        buckets.append(chunk)
    for i, val in enumerate(buckets, 1):
        e.add_field(name=f"Requests ({i})" if len(buckets) > 1 else "Requests", value=val, inline=False)
    try:
        await ch.send(embed=e)
    except Exception:
        pass
# ======================================================
# SECTION 4 / 4 — Events • UI (Market/Lixing) • Reactions
#                    Digests loop • Error/Shutdown • Run
# ======================================================

# -------------------- MARKET / LIXING UI --------------------
# These views/modals are used by the slash commands defined in Section 2.

class MarketCreateModal(discord.ui.Modal, title="New Market Listing"):
    def __init__(self, typ: str):
        super().__init__(timeout=300)
        self.typ = "buy" if typ.lower().startswith("b") else "sell"
        self.item = discord.ui.TextInput(label="Item name", placeholder="E.g., Dragonbone Wand", min_length=2, max_length=120)
        self.qty = discord.ui.TextInput(label="Quantity", placeholder="e.g., 1", default="1")
        self.price = discord.ui.TextInput(label="Price / Range", placeholder="e.g., 500k–650k or 600k firm", required=False)
        self.trades = discord.ui.TextInput(label="Accept trades? (yes/no)", default="no")
        self.offers = discord.ui.TextInput(label="Taking offers? (yes/no)", default="yes")
        for comp in (self.item, self.qty, self.price, self.trades, self.offers):
            self.add_item(comp)

    async def on_submit(self, interaction: discord.Interaction):
        if not await ensure_guild_auth(interaction.guild):
            return await interaction.response.send_message(":lock: This bot is disabled in this server.", ephemeral=True)
        try:
            qty = int(str(self.qty.value).strip())
            trades = str(self.trades.value).strip().lower() in {"y","yes","true","1"}
            offers = str(self.offers.value).strip().lower() in {"y","yes","true","1"}
            item_name = str(self.item.value).strip()
            price = str(self.price.value).strip() or None
        except Exception:
            return await interaction.response.send_message(":warning: Invalid inputs.", ephemeral=True)
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                """INSERT INTO market_listings
                   (guild_id,user_id,type,item_name,quantity,price,accepts_trades,taking_offers,is_active,created_ts)
                   VALUES (?,?,?,?,?,?,?,?,1,?)""",
                (interaction.guild_id, interaction.user.id, self.typ, item_name, qty, price, 1 if trades else 0, 1 if offers else 0, now_ts())
            )
            await db.commit()
            c = await db.execute("SELECT last_insert_rowid()")
            lid = (await c.fetchone())[0]
        await interaction.response.send_message(f":white_check_mark: Listing `#{lid}` posted.", ephemeral=True)

class MarketOfferModal(discord.ui.Modal, title="Make an Offer"):
    def __init__(self, listing_id: int, owner_id: int):
        super().__init__(timeout=300)
        self.listing_id = int(listing_id)
        self.owner_id = int(owner_id)
        self.offer = discord.ui.TextInput(label="Your offer", placeholder="e.g., 575k + 5 tokens", min_length=2, max_length=200)
        self.add_item(self.offer)

    async def on_submit(self, interaction: discord.Interaction):
        if not await ensure_guild_auth(interaction.guild):
            return await interaction.response.send_message(":lock: This bot is disabled in this server.", ephemeral=True)
        text = str(self.offer.value).strip()
        if not text:
            return await interaction.response.send_message(":warning: Offer text required.", ephemeral=True)
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT INTO market_offers (listing_id, guild_id, bidder_id, text, created_ts) VALUES (?,?,?,?,?)",
                (self.listing_id, interaction.guild_id, interaction.user.id, text, now_ts())
            )
            await db.commit()
        # Notify owner if possible
        try:
            owner = interaction.guild.get_member(self.owner_id) or await interaction.guild.fetch_member(self.owner_id)
            if owner and owner.dm_channel is None:
                await owner.create_dm()
            if owner and owner.dm_channel:
                await owner.dm_channel.send(f"💬 New offer on your listing `#{self.listing_id}` from <@{interaction.user.id}>: **{text}**")
        except Exception:
            pass
        await interaction.response.send_message(":white_check_mark: Offer posted and owner notified.", ephemeral=True)

class MarketListingView(discord.ui.View):
    def __init__(self, listing_id: int, owner_id: int, taking_offers: bool):
        super().__init__(timeout=None)
        self.listing_id = int(listing_id)
        self.owner_id = int(owner_id)
        self.taking_offers = bool(taking_offers)
        # Buttons
        self.add_item(discord.ui.Button(label=f"Listing #{listing_id}", style=discord.ButtonStyle.secondary, disabled=True))
        if self.taking_offers:
            self.add_item(MarketOfferButton(listing_id=self.listing_id, owner_id=self.owner_id))
        self.add_item(MarketToggleActiveButton(listing_id=self.listing_id, owner_id=self.owner_id))

class MarketOfferButton(discord.ui.Button):
    def __init__(self, listing_id: int, owner_id: int):
        super().__init__(label="Make Offer", style=discord.ButtonStyle.primary)
        self.listing_id = int(listing_id)
        self.owner_id = int(owner_id)

    async def callback(self, interaction: discord.Interaction):
        if not await ensure_guild_auth(interaction.guild):
            return await interaction.response.send_message(":lock: This bot is disabled in this server.", ephemeral=True)
        modal = MarketOfferModal(listing_id=self.listing_id, owner_id=self.owner_id)
        await interaction.response.send_modal(modal)

class MarketToggleActiveButton(discord.ui.Button):
    def __init__(self, listing_id: int, owner_id: int):
        super().__init__(label="Toggle Active", style=discord.ButtonStyle.danger)
        self.listing_id = int(listing_id)
        self.owner_id = int(owner_id)

    async def callback(self, interaction: discord.Interaction):
        if not await ensure_guild_auth(interaction.guild):
            return await interaction.response.send_message(":lock: This bot is disabled in this server.", ephemeral=True)
        if interaction.user.id != self.owner_id and not interaction.user.guild_permissions.manage_guild:
            return await interaction.response.send_message(":no_entry: Only the owner or a manager can toggle.", ephemeral=True)
        async with aiosqlite.connect(DB_PATH) as db:
            c = await db.execute("SELECT is_active FROM market_listings WHERE id=? AND guild_id=?", (self.listing_id, interaction.guild_id))
            row = await c.fetchone()
            if not row:
                return await interaction.response.send_message(":warning: Listing not found.", ephemeral=True)
            new_state = 0 if int(row[0]) == 1 else 1
            await db.execute("UPDATE market_listings SET is_active=? WHERE id=? AND guild_id=?", (new_state, self.listing_id, interaction.guild_id))
            await db.commit()
        await interaction.response.send_message(f":white_check_mark: Listing `#{self.listing_id}` is now **{'active' if new_state else 'inactive'}**.", ephemeral=True)

class LixingCreateModal(discord.ui.Modal, title="New Lixing Post"):
    def __init__(self):
        super().__init__(timeout=300)
        self.char_name = discord.ui.TextInput(label="Character name", min_length=2, max_length=50)
        self.char_class = discord.ui.TextInput(label="Class", placeholder="e.g., Mage", min_length=2, max_length=30)
        self.level = discord.ui.TextInput(label="Level (or N/A)", default="N/A")
        self.desired = discord.ui.TextInput(label="Desired number of lixes (or N/A)", default="N/A")
        for comp in (self.char_name, self.char_class, self.level, self.desired):
            self.add_item(comp)

    async def on_submit(self, interaction: discord.Interaction):
        if not await ensure_guild_auth(interaction.guild):
            return await interaction.response.send_message(":lock: This bot is disabled in this server.", ephemeral=True)
        lvl_raw = str(self.level.value).strip()
        try:
            level = int(lvl_raw) if lvl_raw.lower() != "n/a" else -1
        except Exception:
            level = -1
        desired = str(self.desired.value).strip()
        cname = str(self.char_name.value).strip()
        cclass = str(self.char_class.value).strip()
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                """INSERT INTO lixing_posts
                   (guild_id,user_id,char_name,char_class,level,desired_lixes,is_active,created_ts)
                   VALUES (?,?,?,?,?,?,1,?)""",
                (interaction.guild_id, interaction.user.id, cname, cclass, level, desired or None, now_ts())
            )
            await db.commit()
            c = await db.execute("SELECT last_insert_rowid()")
            pid = (await c.fetchone())[0]
        await interaction.response.send_message(f":white_check_mark: Lixing post `#{pid}` created.", ephemeral=True)

class LixingPostView(discord.ui.View):
    def __init__(self, post_id: int, owner_id: int):
        super().__init__(timeout=None)
        self.post_id = int(post_id)
        self.owner_id = int(owner_id)
        self.add_item(discord.ui.Button(label=f"Post #{post_id}", style=discord.ButtonStyle.secondary, disabled=True))
        self.add_item(LixingToggleActiveButton(post_id=self.post_id, owner_id=self.owner_id))

class LixingToggleActiveButton(discord.ui.Button):
    def __init__(self, post_id: int, owner_id: int):
        super().__init__(label="Toggle Active", style=discord.ButtonStyle.danger)
        self.post_id = int(post_id)
        self.owner_id = int(owner_id)

    async def callback(self, interaction: discord.Interaction):
        if not await ensure_guild_auth(interaction.guild):
            return await interaction.response.send_message(":lock: This bot is disabled in this server.", ephemeral=True)
        if interaction.user.id != self.owner_id and not interaction.user.guild_permissions.manage_guild:
            return await interaction.response.send_message(":no_entry: Only the owner or a manager can toggle.", ephemeral=True)
        async with aiosqlite.connect(DB_PATH) as db:
            c = await db.execute("SELECT is_active FROM lixing_posts WHERE id=? AND guild_id=?", (self.post_id, interaction.guild_id))
            row = await c.fetchone()
            if not row:
                return await interaction.response.send_message(":warning: Post not found.", ephemeral=True)
            new_state = 0 if int(row[0]) == 1 else 1
            await db.execute("UPDATE lixing_posts SET is_active=? WHERE id=? AND guild_id=?", (new_state, self.post_id, interaction.guild_id))
            await db.commit()
        await interaction.response.send_message(f":white_check_mark: Lixing post `#{self.post_id}` is now **{'active' if new_state else 'inactive'}**.", ephemeral=True)

# -------------------- DIGEST LOOP (6h) --------------------
@tasks.loop(hours=6.0)
async def market_lixing_digest_loop():
    # Post periodic digests if channels are configured
    for g in bot.guilds:
        if not await ensure_guild_auth(g):
            continue
        try:
            await _post_market_digest(g)
            await _post_lixing_digest(g)
        except Exception as e:
            log.warning(f"Digest loop error for guild {g.id}: {e}")

# -------------------- REACTION EVENTS --------------------
# 1) Subscription panel reactions: add/remove per-boss subscriptions
# 2) Reaction-role panel mapping (rr_panels/rr_map)

@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    if not payload.guild_id:
        return
    guild = bot.get_guild(payload.guild_id)
    if not guild or (bot.user and payload.user_id == bot.user.id):
        return
    if not await ensure_guild_auth(guild):
        return

    emoji_str = str(payload.emoji)

    # Subscription panels (stable edit-only messages)
    records = await get_all_panel_records(guild.id)   # {cat: (message_id, channel_id)}
    panel_msg_ids = {mid for (mid, _ch) in records.values() if mid}
    if payload.message_id in panel_msg_ids:
        async with aiosqlite.connect(DB_PATH) as db:
            # map emoji -> boss_id
            c = await db.execute("SELECT boss_id FROM subscription_emojis WHERE guild_id=? AND emoji=?", (guild.id, emoji_str))
            r = await c.fetchone()
            if r:
                boss_id = int(r[0])
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
    if not payload.guild_id:
        return
    guild = bot.get_guild(payload.guild_id)
    if not guild:
        return
    if not await ensure_guild_auth(guild):
        return

    emoji_str = str(payload.emoji)

    # Subscription panels
    records = await get_all_panel_records(guild.id)
    panel_msg_ids = {mid for (mid, _ch) in records.values() if mid}
    if payload.message_id in panel_msg_ids:
        async with aiosqlite.connect(DB_PATH) as db:
            c = await db.execute("SELECT boss_id FROM subscription_emojis WHERE guild_id=? AND emoji=?", (guild.id, emoji_str))
            r = await c.fetchone()
            if r:
                boss_id = int(r[0])
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

# -------------------- MESSAGE SHORTCUT (KILL) --------------------
@bot.event
async def on_message(message: discord.Message):
    if message.author.bot or not message.guild:
        return
    if not await ensure_guild_auth(message.guild):
        return
    # Quick reset with prefix if not a reserved trigger
    try:
        prefix = await get_guild_prefix(bot, message)
    except Exception:
        prefix = DEFAULT_PREFIX
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

# -------------------- READY / JOIN --------------------
@bot.event
async def on_ready():
    try:
        await init_db()
        # ensure defaults and seed per guild
        for g in bot.guilds:
            await upsert_guild_defaults(g.id)
        await meta_set("last_startup_ts", str(now_ts()))
        await boot_offline_processing()
        for g in bot.guilds:
            await ensure_seed_for_guild(g)
        # start loops
        if not timers_tick.is_running():
            timers_tick.start()
        if not uptime_heartbeat.is_running():
            uptime_heartbeat.start()
        if not market_lixing_digest_loop.is_running():
            market_lixing_digest_loop.start()
        # subscription panels refresh (edit-in-place strategy; no re-posting unless missing)
        for g in bot.guilds:
            await refresh_subscription_messages(g)
        # sync slash
        try:
            await bot.tree.sync()
        except Exception as e:
            log.warning(f"App command sync failed: {e}")
        log.info(f"Logged in as {bot.user} (ID: {bot.user.id})")
    except Exception as e:
        log.exception(f"on_ready error: {e}")

@bot.event
async def on_guild_join(guild: discord.Guild):
    try:
        await init_db()
        await upsert_guild_defaults(guild.id)
        await ensure_seed_for_guild(guild)
        await refresh_subscription_messages(guild)
        try:
            await bot.tree.sync(guild=guild)
        except Exception:
            pass
    except Exception as e:
        log.warning(f"on_guild_join error for {guild.id}: {e}")

# -------------------- ERRORS --------------------
@bot.event
async def on_command_error(ctx, error):
    from discord.ext import commands as ext
    if isinstance(error, ext.CommandNotFound):
        return
    try:
        await ctx.send(f":warning: {error}")
    except Exception:
        pass

# -------------------- SHUTDOWN --------------------
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
        conn.commit()
        conn.close()
    except Exception:
        pass

# -------------------- MAIN --------------------
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
