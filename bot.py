# bot.py — Section 1/4
# Core: env, intents, helpers, schema (with self-healing migration), seed/version flags,
# blacklist + user prefs, permission helpers, auth guard plumbing (column in schema).

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
    raise SystemExit("DISCORD_TOKEN missing in .env / environment")

ALLOW_POWERSHELL = os.getenv("ALLOW_POWERSHELL", "0") in {"1", "true", "True", "yes", "YES"}
DB_PATH = os.getenv("DB_PATH", "/data/bosses.db")  # default to /data for Render persistent disk
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

_last_timer_tick_ts: int = 0
_prev_timer_tick_ts: int = 0

# Bump this when seed content changes so we don't reseed repeatedly
SEED_VERSION = "v2025-09-13-authcol-subping-window-market-lixing"

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
RESERVED_TRIGGERS = {
    "help","boss","timers","setprefix","seed_import",
    "setsubchannel","setsubpingchannel","showsubscriptions","setuptime",
    "setheartbeatchannel","setannounce","seteta","status","health",
    "setcatcolor","intervals","market","lixing",
}

muted_due_on_boot: Set[int] = set()
bot._seen_keys = set()

# -------------------- PRE-FLIGHT MIGRATIONS (sync) --------------------
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

    # helper to check existing columns
    def col_exists(table, col):
        cur.execute(f"PRAGMA table_info({table})")
        return any(row[1] == col for row in cur.fetchall())

    # add columns over time
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
    # 🔧 NEW: auth_user_id column for guild auth guard
    if not col_exists("guild_config","auth_user_id"):
        cur.execute("ALTER TABLE guild_config ADD COLUMN auth_user_id TEXT DEFAULT NULL")

    # other tables
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
    conn.commit(); conn.close()
preflight_migrate_sync()

# -------------------- ASYNC DB INIT + SELF-HEAL MIGRATIONS --------------------
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
            auth_user_id TEXT DEFAULT NULL              -- 🔧 NEW column (auth guard)
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
        await db.commit()

        # ---- Self-heal migrations (idempotent) ----
        async def ensure_column(table: str, column: str, ddl: str):
            try:
                await db.execute(f"SELECT {column} FROM {table} LIMIT 1")
            except Exception:
                try:
                    await db.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")
                    await db.commit()
                    log.info(f"Added column {column} to {table}")
                except Exception as e:
                    log.warning(f"Column {column} maybe exists on {table}: {e}")

        await ensure_column("guild_config", "auth_user_id", "auth_user_id TEXT DEFAULT NULL")

async def upsert_guild_defaults(guild_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO guild_config (guild_id, prefix, uptime_minutes, show_eta) VALUES (?,?,?,?) "
            "ON CONFLICT(guild_id) DO NOTHING",
            (guild_id, DEFAULT_PREFIX, DEFAULT_UPTIME_MINUTES, 0)
        ); await db.commit()

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

# -------------------- BLACKLIST --------------------
async def is_blacklisted(guild_id: int, user_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT 1 FROM blacklist WHERE guild_id=? AND user_id=?", (guild_id, user_id))
        return (await c.fetchone()) is not None

def blacklist_check():
    async def predicate(ctx: commands.Context) -> bool:
        if not ctx.guild: return True
        if await is_blacklisted(ctx.guild.id, ctx.author.id):
            try:
                await ctx.send(":no_entry: You are blacklisted from using this bot.")
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
    if not rows: return await ctx.send("No users blacklisted.")
    mentions = " ".join(f"<@{r[0]}>" for r in rows)
    await ctx.send(f"Blacklisted: {mentions}")

bot.add_check(blacklist_check())

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

# -------------------- PERMISSION CHECKS --------------------
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

async def has_trusted(member: discord.Member, guild_id: int, boss_id: Optional[int] = None) -> bool:
    if member.guild_permissions.administrator: return True
    async with aiosqlite.connect(DB_PATH) as db:
        if boss_id:
            c = await db.execute("SELECT trusted_role_id FROM bosses WHERE id=? AND guild_id=?", (boss_id, guild_id))
            r = await c.fetchone()
            if r and r[0]: return any(role.id == r[0] for role in member.roles)
    return member.guild_permissions.manage_messages

# -------------------- AUTH GUARD (requires a specific user to be in guild) --------------------
# We store optional auth_user_id per guild in guild_config.auth_user_id; if set and user not present, bot stays passive.

async def _get_auth_user_id_from_db(guild_id: int) -> Optional[int]:
    async with aiosqlite.connect(DB_PATH) as db:
        try:
            c = await db.execute("SELECT auth_user_id FROM guild_config WHERE guild_id=?", (guild_id,))
            r = await c.fetchone()
            if r and r[0]:
                try:
                    return int(r[0])
                except Exception:
                    return None
        except Exception as e:
            log.warning(f"auth_user_id column missing? {e}")
            return None
    return None

async def ensure_guild_auth(guild: discord.Guild) -> bool:
    """Return True if bot is allowed to operate in this guild."""
    # If you want to hard-pin to a particular user name, set once via DB or a helper command (omitted from help).
    auth_uid = await _get_auth_user_id_from_db(guild.id)
    if not auth_uid:
        return True  # no restriction set
    member = guild.get_member(auth_uid) or await guild.fetch_member(auth_uid) if guild else None
    if member:
        return True
    # If restricted and missing, remain passive
    try:
        ch = guild.system_channel or (guild.text_channels[0] if guild.text_channels else None)
        if can_send(ch):
            await ch.send(":no_entry: Bot is locked — required user is not present in this server.")
    except Exception:
        pass
    return False

# (rest of code continues in Section 2)
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

# -------------------- SUBSCRIPTION PANELS (stable/atomic) --------------------
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
    Atomic refresh:
      - Build new messages first and capture their IDs
      - Upsert subscription_panels to point to the new message IDs
      - Delete any obsolete messages left from the previous render
    User subscriptions are stored in DB and are NOT lost.
    """
    gid = guild.id
    sub_ch_id = await get_subchannel_id(gid)
    if not sub_ch_id:
        return
    channel = guild.get_channel(sub_ch_id)
    if not can_send(channel):
        return

    # snapshot existing
    old_panels = await get_all_panel_records(gid)
    built: Dict[str, int] = {}
    leftovers: List[Tuple[int, int]] = []  # (message_id, channel_id)

    # build in order
    for cat in CATEGORY_ORDER:
        # skip empty categories quickly
        async with aiosqlite.connect(DB_PATH) as db:
            c = await db.execute("SELECT COUNT(*) FROM bosses WHERE guild_id=? AND category=?", (gid, cat))
            count = (await c.fetchone())[0]
        if count == 0:
            continue

        content, embed, emojis = await build_subscription_embed_for_category(gid, cat)
        if not embed:  # nothing to post
            continue

        # if we already have a panel for this cat and it's in the same channel, edit in place
        existing_id, existing_ch = old_panels.get(cat, (None, None))
        if existing_id and existing_ch == sub_ch_id:
            try:
                msg = await channel.fetch_message(existing_id)
                await msg.edit(content=content, embed=embed)
                # ensure all required reactions present
                if can_react(channel):
                    try:
                        existing = set(str(r.emoji) for r in msg.reactions)
                        for e in [e for e in emojis if e not in existing]:
                            await msg.add_reaction(e)
                            await asyncio.sleep(0.2)
                    except Exception as e:
                        log.warning(f"Adding reactions failed for {cat}: {e}")
                built[cat] = msg.id
                continue
            except Exception:
                # fall through to post a new message
                pass

        # post a new message (atomic path)
        try:
            new_msg = await channel.send(content=content, embed=embed)
            if can_react(channel):
                try:
                    for e in emojis:
                        await new_msg.add_reaction(e)
                        await asyncio.sleep(0.2)
                except Exception as e:
                    log.warning(f"Adding reactions failed for {cat}: {e}")
            built[cat] = new_msg.id
            # mark old one, if exists, for deletion after we swap DB pointer
            if existing_id and existing_ch:
                leftovers.append((existing_id, existing_ch))
        except Exception as e:
            log.warning(f"Subscription panel ({cat}) create failed: {e}")

    # swap DB pointers to the new messages
    for cat, mid in built.items():
        await set_panel_record(gid, cat, mid, channel.id)

    # cleanup obsolete messages
    for msg_id, ch_id in leftovers:
        ch = guild.get_channel(ch_id)
        if not ch: continue
        try:
            msg = await ch.fetch_message(msg_id)
            await msg.delete()
            await asyncio.sleep(0.2)
        except Exception:
            pass

# Pings to subscribers (separate channel)
async def send_subscription_ping(guild_id: int, boss_id: int, phase: str, boss_name: str, when_left: Optional[int] = None):
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT sub_ping_channel_id, sub_channel_id FROM guild_config WHERE guild_id=?", (guild_id,))
        r = await c.fetchone()
        sub_ping_id = (r[0] if r else None) or (r[1] if r else None)  # fallback to panel channel if ping channel unset
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

# -------------------- TIMER & HEARTBEAT LOOPS --------------------
def window_label(now: int, next_ts: int, window_m: int) -> str:
    """
    Display only minutes for window state:
      - before window: '<win>m (pending)'
      - during window: '<left>m left (open)'
      - after window but within grace: 'closed'
      - after grace: '-Nada'
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
            if not (prev < int(next_ts) <= now):
                continue
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
            ch = await resolve_announce_channel(gid, ch_id, cat)
            if ch and can_send(ch):
                try:
                    ago = human_ago(boot - int(ts))
                    await ch.send(f":zzz: While I was offline, **{name}** spawned ({ago}).")
                except Exception as e:
                    log.warning(f"Offline notice failed: {e}")
            await send_subscription_ping(gid, bid, phase="window", boss_name=name)

# -------------------- STARTUP SEED --------------------
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
    # DL (✅ 180 fixed to 88/3)
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
    await refresh_subscription_messages(guild)

# -------------------- EVENTS --------------------
@bot.event
async def on_ready():
    await init_db()
    for g in bot.guilds:
        await upsert_guild_defaults(g.id)
    await meta_set("last_startup_ts", str(now_ts()))
    await boot_offline_processing()
    for g in bot.guilds:
        if not await ensure_guild_auth(g):
            continue
        await ensure_seed_for_guild(g)
    if not timers_tick.is_running(): timers_tick.start()
    if not uptime_heartbeat.is_running(): uptime_heartbeat.start()
    for g in bot.guilds:
        if not await ensure_guild_auth(g):
            continue
        await refresh_subscription_messages(g)
    try:
        await bot.tree.sync()
    except Exception as e:
        log.warning(f"App command sync failed: {e}")
    log.info(f"Logged in as {bot.user} (ID: {bot.user.id})")

@bot.event
async def on_guild_join(guild):
    await init_db(); await upsert_guild_defaults(guild.id)
    if await ensure_guild_auth(guild):
        await ensure_seed_for_guild(guild)
        await refresh_subscription_messages(guild)
    try: await bot.tree.sync(guild=guild)
    except Exception: pass

@bot.event
async def on_message(message: discord.Message):
    if message.author.bot or not message.guild: return
    if not await ensure_guild_auth(message.guild):  # passive if auth user absent
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

# Reactions: subscriptions + reaction-role panels
@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    if bot.user and payload.user_id == bot.user.id: return
    guild = bot.get_guild(payload.guild_id)
    if not guild or not await ensure_guild_auth(guild): return
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
                c = await db.execute("SELECT role_id FROM rr_map WHERE panel_message_id=? AND emoji=?", (payload.message_id, str(payload.emoji)))
                row = await c.fetchone()
            if not row: return
            role = guild.get_role(int(row[0]))
            if role:
                await member.remove_roles(role, reason="Reaction role opt-out")
        except Exception as e:
            log.warning(f"Remove reaction-role failed: {e}")

# -------------------- HELP / STATUS / HEALTH --------------------
@bot.command(name="help")
async def help_cmd(ctx):
    # (No auth/blacklist commands listed per your request)
    p = await get_guild_prefix(bot, ctx.message)
    lines = [
        f"**Boss Tracker — Quick Help**",
        f"Prefix: `{p}`  • Change: `{p}setprefix <new>`",
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
        f"**Reaction Roles**",
        f"• `/roles_panel` — create a message where reacting grants/removes roles.",
        "",
        f"**Market & Lixing** (see `/market` and `/lixing` commands for interactive menus).",
    ]
    text = "\n".join(lines)
    if len(text) > 1990: text = text[:1985] + "…"
    if can_send(ctx.channel): await ctx.send(text)

@bot.command(name="status")
async def status_cmd(ctx):
    if not await ensure_guild_auth(ctx.guild):
        return await ctx.send(":no_entry: Bot is locked — required user is not present.")
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
    required = {"bosses","guild_config","meta","category_colors","subscription_emojis","subscription_members","boss_aliases","category_channels","user_timer_prefs","subscription_panels","rr_panels","rr_map","blacklist"}
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
        f"Timers loop: {'running' if timers_tick.is_running() else 'stopped'}",
        f"Heartbeat loop: {'running' if uptime_heartbeat.is_running() else 'stopped'}",
        f"Last timer tick: {ts_to_utc(_last_timer_tick_ts) if _last_timer_tick_ts else '—'}"
        + (f" ({human_ago(tick_age)})" if tick_age is not None else ""),
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
    # Do not nuke existing until the new ones are built — handled by refresh_subscription_messages
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
    if not await ensure_guild_auth(ctx.guild):
        return await ctx.send(":no_entry: Bot is locked — required user is not present.")
    await refresh_subscription_messages(ctx.guild)
    await ctx.send(":white_check_mark: Subscription panels refreshed (one per category).")
# -------------------- USER PREFS (Category toggles for /timers) --------------------
async def get_user_timer_prefs(guild_id: int, user_id: int) -> Dict[str, int]:
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT category, shown FROM user_timer_prefs WHERE guild_id=? AND user_id=?", (guild_id, user_id))
        rows = await c.fetchall()
    return {row[0]: int(row[1]) for row in rows}

async def set_user_timer_pref(guild_id: int, user_id: int, category: str, shown: bool):
    cat = norm_cat(category)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO user_timer_prefs (guild_id,user_id,category,shown) VALUES (?,?,?,?) "
            "ON CONFLICT(guild_id,user_id,category) DO UPDATE SET shown=excluded.shown",
            (guild_id, user_id, cat, 1 if shown else 0)
        ); await db.commit()

# -------------------- TIMER VIEW & EMBED BUILDERS --------------------
def category_emoji(cat: str) -> str:
    base = {
        "meteoric": "☄️", "frozen": "❄️", "dl": "🔶", "edl": "🔷", "midraids": "⚔️",
        "rings": "💍", "eg": "🐉", "misc": "📌"
    }
    return base.get(norm_cat(cat), "📦")

def fmt_eta(now_ts_i: int, target_ts: int, show_eta: bool) -> str:
    if not show_eta: return ""
    if target_ts < now_ts_i: return ""
    return f" • ETA: `{ts_to_utc(target_ts)}`"

async def get_category_color(guild_id: int, category: str) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT color FROM category_colors WHERE guild_id=? AND category=?", (guild_id, norm_cat(category)))
        r = await c.fetchone()
    if r and r[0]:
        try:
            return int(str(r[0]).replace("#",""), 16)
        except Exception:
            pass
    return 0x2F3136  # discord embed bg-ish

async def build_timer_embeds_for_categories(guild: discord.Guild, show_only: Optional[Set[str]] = None, view_shown: Optional[Set[str]] = None) -> List[discord.Embed]:
    gid = guild.id
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT COALESCE(show_eta,0) FROM guild_config WHERE guild_id=?", (gid,))
        row = await c.fetchone()
    show_eta = bool(int(row[0])) if row else False
    nowi = now_ts()

    # Load bosses grouped
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT id,name,category,spawn_minutes,window_minutes,next_spawn_ts FROM bosses WHERE guild_id=?", (gid,))
        rows = await c.fetchall()

    by_cat: Dict[str, List[tuple]] = {}
    for bid, nm, cat, spawn_m, win_m, ts in rows:
        catn = norm_cat(cat or "misc")
        if show_only and catn not in show_only:
            continue
        by_cat.setdefault(catn, []).append((bid, nm, int(spawn_m or 0), int(win_m or 0), int(ts or 0)))

    embeds: List[discord.Embed] = []
    for cat in CATEGORY_ORDER:
        if cat not in by_cat: continue
        shown_flag = (view_shown is None) or (cat in view_shown)
        em = discord.Embed(
            title=f"{category_emoji(cat)} {cat} — Timers",
            color=await get_category_color(gid, cat)
        )
        if not shown_flag:
            em.description = "_Hidden in your view_"
            embeds.append(em)
            continue

        # Sort by status: open -> pending sooner -> nada
        def sort_key(item):
            _bid, _nm, _spawn_m, win_m, ts = item
            lab = window_label(nowi, ts, win_m)
            # 'open' first, then 'pending' by ts left, then 'closed', then '-Nada'
            order = 3
            if lab.endswith("(open)"): order = 0
            elif "(pending)" in lab: order = 1
            elif lab == "closed": order = 2
            return (order, ts)
        items = sorted(by_cat[cat], key=sort_key)

        lines: List[str] = []
        for bid, name, spawn_m, win_m, ts in items:
            lab = window_label(nowi, ts, win_m)
            if "(pending)" in lab:
                left = max(0, ts - nowi)
                row = f"**{name}** — `Spawn in {fmt_delta_for_list(left)}`{fmt_eta(nowi, ts, show_eta)}"
            elif lab.endswith("(open)"):
                left_m = int(lab.split("m")[0])
                row = f"**{name}** — `Window open: {left_m}m left`"
            elif lab == "closed":
                row = f"**{name}** — `closed`"
            else:
                row = f"**{name}** — `-Nada`"
            lines.append(row)

        bucket = ""
        for ln in lines:
            if len(bucket) + len(ln) + 1 > 1000:
                em.add_field(name=cat, value=bucket, inline=False); bucket = ln + "\n"
            else:
                bucket += ln + "\n"
        if bucket:
            em.add_field(name=cat, value=bucket, inline=False)
        embeds.append(em)
    if not embeds:
        embeds.append(discord.Embed(description="No bosses configured for your selected categories.", color=0x2F3136))
    return embeds

class TimersView(discord.ui.View):
    def __init__(self, guild_id: int, user_id: int, shown: Set[str]):
        super().__init__(timeout=180)
        self.guild_id = guild_id
        self.user_id = user_id
        self.shown = shown

        # Build per-category toggle buttons
        row = 0
        for cat in CATEGORY_ORDER:
            style = discord.ButtonStyle.success if cat in shown else discord.ButtonStyle.secondary
            label = f"{'✓ ' if cat in shown else ''}{cat}"
            btn = discord.ui.Button(label=label, style=style, custom_id=f"tv:{cat}")
            btn.callback = self._make_toggle(cat)
            self.add_item(btn)
            row += 1
            if row % 5 == 0:
                # Discord auto wraps; keeping it simple
                pass

        # Refresh button
        refresh = discord.ui.Button(label="Refresh", style=discord.ButtonStyle.primary, custom_id="tv:refresh")
        refresh.callback = self._refresh
        self.add_item(refresh)

    def _make_toggle(self, cat: str):
        async def _cb(interaction: discord.Interaction):
            if interaction.user.id != self.user_id:
                return await interaction.response.send_message("This panel is for someone else. Use `/timers` to get yours.", ephemeral=True)
            new_state = cat not in self.shown
            self.shown = set(self.shown)
            if new_state: self.shown.add(cat)
            else: self.shown.discard(cat)
            await set_user_timer_pref(self.guild_id, self.user_id, cat, new_state)
            embeds = await build_timer_embeds_for_categories(interaction.guild, view_shown=self.shown)
            await interaction.response.edit_message(embeds=embeds, view=self)
        return _cb

    async def _refresh(self, interaction: discord.Interaction):
        if interaction.user.id != self.user_id:
            return await interaction.response.send_message("This panel is for someone else. Use `/timers` to get yours.", ephemeral=True)
        embeds = await build_timer_embeds_for_categories(interaction.guild, view_shown=self.shown)
        await interaction.response.edit_message(embeds=embeds, view=self)

# -------------------- TIMERS / INTERVALS COMMANDS --------------------
@bot.command(name="timers")
async def timers_cmd(ctx):
    if not await ensure_guild_auth(ctx.guild):
        return await ctx.send(":no_entry: Bot is locked — required user is not present.")
    prefs = await get_user_timer_prefs(ctx.guild.id, ctx.author.id)
    shown = {c for c in CATEGORY_ORDER if prefs.get(c, 1) == 1}
    embeds = await build_timer_embeds_for_categories(ctx.guild, view_shown=shown)
    if can_send(ctx.channel):
        await ctx.send(embeds=embeds, view=TimersView(ctx.guild.id, ctx.author.id, shown))

@bot.tree.command(name="timers", description="Show timers with per-category toggles (ephemeral).")
async def slash_timers(interaction: discord.Interaction):
    if not interaction.guild or not await ensure_guild_auth(interaction.guild):
        return await interaction.response.send_message(":no_entry: Bot is locked — required user is not present.", ephemeral=True)
    prefs = await get_user_timer_prefs(interaction.guild.id, interaction.user.id)
    shown = {c for c in CATEGORY_ORDER if prefs.get(c, 1) == 1}
    embeds = await build_timer_embeds_for_categories(interaction.guild, view_shown=shown)
    await interaction.response.send_message(embeds=embeds, view=TimersView(interaction.guild.id, interaction.user.id, shown), ephemeral=True)

@bot.command(name="intervals")
async def intervals_cmd(ctx):
    """Show configured spawn + window + preannounce for each boss."""
    gid = ctx.guild.id
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT name,category,spawn_minutes,window_minutes,COALESCE(pre_announce_min,0) FROM bosses WHERE guild_id=?", (gid,))
        rows = await c.fetchall()
    if not rows:
        return await ctx.send("No bosses configured.")
    rows.sort(key=lambda r: (CATEGORY_ORDER.index(norm_cat(r[1])) if norm_cat(r[1]) in CATEGORY_ORDER else 99, natural_key(r[0])))
    chunks: List[str] = []
    chunk = ""
    for nm, cat, sp, win, pre in rows:
        line = f"**{nm}** [{cat}] — respawn `{sp}m`, window `{win}m`, pre `{pre}m`"
        if len(chunk) + len(line) + 1 > 1900:
            chunks.append(chunk); chunk = line + "\n"
        else:
            chunk += line + "\n"
    if chunk: chunks.append(chunk)
    for idx, ch in enumerate(chunks, 1):
        await ctx.send(ch)

# -------------------- MARKET SYSTEM --------------------
# Schema assumed in Section 1:
#   guild_config: market_channel_id
#   tables: market_listings(id, guild_id, user_id, type, item_name, qty, price_text, accepts_trades, taking_offers, created_ts, active)
#           market_offers(id, listing_id, guild_id, offer_user_id, offer_text, created_ts, status)

async def get_market_channel(guild: discord.Guild) -> Optional[discord.TextChannel]:
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT market_channel_id FROM guild_config WHERE guild_id=?", (guild.id,))
        r = await c.fetchone()
    ch = guild.get_channel(r[0]) if (r and r[0]) else None
    return ch if can_send(ch) else None

async def set_market_channel(guild_id: int, channel_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO guild_config (guild_id,market_channel_id) VALUES (?,?) "
            "ON CONFLICT(guild_id) DO UPDATE SET market_channel_id=excluded.market_channel_id",
            (guild_id, channel_id)
        ); await db.commit()

def _listing_type_emoji(t: str) -> str:
    return "🛒" if t == "sell" else "🔎"

def _bool_yn(b: int) -> str:
    return "Yes" if int(b) else "No"

async def build_market_embed(guild: discord.Guild, filter_type: Optional[str] = None, mine_for: Optional[int] = None, query: Optional[str] = None) -> discord.Embed:
    q = "SELECT id,user_id,type,item_name,qty,price_text,accepts_trades,taking_offers,created_ts FROM market_listings WHERE guild_id=? AND active=1"
    params: List[Any] = [guild.id]
    if filter_type in {"buy","sell"}:
        q += " AND type=?"; params.append(filter_type)
    if mine_for:
        q += " AND user_id=?"; params.append(mine_for)
    if query:
        q += " AND instr(lower(item_name), ?) > 0"; params.append(query.lower())
    q += " ORDER BY created_ts DESC LIMIT 50"

    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute(q, tuple(params))
        rows = await c.fetchall()

    em = discord.Embed(title="Market Listings", color=0x4CAF50)
    if filter_type: em.set_footer(text=f"Filter: {filter_type}")
    if not rows:
        em.description = "No listings match."
        return em

    lines: List[str] = []
    for lid, uid, typ, name, qty, price, trades, offers, created in rows:
        member = guild.get_member(uid)
        who = member.mention if member else f"<@{uid}>"
        lines.append(
            f"[`{lid}`] {_listing_type_emoji(typ)} **{name}** x{qty} • Price: `{price}` • Trades: `{_bool_yn(trades)}` • Offers: `{_bool_yn(offers)}` • by {who} • {human_ago(now_ts()-int(created))}"
        )
    bucket = ""
    for ln in lines:
        if len(bucket) + len(ln) + 1 > 1000:
            em.add_field(name="\u200b", value=bucket, inline=False); bucket = ln + "\n"
        else:
            bucket += ln + "\n"
    if bucket:
        em.add_field(name="\u200b", value=bucket, inline=False)
    return em

class MarketAddModal(discord.ui.Modal, title="Add Market Listing"):
    item = discord.ui.TextInput(label="Item name", placeholder="e.g., Frozen Rems", max_length=80)
    quantity = discord.ui.TextInput(label="Quantity", placeholder="e.g., 10", max_length=10)
    price = discord.ui.TextInput(label="Price or range", placeholder="e.g., 50k or 45–55k", max_length=40)
    accepts_trades = discord.ui.TextInput(label="Accept trades? (yes/no)", default="no", max_length=5)
    taking_offers = discord.ui.TextInput(label="Taking offers? (yes/no)", default="yes", max_length=5)

    def __init__(self, typ: str):
        super().__init__()
        self.typ = typ  # 'buy' or 'sell'

    async def on_submit(self, interaction: discord.Interaction):
        try:
            qty = int(str(self.quantity.value).strip())
            price = str(self.price.value).strip()
            trades = 1 if str(self.accepts_trades.value).strip().lower() in {"y","yes","true","1"} else 0
            offers = 1 if str(self.taking_offers.value).strip().lower() in {"y","yes","true","1"} else 0
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute(
                    "INSERT INTO market_listings (guild_id,user_id,type,item_name,qty,price_text,accepts_trades,taking_offers,created_ts,active) "
                    "VALUES (?,?,?,?,?,?,?,?,?,1)",
                    (interaction.guild.id, interaction.user.id, self.typ, str(self.item.value).strip(), qty, price, trades, offers, now_ts())
                ); await db.commit()
            await interaction.response.send_message(":white_check_mark: Listing added.", ephemeral=True)
        except Exception as e:
            await interaction.response.send_message(f":x: Failed to add listing: {e}", ephemeral=True)

class OfferModal(discord.ui.Modal, title="Make an Offer"):
    offer = discord.ui.TextInput(label="Your offer", style=discord.TextStyle.paragraph, placeholder="e.g., 55k or trade 2x shards + 10k", max_length=300)

    def __init__(self, listing_id: int):
        super().__init__()
        self.listing_id = listing_id

    async def on_submit(self, interaction: discord.Interaction):
        # store offer, ping owner, and append to public thread/message if available
        async with aiosqlite.connect(DB_PATH) as db:
            c = await db.execute("SELECT user_id, item_name FROM market_listings WHERE id=? AND guild_id=? AND active=1", (self.listing_id, interaction.guild.id))
            r = await c.fetchone()
            if not r:
                return await interaction.response.send_message(":x: Listing no longer active.", ephemeral=True)
            owner_id, item_name = int(r[0]), r[1]
            await db.execute(
                "INSERT INTO market_offers (listing_id,guild_id,offer_user_id,offer_text,created_ts,status) VALUES (?,?,?,?,?,?)",
                (self.listing_id, interaction.guild.id, interaction.user.id, str(self.offer.value).strip(), now_ts(), "open")
            ); await db.commit()
        owner = interaction.guild.get_member(owner_id)
        if owner:
            try:
                await owner.send(f"📨 New offer on your listing [`{self.listing_id}`] **{item_name}** from {interaction.user.mention}: {self.offer.value}")
            except Exception:
                pass
        await interaction.response.send_message(":white_check_mark: Offer submitted and the seller has been notified.", ephemeral=True)

class MarketView(discord.ui.View):
    def __init__(self, user_id: int):
        super().__init__(timeout=180)
        self.user_id = user_id
        self.filter_type: Optional[str] = None
        self.query: Optional[str] = None
        # Controls
        self.add_item(discord.ui.Button(label="Show All", style=discord.ButtonStyle.secondary, custom_id="mk:all"))
        self.add_item(discord.ui.Button(label="Buys", style=discord.ButtonStyle.secondary, custom_id="mk:buy"))
        self.add_item(discord.ui.Button(label="Sells", style=discord.ButtonStyle.secondary, custom_id="mk:sell"))
        self.add_item(discord.ui.Button(label="My Listings", style=discord.ButtonStyle.primary, custom_id="mk:mine"))
        self.add_item(discord.ui.Button(label="Add Buy", style=discord.ButtonStyle.success, custom_id="mk:add:buy"))
        self.add_item(discord.ui.Button(label="Add Sell", style=discord.ButtonStyle.success, custom_id="mk:add:sell"))
        self.add_item(discord.ui.Button(label="Refresh", style=discord.ButtonStyle.secondary, custom_id="mk:refresh"))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        # Allow anyone to use; some actions are user-scoped
        return True

    async def _refresh(self, interaction: discord.Interaction, mine: bool = False):
        em = await build_market_embed(interaction.guild, self.filter_type, (interaction.user.id if mine else None), self.query)
        await interaction.response.edit_message(embed=em, view=self)

    @discord.ui.button(label="Make Offer (enter ID)", style=discord.ButtonStyle.blurple, custom_id="mk:offer")
    async def _offer(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(MakeOfferAskModal())

    async def on_timeout(self):
        # nothing — view will just disable; no state to persist
        pass

class MakeOfferAskModal(discord.ui.Modal, title="Make Offer — Choose Listing ID"):
    listing_id = discord.ui.TextInput(label="Listing ID", placeholder="e.g., 123", max_length=10)
    async def on_submit(self, interaction: discord.Interaction):
        try:
            lid = int(str(self.listing_id.value).strip())
        except Exception:
            return await interaction.response.send_message(":x: Invalid listing id.", ephemeral=True)
        await interaction.response.send_modal(OfferModal(lid))

@bot.tree.command(name="market", description="Open the Market panel.")
async def market_cmd(interaction: discord.Interaction):
    if not interaction.guild or not await ensure_guild_auth(interaction.guild):
        return await interaction.response.send_message(":no_entry: Bot is locked — required user is not present.", ephemeral=True)
    em = await build_market_embed(interaction.guild)
    await interaction.response.send_message(embed=em, view=MarketView(interaction.user.id))

@bot.command(name="market")
async def market_text_cmd(ctx):
    em = await build_market_embed(ctx.guild)
    await ctx.send(embed=em, view=MarketView(ctx.author.id))

@bot.command(name="marketsetchannel")
@commands.has_permissions(manage_guild=True)
async def market_set_channel_cmd(ctx, channel: discord.TextChannel):
    await set_market_channel(ctx.guild.id, channel.id)
    await ctx.send(f":white_check_mark: Market digest channel set to {channel.mention}.")

@bot.command(name="marketremove")
async def market_remove_cmd(ctx, listing_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE market_listings SET active=0 WHERE id=? AND guild_id=? AND user_id=?", (listing_id, ctx.guild.id, ctx.author.id))
        await db.commit()
    await ctx.send(f":white_check_mark: Listing `{listing_id}` removed (if it was yours and active).")

@bot.command(name="marketmy")
async def market_my_cmd(ctx):
    em = await build_market_embed(ctx.guild, mine_for=ctx.author.id)
    await ctx.send(embed=em, view=MarketView(ctx.author.id))

# digest every 6 hours
@tasks.loop(hours=6.0)
async def market_digest_loop():
    for g in bot.guilds:
        ch = await get_market_channel(g)
        if not ch: continue
        try:
            em = await build_market_embed(g)
            await ch.send(content="🧾 **Market Digest** (every 6h)", embed=em)
        except Exception as e:
            log.warning(f"Market digest failed in {g.id}: {e}")
if not market_digest_loop.is_running():
    market_digest_loop.start()

# -------------------- LIXING SYSTEM --------------------
# Schema assumed in Section 1:
#   guild_config: lixing_channel_id
#   tables: lixing_listings(id,guild_id,user_id,char_name,char_class,level,desired_lixes_text,created_ts,active)

LIX_CLASSES = ["Warrior", "Ranger", "Mage", "Druid", "Rogue", "Other"]

async def get_lixing_channel(guild: discord.Guild) -> Optional[discord.TextChannel]:
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT lixing_channel_id FROM guild_config WHERE guild_id=?", (guild.id,))
        r = await c.fetchone()
    ch = guild.get_channel(r[0]) if (r and r[0]) else None
    return ch if can_send(ch) else None

async def set_lixing_channel(guild_id: int, channel_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO guild_config (guild_id,lixing_channel_id) VALUES (?,?) "
            "ON CONFLICT(guild_id) DO UPDATE SET lixing_channel_id=excluded.lixing_channel_id",
            (guild_id, channel_id)
        ); await db.commit()

async def build_lixing_embed(guild: discord.Guild, class_filter: Optional[str] = None, mine_for: Optional[int] = None) -> discord.Embed:
    q = "SELECT id,user_id,char_name,char_class,level,desired_lixes_text,created_ts FROM lixing_listings WHERE guild_id=? AND active=1"
    params: List[Any] = [guild.id]
    if class_filter and class_filter in LIX_CLASSES:
        q += " AND char_class=?"; params.append(class_filter)
    if mine_for:
        q += " AND user_id=?"; params.append(mine_for)
    q += " ORDER BY created_ts DESC LIMIT 50"

    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute(q, tuple(params))
        rows = await c.fetchall()

    em = discord.Embed(title="Lixing — Group Finder", color=0x00BCD4)
    if class_filter: em.set_footer(text=f"Class filter: {class_filter}")
    if not rows:
        em.description = "No active Lixing listings."
        return em

    lines: List[str] = []
    for lid, uid, cname, cclass, lvl, want, created in rows:
        who = guild.get_member(uid)
        who_mention = who.mention if who else f"<@{uid}>"
        want_txt = want if want and want.strip() else "N/A"
        lines.append(f"[`{lid}`] **{cname}** ({cclass} {lvl}) • Wants: `{want_txt}` • by {who_mention} • {human_ago(now_ts()-int(created))}")
    bucket = ""
    for ln in lines:
        if len(bucket) + len(ln) + 1 > 1000:
            em.add_field(name="\u200b", value=bucket, inline=False); bucket = ln + "\n"
        else:
            bucket += ln + "\n"
    if bucket:
        em.add_field(name="\u200b", value=bucket, inline=False)
    return em

class LixingAddModal(discord.ui.Modal, title="Add Lixing Listing"):
    char_name = discord.ui.TextInput(label="Character name", max_length=40)
    char_class = discord.ui.TextInput(label="Class (Warrior/Ranger/Mage/Druid/Rogue/Other)", max_length=20)
    level = discord.ui.TextInput(label="Level", placeholder="e.g., 180", max_length=6)
    desired = discord.ui.TextInput(label="Desired number of lixes (or N/A)", default="N/A", max_length=40)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            cclass = str(self.char_class.value).strip().title()
            if cclass not in LIX_CLASSES:
                return await interaction.response.send_message(":x: Class must be one of Warrior, Ranger, Mage, Druid, Rogue, Other.", ephemeral=True)
            lvl = int(str(self.level.value).strip())
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute(
                    "INSERT INTO lixing_listings (guild_id,user_id,char_name,char_class,level,desired_lixes_text,created_ts,active) "
                    "VALUES (?,?,?,?,?,?,?,1)",
                    (interaction.guild.id, interaction.user.id, str(self.char_name.value).strip(), cclass, lvl, str(self.desired.value).strip(), now_ts())
                ); await db.commit()
            await interaction.response.send_message(":white_check_mark: Lixing listing added.", ephemeral=True)
        except Exception as e:
            await interaction.response.send_message(f":x: Failed: {e}", ephemeral=True)

class LixingView(discord.ui.View):
    def __init__(self, user_id: int):
        super().__init__(timeout=180)
        self.user_id = user_id

        self.add_item(discord.ui.Button(label="All", style=discord.ButtonStyle.secondary, custom_id="lx:all"))
        for c in LIX_CLASSES:
            self.add_item(discord.ui.Button(label=c, style=discord.ButtonStyle.secondary, custom_id=f"lx:class:{c}"))
        self.add_item(discord.ui.Button(label="My Listings", style=discord.ButtonStyle.primary, custom_id="lx:mine"))
        self.add_item(discord.ui.Button(label="Add", style=discord.ButtonStyle.success, custom_id="lx:add"))
        self.add_item(discord.ui.Button(label="Refresh", style=discord.ButtonStyle.secondary, custom_id="lx:refresh"))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return True

@bot.tree.command(name="lixing", description="Open Lixing finder.")
async def lixing_cmd(interaction: discord.Interaction):
    if not interaction.guild or not await ensure_guild_auth(interaction.guild):
        return await interaction.response.send_message(":no_entry: Bot is locked — required user is not present.", ephemeral=True)
    em = await build_lixing_embed(interaction.guild)
    await interaction.response.send_message(embed=em, view=LixingView(interaction.user.id))

@bot.command(name="lixing")
async def lixing_text_cmd(ctx):
    em = await build_lixing_embed(ctx.guild)
    await ctx.send(embed=em, view=LixingView(ctx.author.id))

@bot.command(name="lixingsetchannel")
@commands.has_permissions(manage_guild=True)
async def lixing_set_channel_cmd(ctx, channel: discord.TextChannel):
    await set_lixing_channel(ctx.guild.id, channel.id)
    await ctx.send(f":white_check_mark: Lixing digest channel set to {channel.mention}.")

@bot.command(name="lixingremove")
async def lixing_remove_cmd(ctx, listing_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE lixing_listings SET active=0 WHERE id=? AND guild_id=? AND user_id=?", (listing_id, ctx.guild.id, ctx.author.id))
        await db.commit()
    await ctx.send(f":white_check_mark: Lixing listing `{listing_id}` removed (if it was yours and active).")

@bot.command(name="lixingmy")
async def lixing_my_cmd(ctx):
    em = await build_lixing_embed(ctx.guild, mine_for=ctx.author.id)
    await ctx.send(embed=em, view=LixingView(ctx.author.id))

# digest every 6 hours
@tasks.loop(hours=6.0)
async def lixing_digest_loop():
    for g in bot.guilds:
        ch = await get_lixing_channel(g)
        if not ch: continue
        try:
            em = await build_lixing_embed(g)
            await ch.send(content="📣 **Lixing Digest** (every 6h)", embed=em)
        except Exception as e:
            log.warning(f"Lixing digest failed in {g.id}: {e}")
if not lixing_digest_loop.is_running():
    lixing_digest_loop.start()

# -------------------- INTERACTION HANDLERS FOR MARKET & LIXING VIEWS --------------------
@bot.event
async def on_interaction(interaction: discord.Interaction):
    # Let command tree handle slash; here we only intercept our custom_ids for views
    if not interaction.type == discord.InteractionType.component:
        return
    cid = interaction.data.get("custom_id") if interaction.data else None
    if not cid: return

    # MARKET
    if cid.startswith("mk:"):
        # Retrieve current embed state to keep filters if needed
        view = MarketView(interaction.user.id)
        if cid == "mk:all":
            view.filter_type = None
            em = await build_market_embed(interaction.guild)
            return await interaction.response.edit_message(embed=em, view=view)
        if cid == "mk:buy":
            view.filter_type = "buy"
            em = await build_market_embed(interaction.guild, "buy")
            return await interaction.response.edit_message(embed=em, view=view)
        if cid == "mk:sell":
            view.filter_type = "sell"
            em = await build_market_embed(interaction.guild, "sell")
            return await interaction.response.edit_message(embed=em, view=view)
        if cid == "mk:mine":
            em = await build_market_embed(interaction.guild, None, interaction.user.id)
            return await interaction.response.edit_message(embed=em, view=view)
        if cid == "mk:add:buy":
            return await interaction.response.send_modal(MarketAddModal("buy"))
        if cid == "mk:add:sell":
            return await interaction.response.send_modal(MarketAddModal("sell"))
        if cid == "mk:refresh":
            em = await build_market_embed(interaction.guild)
            return await interaction.response.edit_message(embed=em, view=view)
        if cid == "mk:offer":
            # handled inside MarketView via button decorator as well; keep for safety
            return await interaction.response.send_modal(MakeOfferAskModal())

    # LIXING
    if cid.startswith("lx:"):
        view = LixingView(interaction.user.id)
        if cid == "lx:all":
            em = await build_lixing_embed(interaction.guild)
            return await interaction.response.edit_message(embed=em, view=view)
        if cid.startswith("lx:class:"):
            sel = cid.split(":", 2)[2]
            if sel not in LIX_CLASSES:
                return await interaction.response.send_message(":x: Invalid class.", ephemeral=True)
            em = await build_lixing_embed(interaction.guild, class_filter=sel)
            return await interaction.response.edit_message(embed=em, view=view)
        if cid == "lx:mine":
            em = await build_lixing_embed(interaction.guild, mine_for=interaction.user.id)
            return await interaction.response.edit_message(embed=em, view=view)
        if cid == "lx:add":
            return await interaction.response.send_modal(LixingAddModal())
        if cid == "lx:refresh":
            em = await build_lixing_embed(interaction.guild)
            return await interaction.response.edit_message(embed=em, view=view)
# ==================== SECTION 4 — Admin/config, seeding, help, sync, run ====================

# ---------- PERMISSION GUARDS ----------
def is_admin_or_manage_guild():
    async def predicate(ctx: commands.Context):
        if ctx.author.guild_permissions.manage_guild or ctx.author.guild_permissions.administrator:
            return True
        await ctx.reply(":no_entry: You need **Manage Server** for this.")
        return False
    return commands.check(predicate)

# ---------- TIDY HELP (excludes auth commands) ----------
def help_embed() -> discord.Embed:
    em = discord.Embed(title="Help — Celtic Heroes Bot", color=0x5865F2)
    em.description = (
        "Timers, subscriptions, market & lixing tools.\n"
        "_Admin commands require **Manage Server**._"
    )
    em.add_field(
        name="Timers",
        value=(
            "`/timers` • show timers with category toggles (ephemeral)\n"
            "`!timers` • show timers in channel\n"
            "`!intervals` • list respawn/window/preannounce settings"
        ),
        inline=False
    )
    em.add_field(
        name="Subscriptions",
        value=(
            "`!subscribe <category>` • subscribe to pings\n"
            "`!unsubscribe <category>` • unsubscribe\n"
            "`!subscriptions` • open subscription panel\n"
            "`!setsubchannel #channel` • set subscription **panel** channel (admin)\n"
            "`!setsubpingchannel #channel` • set **ping** channel (admin)"
        ),
        inline=False
    )
    em.add_field(
        name="Market",
        value=(
            "`/market` or `!market` • open market panel\n"
            "`!marketmy` • your listings\n"
            "`!marketremove <id>` • remove your listing\n"
            "`!marketsetchannel #channel` • set 6h digest channel (admin)"
        ),
        inline=False
    )
    em.add_field(
        name="Lixing",
        value=(
            "`/lixing` or `!lixing` • open lixing panel\n"
            "`!lixingmy` • your listings\n"
            "`!lixingremove <id>` • remove your listing\n"
            "`!lixingsetchannel #channel` • set 6h digest channel (admin)"
        ),
        inline=False
    )
    em.add_field(
        name="Admin — Boss/Timers",
        value=(
            "`!seed` • (re)create default bosses (keeps existing)\n"
            "`!bossadd <name> <category> <spawn_m> <window_m> [pre_m]` • add\n"
            "`!bossspawn <name> <spawn_m>` • set respawn\n"
            "`!bosswindow <name> <window_m>` • set window\n"
            "`!bosspre <name> <pre_m>` • set pre-announce minutes\n"
            "`!bosslist` • list bosses\n"
            "`!setcolor <category> <#hex>` • set embed color per category\n"
            "`!setshoweta on|off` • show ETA timestamps on timers"
        ),
        inline=False
    )
    em.add_field(
        name="Maintenance",
        value=(
            "`!sync` • rebuild subscription panels (stable IDs)\n"
            "`!resume` • rescan & resume timers from DB\n"
            "`!health` • show loop health\n"
            "`!status` • basic status"
        ),
        inline=False
    )
    return em

@bot.command(name="help")
async def help_cmd(ctx):
    await ctx.send(embed=help_embed())

@bot.tree.command(name="help", description="Show help for this bot.")
async def help_slash(interaction: discord.Interaction):
    await interaction.response.send_message(embed=help_embed(), ephemeral=True)

# ---------- ADMIN: SUBSCRIPTIONS CHANNELS ----------
@bot.command(name="setsubchannel")
@is_admin_or_manage_guild()
async def set_sub_channel_cmd(ctx, channel: discord.TextChannel):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO guild_config (guild_id,sub_panel_channel_id) VALUES (?,?) "
            "ON CONFLICT(guild_id) DO UPDATE SET sub_panel_channel_id=excluded.sub_panel_channel_id",
            (ctx.guild.id, channel.id)
        ); await db.commit()
    await ctx.send(f":white_check_mark: Subscription **panel** channel set to {channel.mention}.")
    await refresh_subscription_messages(ctx.guild)

@bot.command(name="setsubpingchannel")
@is_admin_or_manage_guild()
async def set_sub_ping_channel_cmd(ctx, channel: discord.TextChannel):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO guild_config (guild_id,sub_ping_channel_id) VALUES (?,?) "
            "ON CONFLICT(guild_id) DO UPDATE SET sub_ping_channel_id=excluded.sub_ping_channel_id",
            (ctx.guild.id, channel.id)
        ); await db.commit()
    await ctx.send(f":white_check_mark: Subscription **ping** channel set to {channel.mention}.")

# ---------- ADMIN: SEED BOSSES ----------
DEFAULT_BOSSES = [
    # name, category, spawn_m, window_m, pre_m
    ("Aggy", "EG", 180, 20, 10),
    ("DL 180", "DL", 88, 3, 5),  # << patch: 88m spawn, 3m window
    ("Snorri", "Frozen", 180, 15, 10),
    ("Falgren", "Meteoric", 120, 10, 5),
]

async def upsert_boss(gid: int, name: str, category: str, sp_m: int, win_m: int, pre_m: int):
    name = name.strip()
    cat = norm_cat(category)
    async with aiosqlite.connect(DB_PATH) as db:
        # preserve existing next_spawn_ts if already tracked
        c = await db.execute("SELECT id,next_spawn_ts FROM bosses WHERE guild_id=? AND lower(name)=lower(?)", (gid, name))
        r = await c.fetchone()
        if r:
            bid, cur_ts = int(r[0]), int(r[1] or 0)
            await db.execute(
                "UPDATE bosses SET category=?, spawn_minutes=?, window_minutes=?, pre_announce_min=? WHERE id=?",
                (cat, sp_m, win_m, pre_m, bid)
            )
        else:
            await db.execute(
                "INSERT INTO bosses (guild_id,name,category,spawn_minutes,window_minutes,pre_announce_min,next_spawn_ts) "
                "VALUES (?,?,?,?,?,?,0)",
                (gid, name, cat, sp_m, win_m, pre_m)
            )
        await db.commit()

@bot.command(name="seed")
@is_admin_or_manage_guild()
async def seed_cmd(ctx):
    for nm, cat, sp, win, pre in DEFAULT_BOSSES:
        await upsert_boss(ctx.guild.id, nm, cat, sp, win, pre)
    await ensure_seed_for_guild(ctx.guild)
    await ctx.send(":white_check_mark: Seed complete. Panels refreshed.")
    await refresh_subscription_messages(ctx.guild)

# ---------- ADMIN: BOSS EDITS ----------
def _find_boss_case_insensitive(rows, name) -> Optional[int]:
    for r in rows:
        if r[1].lower() == name.lower():
            return r[0]
    return None

@bot.command(name="bossadd")
@is_admin_or_manage_guild()
async def boss_add_cmd(ctx, name: str, category: str, spawn_minutes: int, window_minutes: int, pre_m: Optional[int] = 0):
    await upsert_boss(ctx.guild.id, name, category, int(spawn_minutes), int(window_minutes), int(pre_m or 0))
    await ctx.send(f":white_check_mark: Boss **{name}** added/updated.")

@bot.command(name="bossspawn")
@is_admin_or_manage_guild()
async def boss_spawn_cmd(ctx, name: str, spawn_minutes: int):
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT id,name FROM bosses WHERE guild_id=?", (ctx.guild.id,))
        rows = await c.fetchall()
        bid = _find_boss_case_insensitive(rows, name)
        if not bid:
            return await ctx.send(":x: Boss not found.")
        await db.execute("UPDATE bosses SET spawn_minutes=? WHERE id=?", (int(spawn_minutes), bid))
        await db.commit()
    await ctx.send(":white_check_mark: Respawn updated.")

@bot.command(name="bosswindow")
@is_admin_or_manage_guild()
async def boss_window_cmd(ctx, name: str, window_minutes: int):
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT id,name FROM bosses WHERE guild_id=?", (ctx.guild.id,))
        rows = await c.fetchall()
        bid = _find_boss_case_insensitive(rows, name)
        if not bid:
            return await ctx.send(":x: Boss not found.")
        await db.execute("UPDATE bosses SET window_minutes=? WHERE id=?", (int(window_minutes), bid))
        await db.commit()
    await ctx.send(":white_check_mark: Window updated.")

@bot.command(name="bosspre")
@is_admin_or_manage_guild()
async def boss_pre_cmd(ctx, name: str, pre_minutes: int):
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT id,name FROM bosses WHERE guild_id=?", (ctx.guild.id,))
        rows = await c.fetchall()
        bid = _find_boss_case_insensitive(rows, name)
        if not bid:
            return await ctx.send(":x: Boss not found.")
        await db.execute("UPDATE bosses SET pre_announce_min=? WHERE id=?", (int(pre_minutes), bid))
        await db.commit()
    await ctx.send(":white_check_mark: Pre-announce updated.")

@bot.command(name="bosslist")
@is_admin_or_manage_guild()
async def boss_list_cmd(ctx):
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT name,category,spawn_minutes,window_minutes,COALESCE(pre_announce_min,0) FROM bosses WHERE guild_id=?", (ctx.guild.id,))
        rows = await c.fetchall()
    if not rows:
        return await ctx.send("No bosses configured.")
    rows.sort(key=lambda r: (CATEGORY_ORDER.index(norm_cat(r[1])) if norm_cat(r[1]) in CATEGORY_ORDER else 99, natural_key(r[0])))
    lines = [f"**{n}** [{cat}] — respawn `{sp}m` • window `{win}m` • pre `{pre}m`" for n, cat, sp, win, pre in rows]
    chunk = ""
    for ln in lines:
        if len(chunk) + len(ln) + 1 > 1900:
            await ctx.send(chunk); chunk = ln + "\n"
        else:
            chunk += ln + "\n"
    if chunk: await ctx.send(chunk)

# ---------- ADMIN: APPEARANCE & OPTIONS ----------
@bot.command(name="setcolor")
@is_admin_or_manage_guild()
async def set_color_cmd(ctx, category: str, hex_color: str):
    cat = norm_cat(category)
    hx = hex_color.strip()
    if not hx.startswith("#") or not (len(hx) in (4,7)):
        return await ctx.send(":x: Use hex like `#3aa657`.")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO category_colors (guild_id,category,color) VALUES (?,?,?) "
            "ON CONFLICT(guild_id,category) DO UPDATE SET color=excluded.color",
            (ctx.guild.id, cat, hx)
        ); await db.commit()
    await ctx.send(f":white_check_mark: Color for **{cat}** set to `{hx}`.")

@bot.command(name="setshoweta")
@is_admin_or_manage_guild()
async def set_show_eta_cmd(ctx, flag: str):
    val = 1 if flag.lower() in {"on","true","yes","1"} else 0
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO guild_config (guild_id,show_eta) VALUES (?,?) "
            "ON CONFLICT(guild_id) DO UPDATE SET show_eta=excluded.show_eta",
            (ctx.guild.id, val)
        ); await db.commit()
    await ctx.send(f":white_check_mark: Show ETA is now **{'on' if val else 'off'}**.")

# ---------- MAINTENANCE ----------
@bot.command(name="sync")
@is_admin_or_manage_guild()
async def sync_cmd(ctx):
    # Rebuild or create subscription panel messages using stable message ids
    await ensure_seed_for_guild(ctx.guild)
    await refresh_subscription_messages(ctx.guild)
    # Sync slash commands to fix "unknown integration command"
    try:
        await bot.tree.sync(guild=discord.Object(id=ctx.guild.id))
    except Exception as e:
        log.warning(f"Slash sync failed in {ctx.guild.id}: {e}")
    await ctx.send(":white_check_mark: Panels rebuilt and slash commands synced.")

@bot.command(name="resume")
@is_admin_or_manage_guild()
async def resume_cmd(ctx):
    await ctx.send("Resuming timers from DB…")
    # No-op: timers run from stored next_spawn_ts + heartbeat/window loop already live
    # We simply poke the loops by refreshing panels.
    await refresh_subscription_messages(ctx.guild)
    await ctx.send(":white_check_mark: Done. If any boss is wrong, set its `next_spawn_ts` via admin or wait for natural rollovers.")

@bot.command(name="status")
async def status_cmd(ctx):
    await ctx.send("✅ Bot online. Timers and digests running.")

@bot.command(name="health")
async def health_cmd(ctx):
    states = []
    states.append(f"window_loop: {'running' if window_loop.is_running() else 'stopped'}")
    states.append(f"market_digest: {'running' if market_digest_loop.is_running() else 'stopped'}")
    states.append(f"lixing_digest: {'running' if lixing_digest_loop.is_running() else 'stopped'}")
    await ctx.send("• " + "\n• ".join(states))

# ---------- READY & SYNC ----------
@bot.event
async def on_ready():
    log.info(f"Logged in as {bot.user} ({bot.user.id})")
    # Ensure DB defaults & panels for each guild, but respect auth lock
    for g in bot.guilds:
        if not await ensure_guild_auth(g):
            log.warning(f"Guild {g.id} locked — required user missing.")
            continue
        try:
            await ensure_seed_for_guild(g)
            await refresh_subscription_messages(g)
            # per-guild slash sync to avoid “unknown integration command”
            try:
                await bot.tree.sync(guild=discord.Object(id=g.id))
            except Exception as e:
                log.warning(f"Slash sync failed in {g.id}: {e}")
        except Exception as e:
            log.exception(f"Ignoring exception in on_ready for guild {g.id}: {e}")

# ---------- APP COMMAND FAILSAFE ----------
@bot.event
async def on_guild_join(guild: discord.Guild):
    try:
        await bot.tree.sync(guild=discord.Object(id=guild.id))
    except Exception as e:
        log.warning(f"Slash sync failed on join {guild.id}: {e}")

# ---------- RUN ----------
if __name__ == "__main__":
    import os
    TOKEN = os.getenv("DISCORD_TOKEN") or os.getenv("BOT_TOKEN")
    if not TOKEN:
        print("Set DISCORD_TOKEN env var.")
    else:
        bot.run(TOKEN)

