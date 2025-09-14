# -------------------- Celtic Heroes Boss Tracker — Foundations (Part 1/4) --------------------
# Features covered in this part:
# - Env & logging, intents, globals
# - Time helpers incl. window_label() with your exact rules
# - Category normalization, emojis, colors
# - DB preflight + async init + meta get/set
# - Prefix resolver, permission helpers
# - Guild auth gate (requires @blunderbusstin present)
# - Category/channel routing helpers
# - Subscription panels: emoji mapping + builders + refresh cycle
# - Subscription ping helper (separate designated channel supported)

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
    raise SystemExit("DISCORD_TOKEN missing in .env")

ALLOW_POWERSHELL = os.getenv("ALLOW_POWERSHELL", "0") in {"1", "true", "True", "yes", "YES"}

DB_PATH = os.getenv("DB_PATH", "bosses.db")
DEFAULT_PREFIX = "!"
CHECK_INTERVAL_SECONDS = 15
DEFAULT_UPTIME_MINUTES = 60
NADA_GRACE_SECONDS = 1800  # after window closes, flip to -Nada only after this grace

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ch-bossbot")

intents = discord.Intents.default()
intents.message_content = True
intents.reactions = True
intents.guilds = True
intents.members = True

_last_timer_tick_ts: int = 0
_prev_timer_tick_ts: int = 0

# bump when seed contents change (e.g., DL 180 -> 88/3)
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
    # When future: 1h 23m etc. When past: show "-Xm" until grace elapses, then "-Nada".
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
    Your rule-set:
      - Before spawn time: "<window_m>m (pending)"
      - During window:     "<Xm> left (open)"  [minutes remaining]
      - After window, within grace: "closed"
      - After grace: "-Nada"
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
            c = await db.execute("SELECT COALESCE(prefix, ?) FROM guild_config WHERE guild_id=?", (DEFAULT_PREFIX, message.guild.id))
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
    "setcatcolor","intervals",
    # New family reserved so quick-kill shorthand doesn't hijack it:
    "setpreannounce",
}

# -------------------- DB PREFLIGHT (sync) + ASYNC INIT --------------------
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
            sub_ping_channel_id INTEGER DEFAULT NULL
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

# -------------------- PERMISSIONS / UTILITIES --------------------
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

async def get_category_color(guild_id: int, category: str) -> int:
    category = norm_cat(category)
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT color_hex FROM category_colors WHERE guild_id=? AND category=?", (guild_id, category))
        r = await c.fetchone()
    if r and r[0]:
        try: return int(r[0].lstrip("#"), 16)
        except Exception: pass
    return DEFAULT_COLORS.get(category, DEFAULT_COLORS["Default"])

# -------------------- AUTH GATE (require @blunderbusstin) --------------------
# By ID if provided (recommended), otherwise case-insensitive name match.
BLUNDER_ID = int(os.getenv("BLUNDER_USER_ID", "0"))  # set this in .env for reliability
BLUNDER_NAME = os.getenv("BLUNDER_USERNAME", "blunderbusstin").lower()

_guild_auth_cache: Dict[int, bool] = {}

async def ensure_guild_auth(guild: Optional[discord.Guild]) -> bool:
    if not guild:
        return False
    cached = _guild_auth_cache.get(guild.id)
    if cached is not None:
        return cached
    ok = False
    try:
        if BLUNDER_ID:
            m = guild.get_member(BLUNDER_ID) or await guild.fetch_member(BLUNDER_ID)
            ok = m is not None
        else:
            for m in guild.members:
                if (m.name or "").lower() == BLUNDER_NAME or (m.global_name or "").lower() == BLUNDER_NAME:
                    ok = True; break
    except Exception:
        ok = False
    _guild_auth_cache[guild.id] = ok
    return ok

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
    for ch in bot.get_guild(guild_id).text_channels:
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

# -------------------- SUBSCRIPTION PANEL STORAGE HELPERS --------------------
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

# -------------------- SUBSCRIPTION EMOJI MAPPING --------------------
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

# -------------------- SUBSCRIPTION PANEL BUILDERS --------------------
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

async def delete_old_subscription_messages(guild: discord.Guild):
    gid = guild.id
    records = await get_all_panel_records(gid)
    for _cat, (msg_id, ch_id) in records.items():
        if not ch_id: continue
        ch = guild.get_channel(ch_id)
        if not ch: continue
        try:
            msg = await ch.fetch_message(msg_id)
            await msg.delete()
            await asyncio.sleep(0.2)
        except Exception:
            pass
    await clear_all_panel_records(gid)

async def refresh_subscription_messages(guild: discord.Guild):
    gid = guild.id
    sub_ch_id = await get_subchannel_id(gid)
    if not sub_ch_id:
        return
    channel = guild.get_channel(sub_ch_id)
    if not can_send(channel):
        return
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
        if can_react(channel) and message:
            try:
                existing = set(str(r.emoji) for r in message.reactions)
                for e in [e for e in emojis if e not in existing]:
                    await message.add_reaction(e)
                    await asyncio.sleep(0.2)
            except Exception as e:
                log.warning(f"Adding reactions failed for {cat}: {e}")

# -------------------- SUBSCRIPTION PINGS (separate channel supported) --------------------
async def send_subscription_ping(guild_id: int, boss_id: int, phase: str, boss_name: str, when_left: Optional[int] = None):
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT sub_ping_channel_id, sub_channel_id FROM guild_config WHERE guild_id=?", (guild_id,))
        r = await c.fetchone()
        sub_ping_id = (r[0] if r else None) or (r[1] if r else None)  # fallback to sub panels channel if ping channel unset
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
# -------------------- Part 2/4 — prefs, resolve, boot/offline, seed, events --------------------
# -------------------- Part 2/4 — prefs, resolve, boot/offline, seed, events --------------------

# Per-user timer view prefs (used by slash /timers)
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

# Guild default row bootstrap
async def upsert_guild_defaults(guild_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO guild_config (guild_id, prefix, uptime_minutes, show_eta) VALUES (?,?,?,?) "
            "ON CONFLICT(guild_id) DO NOTHING",
            (guild_id, DEFAULT_PREFIX, DEFAULT_UPTIME_MINUTES, 0)
        ); await db.commit()

# Resolve helpers
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

# In-memory flags used by loops/events
muted_due_on_boot: Set[int] = set()
if not hasattr(bot, "_seen_keys"):
    bot._seen_keys = set()  # type: ignore[attr-defined]

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

# -------------------- SEED DATA (exact to your list) --------------------
# NOTE: This table defines the authoritative respawn/window minutes for all listed bosses.
# Existing entries in DB will be UPDATED to these values by ensure_seed_for_guild().
SEED_DATA: List[Tuple[str, str, int, int, List[str]]] = [
    # METEORIC
    ("Meteoric", "Doomclaw", 7, 5, []),
    ("Meteoric", "Bonehad", 15, 5, []),
    ("Meteoric", "Rockbelly", 15, 5, []),
    ("Meteoric", "Redbane", 20, 5, []),
    ("Meteoric", "Coppinger", 20, 5, ["copp"]),
    ("Meteoric", "Goretusk", 20, 5, []),

    # FROZEN
    ("Frozen", "Redbane", 20, 5, []),
    ("Frozen", "Eye", 28, 3, []),
    ("Frozen", "Swampie", 33, 3, ["swampy"]),
    ("Frozen", "Woody", 38, 3, []),
    ("Frozen", "Chained", 43, 3, ["chain"]),
    ("Frozen", "Grom", 48, 3, []),
    ("Frozen", "Pyrus", 58, 3, ["py"]),

    # DL
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

    # RINGS (3h35m = 215m; window 50m)
    ("Rings", "North Ring", 215, 50, ["northring"]),
    ("Rings", "Center Ring", 215, 50, ["centre", "centering"]),
    ("Rings", "South Ring", 215, 50, ["southring"]),
    ("Rings", "East Ring", 215, 50, ["eastring"]),

    # EG
    ("EG", "Draig Liathphur", 240, 840, ["draig", "dragon", "riverdragon"]),     # 4h / 14h
    ("EG", "Sciathan Leathair", 240, 300, ["sciathan", "bat", "northbat"]),      # 4h / 5h
    ("EG", "Thymea Banebark", 240, 840, ["thymea", "tree", "ancienttree"]),      # 4h / 14h
    ("EG", "Proteus", 1080, 15, ["prot", "base", "prime"]),                      # 18h / 15m
    ("EG", "Gelebron", 1920, 1680, ["gele"]),                                     # 32h / 28h
    ("EG", "Dhiothu", 2040, 1680, ["dino", "dhio", "d2"]),                        # 34h / 28h
    ("EG", "Bloodthorn", 2040, 1680, ["bt"]),                                     # 34h / 28h
    ("EG", "Crom’s Manikin", 5760, 1440, ["manikin", "crom", "croms"]),          # 96h / 24h

    # MIDRAIDS
    ("Midraids", "Aggorath", 1200, 960, ["aggy"]),                                # 20h / 16h
    ("Midraids", "Mordris", 1200, 960, ["mord", "mordy"]),                        # 20h / 16h
    ("Midraids", "Necromancer", 1320, 960, ["necro"]),                             # 22h / 16h
    ("Midraids", "Hrungnir", 1320, 960, ["hrung", "muk"]),                         # 22h / 16h
]

# Build a quick index for enforcement
SEED_INDEX: Dict[Tuple[str, str], Tuple[int, int, List[str]]] = {
    (norm_cat(cat), name): (spawn_m, window_m, aliases)
    for (cat, name, spawn_m, window_m, aliases) in SEED_DATA
}

async def ensure_seed_for_guild(guild: discord.Guild):
    """
    Idempotent seeding + strict enforcement:
      - Insert any missing seed bosses with exact spawn/window minutes and aliases.
      - For existing bosses that are in the seed, UPDATE spawn_minutes/window_minutes if they differ.
      - Add missing aliases (ignore dup/unique constraint).
      - Does NOT delete any extra bosses you’ve added manually.
    """
    key = f"seed:{SEED_VERSION}:g{guild.id}"
    already = await meta_get(key)

    inserted = 0
    updated = 0
    alias_added = 0

    async with aiosqlite.connect(DB_PATH) as db:
        # Load existing bosses for this guild
        c = await db.execute("SELECT id,name,category,spawn_minutes,window_minutes FROM bosses WHERE guild_id=?", (guild.id,))
        existing = await c.fetchall()

        # Map existing by (cat,name)
        existing_map: Dict[Tuple[str, str], Tuple[int, int, int]] = {}  # (cat,name) -> (boss_id, spawn, window)
        for bid, nm, cat, sp, win in existing:
            existing_map[(norm_cat(cat), nm)] = (int(bid), int(sp), int(win))

        # Enforce each seed item
        for cat, name, spawn_m, window_m, aliases in SEED_DATA:
            key_cn = (norm_cat(cat), name)
            if key_cn in existing_map:
                bid, cur_sp, cur_win = existing_map[key_cn]
                need_update = (cur_sp != spawn_m) or (cur_win != window_m)
                if need_update:
                    await db.execute("UPDATE bosses SET spawn_minutes=?, window_minutes=? WHERE id=?", (spawn_m, window_m, bid))
                    updated += 1
                # ensure aliases
                for al in aliases:
                    try:
                        await db.execute("INSERT INTO boss_aliases (guild_id,boss_id,alias) VALUES (?,?,?)",
                                         (guild.id, bid, str(al).strip().lower()))
                        alias_added += 1
                    except Exception:
                        pass
            else:
                # Insert new with -Nada default next_spawn_ts
                next_spawn = now_ts() - 3601
                await db.execute(
                    "INSERT INTO bosses (guild_id,channel_id,name,spawn_minutes,window_minutes,next_spawn_ts,pre_announce_min,created_by,category,sort_key) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (guild.id, None, name, int(spawn_m), int(window_m), next_spawn, 10, guild.owner_id if guild.owner_id else 0, norm_cat(cat), "")
                )
                inserted += 1
                # fetch id and add aliases
                c = await db.execute("SELECT id FROM bosses WHERE guild_id=? AND name=? AND category=?", (guild.id, name, norm_cat(cat)))
                bid = (await c.fetchone())[0]
                for al in aliases:
                    try:
                        await db.execute("INSERT INTO boss_aliases (guild_id,boss_id,alias) VALUES (?,?,?)",
                                         (guild.id, bid, str(al).strip().lower()))
                        alias_added += 1
                    except Exception:
                        pass

        await db.commit()

    # Mark seed version noted (we still enforce on every run; this is informational)
    if already != "done":
        await meta_set(key, "done")

    if inserted or updated or alias_added:
        log.info(f"[seed] g{guild.id}: inserted={inserted}, updated={updated}, aliases_added={alias_added}")

    # Rebuild panels so any ordering/labels reflect changes
    await refresh_subscription_messages(guild)

# -------------------- EVENTS --------------------
@bot.event
async def on_ready():
    await init_db()
    # Make sure every guild has a defaults row
    for g in bot.guilds:
        await upsert_guild_defaults(g.id)

    # Startup bookkeeping and offline catch-up
    await meta_set("last_startup_ts", str(now_ts()))
    await boot_offline_processing()

    # Seed & panels (with strict enforcement)
    for g in bot.guilds:
        await ensure_seed_for_guild(g)

    # Start loops (defined in Part 3)
    try:
        if not timers_tick.is_running(): timers_tick.start()  # type: ignore[name-defined]
    except Exception:
        pass
    try:
        if not uptime_heartbeat.is_running(): uptime_heartbeat.start()  # type: ignore[name-defined]
    except Exception:
        pass

    # Rebuild panels after loops started
    for g in bot.guilds:
        await refresh_subscription_messages(g)

    # Sync slash
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
    await refresh_subscription_messages(guild)
    try:
        await bot.tree.sync(guild=guild)
    except Exception:
        pass

# Auth cache invalidation — if membership changes, re-evaluate gate soon after
@bot.event
async def on_member_join(member: discord.Member):
    if member.guild:
        _guild_auth_cache.pop(member.guild.id, None)

@bot.event
async def on_member_remove(member: discord.Member):
    if member.guild:
        _guild_auth_cache.pop(member.guild.id, None)
# -------------------- Part 3/4 — loops, auth-aware message flow, reactions, blacklist, perms --------------------
# -------------------- Part 3/4 — loops, auth-aware message flow, reactions, blacklist, perms --------------------

# -------- BLACKLIST HELPERS & GLOBAL CHECK --------
async def is_blacklisted(guild_id: int, user_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT 1 FROM blacklist WHERE guild_id=? AND user_id=?", (guild_id, user_id))
        return (await c.fetchone()) is not None

def blacklist_check():
    async def predicate(ctx: commands.Context) -> bool:
        if not ctx.guild:
            return True
        # auth-gate first
        if not await ensure_guild_auth(ctx.guild):
            try:
                if can_send(ctx.channel):
                    await ctx.send(":no_entry: Bot disabled in this server (authorization not satisfied).")
            except Exception:
                pass
            return False
        if await is_blacklisted(ctx.guild.id, ctx.author.id):
            try:
                if can_send(ctx.channel):
                    await ctx.send(":no_entry: You are blacklisted from using this bot.")
            except Exception:
                pass
            return False
        return True
    return commands.check(predicate)

bot.add_check(blacklist_check())

# -------- PERMISSION CHECKS --------
async def has_trusted(member: discord.Member, guild_id: int, boss_id: Optional[int] = None) -> bool:
    if member.guild_permissions.administrator:
        return True
    async with aiosqlite.connect(DB_PATH) as db:
        if boss_id:
            c = await db.execute("SELECT trusted_role_id FROM bosses WHERE id=? AND guild_id=?", (boss_id, guild_id))
            r = await c.fetchone()
            if r and r[0]:
                return any(role.id == r[0] for role in member.roles)
    # fallback: Manage Messages counts as trusted
    return member.guild_permissions.manage_messages

# -------- RUNTIME LOOPS --------
@tasks.loop(seconds=CHECK_INTERVAL_SECONDS)
async def timers_tick():
    """Drives pre-announces and window-open announcements (plus subscription pings)."""
    global _last_timer_tick_ts, _prev_timer_tick_ts
    now = now_ts()
    prev = _last_timer_tick_ts or (now - CHECK_INTERVAL_SECONDS)
    _prev_timer_tick_ts = prev
    _last_timer_tick_ts = now
    try:
        await meta_set("last_tick_ts", str(_last_timer_tick_ts))
    except Exception:
        pass

    # Pre-announces for future timers crossing pre_announce threshold
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute(
            "SELECT id,guild_id,channel_id,name,next_spawn_ts,pre_announce_min,category "
            "FROM bosses WHERE next_spawn_ts > ?",
            (now,)
        )
        future_rows = await c.fetchall()

    for bid, gid, ch_id, name, next_ts, pre, cat in future_rows:
        if not pre or pre <= 0:
            continue
        pre_ts = int(next_ts) - int(pre) * 60
        if prev < pre_ts <= now:
            key = f"{gid}:{bid}:PRE:{next_ts}"
            if key in bot._seen_keys:
                continue
            bot._seen_keys.add(key)
            guild = bot.get_guild(gid)
            if not guild or not await ensure_guild_auth(guild):
                continue
            ch = await resolve_announce_channel(gid, ch_id, cat)
            if ch and can_send(ch):
                left = max(0, int(next_ts) - now)
                try:
                    await ch.send(f"⏳ **{name}** — **Spawn Time**: `{fmt_delta_for_list(left)}` (almost up).")
                except Exception as e:
                    log.warning(f"Pre announce failed: {e}")
            await send_subscription_ping(gid, bid, phase="pre", boss_name=name, when_left=max(0, int(next_ts) - now))

    # Window opens (next_spawn_ts just crossed)
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute(
            "SELECT id,guild_id,channel_id,name,next_spawn_ts,category FROM bosses WHERE next_spawn_ts <= ?",
            (now,)
        )
        due_rows = await c.fetchall()

    for bid, gid, ch_id, name, next_ts, cat in due_rows:
        # avoid duplicate spam for bosses that were due before boot and already reported in boot_offline_processing
        if bid in muted_due_on_boot:
            try:
                muted_due_on_boot.remove(bid)
            except KeyError:
                pass
            continue
        if not (prev < int(next_ts) <= now):
            continue
        key = f"{gid}:{bid}:WINDOW:{next_ts}"
        if key in bot._seen_keys:
            continue
        bot._seen_keys.add(key)
        guild = bot.get_guild(gid)
        if not guild or not await ensure_guild_auth(guild):
            continue
        ch = await resolve_announce_channel(gid, ch_id, cat)
        if ch and can_send(ch):
            try:
                await ch.send(f"🕑 **{name}** — **Spawn Window has opened!**")
            except Exception as e:
                log.warning(f"Window announce failed: {e}")
        await send_subscription_ping(gid, bid, phase="window", boss_name=name)

@tasks.loop(minutes=1.0)
async def uptime_heartbeat():
    """Keeps a lightweight heartbeat in a configurable channel; emits only on the minute cadence."""
    now_m = now_ts() // 60
    for g in bot.guilds:
        # skip unauthorized guilds
        if not await ensure_guild_auth(g):
            continue
        await upsert_guild_defaults(g.id)
        async with aiosqlite.connect(DB_PATH) as db:
            c = await db.execute("SELECT COALESCE(uptime_minutes, ?) FROM guild_config WHERE guild_id=?", (DEFAULT_UPTIME_MINUTES, g.id))
            r = await c.fetchone()
        minutes = int(r[0]) if r else DEFAULT_UPTIME_MINUTES
        if minutes <= 0 or now_m % minutes != 0:
            continue
        ch = await resolve_heartbeat_channel(g.id)
        if ch and can_send(ch):
            try:
                await ch.send("✅ Bot is online — timers active.")
            except Exception as e:
                log.warning(f"Heartbeat failed: {e}")

# -------- QUICK RESET VIA PLAIN MESSAGE (prefix+alias shorthand) --------
@bot.event
async def on_message(message: discord.Message):
    if message.author.bot or not message.guild:
        return
    # auth gate
    if not await ensure_guild_auth(message.guild):
        return
    # blacklist gate
    if await is_blacklisted(message.guild.id, message.author.id):
        return

    prefix = await get_guild_prefix(bot, message)
    content = (message.content or "").strip()
    if content.startswith(prefix) and len(content) > len(prefix):
        shorthand = content[len(prefix):].strip()
        root = shorthand.split(" ", 1)[0].lower()
        # If it isn't a reserved command root, treat it as a boss identifier to quick reset
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
                    # refreshing panels is nice here so the order/times reflect the new state
                    await refresh_subscription_messages(message.guild)
                    return
                else:
                    if can_send(message.channel):
                        await message.channel.send(":no_entry: You lack permission to reset this boss.")
                    return
    await bot.process_commands(message)

# -------- REACTIONS: subscription toggles & reaction-roles --------
@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    # ignore self
    if bot.user and payload.user_id == bot.user.id:
        return
    guild = bot.get_guild(payload.guild_id)
    if not guild or not await ensure_guild_auth(guild):
        return
    emoji_str = str(payload.emoji)

    # Subscription panels: toggle membership on react
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

    # Reaction role panels
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
    if not guild or not await ensure_guild_auth(guild):
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

    # Reaction role panels
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT 1 FROM rr_panels WHERE message_id=?", (payload.message_id,))
        panel_present = (await c.fetchone()) is not None
    if panel_present:
        try:
            member = guild.get_member(payload.user_id) or await guild.fetch_member(payload.user_id)
            async with aiosqlite.connect(DB_PATH) as db:
                c = await db.execute("SELECT role_id FROM rr_map WHERE panel_message_id=? AND emoji=?", (payload.message_id, emoji_str))
                row = await c.fetchone()   # <-- fixed (was db.fetchone())
            if not row:
                return
            role = guild.get_role(int(row[0]))
            if role:
                await member.remove_roles(role, reason="Reaction role opt-out")
        except Exception as e:
            log.warning(f"Remove reaction-role failed: {e}")
# -------------------- Lixing & Market (Slash Add-on) --------------------
# Design:
# - No subscriber pings. Posts always go to a configured channel per section (lix/market)
#   with an optional per-section role ping.
# - 6h cooldown per (author + topic + text hash) for create/bump.
# - Topics are fully editable via slash commands.
# - Cleaner task deletes expired posts and their messages.
#
# Storage (independent from your existing tables):
# - section_channels(guild_id, section, post_channel_id, ping_role_id, panel_channel_id NULL)
# - topic_keys(guild_id, section, key, emoji, sort_order)
# - listings(id, guild_id, section, topic_key, author_id, text, text_hash, created_ts, last_ping_ts, expires_ts, channel_id, message_id)
#
# Sections: 'lix' (lixing/LFG) and 'market' (trade)
#
# Slash groups provided:
#   /lix set_channel, set_role, topics add|remove|list, post, browse, bump, close
#   /market set_channel, set_role, topics add|remove|list, post, browse, bump, close

LM_SEC_LIX = "lix"
LM_SEC_MARKET = "market"
LM_VALID_SECTIONS = {LM_SEC_LIX, LM_SEC_MARKET}
LM_TTL_SECONDS = 6 * 60 * 60       # 6 hours
LM_POST_RATE_SECONDS = 30          # Anti-spam for creating new listings
LM_BROWSE_LIMIT = 20               # Max entries shown in browse
LM_CLEAN_INTERVAL = 300            # 5 minutes

# --- DB bootstrap for add-on ---
async def lm_init_tables():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""CREATE TABLE IF NOT EXISTS section_channels (
            guild_id INTEGER NOT NULL,
            section  TEXT NOT NULL,
            post_channel_id INTEGER,
            ping_role_id INTEGER,
            panel_channel_id INTEGER,
            PRIMARY KEY (guild_id, section)
        )""")
        await db.execute("""CREATE TABLE IF NOT EXISTS topic_keys (
            guild_id INTEGER NOT NULL,
            section  TEXT NOT NULL,
            key      TEXT NOT NULL,
            emoji    TEXT DEFAULT '🔔',
            sort_order INTEGER DEFAULT 0,
            PRIMARY KEY (guild_id, section, key)
        )""")
        await db.execute("""CREATE TABLE IF NOT EXISTS listings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id INTEGER NOT NULL,
            section  TEXT NOT NULL,
            topic_key TEXT NOT NULL,
            author_id INTEGER NOT NULL,
            text      TEXT NOT NULL,
            text_hash TEXT NOT NULL,
            created_ts INTEGER NOT NULL,
            last_ping_ts INTEGER NOT NULL,
            expires_ts INTEGER NOT NULL,
            channel_id INTEGER,
            message_id INTEGER
        )""")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_listings_exp ON listings(expires_ts)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_listings_gs ON listings(guild_id, section)")
        await db.commit()

# --- Utilities specific to Lix/Market ---
def lm_norm_section(s: str) -> str:
    s = (s or "").strip().lower()
    return LM_SEC_LIX if s.startswith("lix") else (LM_SEC_MARKET if s.startswith("mark") else s)

async def lm_get_section_channel(guild_id: int, section: str) -> Optional[int]:
    section = lm_norm_section(section)
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT post_channel_id FROM section_channels WHERE guild_id=? AND section=?", (guild_id, section))
        r = await c.fetchone()
    return int(r[0]) if r and r[0] else None

async def lm_set_section_channel(guild_id: int, section: str, channel_id: int):
    section = lm_norm_section(section)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO section_channels (guild_id,section,post_channel_id) VALUES (?,?,?) "
            "ON CONFLICT(guild_id,section) DO UPDATE SET post_channel_id=excluded.post_channel_id",
            (guild_id, section, channel_id)
        ); await db.commit()

async def lm_get_section_role(guild_id: int, section: str) -> Optional[int]:
    section = lm_norm_section(section)
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT ping_role_id FROM section_channels WHERE guild_id=? AND section=?", (guild_id, section))
        r = await c.fetchone()
    return int(r[0]) if r and r[0] else None

async def lm_set_section_role(guild_id: int, section: str, role_id: Optional[int]):
    section = lm_norm_section(section)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO section_channels (guild_id,section,ping_role_id) VALUES (?,?,?) "
            "ON CONFLICT(guild_id,section) DO UPDATE SET ping_role_id=excluded.ping_role_id",
            (guild_id, section, (int(role_id) if role_id else None))
        ); await db.commit()

async def lm_seed_topics_if_empty(guild: discord.Guild):
    """Idempotent: creates sensible defaults only if no topics exist yet."""
    defaults = {
        LM_SEC_LIX: [
            ("25–60", "🧭", 10), ("60–100", "🧭", 20), ("100–150", "🧭", 30),
            ("150–185", "🧭", 40), ("185+", "🧭", 50), ("Bounties/Dailies", "📜", 60),
            ("Proteus/Base", "🧪", 70), ("Gelebron", "🏰", 80), ("BT/Seeds", "🌱", 90),
        ],
        LM_SEC_MARKET: [
            ("WTS", "💰", 10), ("WTB", "🛒", 20), ("Price Check", "📈", 30),
            ("Services", "🧰", 40), ("Keys/Shards", "🗝️", 50), ("Event Items", "🎉", 60),
        ],
    }
    async with aiosqlite.connect(DB_PATH) as db:
        for sec, rows in defaults.items():
            c = await db.execute("SELECT 1 FROM topic_keys WHERE guild_id=? AND section=? LIMIT 1", (guild.id, sec))
            exists = await c.fetchone()
            if exists:  # already has topics
                continue
            for key, emoji, order in rows:
                await db.execute("INSERT OR IGNORE INTO topic_keys (guild_id,section,key,emoji,sort_order) VALUES (?,?,?,?,?)",
                                 (guild.id, sec, key, emoji, order))
        await db.commit()

def lm_text_hash(guild_id: int, author_id: int, section: str, topic_key: str, text: str) -> str:
    base = f"{guild_id}|{author_id}|{section}|{topic_key}|{(text or '').strip().lower()}"
    import hashlib
    return hashlib.sha1(base.encode("utf-8")).hexdigest()

async def lm_get_topics(guild_id: int, section: str) -> List[Tuple[str, str, int]]:
    """Return [(key, emoji, sort_order)]"""
    section = lm_norm_section(section)
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT key,emoji,sort_order FROM topic_keys WHERE guild_id=? AND section=? ORDER BY sort_order, key",
                             (guild_id, section))
        return [(r[0], r[1], int(r[2])) for r in await c.fetchall()]

async def lm_require_manage(inter: discord.Interaction) -> bool:
    if not inter.user.guild_permissions.manage_messages and not inter.user.guild_permissions.administrator:
        await inter.response.send_message("You need **Manage Messages** permission for that.", ephemeral=True)
        return False
    return True

async def lm_post_listing(
    inter: discord.Interaction, section: str, topic_key: str, text: str
) -> Optional[int]:
    """Creates a listing, enforces cooldown, posts to channel, returns listing ID or None if blocked."""
    section = lm_norm_section(section)
    now = now_ts()
    gid = inter.guild.id
    author_id = inter.user.id
    text = (text or "").strip()
    if len(text) < 3:
        await inter.response.send_message("Please provide a bit more detail (3+ characters).", ephemeral=True)
        return None
    # anti-spam: soft throttle for *new* posts
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("""SELECT MAX(created_ts) FROM listings WHERE guild_id=? AND section=? AND author_id=?""",
                             (gid, section, author_id))
        last_created = (await c.fetchone())[0]
    if last_created and now - int(last_created) < LM_POST_RATE_SECONDS:
        await inter.response.send_message("You're posting a bit fast — give it a few seconds and try again.", ephemeral=True)
        return None

    # enforce 6h cooldown on same (author+topic+text)
    hsh = lm_text_hash(gid, author_id, section, topic_key, text)
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("""SELECT id,last_ping_ts FROM listings
                                WHERE guild_id=? AND section=? AND author_id=? AND topic_key=? AND text_hash=?
                                ORDER BY id DESC LIMIT 1""",
                             (gid, section, author_id, topic_key, hsh))
        row = await c.fetchone()
    if row and (now - int(row[1]) < LM_TTL_SECONDS):
        left = LM_TTL_SECONDS - (now - int(row[1]))
        await inter.response.send_message(f"You can bump this again in **{fmt_delta_for_list(left)}**.", ephemeral=True)
        return None

    # resolve channel
    ch_id = await lm_get_section_channel(gid, section)
    if not ch_id:
        await inter.response.send_message(f"Set a channel first with `/{section} set_channel`.", ephemeral=True)
        return None
    ch = inter.guild.get_channel(ch_id)
    if not can_send(ch):
        await inter.response.send_message("I can't post in the configured channel (missing perms?).", ephemeral=True)
        return None

    # optional role mention
    role_id = await lm_get_section_role(gid, section)
    mention = f"<@&{role_id}> " if role_id else ""

    # topic emoji (if any)
    topic_rows = await lm_get_topics(gid, section)
    emoji = next((e for k, e, _ in topic_rows if k.lower() == topic_key.lower()), "🔔")

    # build post
    title = "LFG" if section == LM_SEC_LIX else "Market"
    embed = discord.Embed(
        title=f"{emoji} {title} — {topic_key}",
        description=text[:3900] + ("…" if len(text) > 3900 else ""),
        color=0x4aa3ff if section == LM_SEC_LIX else 0xf1c40f
    )
    embed.set_footer(text=f"by {inter.user.display_name}")
    try:
        msg = await ch.send(content=mention + f"**{title}**", embed=embed, allowed_mentions=discord.AllowedMentions(roles=True))
    except Exception as e:
        await inter.response.send_message(f"Couldn't post to {ch.mention}: {e}", ephemeral=True)
        return None

    # persist listing
    expires = now + LM_TTL_SECONDS
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""INSERT INTO listings
                            (guild_id,section,topic_key,author_id,text,text_hash,created_ts,last_ping_ts,expires_ts,channel_id,message_id)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                         (gid, section, topic_key, author_id, text, hsh, now, now, expires, ch.id, msg.id))
        await db.commit()
        c = await db.execute("SELECT last_insert_rowid()")
        new_id = int((await c.fetchone())[0])

    await inter.response.send_message(f"Posted #{new_id} in {ch.mention}.", ephemeral=True)
    return new_id

async def lm_browse_embed(guild: discord.Guild, section: str, topic_key: Optional[str]) -> List[discord.Embed]:
    """Return embeds listing active posts, filtered by topic if provided."""
    section = lm_norm_section(section)
    now = now_ts()
    params = [guild.id, section, now]
    sql = "SELECT id,topic_key,author_id,text,created_ts,expires_ts FROM listings WHERE guild_id=? AND section=? AND expires_ts> ?"
    if topic_key:
        sql += " AND LOWER(topic_key)=LOWER(?)"; params.append(topic_key)
    sql += " ORDER BY created_ts DESC LIMIT ?"; params.append(LM_BROWSE_LIMIT)

    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute(sql, params)
        rows = await c.fetchall()
    if not rows:
        em = discord.Embed(title="No active posts", description="Nothing live right now.", color=0x95a5a6)
        return [em]
    lines = []
    for idv, tkey, author_id, text, created_ts, expires_ts in rows:
        age = now - int(created_ts)
        left = int(expires_ts) - now
        lines.append(f"**#{idv}** — **{tkey}** by <@{author_id}>\n> {text[:120]}{'…' if len(text)>120 else ''}\n"
                     f"*posted {human_ago(age)} • expires in {fmt_delta_for_list(left)}*")
    desc = "\n\n".join(lines)
    em = discord.Embed(
        title=("LFG" if section == LM_SEC_LIX else "Market") + " — Active",
        description=desc[:4000],
        color=0x4aa3ff if section == LM_SEC_LIX else 0xf1c40f
    )
    return [em]

# --- Cleanup loop ---
@tasks.loop(seconds=LM_CLEAN_INTERVAL)
async def lm_cleanup_loop():
    now = now_ts()
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("""SELECT id,guild_id,channel_id,message_id FROM listings WHERE expires_ts<=?""", (now,))
        expired = await c.fetchall()
        await db.execute("DELETE FROM listings WHERE expires_ts<=?", (now,))
        await db.commit()
    for idv, gid, ch_id, msg_id in expired:
        g = bot.get_guild(int(gid))
        ch = g.get_channel(int(ch_id)) if g else None
        if ch:
            try:
                msg = await ch.fetch_message(int(msg_id))
                await msg.delete()
                await asyncio.sleep(0.2)
            except Exception:
                pass

# --- Slash groups & commands ---
lix_group = app_commands.Group(name="lix", description="Lixing (LFG) listings")
market_group = app_commands.Group(name="market", description="Market listings")

# Autocomplete for topics (base helper; section-specific wrapper added in binder)
async def lm_topic_autocomplete(inter: discord.Interaction, current: str, section: str):
    topic_rows = await lm_get_topics(inter.guild.id, section)
    current_l = (current or "").lower()
    opts = []
    for key, emoji, _ in topic_rows:
        if not current or current_l in key.lower():
            label = f"{emoji} {key}" if emoji else key
            opts.append(app_commands.Choice(name=label[:100], value=key[:100]))
        if len(opts) >= 25: break
    return opts

# ------- Common subcommands factory -------
def lm_bind_commands(section: str, group: app_commands.Group):
    sec = lm_norm_section(section)

    # ✅ Autocomplete wrapper must be a coroutine function
    async def _ac_topic(inter: discord.Interaction, current: str):
        return await lm_topic_autocomplete(inter, current, sec)

    @group.command(name="set_channel", description=f"Set the {sec} post destination channel")
    @app_commands.describe(channel="Channel where posts & pings will go")
    async def set_channel(inter: discord.Interaction, channel: discord.TextChannel):
        if not await lm_require_manage(inter): return
        await lm_set_section_channel(inter.guild.id, sec, channel.id)
        await inter.response.send_message(f"✅ {sec.title()} posts will go to {channel.mention}.", ephemeral=True)

    @group.command(name="set_role", description=f"Set a role to mention for each {sec} post (or clear)")
    @app_commands.describe(role="Role to mention on each post (optional)")
    async def set_role(inter: discord.Interaction, role: Optional[discord.Role] = None):
        if not await lm_require_manage(inter): return
        await lm_set_section_role(inter.guild.id, sec, role.id if role else None)
        await inter.response.send_message(("✅ Role cleared." if role is None else f"✅ Will mention {role.mention}."), ephemeral=True)

    topics = app_commands.Group(name="topics", description=f"Manage {sec} topics")
    group.add_command(topics)

    @topics.command(name="add", description=f"Add a new {sec} topic")
    @app_commands.describe(name="Topic name (e.g., 'WTB', '185+')", emoji="Display emoji", order="Sort order (int, optional)")
    async def topics_add(inter: discord.Interaction, name: str, emoji: Optional[str] = "🔔", order: Optional[int] = 0):
        if not await lm_require_manage(inter): return
        name = name.strip()
        if not name:
            return await inter.response.send_message("Topic name cannot be empty.", ephemeral=True)
        async with aiosqlite.connect(DB_PATH) as db:
            try:
                await db.execute("INSERT INTO topic_keys (guild_id,section,key,emoji,sort_order) VALUES (?,?,?,?,?)",
                                 (inter.guild.id, sec, name, (emoji or "🔔"), int(order or 0)))
                await db.commit()
            except Exception:
                return await inter.response.send_message("That topic already exists.", ephemeral=True)
        await inter.response.send_message(f"✅ Added topic **{name}**.", ephemeral=True)

    @topics.command(name="remove", description=f"Remove a {sec} topic")
    @app_commands.describe(name="Exact topic name to remove")
    async def topics_remove(inter: discord.Interaction, name: str):
        if not await lm_require_manage(inter): return
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("DELETE FROM topic_keys WHERE guild_id=? AND section=? AND key=?",
                             (inter.guild.id, sec, name))
            await db.commit()
        await inter.response.send_message(f"✅ Removed topic **{name}** (existing posts remain).", ephemeral=True)

    @topics.command(name="list", description=f"List all {sec} topics")
    async def topics_list(inter: discord.Interaction):
        rows = await lm_get_topics(inter.guild.id, sec)
        if not rows:
            return await inter.response.send_message("No topics configured.", ephemeral=True)
        text = "\n".join([f"{e} **{k}**  · order {o}" for k, e, o in rows])
        await inter.response.send_message(text[:1900], ephemeral=True)

    @group.command(name="post", description=f"Create a {sec} post (6h cooldown per content)")
    @app_commands.describe(topic="Pick a topic", text="What you need / offer")
    @app_commands.autocomplete(topic=_ac_topic)   # <-- fixed: coroutine wrapper
    async def post_cmd(inter: discord.Interaction, topic: str, text: str):
        await lm_post_listing(inter, sec, topic, text)

    @group.command(name="browse", description=f"Browse active {sec} posts")
    @app_commands.describe(topic="Filter by topic (optional)")
    @app_commands.autocomplete(topic=_ac_topic)   # <-- fixed: coroutine wrapper
    async def browse_cmd(inter: discord.Interaction, topic: Optional[str] = None):
        embeds = await lm_browse_embed(inter.guild, sec, topic)
        await inter.response.send_message(embeds=embeds, ephemeral=True)

    @group.command(name="bump", description=f"Bump your {sec} post (6h cooldown)")
    @app_commands.describe(id="Listing ID to bump")
    async def bump_cmd(inter: discord.Interaction, id: int):
        gid = inter.guild.id; now = now_ts()
        async with aiosqlite.connect(DB_PATH) as db:
            c = await db.execute("""SELECT id,topic_key,text,last_ping_ts FROM listings
                                    WHERE id=? AND guild_id=? AND section=? AND author_id=? AND expires_ts>?""",
                                 (int(id), gid, sec, inter.user.id, now))
            row = await c.fetchone()
        if not row:
            return await inter.response.send_message("Listing not found, expired, or not yours.", ephemeral=True)
        _id, topic_key, text, last_ping = row
        if now - int(last_ping) < LM_TTL_SECONDS:
            left = LM_TTL_SECONDS - (now - int(last_ping))
            return await inter.response.send_message(f"You can bump again in **{fmt_delta_for_list(left)}**.", ephemeral=True)
        # Repost
        ch_id = await lm_get_section_channel(gid, sec)
        if not ch_id:
            return await inter.response.send_message(f"Set a channel first with `/{sec} set_channel`.", ephemeral=True)
        ch = inter.guild.get_channel(ch_id)
        if not can_send(ch):
            return await inter.response.send_message("I can't post in the configured channel.", ephemeral=True)
        role_id = await lm_get_section_role(gid, sec)
        mention = f"<@&{role_id}> " if role_id else ""
        topic_rows = await lm_get_topics(gid, sec)
        emoji = next((e for k, e, _ in topic_rows if k.lower() == topic_key.lower()), "🔔")
        title = "LFG" if sec == LM_SEC_LIX else "Market"
        embed = discord.Embed(title=f"{emoji} {title} — {topic_key}", description=text[:3900] + ("…" if len(text) > 3900 else ""),
                              color=0x4aa3ff if sec == LM_SEC_LIX else 0xf1c40f)
        embed.set_footer(text=f"by {inter.user.display_name}")
        try:
            msg = await ch.send(content=mention + f"**{title}**", embed=embed, allowed_mentions=discord.AllowedMentions(roles=True))
        except Exception as e:
            return await inter.response.send_message(f"Couldn't post to {ch.mention}: {e}", ephemeral=True)
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE listings SET last_ping_ts=?, channel_id=?, message_id=? WHERE id=?",
                             (now, ch.id, msg.id, int(id)))
            await db.commit()
        await inter.response.send_message(f"✅ Bumped #{id}.", ephemeral=True)

    @group.command(name="close", description=f"Close (expire) your {sec} post")
    @app_commands.describe(id="Listing ID to close")
    async def close_cmd(inter: discord.Interaction, id: int):
        gid = inter.guild.id
        async with aiosqlite.connect(DB_PATH) as db:
            c = await db.execute("""SELECT channel_id,message_id FROM listings
                                    WHERE id=? AND guild_id=? AND section=? AND author_id=?""",
                                 (int(id), gid, sec, inter.user.id))
            row = await c.fetchone()
            if not row:
                return await inter.response.send_message("Listing not found or not yours.", ephemeral=True)
            ch_id, msg_id = row
            await db.execute("DELETE FROM listings WHERE id=?", (int(id),))
            await db.commit()
        g = inter.guild; ch = g.get_channel(int(ch_id)) if ch_id else None
        if ch:
            try:
                msg = await ch.fetch_message(int(msg_id))
                await msg.delete()
            except Exception:
                pass
        await inter.response.send_message(f"✅ Closed #{id}.", ephemeral=True)

# Bind commands for both sections
lm_bind_commands(LM_SEC_LIX, lix_group)
lm_bind_commands(LM_SEC_MARKET, market_group)

# Register groups & seed topics on ready (without touching your existing on_ready)
@bot.listen("on_ready")
async def _lm_on_ready():
    try:
        await lm_init_tables()
        # Seed default topics if empty
        for g in bot.guilds:
            await lm_seed_topics_if_empty(g)
        # Add groups if not already present
        names = {cmd.name for cmd in bot.tree.get_commands()}
        if "lix" not in names:
            bot.tree.add_command(lix_group)
        if "market" not in names:
            bot.tree.add_command(market_group)
        # Start cleaner
        if not lm_cleanup_loop.is_running():
            lm_cleanup_loop.start()
        # Optional: sync (safe to ignore errors)
        try:
            await bot.tree.sync()
        except Exception:
            pass
        log.info("Lixing & Market add-on ready.")
    except Exception as e:
        log.warning(f"Lix/Market init failed: {e}")
# ------------------ End Lixing & Market Add-on --------------------
