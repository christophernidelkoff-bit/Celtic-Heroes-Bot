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
    # Safety tweak: use the already-fetched guild object
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
        # only when threshold crossed this tick (prevents boot spam)
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
                    # refreshing panels so the order/times reflect the new state
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
                row = await c.fetchone()  # FIXED: use cursor.fetchone(), not db.fetchone()
            if not row:
                return
            role = guild.get_role(int(row[0]))
            if role:
                await member.remove_roles(role, reason="Reaction role opt-out")
        except Exception as e:
            log.warning(f"Remove reaction-role failed: {e}")
# -------- /timers (per-user UI endpoint) --------
@app_commands.guild_only()
@bot.tree.command(name="timers", description="Show timers with per-category toggles (ephemeral, remembers your selection)")
async def slash_timers(interaction: discord.Interaction):
    guild = interaction.guild
    if not guild:
        return await interaction.response.send_message("Use this in a server.", ephemeral=True)
    if not await ensure_guild_auth(guild):
        return await interaction.response.send_message("Bot disabled in this server.", ephemeral=True)
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

# -------- INTERVALS --------
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
async def intervals_cmd(ctx):
    await send_intervals_list(ctx)

@bot.group(name="boss", invoke_without_command=True)
async def boss_group(ctx):
    p = await get_guild_prefix(bot, ctx.message)
    await ctx.send(f"Use `{p}help` for commands.")

# -------- BOSS SUBCOMMANDS --------
@boss_group.command(name="add")
async def boss_add(ctx, *args):
    """
    !boss add "Name" <spawn_m> <window_m> [#channel] [pre_m] [category]
    """
    def _resolve_channel_id_from_arg(ctx, value: Optional[str]) -> Optional[int]:
        if not value: return None
        if isinstance(value, int): return value
        s = str(value)
        if s.startswith("<#") and s.endswith(">"): return int(s[2:-1])
        if s.isdigit(): return int(s)
        found = discord.utils.get(ctx.guild.channels, name=s.strip("#"))
        return found.id if found else None

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
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE bosses SET next_spawn_ts=? WHERE guild_id=?", (now_ts() - 3601, ctx.guild.id))
        await db.commit()
    await ctx.send(":white_check_mark: All timers set to **-Nada**.")
    await refresh_subscription_messages(ctx.guild)

@boss_group.command(name="nada")
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
@commands.has_permissions(manage_guild=True)
async def boss_nadaall(ctx):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE bosses SET next_spawn_ts=? WHERE guild_id=?", (now_ts() - 3601, ctx.guild.id))
        await db.commit()
    await ctx.send(":pause_button: **All bosses** set to **-Nada**.")
    await refresh_subscription_messages(ctx.guild)

@boss_group.command(name="info")
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
    when_small = datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime('%H:%M UTC')
    line1 = f"**{name}**\nCategory: {cat} | Sort: {sort_key or '(none)'}\n"
    line2 = f"Respawn: {spawn_m}m | Window: {window_m}m\n"
    line3 = f"Spawn Time: `{fmt_delta_for_list(left)}`"
    line3b = f"\n> *ETA {when_small}*" if left > 0 else ""
    line4 = f"\nPre: {pre}m | Channel: {f'<#{ch_id}>' if ch_id else 'Default/Category'} | Role: {f'<@&{role_id}>' if role_id else 'None'}"
    await ctx.send(line1 + line2 + line3 + line3b + line4)

@boss_group.command(name="killed")
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
@commands.has_permissions(manage_guild=True)
async def boss_setchannelall(ctx, channel: discord.TextChannel):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE bosses SET channel_id=? WHERE guild_id=?", (channel.id, ctx.guild.id))
        await db.commit()
    await ctx.send(f":satellite: All boss reminders → {channel.mention}.")

@boss_group.command(name="setchannelcat")
@commands.has_permissions(manage_guild=True)
async def boss_setchannelcat(ctx, *, args: str):
    def _resolve_channel_id_from_arg(ctx, value: Optional[str]) -> Optional[int]:
        if not value: return None
        if isinstance(value, int): return value
        s = str(value)
        if s.startswith("<#") and s.endswith(">"): return int(s[2:-1])
        if s.isdigit(): return int(s)
        found = discord.utils.get(ctx.guild.channels, name=s.strip("#"))
        return found.id if found else None

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
    res, err = await resolve_boss(ctx, ident)
    if err: return await ctx.send(f":no_entry: {err}")
    bid, nm, _ = res
    await ctx.send(f"Matched: **{nm}**")

# -------- BLACKLIST COMMANDS --------
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

# -------- SERVER SETTINGS COMMANDS --------
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
    await delete_old_subscription_messages(ctx.guild)
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

# -------- NEW: SETPREANNOUNCE FAMILY --------
@bot.command(name="setpreannounce")
@commands.has_permissions(manage_guild=True)
async def setpreannounce_cmd(ctx, *, args: str):
    """
    Dedicated family to set pre-announce minutes.
    Usage:
      • !setpreannounce "Boss Name" <m|off>
      • !setpreannounce category "<Category>" <m|off>
      • !setpreannounce all <m|off>
    Notes:
      - 'off' / 'none' / '0' disables pre-announces (sets 0).
      - Minutes are capped between 0 and 10080 (7 days) to avoid accidental huge values.
    """
    text = (args or "").strip()
    if not text:
        return await ctx.send('Usage: `!setpreannounce "Boss Name" <m|off>` | `!setpreannounce category "<Category>" <m|off>` | `!setpreannounce all <m|off>`')

    def parse_minutes(tok: str) -> Optional[int]:
        tl = (tok or "").strip().lower()
        if tl in {"off","none","disable","disabled","0"}: return 0
        if tl.endswith("m"): tl = tl[:-1]
        if not tl.lstrip("-").isdigit(): return None
        val = int(tl)
        if val < 0: val = 0
        if val > 10080: val = 10080
        return val

    # all-mode
    if text.lower().startswith("all"):
        rest = text[3:].strip()
        if not rest:
            return await ctx.send("Provide minutes, e.g., `!setpreannounce all 10` or `off`.")
        m = parse_minutes(rest.split()[-1])
        if m is None:
            return await ctx.send("Minutes must be a number or `off`.")
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE bosses SET pre_announce_min=? WHERE guild_id=?", (m, ctx.guild.id))
            await db.commit()
        return await ctx.send(f":white_check_mark: Pre-announce for **all bosses** set to **{m}m**." if m else ":white_check_mark: Pre-announce **disabled** for all bosses.")

    # category-mode
    if text.lower().startswith("category"):
        after = text[len("category"):].strip()
        cat = None; minutes_tok = None
        if after.startswith('"') and after.count('"') >= 2:
            cat = after.split('"',1)[1].split('"',1)[0].strip()
            tail = after.split('"',2)[-1].strip()
            if not tail:
                return await ctx.send('Provide minutes after the category, e.g., `!setpreannounce category "Frozen" 8`.')
            minutes_tok = tail.split()[-1]
        else:
            parts = after.split()
            if len(parts) < 2:
                return await ctx.send('Format: `!setpreannounce category "<Category>" <m|off>`')
            minutes_tok = parts[-1]
            cat = " ".join(parts[:-1]).strip()
        m = parse_minutes(minutes_tok or "")
        if m is None:
            return await ctx.send("Minutes must be a number or `off`.")
        catn = norm_cat(cat)
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE bosses SET pre_announce_min=? WHERE guild_id=? AND category=?", (m, ctx.guild.id, catn))
            await db.commit()
        return await ctx.send(f":white_check_mark: Pre-announce for **{catn}** set to **{m}m**." if m else f":white_check_mark: Pre-announce **disabled** for **{catn}**.")

    # per-boss mode
    name = None; minutes_tok = None
    if text.startswith('"') and text.count('"') >= 2:
        name = text.split('"',1)[1].split('"',1)[0].strip()
        tail = text.split('"',2)[-1].strip()
        if not tail:
            return await ctx.send('Provide minutes after the name, e.g., `!setpreannounce "Grom" 12`.')
        minutes_tok = tail.split()[-1]
    else:
        parts = text.split()
        if len(parts) < 2:
            return await ctx.send('Format: `!setpreannounce "Boss Name" <m|off>`')
        minutes_tok = parts[-1]
        name = " ".join(parts[:-1]).strip()

    m = parse_minutes(minutes_tok or "")
    if m is None:
        return await ctx.send("Minutes must be a number or `off`.")

    res, err = await resolve_boss(ctx, name)
    if err:
        return await ctx.send(f":no_entry: {err}")
    bid, nm, _ = res
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE bosses SET pre_announce_min=? WHERE id=? AND guild_id=?", (m, bid, ctx.guild.id))
        await db.commit()
    await ctx.send(f":white_check_mark: Pre-announce for **{nm}** set to **{m}m**." if m else f":white_check_mark: Pre-announce **disabled** for **{nm}**.")

# -------- REACTION ROLES (slash) --------
@app_commands.guild_only()
@app_commands.default_permissions(manage_roles=True)
@bot.tree.command(name="roles_panel", description="Create a reaction-roles message (react to get/remove roles).")
async def roles_panel(interaction: discord.Interaction,
                      channel: Optional[discord.TextChannel] = None,
                      title: str = "Roles",
                      pairs: str = ""):
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
        if not parts: continue
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

# -------- OPTIONAL POWERSHELL (slash) --------
def _find_pwsh_exe() -> Optional[str]:
    for exe in ("pwsh", "powershell", "powershell.exe", "pwsh.exe"):
        path = shutil.which(exe)
        if path:
            return path
    return None

@app_commands.guild_only()
@bot.tree.command(name="ps", description="Run a PowerShell command on the bot host (Admins only; requires ALLOW_POWERSHELL=1)")
async def ps_run(interaction: discord.Interaction, command: str):
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

# -------- HELP / STATUS / HEALTH --------
@bot.command(name="help")
async def help_cmd(ctx):
    p = await get_guild_prefix(bot, ctx.message)
    lines = [
        f"**Boss Tracker — Commands**",
        "",
        f"**Essentials**",
        f"• Timers: `{p}timers`  • Intervals: `{p}intervals`",
        f"• Quick reset: `{p}<BossOrAlias>`  (e.g., `{p}snorri`)",
        "",
        f"**Boss Ops**",
        f"• Add: `{p}boss add \"Name\" <spawn_m> <window_m> [#chan] [pre_m] [category]`",
        f"• Killed: `{p}boss killed \"Name\"` • Increase/Reduce: `{p}boss increase|reduce \"Name\" <m>`",
        f"• Idle/Nada: `{p}boss nada \"Name\"` • All Idle: `{p}boss nadaall`",
        f"• Edit: `{p}boss edit \"Name\" <spawn_minutes|window_minutes|pre_announce_min|name|category|sort_key> <value>`",
        f"• Channel routing: `{p}boss setchannel \"Name\" #chan` • All: `{p}boss setchannelall #chan` • By category: `{p}boss setchannelcat \"Category\" #chan`",
        f"• Role for reset: `{p}boss setrole @Role` • Clear: `{p}boss setrole none` • Per-boss: `{p}boss setrole \"Name\" @Role`",
        f"• Aliases: `{p}boss alias add|remove \"Name\" \"alias\"` • List: `{p}boss aliases \"Name\"`",
        "",
        f"**Subscriptions**",
        f"• Panels channel: `{p}setsubchannel #panels` • Refresh: `{p}showsubscriptions`",
        f"• Ping channel: `{p}setsubpingchannel #pings`",
        "",
        f"**Server Settings**",
        f"• Announce: `{p}setannounce #chan` • Category route: `{p}setannounce category \"Category\" #chan`",
        f"• ETA: `{p}seteta on|off` • Colors: `{p}setcatcolor <Category> <#hex>`",
        f"• Heartbeat: `{p}setuptime <minutes>` • HB channel: `{p}setheartbeatchannel #chan`",
        f"• Prefix: `{p}setprefix <new>`",
        f"• **Pre-announce**: per-boss `{p}setpreannounce \"Name\" <m|off>` • per-category `{p}setpreannounce category \"Category\" <m|off>` • all `{p}setpreannounce all <m|off>`",
        "",
        f"**Status**",
        f"• `{p}status` • `{p}health`",
        "",
        f"**Slash**",
        f"• `/timers` (ephemeral with per-user category toggles)",
        f"• `/roles_panel channel:<#> title:<...> pairs:\"😀 @Role, 🔔 @Role\"`",
    ]
    text = "\n".join(lines)
    if len(text) > 1990: text = text[:1985] + "…"
    if can_send(ctx.channel): await ctx.send(text)

@bot.command(name="status")
async def status_cmd(ctx):
    gid = ctx.guild.id; p = await get_guild_prefix(bot, ctx.message)
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute(
            "SELECT COALESCE(prefix, ?), default_channel, sub_channel_id, sub_ping_channel_id, "
            "COALESCE(uptime_minutes, ?), heartbeat_channel_id, COALESCE(show_eta,0) "
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
        f"Subscription panels: {ch(sub_id)} • Subscription pings: {ch(sub_ping_id)}",
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
    required = {"bosses","guild_config","meta","category_colors","subscription_emojis","subscription_members",
                "boss_aliases","category_channels","user_timer_prefs","subscription_panels","rr_panels","rr_map","blacklist"}
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

# -------- SHOW ETA FLAG (used by timers displays) --------
async def get_show_eta(guild_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT COALESCE(show_eta,0) FROM guild_config WHERE guild_id=?", (guild_id,))
        r = await c.fetchone()
        return bool(r and int(r[0]) == 1)

# -------- ERRORS --------
@bot.event
async def on_command_error(ctx, error):
    from discord.ext import commands as ext
    if isinstance(error, ext.CommandNotFound): return
    try: await ctx.send(f":warning: {error}")
    except Exception: pass

# --- Interaction reply helper to avoid "Application did not respond" ---
async def ireply(
    inter: discord.Interaction,
    content: Optional[str] = None,
    *,
    embed: Optional[discord.Embed] = None,
    embeds: Optional[List[discord.Embed]] = None,
    ephemeral: bool = True
):
    """Reply safely whether we've already deferred or not."""
    try:
        if inter.response.is_done():
            await inter.followup.send(content=content, embed=embed, embeds=embeds, ephemeral=ephemeral)
        else:
            await inter.response.send_message(content=content, embed=embed, embeds=embeds, ephemeral=ephemeral)
    except Exception as e:
        log.warning(f"ireply error: {e}")

# -------- SHUTDOWN --------
async def graceful_shutdown(_sig=None):
    try: await meta_set("offline_since", str(now_ts()))
    finally: await bot.close()

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
    topics = await lm_get_topics(gid, section)
    emoji = next((e for k, e, _ in topics if k.lower() == topic_key.lower()), "🔔")

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

# Autocomplete for topics (section-aware)
async def lm_topic_autocomplete(inter: discord.Interaction, current: str, section: str):
    topics = await lm_get_topics(inter.guild.id, section)
    current_l = (current or "").lower()
    opts = []
    for key, emoji, _ in topics:
        if not current or current_l in key.lower():
            label = f"{emoji} {key}" if emoji else key
            opts.append(app_commands.Choice(name=label[:100], value=key[:100]))
        if len(opts) >= 25: break
    return opts

# ------- Common subcommands factory (with proper async autocomplete) -------
def lm_bind_commands(section: str, group: app_commands.Group):
    sec = lm_norm_section(section)

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

    # Proper async autocomplete wrappers (fixes lambda/coroutine error)
    async def _topic_autocomplete(inter: discord.Interaction, current: str):
        return await lm_topic_autocomplete(inter, current, sec)

    @group.command(name="post", description=f"Create a {sec} post (6h cooldown per content)")
    @app_commands.describe(topic="Pick a topic", text="What you need / offer")
    @app_commands.autocomplete(topic=_topic_autocomplete)
    async def post_cmd(inter: discord.Interaction, topic: str, text: str):
        await lm_post_listing(inter, sec, topic, text)

    @group.command(name="browse", description=f"Browse active {sec} posts")
    @app_commands.describe(topic="Filter by topic (optional)")
    @app_commands.autocomplete(topic=_topic_autocomplete)
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
        topics = await lm_get_topics(gid, sec)
        emoji = next((e for k, e, _ in topics if k.lower() == topic_key.lower()), "🔔")
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

# Bind commands for both sections (with fixed autocomplete)
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

# ------------------ END Lixing & Market Add-on ------------------

# -------- RUN --------
async def main():
    loop = asyncio.get_running_loop()
    for s in (getattr(signal, "SIGINT", None), getattr(signal, "SIGTERM", None)):
        if s:
            try: loop.add_signal_handler(s, lambda sig=s: asyncio.create_task(graceful_shutdown(sig)))
            except NotImplementedError: pass
    try: await bot.start(TOKEN)
    except KeyboardInterrupt: await graceful_shutdown()

if __name__ == "__main__":
    asyncio.run(main())
