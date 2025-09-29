# -------------------- Celtic Heroes Boss Tracker — Foundations (Part 1/4) --------------------
# Features in this part:
# - Env & logging, intents, globals
# - Render-safe DB path + warmup (WAL) + hardened preflight
# - Time helpers incl. window_label() with your exact rules
# - Category normalization, emojis, colors
# - Prefix resolver, permission helpers
# - Guild auth gate (requires @blunderbusstin present)
# - Category/channel routing helpers
# - Subscription panels: emoji mapping + builders + refresh cycle
# - Subscription ping helper (separate designated channel supported)

from __future__ import annotations
# --- Emoji constants and safe send helper (mojibake fix) ---
EMJ_HOURGLASS = "⏳"
EMJ_CLOCK = "🕓"


# --- timers UI helper: hide window segment when pending ---
def _hide_if_pending(win_label: str, prefix: str = " · ") -> str:
    try:
        lab = str(win_label)
    except Exception:
        return ""
    if "pending" in lab.lower():
        return ""
    if len(lab) > 64:
        lab = lab[:64]
    return f"{prefix}{lab}"

# --- UI sanitizer: fix common mojibake (UTF-8 read as Latin-1) ---
def sanitize_ui(text: str) -> str:
    """
    Convert mojibake like 'Ã', 'Â', 'ðŸ' back to proper Unicode.
    Error checks:
      - Only attempt if suspicious tokens present.
      - Cap output to 6000 chars to avoid Discord limits in callers.
      - Fallback to original on decode failure.
    """
    try:
        if not isinstance(text, str):
            return text
        suspicious = ("Ã", "Â", "ðŸ", "â€¢", "â€”", "â€“", "â€™", "â€œ", "â€", "ã€", "ï»¿")
        if any(tok in text for tok in suspicious):
            fixed = text.encode("latin-1", "ignore").decode("utf-8", "ignore")
            if fixed and (fixed.count(" ") == 0):  # avoid replacement-char mess
                text = fixed
        return text[:6000]
    except Exception:
        return text
async def send_text_safe(ch, content: str):
    """Error-checked send for text content."""
    if not content:
        return None
    # Error check 2: guard message length
    if len(content) > 1990:
        content = content[:1990] + "…"
    try:
        return await ch.send(content)
    except Exception as e:
        import logging as _logging
        _logging.warning(f"send_text_safe failed: {e}")
        return None


# ==================== EARLY SCHEMA BOOTSTRAP (sync; placed after future imports) ====================
# Ensures required tables/columns exist *before* any background tasks run.
try:
    import os as __os_boot, sqlite3 as __sqlite_boot, pathlib as __pl_boot
    def __ensure_db_at(path_dir: str):
        try:
            __pl_boot.Path(path_dir).mkdir(parents=True, exist_ok=True)
            dbp = __pl_boot.Path(path_dir) / "bosses.db"
            conn = __sqlite_boot.connect(str(dbp), timeout=5)
            cur = conn.cursor()
            cur.execute("PRAGMA journal_mode=WAL;")
            cur.execute("PRAGMA synchronous=NORMAL;")
            cur.execute("PRAGMA busy_timeout=5000;")
            # Core tables
            cur.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)")
            cur.execute("""CREATE TABLE IF NOT EXISTS blacklist (
                guild_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                PRIMARY KEY (guild_id, user_id)
            )""")
            cur.execute("CREATE TABLE IF NOT EXISTS guild_config (guild_id INTEGER PRIMARY KEY)")
            # Columns observed missing in logs
            cur.execute("PRAGMA table_info(guild_config)")
            have = {row[1] for row in cur.fetchall()}
            need = [
                ("prefix","TEXT","'!'"),
                ("welcome_channel_id","INTEGER","NULL"),
                ("roster_channel_id","INTEGER","NULL"),
                ("auto_member_role_id","INTEGER","NULL"),
                ("timers_role_id","INTEGER","NULL"),
                ("uptime_channel_id","INTEGER","NULL"),
                ("uptime_interval_min","INTEGER","5"),
                ("sub_channel_id","INTEGER","NULL"),
                ("sub_message_id","INTEGER","NULL"),
                ("sub_ping_channel_id","INTEGER","NULL"),
                ("default_channel","INTEGER","NULL"),
                ("heartbeat_channel_id","INTEGER","NULL"),
                ("uptime_minutes","INTEGER","NULL"),
                ("show_eta","INTEGER","1"),
                ("roster_star_gif","TEXT","NULL"),
            ]
            for name, typ, default in need:
                if name not in have:
                    cur.execute(f"ALTER TABLE guild_config ADD COLUMN {name} {typ} DEFAULT {default}")
            conn.commit()
            conn.close()
            return True
        except Exception:
            return False

    # Try both common locations so whichever the bot chooses is ready.
    ok = False
    for _dir in ("/var/data", "/tmp"):
        if __ensure_db_at(_dir):
            ok = True
    if not ok:
        print("[early-bootstrap] could not prepare DB at /var/data or /tmp")
except Exception as __e_boot:
    try: print(f"[early-bootstrap] skipped: {__e_boot}")
    except Exception: pass
# ==================== END EARLY SCHEMA BOOTSTRAP ====================


import os
import re
import atexit
import signal
import asyncio
import logging
import shutil
import io
import pathlib
from typing import Optional, Tuple, List, Dict, Any, Set
from datetime import datetime, timezone

import aiosqlite
import discord
from discord.ext import commands, tasks
from discord import app_commands

# --- Panel text sanitizer (patch: fix mojibake e.g., "Cromâ€™s") ---
_MOJIBAKE_HINTS = ("Ã", "â", "ðŸ")
def _sanitize_panel_text(s):
    """
    Return a cleaned string for panel display.
    Error checks:
      1) Non-string or empty -> return as-is to avoid crashes.
      2) If mojibake hints present, try latin1->utf8 round-trip.
      3) Validate printable result; if control-chars present, fall back to a simple ASCII apostrophe fix.
    """
    if not isinstance(s, str) or not s:
        return s
    out = s
    try:
        if any(h in out for h in _MOJIBAKE_HINTS):
            try:
                cand = out.encode("latin1", "ignore").decode("utf-8", "ignore")
                if cand:
                    out = cand
            except Exception:
                pass
        # Targeted common fixes
        out = (out.replace("â€™", "’")
                 .replace("â€œ", "“")
                 .replace("â€", "”")
                 .replace("â€“", "–")
                 .replace("â€”", "—"))
        # Error check: strip stray controls
        if any(ord(ch) < 32 for ch in out):
            out = "".join(ch for ch in out if ord(ch) >= 32)
        # Fallback: ensure basic apostrophe looks right
        out = out.replace("’", "’")  # idempotent
        return out
    except Exception:
        # Safe fallback: normalize to ASCII apostrophe only
        return s.replace("â€™", "'")
# --- End sanitizer ---
from dotenv import load_dotenv

# -------------------- ENV / GLOBALS --------------------
load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN")
if not TOKEN:
    raise SystemExit("DISCORD_TOKEN missing in .env")

ALLOW_POWERSHELL = os.getenv("ALLOW_POWERSHELL", "0") in {"1", "true", "True", "yes", "YES"}

DEFAULT_PREFIX = "!"
CHECK_INTERVAL_SECONDS = 15
DEFAULT_UPTIME_MINUTES = 60
NADA_GRACE_SECONDS = 1800  # after window closes, flip to -Nada only after this grace

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ch-bossbot")

# -------------------- RENDER-SAFE SQLITE LOCATION + WARMUP --------------------
# Prefer persistent Render Disk at /var/data (or set DATA_DIR/RENDER_DISK_PATH). Fallback to /tmp (ephemeral).
DATA_DIR = os.environ.get("DATA_DIR") or os.environ.get("RENDER_DISK_PATH") or "/var/data"
try:
    pathlib.Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
except Exception as e:
    log.warning(f"[startup] Could not use {DATA_DIR} ({e}); falling back to /tmp")
    DATA_DIR = "/tmp"
    pathlib.Path(DATA_DIR).mkdir(parents=True, exist_ok=True)

DB_PATH = os.path.join(DATA_DIR, os.environ.get("DB_FILE", "bosses.db"))
log.info(f"[startup] SQLite path: {DB_PATH}")

async def sqlite_warmup():
    """Error-check 2: open DB, set WAL, ensure meta table exists."""
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("PRAGMA journal_mode=WAL;")
            await db.execute("PRAGMA synchronous=NORMAL;")
            await db.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)")
            await db.commit()
        log.info("[startup] SQLite warmup complete.")
    except Exception as e:
        log.warning(f"[startup] SQLite warmup failed: {e}")

# -------------------- INTENTS / BOT --------------------
intents = discord.Intents.default()
intents.message_content = True
intents.reactions = True
intents.guilds = True
intents.members = True

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
    Rule-set:
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
    # Robust category emoji mapping with ASCII-safe fallback
    c = norm_cat(c)
    mapping = {
        "Warden": "🛡️",
        "Meteoric": "☄️",
        "Frozen": "🧊",
        "DL": "🐉",
        "EDL": "🐲",
        "Midraids": "⚔️",
        "Rings": "💍",
        "EG": "🔱",
        "Default": "📄",
    }
    emo = mapping.get(c, "📄")
    # Error check 1: ensure short grapheme length
    try:
        if len(emo) == 0 or len(emo) > 4:
            emo = "📄"
    except Exception:
        emo = "📄"
    return emo

DEFAULT_COLORS = {
    "Warden": 0x2ecc71, "Meteoric": 0xe67e22, "Frozen": 0x3498db,
    "DL": 0xe74c3c, "EDL": 0x8e44ad, "Midraids": 0x34495e,
    "Rings": 0x1abc9c, "EG": 0xf1c40f, "Default": 0x95a5a6,
}

EMOJI_PALETTE = [
    "ðŸŸ¥","ðŸŸ§","ðŸŸ¨","ðŸŸ©","ðŸŸ¦","ðŸŸª","â¬›","â¬œ","ðŸŸ«",
    "ðŸ”´","ðŸŸ ","ðŸŸ¡","ðŸŸ¢","ðŸ”µ","ðŸŸ£","âš«","âšª","ðŸŸ¤",
    "â­","✨","âš¡","ðŸ”¥","âš”ï¸","ðŸ—¡ï¸","ðŸ›¡ï¸","ðŸ¹","ðŸ—¿","ðŸ§ª","ðŸ§¿","ðŸ‘‘","ðŸŽ¯","ðŸª™",
    "ðŸ‰","ðŸ²","ðŸ”±","â˜„ï¸","ðŸ§Š","ðŸŒ‹","ðŸŒªï¸","ðŸŒŠ","ðŸŒ«ï¸","ðŸŒ©ï¸","ðŸª½","ðŸª“",
    "0ï¸âƒ£","1ï¸âƒ£","2ï¸âƒ£","3ï¸âƒ£","4ï¸âƒ£","5ï¸âƒ£","6ï¸âƒ£","7ï¸âƒ£","8ï¸âƒ£","9ï¸âƒ£","ðŸ”Ÿ",
]
EXTRA_EMOJIS = [
    "â“ª","â‘ ","â‘¡","â‘¢","â‘£","â‘¤","â‘¥","â‘¦","â‘§","â‘¨","â‘©","â‘ª","â‘«","â‘¬","â‘­","â‘®","â‘¯","â‘°","â‘±","â‘²","â‘³",
    "ðŸ…°ï¸","ðŸ…±ï¸","ðŸ†Ž","ðŸ†‘","ðŸ†’","ðŸ†“","ðŸ†”","ðŸ†•","ðŸ†–","ðŸ…¾ï¸","ðŸ†—","ðŸ…¿ï¸","ðŸ†˜","ðŸ†™","ðŸ†š",
    "â™ˆ","â™‰","â™Š","â™‹","â™Œ","â™","â™Ž","â™","â™","â™‘","â™’","â™“",
]

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
    """Error-check 3: hardened preflight with clear messaging on read-only failures."""
    import sqlite3
    db_dir = os.path.dirname(DB_PATH) or "."
    try:
        pathlib.Path(db_dir).mkdir(parents=True, exist_ok=True)
    except Exception as e:
        log.critical(f"[db] Cannot create DB directory '{db_dir}': {e}")
        raise SystemExit(1)

    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        # Pragmas early for stability
        try:
            cur.execute("PRAGMA journal_mode=WAL;")
            cur.execute("PRAGMA synchronous=NORMAL;")
        except Exception:
            pass

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
        conn.commit()
        conn.close()
    except sqlite3.OperationalError as e:
        log.critical(f"[db] SQLite OperationalError: {e}")
        log.critical("[db] If this is a Render deploy, ensure a writable disk is mounted at /var/data "
                     "or that the app can use /tmp. You can set DATA_DIR=/var/data.")
        raise SystemExit(1)

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

# Warmup listener (runs alongside your main on_ready in Section 2)
@bot.listen("on_ready")
async def _db_warmup_on_ready():
    await sqlite_warmup()

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
        nm = _sanitize_panel_text(name)
        e = emoji_map.get(bid, "â­")
        if e in per_message_emojis:  # avoid dup reactions in one message
            continue
        per_message_emojis.append(e)
        lines.append(f"{e} — **{nm}**")
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
        txt = f"{EMJ_HOURGLASS} {mentions} — **{boss_name}** Spawn Time: `{fmt_delta_for_list(left)}` (almost up)."
    else:
        txt = f"{EMJ_CLOCK} {mentions} — **{boss_name}** Spawn Window has opened!"
    try: await ch.send(txt)
    except Exception as e: log.warning(f"Sub ping failed: {e}")

# -------------------- End of Section 1/4 --------------------
# -------------------- Part 2/4 — prefs, resolve, boot/offline, seed, events --------------------

# Per-user timer view prefs (used by slash /timers)
async def get_user_shown_categories(guild_id: int, user_id: int) -> List[str]:
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute(
            "SELECT categories FROM user_timer_prefs WHERE guild_id=? AND user_id=?",
            (guild_id, user_id)
        )
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

# Guild default row bootstrap
async def upsert_guild_defaults(guild_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO guild_config (guild_id, prefix, uptime_minutes, show_eta) VALUES (?,?,?,?) "
            "ON CONFLICT(guild_id) DO NOTHING",
            (guild_id, DEFAULT_PREFIX, DEFAULT_UPTIME_MINUTES, 0)
        )
        await db.commit()

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
            if len(rows) == 1:
                return rows[0], None
            if len(rows) > 1:
                return None, f"Multiple matches for '{identifier}'. Use the exact name (quotes OK)."
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
            if len(rows) == 1:
                return rows[0], None
            if len(rows) > 1:
                return None, f"Multiple alias matches for '{identifier}'. Use exact alias."
    return None, f"No boss found for '{identifier}'."

# In-memory flags used by loops/events
muted_due_on_boot: Set[int] = set()
if not hasattr(bot, "_seen_keys"):
    bot._seen_keys = set()  # type: ignore[attr-defined]

# -------------------- BOOT OFFLINE PROCESSING (extra guards) --------------------
async def boot_offline_processing():
    boot = now_ts()
    off_since: Optional[int] = None
    try:
        off_explicit = await meta_get("offline_since")
        if off_explicit and off_explicit.isdigit():
            off_since = int(off_explicit)
    except Exception as e:
        log.warning(f"[boot] Failed reading offline_since: {e}")

    try:
        last_tick = await meta_get("last_tick_ts")
        if (off_since is None) and last_tick and last_tick.isdigit():
            last_tick_i = int(last_tick)
            if boot - last_tick_i > CHECK_INTERVAL_SECONDS * 2:
                off_since = last_tick_i
    except Exception as e:
        log.warning(f"[boot] Failed reading last_tick_ts: {e}")

    try:
        await meta_set("offline_since", "")
    except Exception:
        pass

    # Load all timers once
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT id,guild_id,channel_id,name,next_spawn_ts,category FROM bosses")
        rows = await c.fetchall()

    # Track those already due at boot to avoid duplicate window spam in the first tick
    due_at_boot = [(int(bid), int(gid), ch, nm, int(ts), cat) for bid, gid, ch, nm, ts, cat in rows if int(ts) <= boot]
    for bid, *_ in due_at_boot:
        muted_due_on_boot.add(int(bid))

    # Send catch-up messages for events that elapsed while the bot was offline (between off_since and boot)
    if off_since:
        just_due = [(bid, gid, ch, nm, ts, cat) for (bid, gid, ch, nm, ts, cat) in due_at_boot if off_since <= int(ts) <= boot]
        for bid, gid, ch_id, name, ts, cat in just_due:
            guild = bot.get_guild(gid)
            ch = await resolve_announce_channel(gid, ch_id, cat) if guild else None
            if ch and can_send(ch):
                try:
                    ago = human_ago(boot - int(ts))
                    await ch.send(f":zzz: While I was offline, **{name}** spawned ({ago}).")
                except Exception as e:
                    log.warning(f"[boot] Offline notice failed: {e}")
            # fire a subscription "window" ping as well
            try:
                await send_subscription_ping(gid, bid, phase="window", boss_name=name)
            except Exception as e:
                log.warning(f"[boot] Sub ping failed: {e}")

# -------------------- SEED DATA (authoritative respawn/window minutes + aliases) --------------------
# NOTE: Existing entries in DB will be UPDATED to these values by ensure_seed_for_guild().
SEED_DATA: List[Tuple[str, str, int, int, List[str]]] = [
    # METEORIC
    ("Meteoric", "Doomclaw", 7, 5, []),
    ("Meteoric", "Bonehad", 15, 5, []),
    ("Meteoric", "Rockbelly", 15, 5, []),
    ("Meteoric", "Redbane", 20, 5, []),
    ("Meteoric", "Coppinger", 20, 5, ["copp"]),
    ("Meteoric", "Goretusk", 20, 5, []),

    # FROZEN
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
    ("EG", "Cromâ€™s Manikin", 5760, 1440, ["manikin", "crom", "croms"]),          # 96h / 24h

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
      - Does NOT delete any extra bosses youâ€™ve added manually.
    """
    key = f"seed:{SEED_VERSION}:g{guild.id}"
    already = await meta_get(key)

    inserted = 0
    updated = 0
    alias_added = 0

    try:
        async with aiosqlite.connect(DB_PATH) as db:
            # Load existing bosses for this guild
            c = await db.execute(
                "SELECT id,name,category,spawn_minutes,window_minutes FROM bosses WHERE guild_id=?",
                (guild.id,)
            )
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
                    if (cur_sp != spawn_m) or (cur_win != window_m):
                        await db.execute(
                            "UPDATE bosses SET spawn_minutes=?, window_minutes=? WHERE id=?",
                            (spawn_m, window_m, bid)
                        )
                        updated += 1
                    # ensure aliases
                    for al in aliases:
                        try:
                            await db.execute(
                                "INSERT INTO boss_aliases (guild_id,boss_id,alias) VALUES (?,?,?)",
                                (guild.id, bid, str(al).strip().lower())
                            )
                            alias_added += 1
                        except Exception:
                            # unique constraint or similar â€“ safe to ignore
                            pass
                else:
                    # Insert new with -Nada default next_spawn_ts
                    next_spawn = now_ts() - 3601
                    await db.execute(
                        "INSERT INTO bosses (guild_id,channel_id,name,spawn_minutes,window_minutes,next_spawn_ts,pre_announce_min,created_by,category,sort_key) "
                        "VALUES (?,?,?,?,?,?,?,?,?,?)",
                        (guild.id, None, name, int(spawn_m), int(window_m), next_spawn, 10,
                         guild.owner_id if guild.owner_id else 0, norm_cat(cat), "")
                    )
                    inserted += 1
                    # fetch id and add aliases
                    c = await db.execute(
                        "SELECT id FROM bosses WHERE guild_id=? AND name=? AND category=?",
                        (guild.id, name, norm_cat(cat))
                    )
                    bid_row = await c.fetchone()
                    if bid_row:
                        bid = int(bid_row[0])
                        for al in aliases:
                            try:
                                await db.execute(
                                    "INSERT INTO boss_aliases (guild_id,boss_id,alias) VALUES (?,?,?)",
                                    (guild.id, bid, str(al).strip().lower())
                                )
                                alias_added += 1
                            except Exception:
                                pass

            await db.commit()
    except Exception as e:
        log.warning(f"[seed] Enforcement failed for g{guild.id}: {e}")

    # Mark seed version noted (informational)
    if already != "done":
        try:
            await meta_set(key, "done")
        except Exception:
            pass

    if inserted or updated or alias_added:
        log.info(f"[seed] g{guild.id}: inserted={inserted}, updated={updated}, aliases_added={alias_added}")

    # Rebuild panels so any ordering/labels reflect changes
    try:
        await refresh_subscription_messages(guild)
    except Exception as e:
        log.warning(f"[seed] Refresh panels failed for g{guild.id}: {e}")

# -------------------- EVENTS --------------------
@bot.event
async def on_ready():
    # Basic DB init
    try:
        await init_db()
    except Exception as e:
        log.warning(f"[ready] init_db failed: {e}")

    # Make sure every guild has a defaults row
    for g in bot.guilds:
        try:
            await upsert_guild_defaults(g.id)
        except Exception as e:
            log.warning(f"[ready] upsert_guild_defaults failed for g{g.id}: {e}")

    # Startup bookkeeping and offline catch-up
    try:
        await meta_set("last_startup_ts", str(now_ts()))
    except Exception as e:
        log.warning(f"[ready] meta_set last_startup_ts failed: {e}")

    try:
        await boot_offline_processing()
    except Exception as e:
        log.warning(f"[ready] boot_offline_processing failed: {e}")

    # Seed & panels (with strict enforcement)
    for g in bot.guilds:
        try:
            await ensure_seed_for_guild(g)
        except Exception as e:
            log.warning(f"[ready] ensure_seed_for_guild failed for g{g.id}: {e}")

    # Start loops (defined in Part 3)
    try:
        if 'timers_tick' in globals():
            if not timers_tick.is_running():  # type: ignore[name-defined]
                timers_tick.start()  # type: ignore[name-defined]
    except Exception as e:
        log.warning(f"[ready] timers_tick start failed: {e}")

    try:
        if 'uptime_heartbeat' in globals():
            if not uptime_heartbeat.is_running():  # type: ignore[name-defined]
                uptime_heartbeat.start()  # type: ignore[name-defined]
    except Exception as e:
        log.warning(f"[ready] uptime_heartbeat start failed: {e}")

    # Rebuild panels after loops started
    for g in bot.guilds:
        try:
            await refresh_subscription_messages(g)
        except Exception as e:
            log.warning(f"[ready] refresh_subscription_messages failed for g{g.id}: {e}")

    # Sync slash
    try:
        await bot.tree.sync()
    except Exception as e:
        log.warning(f"[ready] App command sync failed: {e}")

    try:
        log.info(f"Logged in as {bot.user} (ID: {bot.user.id})")
    except Exception:
        pass

@bot.event
async def on_guild_join(guild: discord.Guild):
    try:
        await init_db()
    except Exception:
        pass
    try:
        await upsert_guild_defaults(guild.id)
    except Exception:
        pass
    try:
        await ensure_seed_for_guild(guild)
    except Exception:
        pass
    try:
        await refresh_subscription_messages(guild)
    except Exception:
        pass
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
                    await send_text_safe(ch, f"{EMJ_HOURGLASS} **{name}** — **Spawn Time**: `{fmt_delta_for_list(left)}` (almost up).")
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
        # mute noisy spam that was already due before boot to avoid duplicate messages
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
                await send_text_safe(ch, f"{EMJ_CLOCK} **{name}** — **Spawn Window has opened!**")
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
                await ch.send("âœ… Bot is online — timers active.")
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
                row = await db.fetchone()
            if not row:
                return
            role = guild.get_role(int(row[0]))
            if role:
                await member.remove_roles(role, reason="Reaction role opt-out")
        except Exception as e:
            log.warning(f"Remove reaction-role failed: {e}")

# -------- /timers UI helpers (MUST exist before /timers runs) --------
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
        for sk, nm, tts, win in items:
            delta = tts - now; t = fmt_delta_for_list(delta)
            (nada_list if t == "-Nada" else normal).append((sk, nm, t, tts, win))
        blocks: List[str] = []
        for sk, nm, t, ts, win_m in normal:
            win_status = window_label(now, ts, win_m)
            try:
                _ws = str(win_status)
            except Exception:
                _ws = ""
            _win_seg = (f" • Window: `{_ws}`" if _ws and "pending" not in _ws.lower() else "")
            line1 = f"ã€” **{nm}** • Spawn: `{t}`{_win_seg} ã€•"
            eta_line = f"\n> *ETA {datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%H:%M UTC')}*" if show_eta and (ts - now) > 0 else ""
            blocks.append(line1 + (eta_line if eta_line else ""))
        if nada_list:
            blocks.append("*Lost (-Nada):*")
            for sk, nm, t, ts, win_m in nada_list:
                blocks.append(f"• **{nm}** — `{t}`")
        description = "\n\n".join(blocks) if blocks else "No timers."
        em = discord.Embed(
            title=sanitize_ui(f"{category_emoji(cat)} {cat}"),
            description=sanitize_ui(description),
            color=await get_category_color(gid, cat)
        )
        embeds.append(em)
    return embeds[:10]
# -------------------- Part 4/4 — commands, slash, errors, shutdown, run --------------------

# -------- HELP (tidy, no auth-config details) --------
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
        f"• `/roles_panel channel:<#> title:<...> pairs:\"ðŸ˜€ @Role, ðŸ”” @Role\"`",
    ]
    text = "\n".join(lines)
    if len(text) > 1990:
        text = text[:1985] + "â€¦"
    if can_send(ctx.channel):
        await ctx.send(text)

# -------- STATUS / HEALTH --------
@bot.command(name="status")
async def status_cmd(ctx):
    gid = ctx.guild.id
    p = await get_guild_prefix(bot, ctx.message)
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute(
            "SELECT COALESCE(prefix, ?), default_channel, sub_channel_id, sub_ping_channel_id, "
            "COALESCE(uptime_minutes, ?), heartbeat_channel_id, COALESCE(show_eta,0) "
            "FROM guild_config WHERE guild_id=?",
            (DEFAULT_PREFIX, DEFAULT_UPTIME_MINUTES, gid)
        )
        r = await c.fetchone()
        prefix, ann_id, sub_id, sub_ping_id, hb_min, hb_ch, show_eta = (
            r if r else (DEFAULT_PREFIX, None, None, None, DEFAULT_UPTIME_MINUTES, None, 0)
        )
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
        f"Category overrides: " + (", ".join(f"{k}â†’{ch(v)}" for k, v in cat_map.items()) if cat_map else "none"),
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
    required = {
        "bosses", "guild_config", "meta", "category_colors", "subscription_emojis", "subscription_members",
        "boss_aliases", "category_channels", "user_timer_prefs", "subscription_panels", "rr_panels", "rr_map", "blacklist"
    }
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

# -------- SHOW ETA FLAG --------
async def get_show_eta(guild_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT COALESCE(show_eta,0) FROM guild_config WHERE guild_id=?", (guild_id,))
        r = await c.fetchone()
        return bool(r and int(r[0]) == 1)

# -------- TIMERS (text) --------
@bot.command(name="timers")
async def timers_cmd(ctx):
    gid = ctx.guild.id
    show_eta = await get_show_eta(gid)
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute(
            "SELECT name,next_spawn_ts,category,sort_key,window_minutes FROM bosses WHERE guild_id=?",
            (gid,)
        )
        rows = await c.fetchall()
    if not rows:
        return await ctx.send("No timers. Add with `boss add \"Name\" <spawn_m> <window_m> [#chan] [pre_m] [cat]`.")
    now = now_ts()
    grouped: Dict[str, List[tuple]] = {k: [] for k in CATEGORY_ORDER}
    for name, ts, cat, sk, win in rows:
        grouped.setdefault(norm_cat(cat), []).append((sk or "", name, int(ts), int(win)))
    for cat in CATEGORY_ORDER:
        items = grouped.get(cat, [])
        if not items:
            continue
        normal: List[tuple] = []
        nada_list: List[tuple] = []
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
            line1 = f"ã€” **{nm}** • Spawn: `{t}` • Window: `{win_status}` ã€•"
            eta_line = f"\n> *ETA {datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%H:%M UTC')}*" if show_eta and (ts - now) > 0 else ""
            blocks.append(line1 + (eta_line if eta_line else ""))
        if nada_list:
            blocks.append("*Lost (-Nada):*")
            for sk, nm, t, ts, win_m in nada_list:
                blocks.append(f"• **{nm}** — `{t}`")
        description = "\n\n".join(blocks) if blocks else "No timers."
        em = discord.Embed(
            title=sanitize_ui(f"{category_emoji(cat)} {cat}"),
            description=sanitize_ui(description),
            color=await get_category_color(gid, cat)
        )
        await ctx.send(embed=em)

# -------- /timers (per-user UI) --------
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
        c = await db.execute(
            "SELECT name,category,spawn_minutes,window_minutes,pre_announce_min,sort_key FROM bosses WHERE guild_id=?",
            (gid,)
        )
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
                text_fallback = text_fallback[:1985] + "â€¦"
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

    def _smart_parse_add(args: List[str], ctx: commands.Context) -> Tuple[str, int, int, Optional[int], int, str]:
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
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE bosses SET next_spawn_ts=? WHERE guild_id=?", (now_ts() - 3601, ctx.guild.id))
        await db.commit()
    await ctx.send(":white_check_mark: All timers set to **-Nada**.")
    await refresh_subscription_messages(ctx.guild)

@boss_group.command(name="nada")
@commands.has_permissions(manage_guild=True)
async def boss_nada(ctx, *, name: str):
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
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE bosses SET next_spawn_ts=? WHERE guild_id=?", (now_ts() - 3601, ctx.guild.id))
        await db.commit()
    await ctx.send(":pause_button: **All bosses** set to **-Nada**.")
    await refresh_subscription_messages(ctx.guild)

@boss_group.command(name="info")
async def boss_info(ctx, *, name: str):
    res, err = await resolve_boss(ctx, name)
    if err:
        return await ctx.send(f":no_entry: {err}")
    bid, nm, _ = res
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute(
            "SELECT name,spawn_minutes,window_minutes,next_spawn_ts,channel_id,pre_announce_min,trusted_role_id,category,sort_key "
            "FROM bosses WHERE id=? AND guild_id=?",
            (bid, ctx.guild.id)
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
    res, err = await resolve_boss(ctx, name)
    if err:
        return await ctx.send(f":no_entry: {err}")
    bid, nm, _ = res
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE bosses SET next_spawn_ts=next_spawn_ts+(?*60) WHERE id=? AND guild_id=?",
            (int(minutes), bid, ctx.guild.id)
        )
        await db.commit()
        c = await db.execute("SELECT next_spawn_ts FROM bosses WHERE id=? AND guild_id=?", (bid, ctx.guild.id))
        ts = (await c.fetchone())[0]
    await ctx.send(f":arrow_up: Increased **{nm}** by {minutes}m. Spawn Time: `{fmt_delta_for_list(int(ts) - now_ts())}`.")
    await refresh_subscription_messages(ctx.guild)

@boss_group.command(name="reduce")
async def boss_reduce(ctx, name: str, minutes: int):
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
    await ctx.send(f":label: **{nm}** â†’ **{norm_cat(category)}**.")
    await refresh_subscription_messages(ctx.guild)

@boss_group.command(name="setsort")
async def boss_setsort(ctx, name: str, sort_key: str):
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
    if name.lower() in {"all"}:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE bosses SET channel_id=? WHERE guild_id=?", (channel.id, ctx.guild.id))
            await db.commit()
        return await ctx.send(f":satellite: All boss reminders â†’ {channel.mention}.")
    elif name.lower() in {"category", "cat"}:
        return await ctx.send('Use `!boss setchannelcat "<Category>" #chan`.')
    res, err = await resolve_boss(ctx, name)
    if err:
        return await ctx.send(f":no_entry: {err}")
    bid, nm, _ = res
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE bosses SET channel_id=? WHERE id=? AND guild_id=?", (channel.id, bid, ctx.guild.id))
        await db.commit()
    await ctx.send(f":satellite: **{nm}** reminders â†’ {channel.mention}.")

@boss_group.command(name="setchannelall")
@commands.has_permissions(manage_guild=True)
async def boss_setchannelall(ctx, channel: discord.TextChannel):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE bosses SET channel_id=? WHERE guild_id=?", (channel.id, ctx.guild.id))
        await db.commit()
    await ctx.send(f":satellite: All boss reminders â†’ {channel.mention}.")

@boss_group.command(name="setchannelcat")
@commands.has_permissions(manage_guild=True)
async def boss_setchannelcat(ctx, *, args: str):
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
    await ctx.send(f":satellite: **{catn}** boss reminders â†’ <#{ch_id}>.")

@boss_group.command(name="setrole")
@commands.has_permissions(manage_guild=True)
async def boss_setrole(ctx, *args):
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
                    await db.execute(
                        "INSERT INTO boss_aliases (guild_id,boss_id,alias) VALUES (?,?,?)",
                        (ctx.guild.id, bid, alias.lower())
                    )
                    await db.commit()
                    await ctx.send(f":white_check_mark: Added alias **{alias}** â†’ **{nm}**.")
                except Exception:
                    await ctx.send(f":warning: Could not add alias (maybe already used?)")
            else:
                await db.execute(
                    "DELETE FROM boss_aliases WHERE guild_id=? AND boss_id=? AND alias=?",
                    (ctx.guild.id, bid, alias.lower())
                )
                await db.commit()
                await ctx.send(f":white_check_mark: Removed alias **{alias}** from **{nm}**.")
        return

    name = args.strip().strip('"')
    if not name:
        return await ctx.send('Format: `!boss aliases "Boss Name"`')
    res, err = await resolve_boss(ctx, name)
    if err:
        return await ctx.send(f":no_entry: {err}")
    bid, nm, _ = res
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute(
            "SELECT alias FROM boss_aliases WHERE guild_id=? AND boss_id=? ORDER BY alias",
            (ctx.guild.id, bid)
        )
        rows = [r[0] for r in await c.fetchall()]
    await ctx.send(f"**Aliases for {nm}:** " + (", ".join(rows) if rows else "*none*"))

@boss_group.command(name="find")
async def boss_find(ctx, *, ident: str):
    res, err = await resolve_boss(ctx, ident)
    if err:
        return await ctx.send(f":no_entry: {err}")
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
    if not rows:
        return await ctx.send("No users blacklisted.")
    mentions = " ".join(f"<@{r[0]}>" for r in rows)
    await ctx.send(f"Blacklisted: {mentions}")

# -------- SERVER SETTINGS COMMANDS --------
@bot.command(name="setprefix")
@commands.has_permissions(manage_guild=True)
async def setprefix_cmd(ctx, new_prefix: str):
    if not new_prefix or len(new_prefix) > 5:
        return await ctx.send("Pick a prefix 1â€“5 characters.")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO guild_config (guild_id,prefix) VALUES (?,?) "
            "ON CONFLICT(guild_id) DO UPDATE SET prefix=excluded.prefix",
            (ctx.guild.id, new_prefix)
        )
        await db.commit()
    await ctx.send(f":white_check_mark: Prefix set to `{new_prefix}`.")

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
        return await ctx.send(f":white_check_mark: Global announce channel set to <#{channel_id}>.")
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
                cat = " ".join(args[1:-1]).strip()
                ch_id = _resolve_channel_id_from_arg(ctx, args[-1])
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
            return await ctx.send(f":white_check_mark: **{catn}** reminders â†’ <#{ch_id}>.")
        else:
            if len(args) < 2:
                return await ctx.send('Format: `!setannounce categoryclear "<Category>"`')
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
    if val not in {"on", "off", "true", "false", "1", "0", "yes", "no"}:
        return await ctx.send("Use `!seteta on` or `!seteta off`.")
    on = val in {"on", "true", "1", "yes"}
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO guild_config (guild_id,show_eta) VALUES (?,?) "
            "ON CONFLICT(guild_id) DO UPDATE SET show_eta=excluded.show_eta",
            (ctx.guild.id, 1 if on else 0)
        )
        await db.commit()
    await ctx.send(f":white_check_mark: UTC ETA display {'enabled' if on else 'disabled'}.")

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
                   else f":white_check_mark: Uptime heartbeat set to every {minutes} minutes.")

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
        )
        await db.commit()
    await ctx.send(f":white_check_mark: Subscription **panels** channel set to {channel.mention}. Rebuilding panelsâ€¦")
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
        )
        await db.commit()
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
        if tl in {"off", "none", "disable", "disabled", "0"}:
            return 0
        if tl.endswith("m"):
            tl = tl[:-1]
        if not tl.lstrip("-").isdigit():
            return None
        val = int(tl)
        if val < 0:
            val = 0
        if val > 10080:
            val = 10080
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
        cat = None
        minutes_tok = None
        if after.startswith('"') and after.count('"') >= 2:
            cat = after.split('"', 1)[1].split('"', 1)[0].strip()
            tail = after.split('"', 2)[-1].strip()
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
    name = None
    minutes_tok = None
    if text.startswith('"') and text.count('"') >= 2:
        name = text.split('"', 1)[1].split('"', 1)[0].strip()
        tail = text.split('"', 2)[-1].strip()
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
            return await interaction.followup.send("â±ï¸ Timed out after 20s.", ephemeral=True)
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

# -------- ERRORS --------
@bot.event
async def on_command_error(ctx, error):
    from discord.ext import commands as ext
    if isinstance(error, ext.CommandNotFound):
        return
    try:
        await ctx.send(f":warning: {error}")
    except Exception:
        pass

# -------- SHUTDOWN --------
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

# -------- RUN --------
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

# -------------------- Lixing & Market (Slash Add-on) — Simplified --------------------
# Design summary:
# - Two sections: 'market' and 'lix'
# - Listings last 24h (TTL). Digest pings every 6h in the configured channel if any listings are active.
# - Market listing captures: item, trades_ok, price_text (optional), taking_offers (bool), notes (optional).
#   • Button "Make Offer" (if taking_offers): opens a modal; offers are saved and echoed in a thread + top 3 shown on main embed.
#   • Button "Close" for author/admin.
# - Lix listing captures: name, class, level_text, lixes_text (number or 'N/A'), notes (optional).
#   • Button "Close" for author/admin.
# - Admin/author commands: set_channel, set_role, post, browse, close, clear.
# - Keeps anti-spam + auth gate via global bot checks.

LM_SEC_LIX = "lix"
LM_SEC_MARKET = "market"
LM_VALID_SECTIONS = {LM_SEC_LIX, LM_SEC_MARKET}

LM_TTL_SECONDS = 24 * 60 * 60         # 24h lifetime
LM_DIGEST_CADENCE_HOURS = 6           # post digest every 6 hours if active listings exist
LM_POST_RATE_SECONDS = 30             # basic anti-spam per author for creating new listings
LM_BROWSE_LIMIT = 20                  # max lines in browse output
LM_CLEAN_INTERVAL = 300               # sweep every 5 minutes

# ---------- DB bootstrap / migrations ----------
async def lm_init_tables():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""CREATE TABLE IF NOT EXISTS section_channels (
            guild_id INTEGER NOT NULL,
            section  TEXT NOT NULL,
            post_channel_id INTEGER,
            ping_role_id INTEGER,
            PRIMARY KEY (guild_id, section)
        )""")
        # listings: shared table for both sections, with superset of fields (NULL where not applicable)
        await db.execute("""CREATE TABLE IF NOT EXISTS listings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id INTEGER NOT NULL,
            section  TEXT NOT NULL,
            author_id INTEGER NOT NULL,
            # generic lifecycle
            created_ts INTEGER NOT NULL,
            expires_ts INTEGER NOT NULL,
            channel_id INTEGER,
            message_id INTEGER,
            thread_id INTEGER,
            # MARKET fields
            item_name TEXT,
            trades_ok INTEGER,
            price_text TEXT,
            taking_offers INTEGER,
            m_notes TEXT,
            # LIX fields
            player_name TEXT,
            player_class TEXT,
            level_text TEXT,
            lixes_text TEXT,
            l_notes TEXT
        )""")
        # Add columns if older schema exists
        async def addcol(table, coldef):
            try:
                await db.execute(f"ALTER TABLE {table} ADD COLUMN {coldef}")
            except Exception:
                pass
        # Ensure all listing columns exist (safe if already present)
        await addcol("listings", "thread_id INTEGER")
        await addcol("listings", "item_name TEXT")
        await addcol("listings", "trades_ok INTEGER")
        await addcol("listings", "price_text TEXT")
        await addcol("listings", "taking_offers INTEGER")
        await addcol("listings", "m_notes TEXT")
        await addcol("listings", "player_name TEXT")
        await addcol("listings", "player_class TEXT")
        await addcol("listings", "level_text TEXT")
        await addcol("listings", "lixes_text TEXT")
        await addcol("listings", "l_notes TEXT")

        # Offers for Market
        await db.execute("""CREATE TABLE IF NOT EXISTS offers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            listing_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            amount_text TEXT NOT NULL,
            note TEXT,
            created_ts INTEGER NOT NULL
        )""")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_listings_exp ON listings(expires_ts)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_listings_gs ON listings(guild_id, section)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_offers_list ON offers(listing_id, created_ts)")
        await db.commit()

# ---------- Utilities ----------
def lm_norm_section(s: str) -> str:
    s = (s or "").strip().lower()
    if s.startswith("lix"): return LM_SEC_LIX
    if s.startswith("mark"): return LM_SEC_MARKET
    return s

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
        )
        await db.commit()

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
        )
        await db.commit()

async def lm_require_manage(inter: discord.Interaction) -> bool:
    if not inter.user.guild_permissions.manage_messages and not inter.user.guild_permissions.administrator:
        await inter.response.send_message("You need **Manage Messages** permission.", ephemeral=True)
        return False
    return True

def _author_or_admin(inter: discord.Interaction, author_id: int) -> bool:
    return inter.user.id == author_id or inter.user.guild_permissions.manage_messages or inter.user.guild_permissions.administrator

# ---------- Embed builders ----------
def _market_embed(item: str, trades_ok: bool, price_text: Optional[str], taking_offers: bool, notes: Optional[str],
                  author: discord.Member, expires_ts: int, recent_offers: Optional[List[Tuple[str, str]]] = None) -> discord.Embed:
    em = discord.Embed(
        title=f"ðŸ›’ Market — {item}",
        color=0xf1c40f
    )
    em.add_field(name="Trades Accepted", value=("Yes" if trades_ok else "No"), inline=True)
    if price_text:
        em.add_field(name="Price", value=price_text, inline=True)
    if taking_offers:
        em.add_field(name="Taking Offers", value="Yes", inline=True)
    if notes:
        em.add_field(name="Notes", value=notes[:1024], inline=False)
    em.add_field(name="Seller", value=author.mention, inline=True)
    em.add_field(name="Expires", value=ts_to_utc(expires_ts), inline=True)
    if recent_offers:
        # recent_offers: List[(user_mention, amount_text)]
        lines = [f"{who}: **{amt}**" + (f" — {note}" if note else "") for who, amt, note in recent_offers]  # type: ignore
        em.add_field(name="Recent Offers", value="\n".join(lines)[:1024], inline=False)
    return em

def _lix_embed(player_name: str, player_class: str, level_text: str, lixes_text: str,
               notes: Optional[str], author: discord.Member, expires_ts: int) -> discord.Embed:
    em = discord.Embed(
        title="ðŸ§­ Lixing (LFG)",
        color=0x4aa3ff
    )
    em.add_field(name="Name", value=player_name, inline=True)
    em.add_field(name="Class", value=player_class, inline=True)
    em.add_field(name="Level", value=level_text, inline=True)
    em.add_field(name="Desired Lixes", value=lixes_text, inline=True)
    if notes:
        em.add_field(name="Notes", value=notes[:1024], inline=False)
    em.add_field(name="Posted by", value=author.mention, inline=True)
    em.add_field(name="Expires", value=ts_to_utc(expires_ts), inline=True)
    return em

# ---------- Offers helpers ----------
async def _fetch_recent_offers(listing_id: int, limit: int = 3) -> List[Tuple[str, str, Optional[str]]]:
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT user_id, amount_text, COALESCE(note,'') FROM offers WHERE listing_id=? ORDER BY created_ts DESC LIMIT ?",
                             (listing_id, int(limit)))
        rows = await c.fetchall()
    return [(f"<@{uid}>", amt, (note or None)) for uid, amt, note in rows]

async def _update_market_message_embed(guild: discord.Guild, listing_row: tuple):
    # listing_row fields we rely on: channel_id, message_id, item_name, trades_ok, price_text, taking_offers, m_notes, author_id, expires_ts
    (_id, gid, section, author_id, created_ts, expires_ts, channel_id, message_id, thread_id,
     item_name, trades_ok, price_text, taking_offers, m_notes,
     player_name, player_class, level_text, lixes_text, l_notes) = listing_row  # noqa
    if not channel_id or not message_id:
        return
    ch = guild.get_channel(int(channel_id))
    if not ch: return
    try:
        msg = await ch.fetch_message(int(message_id))
    except Exception:
        return
    author = guild.get_member(int(author_id)) or (await guild.fetch_member(int(author_id)))
    recent = await _fetch_recent_offers(int(_id), limit=3)
    em = _market_embed(
        item=item_name or "Item",
        trades_ok=bool(trades_ok),
        price_text=price_text,
        taking_offers=bool(taking_offers),
        notes=m_notes,
        author=author,
        expires_ts=int(expires_ts),
        recent_offers=recent
    )
    try:
        await msg.edit(embed=em)
    except Exception:
        pass

# ---------- Interactive UI ----------
class OfferModal(discord.ui.Modal, title="Submit Offer"):
    def __init__(self, listing_id: int, thread_id: Optional[int]):
        super().__init__(timeout=180)
        self.listing_id = listing_id
        self.thread_id = thread_id
        self.amount = discord.ui.TextInput(label="Offer Amount", placeholder="e.g., 1.5m / 150k / $20", required=True, max_length=100)
        self.note = discord.ui.TextInput(label="Notes (optional)", style=discord.TextStyle.paragraph, required=False, max_length=500)
        self.add_item(self.amount); self.add_item(self.note)

    async def on_submit(self, interaction: discord.Interaction):
        now = now_ts()
        amt = str(self.amount.value).strip()
        note = str(self.note.value).strip() if self.note.value else None
        # Save
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("INSERT INTO offers (listing_id,user_id,amount_text,note,created_ts) VALUES (?,?,?,?,?)",
                             (int(self.listing_id), interaction.user.id, amt, note, now))
            await db.commit()
            c = await db.execute("SELECT * FROM listings WHERE id=?", (int(self.listing_id),))
            listing_row = await c.fetchone()
        # Echo in thread
        if self.thread_id:
            thread = interaction.guild.get_thread(int(self.thread_id))
            if thread and can_send(thread):
                try:
                    await thread.send(f"{interaction.user.mention} offered **{amt}**" + (f" — {note}" if note else ""))
                except Exception:
                    pass
        # Update main embed (top 3 offers)
        if listing_row:
            await _update_market_message_embed(interaction.guild, listing_row)
        await ireply(interaction, "âœ… Offer submitted.", ephemeral=True)

class ListingView(discord.ui.View):
    def __init__(self, *, listing_id: int, section: str, author_id: int, taking_offers: bool, thread_id: Optional[int]):
        super().__init__(timeout=LM_TTL_SECONDS)
        self.listing_id = listing_id
        self.section = lm_norm_section(section)
        self.author_id = author_id
        self.thread_id = thread_id
        if self.section == LM_SEC_MARKET and taking_offers:
            self.add_item(self.MakeOfferButton(view=self))
        self.add_item(self.CloseButton(view=self))

    class MakeOfferButton(discord.ui.Button):
        def __init__(self, view: 'ListingView'):
            super().__init__(label="Make Offer", style=discord.ButtonStyle.primary)
            self._parent = view
        async def callback(self, interaction: discord.Interaction):
            # any user can offer
            modal = OfferModal(self._parent.listing_id, self._parent.thread_id)
            await interaction.response.send_modal(modal)

    class CloseButton(discord.ui.Button):
        def __init__(self, view: 'ListingView'):
            super().__init__(label="Close", style=discord.ButtonStyle.danger)
            self._parent = view
        async def callback(self, interaction: discord.Interaction):
            if not _author_or_admin(interaction, self._parent.author_id):
                return await ireply(interaction, "You can't close this (not the author).", ephemeral=True)
            # delete listing + message
            gid = interaction.guild.id
            async with aiosqlite.connect(DB_PATH) as db:
                c = await db.execute("SELECT channel_id,message_id,thread_id FROM listings WHERE id=? AND guild_id=?",
                                     (int(self._parent.listing_id), gid))
                row = await c.fetchone()
                await db.execute("DELETE FROM listings WHERE id=? AND guild_id=?", (int(self._parent.listing_id), gid))
                await db.commit()
            if row:
                ch = interaction.guild.get_channel(int(row[0])) if row[0] else None
                try:
                    if ch:
                        msg = await ch.fetch_message(int(row[1]))
                        await msg.delete()
                except Exception:
                    pass
                # optionally delete thread
                try:
                    if row[2]:
                        th = interaction.guild.get_thread(int(row[2]))
                        if th: await th.delete(reason="Listing closed")
                except Exception:
                    pass
            await ireply(interaction, "âœ… Listing closed.", ephemeral=True)

# ---------- Commands ----------
lix_group = app_commands.Group(name="lix", description="Lixing (LFG) listings")
market_group = app_commands.Group(name="market", description="Market listings")

@market_group.command(name="set_channel", description="Set the Market post channel")
@app_commands.describe(channel="Channel where Market listings will be posted")
async def market_set_channel(inter: discord.Interaction, channel: discord.TextChannel):
    if not await lm_require_manage(inter): return
    await lm_set_section_channel(inter.guild.id, LM_SEC_MARKET, channel.id)
    await ireply(inter, f"âœ… Market posts will go to {channel.mention}.", ephemeral=True)

@market_group.command(name="set_role", description="Set/clear a role to mention in Market digests")
@app_commands.describe(role="Role to mention (omit to clear)")
async def market_set_role(inter: discord.Interaction, role: Optional[discord.Role] = None):
    if not await lm_require_manage(inter): return
    await lm_set_section_role(inter.guild.id, LM_SEC_MARKET, role.id if role else None)
    await ireply(inter, ("âœ… Role cleared." if role is None else f"âœ… Will mention {role.mention}."), ephemeral=True)

@market_group.command(name="post", description="Post a Market listing (24h).")
@app_commands.describe(
    item="Item name",
    trades="Are trades accepted? (true/false)",
    price="Fixed price (optional)",
    offers="Taking offers? (true/false)",
    notes="Notes (optional)"
)
async def market_post(inter: discord.Interaction, item: str, trades: bool, offers: bool, price: Optional[str] = None, notes: Optional[str] = None):
    gid = inter.guild.id; now = now_ts()
    # anti-spam: simple throttle on create
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT MAX(created_ts) FROM listings WHERE guild_id=? AND section=? AND author_id=?",
                             (gid, LM_SEC_MARKET, inter.user.id))
        last_created = (await c.fetchone())[0]
    if last_created and now - int(last_created) < LM_POST_RATE_SECONDS:
        return await ireply(inter, "You're posting a little fast — try again in a moment.", ephemeral=True)

    ch_id = await lm_get_section_channel(gid, LM_SEC_MARKET)
    ch = inter.guild.get_channel(ch_id) if ch_id else inter.channel
    if not ch or not can_send(ch):
        return await ireply(inter, "I can't post in the configured channel. Set it with `/market set_channel`.", ephemeral=True)

    expires = now + LM_TTL_SECONDS
    embed = _market_embed(item=item, trades_ok=trades, price_text=(price or None), taking_offers=offers, notes=notes,
                          author=inter.user, expires_ts=expires, recent_offers=None)
    await inter.response.defer(ephemeral=True)
    try:
        msg = await ch.send(embed=embed)
    except Exception as e:
        return await inter.followup.send(f"Couldn't post in {ch.mention}: {e}", ephemeral=True)

    thread_id = None
    # create a thread to collect offers/log
    try:
        th = await msg.create_thread(name=f"{item} — offers")
        thread_id = th.id
    except Exception:
        thread_id = None

    # persist
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""INSERT INTO listings
            (guild_id,section,author_id,created_ts,expires_ts,channel_id,message_id,thread_id,
             item_name,trades_ok,price_text,taking_offers,m_notes)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (gid, LM_SEC_MARKET, inter.user.id, now, expires, msg.channel.id, msg.id, thread_id,
             item, (1 if trades else 0), (price or None), (1 if offers else 0), (notes or None)))
        await db.commit()
        c = await db.execute("SELECT last_insert_rowid()")
        listing_id = int((await c.fetchone())[0])

    # attach view
    view = ListingView(listing_id=listing_id, section=LM_SEC_MARKET, author_id=inter.user.id, taking_offers=offers, thread_id=thread_id)
    try:
        await msg.edit(view=view)
    except Exception:
        pass

    await inter.followup.send(f"âœ… Market post created in {ch.mention}.", ephemeral=True)

@market_group.command(name="browse", description="Browse active Market listings")
@app_commands.describe(mine="Only show your listings (true/false)")
async def market_browse(inter: discord.Interaction, mine: Optional[bool] = False):
    now = now_ts(); gid = inter.guild.id
    params = [gid, LM_SEC_MARKET, now]
    sql = "SELECT id,item_name,author_id,channel_id,message_id,expires_ts FROM listings WHERE guild_id=? AND section=? AND expires_ts> ?"
    if mine:
        sql += " AND author_id=?"; params.append(inter.user.id)
    sql += " ORDER BY created_ts DESC LIMIT ?"; params.append(LM_BROWSE_LIMIT)
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute(sql, params)
        rows = await c.fetchall()
    if not rows:
        return await ireply(inter, "No active Market listings.", ephemeral=True)
    lines = []
    for idv, item, author_id, ch_id, msg_id, exp in rows:
        lines.append(f"**#{idv}** — **{item}** by <@{author_id}> • expires {fmt_delta_for_list(int(exp)-now)} • <#{ch_id}> [[jump]](https://discord.com/channels/{inter.guild.id}/{int(ch_id)}/{int(msg_id)})")
    await ireply(inter, "\n".join(lines)[:1900], ephemeral=True)

@market_group.command(name="close", description="Close your Market listing")
@app_commands.describe(id="Listing ID")
async def market_close(inter: discord.Interaction, id: int):
    gid = inter.guild.id
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT author_id,channel_id,message_id,thread_id FROM listings WHERE id=? AND guild_id=? AND section=?",
                             (int(id), gid, LM_SEC_MARKET))
        row = await c.fetchone()
    if not row:
        return await ireply(inter, "Listing not found.", ephemeral=True)
    author_id, ch_id, msg_id, th_id = row
    if not _author_or_admin(inter, int(author_id)):
        return await ireply(inter, "You can't close this (not the author).", ephemeral=True)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM listings WHERE id=? AND guild_id=? AND section=?", (int(id), gid, LM_SEC_MARKET))
        await db.commit()
    ch = inter.guild.get_channel(int(ch_id)) if ch_id else None
    if ch:
        try:
            msg = await ch.fetch_message(int(msg_id))
            await msg.delete()
        except Exception:
            pass
    if th_id:
        try:
            th = inter.guild.get_thread(int(th_id))
            if th: await th.delete(reason="Listing closed")
        except Exception:
            pass
    await ireply(inter, f"âœ… Closed Market listing #{id}.", ephemeral=True)

@market_group.command(name="clear", description="Clear ALL active Market listings (Admin/Manage Messages)")
async def market_clear(inter: discord.Interaction):
    if not await lm_require_manage(inter): return
    gid = inter.guild.id
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT id,channel_id,message_id,thread_id FROM listings WHERE guild_id=? AND section=?", (gid, LM_SEC_MARKET))
        rows = await c.fetchall()
        await db.execute("DELETE FROM listings WHERE guild_id=? AND section=?", (gid, LM_SEC_MARKET))
        await db.commit()
    # best-effort delete
    for _id, ch_id, msg_id, th_id in rows:
        ch = inter.guild.get_channel(int(ch_id)) if ch_id else None
        if ch:
            try:
                msg = await ch.fetch_message(int(msg_id))
                await msg.delete()
            except Exception:
                pass
        if th_id:
            try:
                th = inter.guild.get_thread(int(th_id))
                if th: await th.delete(reason="Cleared by admin")
            except Exception:
                pass
    await ireply(inter, "ðŸ§¹ Cleared Market listings.", ephemeral=True)

# ----- Lixing commands -----
@lix_group.command(name="set_channel", description="Set the Lixing (LFG) post channel")
@app_commands.describe(channel="Channel where Lixing posts will go")
async def lix_set_channel(inter: discord.Interaction, channel: discord.TextChannel):
    if not await lm_require_manage(inter): return
    await lm_set_section_channel(inter.guild.id, LM_SEC_LIX, channel.id)
    await ireply(inter, f"âœ… Lixing posts will go to {channel.mention}.", ephemeral=True)

@lix_group.command(name="set_role", description="Set/clear a role to mention in Lixing digests")
@app_commands.describe(role="Role to mention (omit to clear)")
async def lix_set_role(inter: discord.Interaction, role: Optional[discord.Role] = None):
    if not await lm_require_manage(inter): return
    await lm_set_section_role(inter.guild.id, LM_SEC_LIX, role.id if role else None)
    await ireply(inter, ("âœ… Role cleared." if role is None else f"âœ… Will mention {role.mention}."), ephemeral=True)

@lix_group.command(name="post", description="Post a Lixing (LFG) card (24h).")
@app_commands.describe(
    name="Your player name",
    class_="Your class (e.g., Warrior, Rogue, Ranger, Mage, Druid)",
    level="Your level or range (e.g., 152 or 150-155)",
    lixes="Desired lixes (number or 'N/A')",
    notes="Notes (optional)"
)
async def lix_post(inter: discord.Interaction, name: str, class_: str, level: str, lixes: str, notes: Optional[str] = None):
    gid = inter.guild.id; now = now_ts()
    # anti-spam: simple throttle on create
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT MAX(created_ts) FROM listings WHERE guild_id=? AND section=? AND author_id=?",
                             (gid, LM_SEC_LIX, inter.user.id))
        last_created = (await c.fetchone())[0]
    if last_created and now - int(last_created) < LM_POST_RATE_SECONDS:
        return await ireply(inter, "You're posting a little fast — try again in a moment.", ephemeral=True)

    ch_id = await lm_get_section_channel(gid, LM_SEC_LIX)
    ch = inter.guild.get_channel(ch_id) if ch_id else inter.channel
    if not ch or not can_send(ch):
        return await ireply(inter, "I can't post in the configured channel. Set it with `/lix set_channel`.", ephemeral=True)

    # normalize lixes
    lx = lixes.strip()
    if not lx or lx.lower() in {"na", "n/a"}:
        lx = "N/A"

    expires = now + LM_TTL_SECONDS
    embed = _lix_embed(player_name=name, player_class=class_, level_text=level, lixes_text=lx,
                       notes=notes, author=inter.user, expires_ts=expires)
    await inter.response.defer(ephemeral=True)
    try:
        msg = await ch.send(embed=embed)
    except Exception as e:
        return await inter.followup.send(f"Couldn't post in {ch.mention}: {e}", ephemeral=True)

    # persist
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""INSERT INTO listings
            (guild_id,section,author_id,created_ts,expires_ts,channel_id,message_id,
             player_name,player_class,level_text,lixes_text,l_notes)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (gid, LM_SEC_LIX, inter.user.id, now, expires, msg.channel.id, msg.id,
             name, class_, level, lx, (notes or None)))
        await db.commit()
        c = await db.execute("SELECT last_insert_rowid()")
        listing_id = int((await c.fetchone())[0])

    # attach view (close only)
    view = ListingView(listing_id=listing_id, section=LM_SEC_LIX, author_id=inter.user.id, taking_offers=False, thread_id=None)
    try:
        await msg.edit(view=view)
    except Exception:
        pass

    await inter.followup.send(f"âœ… Lixing post created in {ch.mention}.", ephemeral=True)

@lix_group.command(name="browse", description="Browse active Lixing (LFG) posts")
@app_commands.describe(mine="Only show your posts (true/false)")
async def lix_browse(inter: discord.Interaction, mine: Optional[bool] = False):
    now = now_ts(); gid = inter.guild.id
    params = [gid, LM_SEC_LIX, now]
    sql = "SELECT id,player_name,player_class,level_text,lixes_text,author_id,channel_id,message_id,expires_ts FROM listings WHERE guild_id=? AND section=? AND expires_ts> ?"
    if mine:
        sql += " AND author_id=?"; params.append(inter.user.id)
    sql += " ORDER BY created_ts DESC LIMIT ?"; params.append(LM_BROWSE_LIMIT)
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute(sql, params)
        rows = await c.fetchall()
    if not rows:
        return await ireply(inter, "No active Lixing posts.", ephemeral=True)
    lines = []
    for idv, pn, pc, lvl, lx, author_id, ch_id, msg_id, exp in rows:
        lines.append(f"**#{idv}** — **{pn}** ({pc}, {lvl}, lixes: {lx}) by <@{author_id}> • expires {fmt_delta_for_list(int(exp)-now)} • <#{ch_id}> [[jump]](https://discord.com/channels/{inter.guild.id}/{int(ch_id)}/{int(msg_id)})")
    await ireply(inter, "\n".join(lines)[:1900], ephemeral=True)

@lix_group.command(name="close", description="Close your Lixing post")
@app_commands.describe(id="Listing ID")
async def lix_close(inter: discord.Interaction, id: int):
    gid = inter.guild.id
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT author_id,channel_id,message_id FROM listings WHERE id=? AND guild_id=? AND section=?",
                             (int(id), gid, LM_SEC_LIX))
        row = await c.fetchone()
    if not row:
        return await ireply(inter, "Post not found.", ephemeral=True)
    author_id, ch_id, msg_id = row
    if not _author_or_admin(inter, int(author_id)):
        return await ireply(inter, "You can't close this (not the author).", ephemeral=True)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM listings WHERE id=? AND guild_id=? AND section=?", (int(id), gid, LM_SEC_LIX))
        await db.commit()
    ch = inter.guild.get_channel(int(ch_id)) if ch_id else None
    if ch:
        try:
            msg = await ch.fetch_message(int(msg_id))
            await msg.delete()
        except Exception:
            pass
    await ireply(inter, f"âœ… Closed Lixing post #{id}.", ephemeral=True)

@lix_group.command(name="clear", description="Clear ALL active Lixing posts (Admin/Manage Messages)")
async def lix_clear(inter: discord.Interaction):
    if not await lm_require_manage(inter): return
    gid = inter.guild.id
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT id,channel_id,message_id FROM listings WHERE guild_id=? AND section=?", (gid, LM_SEC_LIX))
        rows = await c.fetchall()
        await db.execute("DELETE FROM listings WHERE guild_id=? AND section=?", (gid, LM_SEC_LIX))
        await db.commit()
    for _id, ch_id, msg_id in rows:
        ch = inter.guild.get_channel(int(ch_id)) if ch_id else None
        if ch:
            try:
                msg = await ch.fetch_message(int(msg_id))
                await msg.delete()
            except Exception:
                pass
    await ireply(inter, "ðŸ§¹ Cleared Lixing posts.", ephemeral=True)

# ---------- Cleanup + Digest loops ----------
@tasks.loop(seconds=LM_CLEAN_INTERVAL)
async def lm_cleanup_loop():
    now = now_ts()
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT id,guild_id,channel_id,message_id,thread_id FROM listings WHERE expires_ts<=?", (now,))
        expired = await c.fetchall()
        await db.execute("DELETE FROM listings WHERE expires_ts<=?", (now,))
        await db.commit()
    # best effort delete
    for idv, gid, ch_id, msg_id, th_id in expired:
        g = bot.get_guild(int(gid))
        ch = g.get_channel(int(ch_id)) if g else None
        if ch:
            try:
                msg = await ch.fetch_message(int(msg_id))
                await msg.delete()
            except Exception:
                pass
        if g and th_id:
            try:
                th = g.get_thread(int(th_id))
                if th: await th.delete(reason="Expired")
            except Exception:
                pass

@tasks.loop(minutes=60.0)
async def lm_digest_loop():
    # Runs hourly; posts digest at 00/06/12/18 UTC once per hour per guild/section if active listings exist.
    now = datetime.now(tz=timezone.utc)
    if (now.hour % LM_DIGEST_CADENCE_HOURS) != 0:
        return
    hour_key = now.strftime("%Y-%m-%dT%H")
    for g in bot.guilds:
        # skip unauthorized guilds to respect your global auth gate
        if not await ensure_guild_auth(g):
            continue
        for section in (LM_SEC_MARKET, LM_SEC_LIX):
            # de-dupe via meta key
            meta_key = f"lm_digest:{g.id}:{section}:{hour_key}"
            already = await meta_get(meta_key)
            if already == "done":
                continue
            # active listings?
            async with aiosqlite.connect(DB_PATH) as db:
                c = await db.execute("SELECT id,channel_id,message_id,author_id FROM listings WHERE guild_id=? AND section=? AND expires_ts>?",
                                     (g.id, section, int(now.timestamp())))
                rows = await c.fetchall()
            if not rows:
                await meta_set(meta_key, "done")
                continue
            ch_id = await lm_get_section_channel(g.id, section)
            ch = g.get_channel(ch_id) if ch_id else None
            if not ch or not can_send(ch):
                await meta_set(meta_key, "done")
                continue
            role_id = await lm_get_section_role(g.id, section)
            mention = f"<@&{role_id}> " if role_id else ""
            # compact digest with jump links
            lines = []
            for idv, cid, mid, author_id in rows[:LM_BROWSE_LIMIT]:
                lines.append(f"• **#{idv}** by <@{author_id}> — [[jump]](https://discord.com/channels/{g.id}/{int(cid)}/{int(mid)})")
            title = "ðŸ›’ Market — Active (24h)" if section == LM_SEC_MARKET else "ðŸ§­ Lixing — Active (24h)"
            try:
                await ch.send(content=mention + title + "\n" + "\n".join(lines),
                              allowed_mentions=discord.AllowedMentions(roles=True))
            except Exception:
                pass
            await meta_set(meta_key, "done")

# ---------- Register groups & start loops on ready ----------
@bot.listen("on_ready")
async def _lm_on_ready():
    try:
        await lm_init_tables()
        names = {cmd.name for cmd in bot.tree.get_commands()}
        if "lix" not in names:
            bot.tree.add_command(lix_group)
        if "market" not in names:
            bot.tree.add_command(market_group)
        if not lm_cleanup_loop.is_running():
            lm_cleanup_loop.start()
        if not lm_digest_loop.is_running():
            lm_digest_loop.start()
        try:
            await bot.tree.sync()
        except Exception:
            pass
        log.info("Lixing & Market (simplified) ready.")
    except Exception as e:
        log.warning(f"Lix/Market init failed: {e}")
# ------------------ End Lixing & Market Add-on ------------------

# ==================== ROSTER SAVE FIX + OPTIONAL ALT INTAKE ====================
# Imports needed for this patch
import json as __json
import re as __re

# Safe override for upsert (ensures table, uses json reliably)
async def _upsert_roster(gid: int, uid: int, main_name: str, main_level: int, main_class: str, alts: list, tz_raw: str, tz_norm: str):
    now = now_ts()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS roster_members (
                guild_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                main_name TEXT NOT NULL,
                main_level INTEGER NOT NULL,
                main_class TEXT NOT NULL,
                alts_json TEXT NOT NULL,
                timezone_raw TEXT NOT NULL,
                timezone_norm TEXT,
                submitted_at INTEGER,
                updated_at INTEGER,
                PRIMARY KEY (guild_id, user_id)
            )
        """)
        await db.execute(
            """INSERT INTO roster_members (guild_id,user_id,main_name,main_level,main_class,alts_json,timezone_raw,timezone_norm,submitted_at,updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(guild_id,user_id) DO UPDATE SET
                 main_name=excluded.main_name,
                 main_level=excluded.main_level,
                 main_class=excluded.main_class,
                 alts_json=excluded.alts_json,
                 timezone_raw=excluded.timezone_raw,
                 timezone_norm=excluded.timezone_norm,
                 updated_at=excluded.updated_at
            """,
            (gid, uid, main_name, int(main_level), main_class, __json.dumps(alts or []), tz_raw, tz_norm, now, now)
        )
        await db.commit()

# Dropdown for adding alts
class AltClassSelect(discord.ui.Select):
    def __init__(self):
        opts = [discord.SelectOption(label=c, value=c) for c in ["Ranger","Rogue","Warrior","Mage","Druid"]]
        super().__init__(placeholder="Select alt class (optional)", min_values=1, max_values=1, options=opts)
    async def callback(self, interaction: discord.Interaction):
        self.view.selected_alt_class = self.values[0]
        await interaction.response.edit_message(content=self.view._summary_text(), view=self.view)

# Optional alt modal (name + level only; class comes from dropdown)
class AltModal(discord.ui.Modal, title="Add Alt (optional)"):
    alt_name = discord.ui.TextInput(label="Alt name", required=False, max_length=32, placeholder="e.g., PocketHeals")
    alt_level = discord.ui.TextInput(label="Alt level 1â€“250", required=False, max_length=3, placeholder="e.g., 120")

    def __init__(self, parent_view: "RosterConfirmView"):
        super().__init__(timeout=300)
        self.parent_view = parent_view

    async def on_submit(self, interaction: discord.Interaction):
        name = str(self.alt_name).strip()
        lvl = None
        if str(self.alt_level).strip():
            try:
                lvl = int(__re.sub(r"[^0-9]", "", str(self.alt_level)))
            except Exception:
                lvl = None
        cls = self.parent_view.selected_alt_class or "Ranger"
        # If user provided nothing, just re-render current view
        if not name and not lvl and not cls:
            return await interaction.response.send_message(self.parent_view._summary_text(), ephemeral=True, view=self.parent_view)
        alt = {"name": name[:32] if name else "N/A", "level": (lvl if (isinstance(lvl, int) and 1 <= lvl <= 250) else "N/A"), "class": cls}
        # Append to alts in payload
        mname, mlvl, mcls, alts, tz_raw, tz_norm = self.parent_view.payload
        new_alts = list(alts or []) + [alt]
        new_view = RosterConfirmView(mname, mlvl, mcls, new_alts, tz_raw, tz_norm)
        await interaction.response.send_message(new_view._summary_text(), ephemeral=True, view=new_view)

# Override RosterConfirmView to include optional alt intake
class RosterConfirmView(discord.ui.View):
    def __init__(self, main_name: str, lvl: int, cls: str, alts: list, tz_raw: str, tz_norm: str):
        super().__init__(timeout=900)
        self.payload = (main_name, lvl, cls, alts or [], tz_raw, tz_norm)
        self.selected_alt_class = None
        # Add alt class selector
        self.add_item(AltClassSelect())

    def _summary_text(self) -> str:
        mname, mlvl, mcls, alts, tz_raw, tz_norm = self.payload
        alts_line = ", ".join([f"{a.get('name','?')} • {a.get('level','?')} • {a.get('class','?')}" for a in (alts or [])]) if alts else "N/A"
        extra = f"\nSelect an alt class and press 'Add Alt' to include optional alts." 
        return f"Review your info:\n**Main:** {mname} • {mlvl} • {mcls}\n**Alts:** {alts_line}\n**Timezone:** {tz_raw}" + (f" ({tz_norm})" if tz_norm else "") + extra

    @discord.ui.button(label="Add Alt", style=discord.ButtonStyle.secondary)
    async def add_alt(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.selected_alt_class:
            return await interaction.response.send_message("Pick an alt class from the dropdown first.", ephemeral=True)
        await interaction.response.send_modal(AltModal(self))

    @discord.ui.button(label="Join the server!", style=discord.ButtonStyle.success)
    async def join_server(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild = interaction.guild; user = interaction.user
        if not guild:
            return await interaction.response.send_message("Guild not found.", ephemeral=True)
        gid = guild.id
        try:
            await _upsert_roster(gid, user.id, *self.payload)
        except Exception as e:
            log.warning(f"[roster] upsert failed: {e}")
            return await interaction.response.send_message("Could not save your info.", ephemeral=True)
        rid = await get_auto_member_role_id(gid)
        if rid:
            role = guild.get_role(rid)
            if role:
                try: await user.add_roles(role, reason="Roster intake complete")
                except Exception as e: log.warning(f"[roster] role grant failed: {e}")
        roster_ch_id = await get_roster_channel_id(gid)
        if roster_ch_id:
            ch = guild.get_channel(roster_ch_id)
            if can_send(ch):
                try:
                    e = _build_roster_embed(user, *self.payload)
                    await ch.send(embed=e)
                except Exception as e:
                    log.warning(f"[roster] post failed: {e}")
        await interaction.response.edit_message(content="You're set. Welcome.", view=None)
# ==================== END ROSTER SAVE FIX + OPTIONAL ALT INTAKE ====================

# ==================== CONFIG HELPERS + SCHEMA ====================
async def _cfg_get_int(gid: int, field: str):
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute(f"SELECT {field} FROM guild_config WHERE guild_id=?", (gid,))
        r = await c.fetchone()
        return int(r[0]) if r and r[0] is not None else None

async def _cfg_set_int(gid: int, field: str, val: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("CREATE TABLE IF NOT EXISTS guild_config (guild_id INTEGER PRIMARY KEY)")
        c = await db.execute("PRAGMA table_info(guild_config)")
        cols = {row[1] for row in await c.fetchall()}
        if field not in cols:
            coltype = "TEXT" if field == "prefix" else "INTEGER"
            await db.execute(f"ALTER TABLE guild_config ADD COLUMN {field} {coltype} DEFAULT NULL")
        await db.execute(
            f"INSERT INTO guild_config (guild_id,{field}) VALUES (?,?) "
            f"ON CONFLICT(guild_id) DO UPDATE SET {field}=excluded.{field}",
            (gid, val)
        ); await db.commit()

async def get_welcome_channel_id(gid: int): return await _cfg_get_int(gid, "welcome_channel_id")
async def set_welcome_channel_id(gid: int, cid: int): return await _cfg_set_int(gid, "welcome_channel_id", int(cid))
async def get_roster_channel_id(gid: int): return await _cfg_get_int(gid, "roster_channel_id")
async def set_roster_channel_id(gid: int, cid: int): return await _cfg_set_int(gid, "roster_channel_id", int(cid))
async def get_auto_member_role_id(gid: int): return await _cfg_get_int(gid, "auto_member_role_id")
async def set_auto_member_role_id(gid: int, rid: int): return await _cfg_set_int(gid, "auto_member_role_id", int(rid))
async def get_welcome_message_id(gid: int): return await _cfg_get_int(gid, "welcome_message_id")
async def set_welcome_message_id(gid: int, mid: int): return await _cfg_set_int(gid, "welcome_message_id", int(mid))

@bot.listen("on_ready")
async def __cfg_helpers_migrate_on_ready():
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("CREATE TABLE IF NOT EXISTS guild_config (guild_id INTEGER PRIMARY KEY)")
            needed = ["welcome_channel_id","roster_channel_id","auto_member_role_id","welcome_message_id",
                      "heartbeat_channel_id","uptime_minutes"]
            c = await db.execute("PRAGMA table_info(guild_config)")
            cols = {row[1] for row in await c.fetchall()}
            for col in needed:
                if col not in cols:
                    await db.execute(f"ALTER TABLE guild_config ADD COLUMN {col} INTEGER DEFAULT NULL")
            await db.commit()
    except Exception as e:
        log.warning(f"[migrate] cfg helpers init failed: {e}")
# ==================== END CONFIG HELPERS + SCHEMA ====================

# ==================== WELCOME PROMPT ====================
class WelcomeRootView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Start Roster", style=discord.ButtonStyle.primary, custom_id="start_roster_btn")
    async def start_roster(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = RosterStartView() if "RosterStartView" in globals() else None
        if view:
            await interaction.response.send_message("Let's get your roster set up.", ephemeral=True, view=view)
        else:
            await interaction.response.send_message("Roster intake is unavailable in this build.", ephemeral=True)

async def ensure_welcome_prompt(guild: discord.Guild):
    gid = guild.id
    ch_id = await get_welcome_channel_id(gid)
    if not ch_id:
        return
    ch = guild.get_channel(ch_id)
    if not ch or not can_send(ch):
        return
    view = WelcomeRootView()
    content = "New here? Tap to start."
    msg_id = await get_welcome_message_id(gid)
    if msg_id:
        try:
            msg = await ch.fetch_message(msg_id)
            try:
                await msg.edit(content=content, view=view)
                return
            except Exception:
                pass
        except Exception:
            pass
    try:
        msg = await ch.send(content, view=view)
        await set_welcome_message_id(gid, msg.id)
    except Exception as e:
        log.warning(f"[welcome] ensure prompt failed for g{gid}: {e}")

_ensure_welcome_prompt = ensure_welcome_prompt
# ==================== END WELCOME PROMPT ====================

# ==================== MINIMAL CONFIG COMMANDS (additive) ====================
from discord import app_commands as _ac_cfg

@_ac_cfg.command(name="setup-welcome", description="Set the channel that shows the Start Roster button")
@_ac_cfg.checks.has_permissions(manage_guild=True)
async def setup_welcome(interaction: discord.Interaction, channel: discord.TextChannel):
    await set_welcome_channel_id(interaction.guild.id, channel.id)
    await interaction.response.send_message(f"Welcome channel set to {channel.mention}.", ephemeral=True)
    await ensure_welcome_prompt(interaction.guild)

@_ac_cfg.command(name="setup-roster", description="Set the public roster channel")
@_ac_cfg.checks.has_permissions(manage_guild=True)
async def setup_roster(interaction: discord.Interaction, channel: discord.TextChannel):
    await set_roster_channel_id(interaction.guild.id, channel.id)
    await interaction.response.send_message(f"Roster channel set to {channel.mention}.", ephemeral=True)

@_ac_cfg.command(name="setup-role", description="Set the role granted on roster submit")
@_ac_cfg.checks.has_permissions(manage_roles=True)
async def setup_role(interaction: discord.Interaction, role: discord.Role):
    await set_auto_member_role_id(interaction.guild.id, role.id)
    await interaction.response.send_message(f"Auto role set to {role.mention}.", ephemeral=True)

@_ac_cfg.command(name="welcome-post", description="Post or refresh the Start Roster button message in the welcome channel")
@_ac_cfg.checks.has_permissions(manage_guild=True)
async def welcome_post_cmd(interaction: discord.Interaction):
    await ensure_welcome_prompt(interaction.guild)
    ch_id = await get_welcome_channel_id(interaction.guild.id)
    ch = interaction.guild.get_channel(ch_id) if ch_id else None
    if ch:
        await interaction.response.send_message(f"Welcome prompt ensured in {ch.mention}.", ephemeral=True)
    else:
        await interaction.response.send_message("Welcome channel not set. Run /setup-welcome first.", ephemeral=True)

@_ac_cfg.command(name="start-roster", description="Open the roster intake (ephemeral) in the welcome channel")
async def start_roster_cmd(interaction: discord.Interaction):
    gid = interaction.guild.id if interaction.guild else None
    if not gid:
        return await interaction.response.send_message("Guild not found.", ephemeral=True)
    wcid = await get_welcome_channel_id(gid)
    if not wcid:
        return await interaction.response.send_message("Welcome channel not set. Run /setup-welcome first.", ephemeral=True)
    if not isinstance(interaction.channel, discord.TextChannel) or interaction.channel.id != wcid:
        ch = interaction.guild.get_channel(wcid)
        return await interaction.response.send_message(f"Run this in {ch.mention}.", ephemeral=True)
    view = RosterStartView()
    await interaction.response.send_message("Let's get your roster set up.", ephemeral=True, view=view)

@_ac_cfg.command(name="roster-repost", description="Repost a user's roster card to the roster channel")
@_ac_cfg.checks.has_permissions(manage_guild=True)
async def roster_repost_cmd(interaction: discord.Interaction, member: discord.Member | None = None):
    gid = interaction.guild.id if interaction.guild else None
    if not gid:
        return await interaction.response.send_message("Guild not found.", ephemeral=True)
    user = member or interaction.user
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT main_name, main_level, main_class, alts_json, timezone_raw, timezone_norm FROM roster_members WHERE guild_id=? AND user_id=?", (gid, user.id))
        row = await c.fetchone()
    if not row:
        return await interaction.response.send_message("No roster data found for that user.", ephemeral=True)
    main_name, main_level, main_class, alts_json, tz_raw, tz_norm = row
    try:
        alts = json.loads(alts_json) if isinstance(alts_json, str) else (alts_json or [])
    except Exception:
        alts = []
    rcid = await get_roster_channel_id(gid)
    if not rcid:
        return await interaction.response.send_message("Roster channel not set. Run /setup-roster first.", ephemeral=True)
    ch = interaction.guild.get_channel(rcid)
    if not ch or not can_send(ch):
        return await interaction.response.send_message("I lack permission to post in the roster channel.", ephemeral=True)
    e = _build_roster_embed(user, main_name, int(main_level), main_class, alts, tz_raw, tz_norm)
    await ch.send(embed=e)
    await interaction.response.send_message(f"Reposted in {ch.mention}.", ephemeral=True)

@_ac_cfg.command(name="setup-uptime", description="Set heartbeat channel and interval in minutes (use 0 to disable)")
@_ac_cfg.checks.has_permissions(manage_guild=True)
async def setup_uptime(interaction: discord.Interaction, channel: discord.TextChannel, minutes: int):
    await _cfg_set_int(interaction.guild.id, "heartbeat_channel_id", channel.id)
    await _cfg_set_int(interaction.guild.id, "uptime_minutes", max(0, int(minutes)))
    await interaction.response.send_message(f"Heartbeat channel set to {channel.mention}; interval {minutes} minutes.", ephemeral=True)

# Manual admin sync
@_ac_cfg.command(name="sync-now", description="Force sync slash commands in this guild")
@_ac_cfg.checks.has_permissions(administrator=True)
async def sync_now(interaction: discord.Interaction):
    try:
        await bot.tree.sync(guild=interaction.guild)
        await interaction.response.send_message("Synced.", ephemeral=True)
    except Exception as e:
        await interaction.response.send_message(f"Sync failed: {e}", ephemeral=True)

# Binder
@bot.listen("on_ready")
async def __bind_config_commands_and_sync():
    cmds = [setup_welcome, setup_roster, setup_role, welcome_post_cmd, start_roster_cmd, roster_repost_cmd, setup_uptime, sync_now]
    for g in bot.guilds:
        for cmd in cmds:
            try:
                bot.tree.add_command(cmd, guild=g)
            except Exception:
                pass
        try:
            await bot.tree.sync(guild=g)
            log.info(f"[sync] Config commands synced for guild {g.id}")
        except Exception as e:
            log.warning(f"[sync] {g.id}: {e}")
# ==================== END MINIMAL CONFIG COMMANDS ====================

# ==================== ROSTER INTAKE UI (required) ====================

def _alts_line(alts):
    try:
        return ", ".join(f"{a.get('name','?')} • {a.get('level','?')} • {a.get('class','?')}" for a in (alts or [])) or "N/A"
    except Exception:
        return "N/A"
import json as _json
import re as _re

_ALLOWED_CLASSES = ["Ranger", "Rogue", "Warrior", "Mage", "Druid"]

def _norm_class(s: str) -> str:
    s = (s or "").strip().title()
    for c in _ALLOWED_CLASSES:
        if s == c: return c
    return _ALLOWED_CLASSES[0]

def _parse_timezone(tz: str):
    raw = (tz or "").strip()
    norm = ""
    if "/" in raw and len(raw) <= 64:
        norm = raw
    else:
        m = _re.match(r"^(?:UTC)?\s*([+-]?)(\d{1,2})(?::?(\d{2}))?$", raw)
        if m:
            sign = -1 if m.group(1) == "-" else 1
            hh = int(m.group(2)); mm = int(m.group(3) or 0)
            if 0 <= hh <= 14 and 0 <= mm < 60:
                norm = f"UTC{'-' if sign<0 else '+'}{hh:02d}:{mm:02d}"
    return raw or "N/A", norm

async def _upsert_roster(gid: int, uid: int, main_name: str, main_level: int, main_class: str, alts: list, tz_raw: str, tz_norm: str):
    now = now_ts()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS roster_members (
                guild_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                main_name TEXT NOT NULL,
                main_level INTEGER NOT NULL,
                main_class TEXT NOT NULL,
                alts_json TEXT NOT NULL,
                timezone_raw TEXT NOT NULL,
                timezone_norm TEXT,
                submitted_at INTEGER,
                updated_at INTEGER,
                PRIMARY KEY (guild_id, user_id)
            )
        """)
        await db.execute(
            """INSERT INTO roster_members (guild_id,user_id,main_name,main_level,main_class,alts_json,timezone_raw,timezone_norm,submitted_at,updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(guild_id,user_id) DO UPDATE SET
                 main_name=excluded.main_name,
                 main_level=excluded.main_level,
                 main_class=excluded.main_class,
                 alts_json=excluded.alts_json,
                 timezone_raw=excluded.timezone_raw,
                 timezone_norm=excluded.timezone_norm,
                 updated_at=excluded.updated_at
            """,
            (gid, uid, main_name, int(main_level), main_class, _json.dumps(alts or []), tz_raw, tz_norm, now, now)
        )
        await db.commit()

def _build_roster_embed(member: discord.Member, main_name: str, main_level: int, main_class: str, alts: list, tz_raw: str, tz_norm: str):
    title = f"New Member: {member.display_name}"
    alts_line = ", ".join([f"{a.get('name','?')} • {a.get('level','?')} • {a.get('class','?')}" for a in (alts or [])]) if alts else "N/A"
    desc = f"**Main:** {main_name} • {main_level} • {main_class}\n**Alts:** {alts_line}\n**Timezone:** {tz_raw}" + (f" ({tz_norm})" if tz_norm else "")
    e = discord.Embed(title=sanitize_ui(title), description=sanitize_ui(desc), color=discord.Color.blurple())
    e.set_footer(text="Welcome!")
    return e

class ClassSelect(discord.ui.Select):
    def __init__(self):
        opts = [discord.SelectOption(label=c, value=c) for c in _ALLOWED_CLASSES]
        super().__init__(placeholder="Select your main class", min_values=1, max_values=1, options=opts)
    async def callback(self, interaction: discord.Interaction):
        self.view.selected_class = self.values[0]
        await interaction.response.edit_message(content=f"Selected class: **{self.view.selected_class}**", view=self.view)

class RosterStartView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=600)
        self.selected_class = None
        self.add_item(ClassSelect())

    @discord.ui.button(label="Continue", style=discord.ButtonStyle.primary)
    async def _continue(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.selected_class:
            return await interaction.response.send_message("Pick a class first.", ephemeral=True)
        await interaction.response.send_modal(RosterModal(self.selected_class))

class RosterModal(discord.ui.Modal, title="Start Roster"):
    main_name = discord.ui.TextInput(label="Main name", placeholder="Blunderbuss", required=True, max_length=32)
    main_level = discord.ui.TextInput(label="Main level (1â€“250)", placeholder="215", required=True, max_length=3)
    alts = discord.ui.TextInput(label="Alts (name / level / class; or N/A)", style=discord.TextStyle.paragraph, required=False, placeholder="N/A", max_length=400)
    timezone = discord.ui.TextInput(label="Timezone (IANA or offset)", placeholder="America/Chicago or UTC-05:00", required=False, max_length=64)

    def __init__(self, selected_class: str):
        super().__init__(timeout=600)
        self.selected_class = selected_class

    async def on_submit(self, interaction: discord.Interaction):
        try:
            lvl = int(str(self.main_level))
        except Exception:
            return await interaction.response.send_message("Level must be a number.", ephemeral=True)
        if not (1 <= lvl <= 250):
            return await interaction.response.send_message("Level must be between 1 and 250.", ephemeral=True)
        main_name = str(self.main_name).strip()
        cls = _norm_class(self.selected_class)
        raw_alts = (str(self.alts) or "").strip()
        alts: list = []
        if raw_alts and raw_alts.lower() != "n/a":
            chunks = _re.split(r"[;\n]+", raw_alts)
            for ch in chunks:
                parts = [p.strip() for p in ch.split("/") if p.strip()]
                if len(parts) >= 3:
                    nm, lv, cl = parts[0], parts[1], parts[2]
                    try: lv = int(_re.sub(r"[^0-9]", "", lv))
                    except Exception: pass
                    alts.append({"name": nm[:32], "level": lv, "class": _norm_class(cl)})
        tz_raw, tz_norm = _parse_timezone(str(self.timezone))

        summary = f"**Main:** {main_name} • {lvl} • {cls}\n**Alts:** " + (_alts_line(alts)) + f"\n**Timezone:** {tz_raw}" + (f" ({tz_norm})" if tz_norm else "")
        view = RosterConfirmView(main_name, lvl, cls, alts, tz_raw, tz_norm)
        await interaction.response.send_message(f"Review your info:\n{summary}", ephemeral=True, view=view)

class RosterConfirmView(discord.ui.View):
    def __init__(self, main_name: str, lvl: int, cls: str, alts: list, tz_raw: str, tz_norm: str):
        super().__init__(timeout=900)
        self.payload = (main_name, lvl, cls, alts or [], tz_raw, tz_norm)

    @discord.ui.button(label="Join the server!", style=discord.ButtonStyle.success)
    async def join_server(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild = interaction.guild; user = interaction.user
        if not guild:
            return await interaction.response.send_message("Guild not found.", ephemeral=True)
        gid = guild.id
        try:
            await _upsert_roster(gid, user.id, *self.payload)
        except Exception as e:
            log.warning(f"[roster] upsert failed: {e}")
            return await interaction.response.send_message("Could not save your info.", ephemeral=True)
        rid = await get_auto_member_role_id(gid)
        if rid:
            role = guild.get_role(rid)
            if role:
                try: await user.add_roles(role, reason="Roster intake complete")
                except Exception as e: log.warning(f"[roster] role grant failed: {e}")
        roster_ch_id = await get_roster_channel_id(gid)
        if roster_ch_id:
            ch = guild.get_channel(roster_ch_id)
            if can_send(ch):
                try:
                    e = _build_roster_embed(user, *self.payload)
                    await ch.send(embed=e)
                except Exception as e:
                    log.warning(f"[roster] post failed: {e}")
        await interaction.response.edit_message(content="You're set. Welcome.", view=None)
# ==================== END ROSTER INTAKE UI ====================

# ==================== OVERRIDES: ALT FLOW + PERMISSION GATE ====================
# Alt flow identical to mains: class dropdown then modal with name+level.
# The alt fields are optional; main fields remain required.
import json as __json2
import re as __re2

class AltClassSelect(discord.ui.Select):
    def __init__(self):
        opts = [discord.SelectOption(label=c, value=c) for c in ["Ranger","Rogue","Warrior","Mage","Druid"]]
        super().__init__(placeholder="Select alt class", min_values=1, max_values=1, options=opts)
    async def callback(self, interaction: discord.Interaction):
        self.view.selected_alt_class = self.values[0]
        await interaction.response.edit_message(content=self.view._summary_text(), view=self.view)

class AltModal(discord.ui.Modal, title="Add Alt"):
    alt_name = discord.ui.TextInput(label="Alt name", required=False, max_length=32, placeholder="e.g., PocketHeals")
    alt_level = discord.ui.TextInput(label="Alt level 1â€“250", required=False, max_length=3, placeholder="e.g., 120")

    def __init__(self, parent_view: "RosterConfirmView"):
        super().__init__(timeout=300)
        self.parent_view = parent_view

    async def on_submit(self, interaction: discord.Interaction):
        name = str(self.alt_name).strip()
        lvl = str(self.alt_level).strip()
        lvl_int = None
        if lvl:
            try:
                lvl_int = int(__re2.sub(r"[^0-9]", "", lvl))
            except Exception:
                lvl_int = None
        if lvl_int is not None and not (1 <= lvl_int <= 250):
            return await interaction.response.send_message("Alt level must be between 1 and 250.", ephemeral=True)
        cls = self.parent_view.selected_alt_class or "Ranger"
        alt = {}
        if name: alt["name"] = name[:32]
        if lvl_int is not None: alt["level"] = lvl_int
        alt["class"] = cls
        # If user provided nothing, do not append
        if not name and lvl_int is None:
            return await interaction.response.send_message(self.parent_view._summary_text(), ephemeral=True, view=self.parent_view)
        mname, mlvl, mcls, alts, tz_raw, tz_norm = self.parent_view.payload
        new_alts = list(alts or []) + [alt]
        new_view = RosterConfirmView(mname, mlvl, mcls, new_alts, tz_raw, tz_norm)
        await interaction.response.send_message(new_view._summary_text(), ephemeral=True, view=new_view)

# Override RosterConfirmView to include alt class dropdown and Add Alt button
class RosterConfirmView(discord.ui.View):
    def __init__(self, main_name: str, lvl: int, cls: str, alts: list, tz_raw: str, tz_norm: str):
        super().__init__(timeout=900)
        self.payload = (main_name, lvl, cls, alts or [], tz_raw, tz_norm)
        self.selected_alt_class = None
        self.add_item(AltClassSelect())

    def _summary_text(self) -> str:
        mname, mlvl, mcls, alts, tz_raw, tz_norm = self.payload
        try:
            alts_line = ", ".join(f"{a.get('name','?')} • {a.get('level','?')} • {a.get('class','?')}" for a in (alts or [])) or "N/A"
        except Exception:
            alts_line = "N/A"
        return f"Review your info:\n**Main:** {mname} • {mlvl} • {mcls}\n**Alts:** {alts_line}\n**Timezone:** {tz_raw}" + (f" ({tz_norm})" if tz_norm else "")

    @discord.ui.button(label="Add Alt", style=discord.ButtonStyle.secondary)
    async def add_alt(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.selected_alt_class:
            return await interaction.response.send_message("Pick an alt class from the dropdown first.", ephemeral=True)
        await interaction.response.send_modal(AltModal(self))

    @discord.ui.button(label="Join the server!", style=discord.ButtonStyle.success)
    async def join_server(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild = interaction.guild; user = interaction.user
        if not guild:
            return await interaction.response.send_message("Guild not found.", ephemeral=True)
        gid = guild.id
        try:
            await _upsert_roster(gid, user.id, *self.payload)
        except Exception as e:
            log.warning(f"[roster] upsert failed: {e}")
            return await interaction.response.send_message("Could not save your info.", ephemeral=True)
        rid = await get_auto_member_role_id(gid)
        if rid:
            role = guild.get_role(rid)
            if role:
                try: await user.add_roles(role, reason="Roster intake complete")
                except Exception as e: log.warning(f"[roster] role grant failed: {e}")
        roster_ch_id = await get_roster_channel_id(gid)
        if roster_ch_id:
            ch = guild.get_channel(roster_ch_id)
            if can_send(ch):
                try:
                    e = _build_roster_embed(user, *self.payload)
                    await ch.send(embed=e)
                except Exception as e:
                    log.warning(f"[roster] post failed: {e}")
        await interaction.response.edit_message(content="You're set. Welcome.", view=None)

# Global slash permission gate
async def _get_timers_role_id(gid: int):
    try:
        return await _cfg_get_int(gid, "timers_role_id")
    except Exception:
        return None

async def __global_slash_permission_gate(interaction: discord.Interaction):
    # Admin bypass
    if interaction.user.guild_permissions.administrator:
        return True
    # Timers commands: require role if configured
    qn = interaction.command.qualified_name if interaction.command else ""
    gid = interaction.guild.id if interaction.guild else None
    if qn.startswith("timers") and gid:
        rid = await _get_timers_role_id(gid)
        if rid:
            role = interaction.guild.get_role(int(rid))
            if role and role in getattr(interaction.user, "roles", []):
                return True
            raise app_commands.CheckFailure("Missing the required timers role.")
        # If no role configured, allow only Manage Messages
        if interaction.user.guild_permissions.manage_messages:
            return True
        raise app_commands.CheckFailure("Timers role not configured. Ask an admin to set it with /setup-timersrole.")
    # All other slash commands require Manage Messages
    if interaction.user.guild_permissions.manage_messages:
        return True
    raise app_commands.CheckFailure("You need Manage Messages to use this command.")

# Error handler to show clean messages
@bot.tree.error
async def __on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    try:
        msg = str(error)
        await interaction.response.send_message(msg or "Permission denied.", ephemeral=True)
    except Exception:
        try:
            await interaction.followup.send(msg or "Permission denied.", ephemeral=True)
        except Exception:
            pass

# Config command to set timers role
from discord import app_commands as __ac_perm
@__ac_perm.command(name="setup-timersrole", description="Set the role allowed to use /timers commands")
@__ac_perm.checks.has_permissions(manage_guild=True)
async def setup_timersrole(interaction: discord.Interaction, role: discord.Role):
    await _cfg_set_int(interaction.guild.id, "timers_role_id", role.id)
    await interaction.response.send_message(f"Timers role set to {role.mention}.", ephemeral=True)

# Bind new command per guild and sync
@bot.listen("on_ready")
async def __bind_perm_setup_and_sync():
    cmds = [setup_timersrole]
    for g in bot.guilds:
        for cmd in cmds:
            try:
                bot.tree.add_command(cmd, guild=g)
            except Exception:
                pass
        try:
            await bot.tree.sync(guild=g)
            log.info(f"[sync] Permission setup commands synced for guild {g.id}")
        except Exception as e:
            log.warning(f"[sync] {g.id}: {e}")
# ==================== END OVERRIDES ====================

# ===== Global slash permission gate (strict) =====
async def _get_timers_role_id(gid: int):
    try:
        return await _cfg_get_int(gid, "timers_role_id")
    except Exception:
        return None

async def _global_slash_permission_gate(interaction: discord.Interaction):
    # Admin bypass
    if interaction.user.guild_permissions.administrator:
        return True
    if not interaction.guild:
        raise app_commands.CheckFailure("Guild only.")
    qn = (interaction.command.qualified_name if interaction.command else "") or ""
    gid = interaction.guild.id
    # Timers commands: role only
    if qn in ("my-level-main", "my-level-alt"):
        return True
    if qn.startswith("timers"):
        rid = await _get_timers_role_id(gid)
        if not rid:
            raise app_commands.CheckFailure("Timers role not configured. Ask an admin to run /setup-timersrole.")
        role = interaction.guild.get_role(int(rid))
        if role and role in getattr(interaction.user, "roles", []):
            return True
        raise app_commands.CheckFailure("Missing required timers role.")
    # All other slash commands: require Manage Messages
    if interaction.user.guild_permissions.manage_messages:
        return True
    raise app_commands.CheckFailure("You need Manage Messages to use this command.")
# ================================================

@bot.listen("on_ready")
async def __add_global_checks_and_errors():
    try:
        bot.tree.add_check(_global_slash_permission_gate)
    except Exception as e:
        try:
            log.info(f"[checks] add_check failed or already added: {e}")
        except Exception:
            pass
    try:
        bot.tree.on_error = __on_app_command_error
    except Exception as e:
        try:
            log.info(f"[checks] set on_error failed: {e}")
        except Exception:
            pass

# ==================== POLISH: clearer alt flow + star-decorated embeds ====================
# Text config helpers for star GIF
async def _cfg_get_text(gid: int, field: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("CREATE TABLE IF NOT EXISTS guild_config (guild_id INTEGER PRIMARY KEY)")
        c = await db.execute("PRAGMA table_info(guild_config)")
        cols = {row[1] for row in await c.fetchall()}
        if field not in cols:
            await db.execute(f"ALTER TABLE guild_config ADD COLUMN {field} TEXT DEFAULT NULL")
            await db.commit()
        c2 = await db.execute(f"SELECT {field} FROM guild_config WHERE guild_id=?", (gid,))
        r = await c2.fetchone()
        return (r[0] if r else None)

async def _cfg_set_text(gid: int, field: str, val: str | None):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("CREATE TABLE IF NOT EXISTS guild_config (guild_id INTEGER PRIMARY KEY)")
        c = await db.execute("PRAGMA table_info(guild_config)")
        cols = {row[1] for row in await c.fetchall()}
        if field not in cols:
            await db.execute(f"ALTER TABLE guild_config ADD COLUMN {field} TEXT DEFAULT NULL")
        await db.execute(
            f"INSERT INTO guild_config (guild_id,{field}) VALUES (?,?) "
            f"ON CONFLICT(guild_id) DO UPDATE SET {field}=excluded.{field}",
            (gid, val)
        ); await db.commit()

# Decorate embed with stars: uses configured GIF when available; falls back to unicode sparkles

async def decorate_embed_with_stars(e: discord.Embed, guild_id: int):
    gif = None
    try:
        gif = await _cfg_get_text(guild_id, "roster_star_gif")
    except Exception:
        gif = None
    try:
        if isinstance(gif, str) and _is_valid_http_url(gif):
            try:
                e.set_thumbnail(url=gif)
            except Exception:
                pass
    except Exception:
        pass
    return e

# Setup command to configure the star GIF
from discord import app_commands as _ac_star
@_ac_star.command(name="setup-stargif", description="Set a GIF URL to decorate roster embeds with stars (use 'clear' to unset)")
@_ac_star.checks.has_permissions(manage_guild=True)
async def setup_stargif(interaction: discord.Interaction, url: str):
    val = None if url.lower() == "clear" else url.strip()
    await _cfg_set_text(interaction.guild.id, "roster_star_gif", val)
    msg = "Cleared." if val is None else f"Set to {val}."
    await interaction.response.send_message(f"Star GIF {msg}", ephemeral=True)

# Bind the star setup command
@bot.listen("on_ready")
async def __bind_star_setup_and_sync():
    for g in bot.guilds:
        try:
            bot.tree.add_command(setup_stargif, guild=g)
        except Exception:
            pass
        try:
            await bot.tree.sync(guild=g)
        except Exception:
            pass

# Improve the alt select UX: enable the Add Alt button and make next step obvious.
def _enable_add_alt_on_view(view: discord.ui.View, selected: str):
    try:
        for child in view.children:
            if isinstance(child, discord.ui.Button) and getattr(child, "custom_id", "") == "add_alt_btn":
                child.disabled = False
                child.style = discord.ButtonStyle.primary
                child.label = f"Add Alt — {selected}"
    except Exception:
        pass

# If an AltClassSelect exists, replace its callback to enable the button and show next step
for cls in list(globals().values()):
    if isinstance(cls, type) and cls.__name__ == "AltClassSelect":
        old_cb = getattr(cls, "callback", None)
        async def _new_cb(self, interaction: discord.Interaction):
            self.view.selected_alt_class = self.values[0]
            _enable_add_alt_on_view(self.view, self.view.selected_alt_class)
            hint = "\n\nNext: press **Add Alt** to enter name and level, or press **Join the server!** to finish."
            try:
                await interaction.response.edit_message(content=(self.view._summary_text() + hint), view=self.view)
            except Exception:
                # Fallback to old behavior
                if callable(old_cb):
                    await old_cb(self, interaction)
        setattr(cls, "callback", _new_cb)
        break

# Override RosterConfirmView to add custom_id on Add Alt button and decorate embed with stars when posting
class RosterConfirmView(discord.ui.View):
    def __init__(self, main_name: str, lvl: int, cls: str, alts: list, tz_raw: str, tz_norm: str):
        super().__init__(timeout=900)
        self.payload = (main_name, lvl, cls, alts or [], tz_raw, tz_norm)
        self.selected_alt_class = None
        # Ensure an AltClassSelect exists; if not, add our own
        has_alt_select = any(isinstance(ch, discord.ui.Select) for ch in self.children)
        if not has_alt_select:
            # Lightweight inline select
            opts = [discord.SelectOption(label=c, value=c) for c in ["Ranger","Rogue","Warrior","Mage","Druid"]]
            sel = discord.ui.Select(placeholder="Select alt class", min_values=1, max_values=1, options=opts)
            async def sel_cb(interaction: discord.Interaction):
                self.selected_alt_class = sel.values[0]
                _enable_add_alt_on_view(self, self.selected_alt_class)
                hint = "\n\nNext: press **Add Alt** to enter name and level, or press **Join the server!** to finish."
                await interaction.response.edit_message(content=(self._summary_text() + hint), view=self)
            sel.callback = sel_cb
            self.add_item(sel)

    def _summary_text(self) -> str:
        mname, mlvl, mcls, alts, tz_raw, tz_norm = self.payload
        try:
            alts_line = ", ".join(f"{a.get('name','?')} • {a.get('level','?')} • {a.get('class','?')}" for a in (alts or [])) or "N/A"
        except Exception:
            alts_line = "N/A"
        return (
            f"Step 2/2 — Review:\n"
            f"**Main:** {mname} • {mlvl} • {mcls}\n"
            f"**Alts:** {alts_line}\n"
            f"**Timezone:** {tz_raw}" + (f" ({tz_norm})" if tz_norm else "")
        )

    @discord.ui.button(label="Add Alt", style=discord.ButtonStyle.secondary, custom_id="add_alt_btn", disabled=True)
    async def add_alt(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.selected_alt_class:
            return await interaction.response.send_message("Pick an alt class from the dropdown first.", ephemeral=True)
        # Modal defined earlier in your build or our patch; fallback if missing
        AltModalClass = globals().get("AltModal")
        if AltModalClass is None:
            # Lightweight inline modal substitute
            modal = discord.ui.Modal(title="Add Alt")
            name = discord.ui.TextInput(label="Alt name", required=False, max_length=32)
            level = discord.ui.TextInput(label="Alt level 1â€“250", required=False, max_length=3)
            modal.add_item(name); modal.add_item(level)
            async def on_submit(modal_inter: discord.Interaction):
                nm = str(name).strip()
                lv = str(level).strip()
                try: lv_i = int(re.sub(r"[^0-9]", "", lv)) if lv else None
                except Exception: lv_i = None
                a = {"class": self.selected_alt_class}
                if nm: a["name"] = nm[:32]
                if lv_i is not None: a["level"] = lv_i
                mname, mlvl, mcls, alts, tz_raw, tz_norm = self.payload
                self.payload = (mname, mlvl, mcls, (alts or []) + [a], tz_raw, tz_norm)
                await modal_inter.response.send_message(self._summary_text(), ephemeral=True, view=self)
            modal.on_submit = on_submit
            return await interaction.response.send_modal(modal)
        else:
            return await interaction.response.send_modal(AltModalClass(self))

    @discord.ui.button(label="Join the server!", style=discord.ButtonStyle.success)
    async def join_server(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild = interaction.guild; user = interaction.user
        if not guild:
            return await interaction.response.send_message("Guild only.", ephemeral=True)
        gid = guild.id
        try:
            await _upsert_roster(gid, user.id, *self.payload)
        except Exception as e:
            log.warning(f"[roster] upsert failed: {e}")
            return await interaction.response.send_message("Could not save your info.", ephemeral=True)
        rid = await get_auto_member_role_id(gid)
        if rid:
            role = guild.get_role(rid)
            if role:
                try: await user.add_roles(role, reason="Roster intake complete")
                except Exception as e: log.warning(f"[roster] role grant failed: {e}")
        roster_ch_id = await get_roster_channel_id(gid)
        if roster_ch_id:
            ch = guild.get_channel(roster_ch_id)
            if can_send(ch):
                try:
                    # Build and decorate embed
                    e = _build_roster_embed(user, *self.payload)
                    await decorate_embed_with_stars(e, gid)
                    await ch.send(embed=e)
                except Exception as e:
                    log.warning(f"[roster] post failed: {e}")
        await interaction.response.edit_message(content="You're set. Welcome.", view=None)
# ==================== END POLISH ====================

# ==================== CLEAN ALT FLOW + SAFE STAR GIF ====================
import urllib.parse as _urlparse

def _is_valid_http_url(url: str) -> bool:
    try:
        u = _urlparse.urlparse(url.strip())
        return u.scheme in ("http", "https") and bool(u.netloc)
    except Exception:
        return False

# Override RosterStartView to show step text
class RosterStartView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=600)
        self.selected_class = None
        opts = [discord.SelectOption(label=c, value=c) for c in ["Ranger","Rogue","Warrior","Mage","Druid"]]
        sel = discord.ui.Select(placeholder="Step 1/2 — pick your MAIN class", min_values=1, max_values=1, options=opts)
        async def sel_cb(interaction: discord.Interaction):
            self.selected_class = sel.values[0]
            await interaction.response.edit_message(content=f"Main class selected: **{self.selected_class}**\nNext: press **Continue**.", view=self)
        sel.callback = sel_cb
        self.add_item(sel)

    @discord.ui.button(label="Continue", style=discord.ButtonStyle.primary)
    async def _continue(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.selected_class:
            return await interaction.response.send_message("Pick a main class first.", ephemeral=True)
        await interaction.response.send_modal(RosterModal(self.selected_class))

# Force our RosterModal without any alts textbox
class RosterModal(discord.ui.Modal, title="Start Roster — Step 1/2"):
    main_name = discord.ui.TextInput(label="Main name", placeholder="Blunderbuss", required=True, max_length=32)
    main_level = discord.ui.TextInput(label="Main level (1â€“250)", placeholder="215", required=True, max_length=3)
    timezone = discord.ui.TextInput(label="Timezone (IANA or offset)", placeholder="America/Chicago or UTC-05:00", required=False, max_length=64)

    def __init__(self, selected_class: str):
        super().__init__(timeout=600)
        self.selected_class = selected_class

    async def on_submit(self, interaction: discord.Interaction):
        # Validate level
        try:
            lvl = int(str(self.main_level))
        except Exception:
            return await interaction.response.send_message("Level must be a number.", ephemeral=True)
        if not (1 <= lvl <= 250):
            return await interaction.response.send_message("Level must be between 1 and 250.", ephemeral=True)

        main_name = str(self.main_name).strip()
        cls = _norm_class(self.selected_class)
        tz_raw, tz_norm = _parse_timezone(str(self.timezone))

        view = RosterConfirmView(main_name, lvl, cls, [], tz_raw, tz_norm)
        hint = "\n\nStep 2/2 — Optional alts: pick a class, press **Add Alt**, or press **Join the server!** to finish."
        await interaction.response.send_message(view._summary_text() + hint, ephemeral=True, view=view)

# Improve AltClassSelect callback to enable the button and show hint
for cls in list(globals().values()):
    if isinstance(cls, type) and cls.__name__ == "AltClassSelect":
        async def _cb(self, interaction: discord.Interaction):
            self.view.selected_alt_class = self.values[0]
            # Enable Add Alt button if present
            for ch in self.view.children:
                if isinstance(ch, discord.ui.Button) and getattr(ch, "custom_id", "") == "add_alt_btn":
                    ch.disabled = False
                    ch.style = discord.ButtonStyle.primary
                    ch.label = f"Add Alt — {self.view.selected_alt_class}"
            hint = "\n\nNext: press **Add Alt** to enter name and level, or **Join the server!** to finish."
            await interaction.response.edit_message(content=self.view._summary_text() + hint, view=self.view)
        cls.callback = _cb
        break

# Harden star decoration: validate URL before applying. If invalid, skip images.

async def decorate_embed_with_stars(e: discord.Embed, guild_id: int):
    gif = None
    try:
        gif = await _cfg_get_text(guild_id, "roster_star_gif")
    except Exception:
        gif = None
    try:
        if isinstance(gif, str) and _is_valid_http_url(gif):
            try:
                e.set_thumbnail(url=gif)
            except Exception:
                pass
    except Exception:
        pass
    return e

# Validate /setup-stargif input before saving
from discord import app_commands as _ac_starfix
@_ac_starfix.command(name="setup-stargif", description="Set a GIF URL to decorate roster embeds, or 'clear' to unset")
@_ac_starfix.checks.has_permissions(manage_guild=True)
async def setup_stargif(interaction: discord.Interaction, url: str):
    if url.lower().strip() == "clear":
        await _cfg_set_text(interaction.guild.id, "roster_star_gif", None)
        return await interaction.response.send_message("Star GIF cleared.", ephemeral=True)
    if not _is_valid_http_url(url):
        return await interaction.response.send_message("Invalid URL. Provide an http(s) URL to an image/GIF, or 'clear'.", ephemeral=True)
    await _cfg_set_text(interaction.guild.id, "roster_star_gif", url.strip())
    await interaction.response.send_message("Star GIF set.", ephemeral=True)

# Bind the command once per guild
@bot.listen("on_ready")
async def __bind_setup_stargif_once():
    for g in bot.guilds:
        try:
            bot.tree.add_command(setup_stargif, guild=g)
        except Exception:
            pass
    try:
        for g in bot.guilds:
            await bot.tree.sync(guild=g)
    except Exception:
        pass
# ==================== END CLEAN ALT FLOW + SAFE STAR GIF ====================

# ===== Roster message id migration =====
async def _ensure_roster_msg_id_column():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""CREATE TABLE IF NOT EXISTS roster_members (
            guild_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            main_name TEXT NOT NULL,
            main_level INTEGER NOT NULL,
            main_class TEXT NOT NULL,
            alts_json TEXT NOT NULL,
            timezone_raw TEXT NOT NULL,
            timezone_norm TEXT,
            submitted_at INTEGER,
            updated_at INTEGER,
            PRIMARY KEY (guild_id, user_id)
        )""")
        c = await db.execute("PRAGMA table_info(roster_members)")
        cols = {row[1] for row in await c.fetchall()}
        if "roster_msg_id" not in cols:
            await db.execute("ALTER TABLE roster_members ADD COLUMN roster_msg_id INTEGER DEFAULT NULL")
        await db.commit()

@bot.listen("on_ready")
async def __migrate_roster_msg_id_col():
    try:
        await _ensure_roster_msg_id_column()
    except Exception as e:
        try: log.warning(f"[migrate] roster_msg_id: {e}")
        except Exception: pass
# ======================================

# ===== Roster row helpers + embed edit =====
async def _roster_load(gid: int, uid: int):
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT main_name, main_level, main_class, alts_json, timezone_raw, timezone_norm, roster_msg_id FROM roster_members WHERE guild_id=? AND user_id=?", (gid, uid))
        return await c.fetchone()

async def _roster_save_embed_message_id(gid: int, uid: int, msg_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE roster_members SET roster_msg_id=? WHERE guild_id=? AND user_id=?", (int(msg_id), gid, uid))
        await db.commit()

async def _roster_edit_or_post(guild: discord.Guild, member: discord.Member, row):
    main_name, main_level, main_class, alts_json, tz_raw, tz_norm, roster_msg_id = row
    try:
        alts = json.loads(alts_json) if isinstance(alts_json, str) else (alts_json or [])
    except Exception:
        alts = []
    e = _build_roster_embed(member, main_name, int(main_level), main_class, alts, tz_raw, tz_norm)
    await decorate_embed_with_stars(e, guild.id)
    rcid = await get_roster_channel_id(guild.id)
    ch = guild.get_channel(rcid) if rcid else None
    if not ch or not can_send(ch):
        raise RuntimeError("Roster channel not configured or no permission.")
    if roster_msg_id:
        try:
            msg = await ch.fetch_message(int(roster_msg_id))
            await msg.edit(embed=e)
            return msg
        except Exception:
            pass
    # Post new and store id
    msg = await ch.send(embed=e)
    await _roster_save_embed_message_id(guild.id, member.id, msg.id)
    return msg
# ===========================================

# ===== Player self-service level updates =====
from discord import app_commands as _ac_levels

@_ac_levels.command(name="my-level-main", description="Update your main level (1â€“250) without reposting")
async def my_level_main(interaction: discord.Interaction, level: int):
    if not interaction.guild:
        return await interaction.response.send_message("Guild only.", ephemeral=True)
    if not (1 <= level <= 250):
        return await interaction.response.send_message("Level must be 1â€“250.", ephemeral=True)
    gid = interaction.guild.id; uid = interaction.user.id
    row = await _roster_load(gid, uid)
    if not row:
        return await interaction.response.send_message("No roster on file. Use the welcome intake first.", ephemeral=True)
    main_name, main_level, main_class, alts_json, tz_raw, tz_norm, roster_msg_id = row
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE roster_members SET main_level=?, updated_at=? WHERE guild_id=? AND user_id=?", (int(level), now_ts(), gid, uid))
        await db.commit()
    row = (main_name, int(level), main_class, alts_json, tz_raw, tz_norm, roster_msg_id)
    try:
        await _roster_edit_or_post(interaction.guild, interaction.user, row)
        await interaction.response.send_message(f"Main level updated to {level}.", ephemeral=True)
    except Exception as e:
        await interaction.response.send_message(f"Saved. Could not update the roster message: {e}", ephemeral=True)

@_ac_levels.command(name="my-level-alt", description="Update one alt level by slot number (1..N)")
async def my_level_alt(interaction: discord.Interaction, slot: int, level: int):
    if not interaction.guild:
        return await interaction.response.send_message("Guild only.", ephemeral=True)
    if not (1 <= level <= 250):
        return await interaction.response.send_message("Level must be 1â€“250.", ephemeral=True)
    if slot < 1:
        return await interaction.response.send_message("Slot must be 1 or greater.", ephemeral=True)
    gid = interaction.guild.id; uid = interaction.user.id
    row = await _roster_load(gid, uid)
    if not row:
        return await interaction.response.send_message("No roster on file. Use the welcome intake first.", ephemeral=True)
    main_name, main_level, main_class, alts_json, tz_raw, tz_norm, roster_msg_id = row
    try:
        alts = json.loads(alts_json) if isinstance(alts_json, str) else (alts_json or [])
    except Exception:
        alts = []
    if slot > len(alts):
        return await interaction.response.send_message(f"You only have {len(alts)} alts saved.", ephemeral=True)
    # Update
    alts[slot-1]["level"] = int(level)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE roster_members SET alts_json=?, updated_at=? WHERE guild_id=? AND user_id=?", (json.dumps(alts), now_ts(), gid, uid))
        await db.commit()
    row = (main_name, main_level, main_class, json.dumps(alts), tz_raw, tz_norm, roster_msg_id)
    try:
        await _roster_edit_or_post(interaction.guild, interaction.user, row)
        await interaction.response.send_message(f"Alt #{slot} level updated to {level}.", ephemeral=True)
    except Exception as e:
        await interaction.response.send_message(f"Saved. Could not update the roster message: {e}", ephemeral=True)

# Bind and sync
@bot.listen("on_ready")
async def __bind_player_level_cmds():
    for g in bot.guilds:
        for cmd in (my_level_main, my_level_alt):
            try:
                bot.tree.add_command(cmd, guild=g)
            except Exception:
                pass
        try:
            await bot.tree.sync(guild=g)
        except Exception:
            pass
# ===========================================

# ==================== PATCH: Alt levels + persistent DB path + final rebinds ====================
import os as __os_v3, re as __re_v3, json as __json_v3, pathlib as __pl_v3

# --- Persistent DB path selection (replaces simple /var/data -> /tmp fallback) ---
try:
    _DB_DIR_CANDIDATES = [
        __os_v3.getenv("DATA_DIR", ""),
        "/opt/render/project/data",
        "./data",
        "/var/data",   # if container allows
        "/tmp",
    ]
    _DB_DIR_CANDIDATES = [d for d in _DB_DIR_CANDIDATES if d]
    for _d in _DB_DIR_CANDIDATES:
        try:
            __pl_v3.Path(_d).mkdir(parents=True, exist_ok=True)
            testf = __pl_v3.Path(_d) / ".wtest"
            with open(testf, "w") as fh: fh.write("ok")
            testf.unlink(missing_ok=True)
            DB_PATH = str(__pl_v3.Path(_d) / "bosses.db")
            break
        except Exception:
            continue
except Exception:
    pass  # keep existing DB_PATH if set earlier

# --- Robust level coercion for alts on SAVE ---
def __coerce_lvl_v3(val):
    try:
        if val is None: return None
        if isinstance(val, int): return val
        m = __re_v3.search(r'\d+', str(val))
        return int(m.group(0)) if m else None
    except Exception:
        return None

# Wrap original _upsert_roster to coerce levels before persist
try:
    _orig__upsert_roster = _upsert_roster  # noqa
except NameError:
    _orig__upsert_roster = None

async def _upsert_roster_v3(gid: int, uid: int, main_name: str, main_level: int, main_class: str, alts, tz_raw: str, tz_norm: str):
    # Coerce main
    ml = __coerce_lvl_v3(main_level) or main_level
    # Normalize alts list
    alt_list = []
    try:
        if isinstance(alts, str):
            alt_list = __json_v3.loads(alts) or []
        elif isinstance(alts, list):
            alt_list = list(alts)
    except Exception:
        alt_list = []
    for a in alt_list:
        if isinstance(a, dict):
            a["level"] = __coerce_lvl_v3(a.get("level") or a.get("lvl") or a.get("lv"))
    # Delegate to original if exists, else try to persist directly
    if _orig__upsert_roster:
        return await _orig__upsert_roster(gid, uid, main_name, ml, main_class, alt_list, tz_raw, tz_norm)
    # Fallback storage
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""CREATE TABLE IF NOT EXISTS roster_members (
            guild_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            main_name TEXT NOT NULL,
            main_level INTEGER NOT NULL,
            main_class TEXT NOT NULL,
            alts_json TEXT NOT NULL,
            timezone_raw TEXT NOT NULL,
            timezone_norm TEXT,
            submitted_at INTEGER,
            updated_at INTEGER,
            roster_msg_id INTEGER,
            PRIMARY KEY (guild_id, user_id)
        )""")
        await db.execute("""INSERT INTO roster_members
            (guild_id,user_id,main_name,main_level,main_class,alts_json,timezone_raw,timezone_norm,submitted_at,updated_at,roster_msg_id)
            VALUES (?,?,?,?,?,?,?,?,?,?,COALESCE((SELECT roster_msg_id FROM roster_members WHERE guild_id=? AND user_id=?),NULL))
            ON CONFLICT(guild_id,user_id) DO UPDATE SET
              main_name=excluded.main_name,
              main_level=excluded.main_level,
              main_class=excluded.main_class,
              alts_json=excluded.alts_json,
              timezone_raw=excluded.timezone_raw,
              timezone_norm=excluded.timezone_norm,
              updated_at=excluded.updated_at
        """, (gid, uid, main_name, int(ml) if isinstance(ml,int) else ml, main_class, __json_v3.dumps(alt_list), tz_raw, tz_norm, now_ts(), now_ts(), gid, uid))
        await db.commit()

# Prefer v3 wrapper
try:
    _upsert_roster = _upsert_roster_v3
except Exception:
    pass

# --- Final rebind of roster embed on_ready to defeat later overrides ---
def _build_roster_embed_final(member: discord.Member, main_name: str, main_level: int, main_class: str, alts, tz_raw: str, tz_norm: str):
    # Stars at both ends of full main info. Alts per line with levels.
    alt_lines = []
    try:
        if isinstance(alts, str):
            alts = __json_v3.loads(alts) or []
        for i, a in enumerate(alts or [], 1):
            nm = str(a.get("name","?"))[:32] if isinstance(a, dict) else "?"
            lv = __coerce_lvl_v3(a.get("level") if isinstance(a, dict) else None)
            cls = a.get("class","?") if isinstance(a, dict) else "?"
            alt_lines.append(f"{i}. {nm} • {'Lv '+str(lv) if isinstance(lv,int) else 'Lv N/A'} • {cls}")
    except Exception:
        alt_lines = []
    m_lv = __coerce_lvl_v3(main_level) or main_level
    main_line = f"**✨ {main_name} • Lv {m_lv} • {main_class} ✨**"
    e = discord.Embed(title=f"New Member: {member.display_name}", color=discord.Color.blurple())
    e.add_field(name="Main", value=main_line, inline=False)
    e.add_field(name="Alts", value="\n".join(alt_lines) if alt_lines else "N/A", inline=False)
    tz = f"{tz_raw}" + (f" ({tz_norm})" if tz_norm else "")
    e.add_field(name="Timezone", value=tz or "N/A", inline=False)
    e.set_footer(text="Welcome!")
    return e

@bot.listen("on_ready")
async def __finalize_embed_binding_v3():
    try:
        globals()["_build_roster_embed"] = _build_roster_embed_final
        log.info("[embed] final bind applied")
    except Exception:
        pass

# --- Timer persistence warning when DB is ephemeral ---
@bot.listen("on_ready")
async def __warn_ephemeral_db_if_needed():
    try:
        if DB_PATH.startswith("/tmp"):
            log.warning("[startup] DB on /tmp. Timers will reset on reboot. Set DATA_DIR or mount a persistent volume.")
    except Exception:
        pass
# ==================== END PATCH ====================

# ==================== ALT INTAKE: STRICT VALIDATION + RENDER (no removals) ====================
# Require alt name, level(1-250), class when user adds an alt. Render per line with level.
import json as __json_altv, re as __re_altv, asyncio as __asyncio_altv
import aiosqlite as __aiosqlite_altv
import discord as __discord_altv

def __altv_coerce_level(v):
    try:
        if v is None: return None
        if isinstance(v, int): return v
        m = __re_altv.search(r"\d{1,3}", str(v))
        if not m: return None
        n = int(m.group(0))
        if 1 <= n <= 250: return n
        return None
    except Exception: return None

def __altv_norm(entry):
    name, lvl, cls = "?", None, "?"
    if isinstance(entry, dict):
        name = str(entry.get("name") or entry.get("Name") or entry.get("toon") or "?").strip()[:32] or "?"
        for k in ("level","lvl","lv","alt_level","Level"):
            if k in entry:
                lvl = __altv_coerce_level(entry.get(k)); break
        cls = str(entry.get("class") or entry.get("Class") or entry.get("cls") or "?").strip()[:16] or "?"
    elif isinstance(entry, (list,tuple)) and entry:
        name = str(entry[0]).strip()[:32] or "?"
        lvl = __altv_coerce_level(entry[1] if len(entry)>1 else None)
        cls = str(entry[2]).strip()[:16] if len(entry)>2 else "?"
    else:
        parts = [p.strip() for p in str(entry).split("•")]
        if parts: name = parts[0][:32] or "?"
        lvl = __altv_coerce_level(parts[1] if len(parts)>=2 else None)
        if len(parts)>=3: cls = parts[2][:16] or "?"
    return {"name": name, "level": lvl, "class": cls}

async def __altv_notify_missing(gid: int, uid: int, bad_rows):
    try:
        user = (bot.get_user(uid) or await bot.fetch_user(uid))
        if not user: return
        details = "\n".join(f"- #{i+1}: name='{r.get('name','?')}', level='{r.get('level')}', class='{r.get('class','?')}'" for i,r in enumerate(bad_rows))
        txt = ("Your alt submission had missing fields. "
               "Each alt must include **Name**, **Level 1â€“250**, and **Class**.\n"
               f"The following were skipped:\n{details}\n"
               "Use the intake again and fill all fields.")
        try: await user.send(txt)
        except __discord_altv.Forbidden: pass
    except Exception: pass

try:
    __orig_build_roster_embed_altv = _build_roster_embed  # noqa: F821
except NameError:
    __orig_build_roster_embed_altv = None

def _build_roster_embed(member, main_name, main_level, main_class, alts, tz_raw, tz_norm):
    try:
        if isinstance(alts, str):
            try: alts_raw = __json_altv.loads(alts) or []
            except Exception: alts_raw = []
        else:
            alts_raw = alts or []
        norm = [__altv_norm(a) for a in alts_raw]
        lines = []
        for i, a in enumerate(norm, 1):
            lv = a.get("level")
            lines.append(f"{i}. {a['name']} • {'Lv '+str(lv) if isinstance(lv,int) else 'Lv N/A'} • {a['class']}")
        e = __discord_altv.Embed(title=f"New Member: {member.display_name}", color=__discord_altv.Color.blurple())
        e.add_field(name="Main", value=f"**✨ {main_name} • Lv {main_level} • {main_class} ✨**", inline=False)
        e.add_field(name="Alts", value=("\n".join(lines) if lines else "N/A"), inline=False)
        tz = f"{tz_raw}" + (f" ({tz_norm})" if tz_norm else "")
        e.add_field(name="Timezone", value=tz or "N/A", inline=False)
        e.set_footer(text="Welcome!"); return e
    except Exception:
        return __discord_altv.Embed(title=f"New Member: {getattr(member,'display_name','?')}", description=f"{main_name} • {main_level} • {main_class}")

@bot.listen("on_ready")
async def __altv_bind_embed():
    try:
        globals()["_build_roster_embed"] = _build_roster_embed
        if 'log' in globals(): log.info("[altv] embed builder bound")
    except Exception: pass

try:
    __orig_upsert_roster_altv = _upsert_roster  # noqa: F821
except NameError:
    __orig_upsert_roster_altv = None

async def _upsert_roster(gid, uid, main_name, main_level, main_class, alts, tz_raw, tz_norm):
    # Normalize and require alt fields if any provided
    def _norm_list(a):
        if isinstance(a, str):
            try: return __json_altv.loads(a) or []
            except Exception: return []
        return list(a or [])
    norm = [__altv_norm(x) for x in _norm_list(alts)]
    bad = [r for r in norm if not r["name"] or r["name"]=="?" or r["class"]=="?" or not isinstance(r["level"], int)]
    if bad:
        try: import asyncio as _aio; _aio.get_running_loop().create_task(__altv_notify_missing(gid, uid, bad))
        except Exception: pass
    norm_valid = [r for r in norm if r not in bad]
    if __orig_upsert_roster_altv:
        return await __orig_upsert_roster_altv(gid, uid, main_name, main_level, main_class, norm_valid, tz_raw, tz_norm)
    # Fallback store
    async with __aiosqlite_altv.connect(DB_PATH) as db:
        await db.execute("""CREATE TABLE IF NOT EXISTS roster_members (
            guild_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            main_name TEXT NOT NULL,
            main_level INTEGER NOT NULL,
            main_class TEXT NOT NULL,
            alts_json TEXT NOT NULL,
            timezone_raw TEXT NOT NULL,
            timezone_norm TEXT,
            submitted_at INTEGER,
            updated_at INTEGER,
            roster_msg_id INTEGER,
            PRIMARY KEY (guild_id, user_id)
        )""")
        await db.execute("""INSERT INTO roster_members
            (guild_id,user_id,main_name,main_level,main_class,alts_json,timezone_raw,timezone_norm,submitted_at,updated_at,roster_msg_id)
            VALUES (?,?,?,?,?,?,?, ?, strftime('%s','now'),strftime('%s','now'),
                COALESCE((SELECT roster_msg_id FROM roster_members WHERE guild_id=? AND user_id=?),NULL))
            ON CONFLICT(guild_id,user_id) DO UPDATE SET
                main_name=excluded.main_name,
                main_level=excluded.main_level,
                main_class=excluded.main_class,
                alts_json=excluded.alts_json,
                timezone_raw=excluded.timezone_raw,
                timezone_norm=excluded.timezone_norm,
                updated_at=excluded.updated_at
        """, (gid, uid, main_name, main_level, main_class, __json_altv.dumps(norm_valid), tz_raw, tz_norm, gid, uid))
        await db.commit()
# ==================== END ALT INTAKE: STRICT VALIDATION + RENDER ====================

# ==================== ALT INTAKE: STRICTER VALIDATION + ROBUST RENDER ====================
# Guarantees alt name, level(1-250), class are present when user chooses to add an alt.
# Roster embed always shows per-alt "Lv N".
import json as __json_altv2, re as __re_altv2, asyncio as __aio_altv2
import aiosqlite as __asql_altv2
import discord as __d_altv2

def __altv2_pick(d: dict, contains: str, fallback: str = None):
    contains = contains.lower()
    for k,v in d.items():
        if contains in str(k).lower():
            return v
    return d.get(fallback) if fallback else None

def __altv2_coerce_level(v):
    if v is None: return None
    try:
        if isinstance(v, int): n = v
        else:
            m = __re_altv2.search(r'\d{1,3}', str(v))
            n = int(m.group(0)) if m else None
        if n is None: return None
        if 1 <= n <= 250: return n
    except Exception:
        pass
    return None

def __altv2_norm(entry):
    # Accept dicts with arbitrary key names and strings like "Name • 150 • Mage"
    name, lvl, cls = "?", None, "?"
    if isinstance(entry, dict):
        name = str(__altv2_pick(entry, "name") or entry.get("toon") or entry.get("main") or "?").strip()[:32] or "?"
        raw_lvl = (__altv2_pick(entry, "lev") or __altv2_pick(entry, "lvl") or entry.get("level") or entry.get("alt_level"))
        lvl = __altv2_coerce_level(raw_lvl)
        cls = str(__altv2_pick(entry, "class") or entry.get("cls") or "?").strip()[:16] or "?"
    elif isinstance(entry, (list,tuple)):
        name = str(entry[0]).strip()[:32] if entry else "?"
        lvl = __altv2_coerce_level(entry[1] if len(entry)>1 else None)
        cls = str(entry[2]).strip()[:16] if len(entry)>2 else "?"
    else:
        parts = [p.strip() for p in str(entry).split("•")]
        if parts: name = parts[0][:32] or "?"
        lvl = __altv2_coerce_level(parts[1] if len(parts)>=2 else None)
        cls = parts[2][:16] if len(parts)>=3 else "?"
    return {"name": name, "level": lvl, "class": cls}

async def __altv2_notify_missing(gid: int, uid: int, bad_rows):
    try:
        user = (bot.get_user(uid) or await bot.fetch_user(uid))
        if not user: return
        details = "\n".join(f"- #{i+1}: name='{r.get('name','?')}', level='{r.get('level')}', class='{r.get('class','?')}'" for i,r in enumerate(bad_rows))
        msg = ("Your alt submission had missing fields. Each alt must include **Name**, **Level 1â€“250**, and **Class**.\n"
               f"Skipped:\n{details}")
        try: await user.send(msg)
        except __d_altv2.Forbidden: pass
    except Exception: pass

# Wrap the roster embed builder to render levels per alt line
try:
    __orig_build_roster_embed_altv2 = _build_roster_embed  # noqa
except NameError:
    __orig_build_roster_embed_altv2 = None

def _build_roster_embed(member, main_name, main_level, main_class, alts, tz_raw, tz_norm):
    try:
        if isinstance(alts, str):
            try: raw = __json_altv2.loads(alts) or []
            except Exception: raw = []
        else:
            raw = alts or []
        norm = [__altv2_norm(x) for x in raw]
        lines = []
        for i, a in enumerate(norm, 1):
            lv = a.get("level")
            lines.append(f"{i}. {a['name']} • {'Lv '+str(lv) if isinstance(lv,int) else 'Lv N/A'} • {a['class']}")
        e = __d_altv2.Embed(title=f"New Member", color=__d_altv2.Color.blurple())
        e.add_field(name="Main", value=f"**✨ {main_name} • Lv {main_level} • {main_class} ✨**", inline=False)
        e.add_field(name="Alts", value=("\n".join(lines) if lines else "N/A"), inline=False)
        tz = f"{tz_raw}" + (f" ({tz_norm})" if tz_norm else "")
        e.add_field(name="Timezone", value=tz or "N/A", inline=False)
        e.set_author(name=getattr(member, "display_name", "New member"))
        e.set_footer(text="Welcome!")
        return e
    except Exception:
        return __d_altv2.Embed(title=f"New Member: {getattr(member,'display_name','?')}", description=f"{main_name} • {main_level} • {main_class}")

# Wrap storage to enforce required alt fields when user added any alts
try:
    __orig_upsert_roster_altv2 = _upsert_roster  # noqa
except NameError:
    __orig_upsert_roster_altv2 = None

async def _upsert_roster(gid, uid, main_name, main_level, main_class, alts, tz_raw, tz_norm):
    def _as_list(a):
        if isinstance(a, str):
            try: return __json_altv2.loads(a) or []
            except Exception: return []
        return list(a or [])
    norm = [__altv2_norm(x) for x in _as_list(alts)]
    bad = [r for r in norm if (not r["name"] or r["name"]=="?") or (not isinstance(r["level"], int)) or (r["class"]=="?")]
    if bad:
        try: __aio_altv2.get_running_loop().create_task(__altv2_notify_missing(gid, uid, bad))
        except Exception: pass
    norm_valid = [r for r in norm if r not in bad]
    if __orig_upsert_roster_altv2:
        return await __orig_upsert_roster_altv2(gid, uid, main_name, main_level, main_class, norm_valid, tz_raw, tz_norm)
    # Fallback minimal store if original missing
    async with __asql_altv2.connect(DB_PATH) as db:
        await db.execute("""CREATE TABLE IF NOT EXISTS roster_members (
            guild_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            main_name TEXT NOT NULL,
            main_level INTEGER NOT NULL,
            main_class TEXT NOT NULL,
            alts_json TEXT NOT NULL,
            timezone_raw TEXT NOT NULL,
            timezone_norm TEXT,
            submitted_at INTEGER,
            updated_at INTEGER,
            roster_msg_id INTEGER,
            PRIMARY KEY (guild_id, user_id)
        )""")
        await db.execute("""INSERT INTO roster_members
            (guild_id,user_id,main_name,main_level,main_class,alts_json,timezone_raw,timezone_norm,submitted_at,updated_at,roster_msg_id)
            VALUES (?,?,?,?,?,?,?, ?, strftime('%s','now'),strftime('%s','now'),
                COALESCE((SELECT roster_msg_id FROM roster_members WHERE guild_id=? AND user_id=?),NULL))
            ON CONFLICT(guild_id,user_id) DO UPDATE SET
                main_name=excluded.main_name,
                main_level=excluded.main_level,
                main_class=excluded.main_class,
                alts_json=excluded.alts_json,
                timezone_raw=excluded.timezone_raw,
                timezone_norm=excluded.timezone_norm,
                updated_at=excluded.updated_at
        """, (gid, uid, main_name, main_level, main_class, __json_altv2.dumps(norm_valid), tz_raw, tz_norm, gid, uid))
        await db.commit()

@bot.listen("on_ready")
async def __altv2_bind_log():
    try:
        globals()["_build_roster_embed"] = _build_roster_embed
        if 'log' in globals(): log.info("[altfix] v2 embed builder bound")
    except Exception: pass
# ==================== END ALT INTAKE PATCH ====================

# ==================== ALT LEVEL DISPLAY HOTFIX (additive, no removals) ====================
try:
    import re as __re_alts, json as __json_alts, discord as __d_alts
except Exception:
    pass
def __alts_coerce_level(v):
    try:
        if v is None: return None
        if isinstance(v, int): return v
        m = __re_alts.search(r'\d{1,3}', str(v)); n = int(m.group(0)) if m else None
        if n is None or not (1 <= n <= 250): return None
        return n
    except Exception: return None
def __alts_norm_one(a):
    if isinstance(a, dict):
        name = str(a.get("name") or a.get("Name") or a.get("toon") or "?").strip()[:32] or "?"
        cls  = str(a.get("class") or a.get("Class") or a.get("cls") or "?").strip()[:16] or "?"
        lvl  = a.get("level"); 
        if lvl is None:
            for k in ("lvl","lv","alt_level","Level"):
                if k in a: lvl = a.get(k); break
        return {"name": name, "class": cls, "level": __alts_coerce_level(lvl)}
    if isinstance(a, (list,tuple)) and a:
        return {"name": str(a[0]).strip()[:32] or "?", "class": (str(a[2]).strip()[:16] if len(a)>2 else "?"), "level": __alts_coerce_level(a[1] if len(a)>1 else None)}
    parts = [p.strip() for p in str(a).split("•")]
    return {"name": (parts[0][:32] if parts else "?") or "?", "class": (parts[2][:16] if len(parts)>=3 else "?") or "?", "level": __alts_coerce_level(parts[1] if len(parts)>=2 else None)}
def __alts_norm_list(alts):
    try:
        if isinstance(alts, str):
            try: alts = __json_alts.loads(alts) or []
            except Exception: alts = []
        return [__alts_norm_one(x) for x in (alts or [])]
    except Exception: return []
# Patch AltModal.on_submit to read .value and store integer level
try:
    AltModal; RosterConfirmView
    async def __alt_submit_fixed(self, interaction: __d_alts.Interaction):
        name_txt = (getattr(self.alt_name, "value", "") or "").strip()
        lvl_txt  = (getattr(self.alt_level, "value", "") or "").strip()
        lvl = __alts_coerce_level(lvl_txt)
        if not name_txt or lvl is None:
            return await interaction.response.send_message("Alt name and a numeric level 1â€“250 are required.", ephemeral=True)
        cls = getattr(self.parent_view, "selected_alt_class", None) or "Ranger"
        mname, mlvl, mcls, alts, tz_raw, tz_norm = self.parent_view.payload
        alt = {"name": name_txt[:32], "level": int(lvl), "class": cls}
        new_alts = list(alts or []) + [alt]
        new_view = RosterConfirmView(mname, mlvl, mcls, new_alts, tz_raw, tz_norm)
        await interaction.response.send_message(new_view._summary_text(), ephemeral=True, view=new_view)
    AltModal.on_submit = __alt_submit_fixed
except Exception: pass
# Wrap roster embed so levels always display
try:
    __orig_build_embed = _build_roster_embed
except Exception:
    __orig_build_embed = None
def _build_roster_embed(member, main_name, main_level, main_class, alts, tz_raw, tz_norm):
    norm = __alts_norm_list(alts)
    if __orig_build_embed:
        try: return __orig_build_embed(member, main_name, main_level, main_class, norm, tz_raw, tz_norm)
        except Exception: pass
    e = __d_alts.Embed(title=f"New Member: {getattr(member,'display_name','?')}", color=__d_alts.Color.blurple())
    e.add_field(name="Main", value=f"**✨ {main_name} • Lv {__alts_coerce_level(main_level) or main_level} • {main_class} ✨**", inline=False)
    alt_lines = [f"{i}. {a['name']} • {'Lv '+str(a['level']) if isinstance(a['level'],int) else 'Lv N/A'} • {a['class']}" for i,a in enumerate(norm,1)] or ["N/A"]
    e.add_field(name="Alts", value="\n".join(alt_lines), inline=False)
    tz = f"{tz_raw}" + (f" ({tz_norm})" if tz_norm else "")
    e.add_field(name="Timezone", value=tz or "N/A", inline=False)
    e.set_footer(text="Welcome!")
    return e
# Normalize before posting in the join flow
try:
    _orig_join = RosterConfirmView.join_server
    async def _join_server_norm(self, interaction: __d_alts.Interaction, button: __d_alts.ui.Button):
        try: await _upsert_roster(interaction.guild.id, interaction.user.id, *self.payload)
        except Exception: pass
        try: await _orig_join(self, interaction, button)
        except TypeError: await _orig_join(self, interaction)
        except Exception:
            g = interaction.guild; u = interaction.user
            rcid = await get_roster_channel_id(g.id)
            ch = g.get_channel(rcid) if rcid else None
            if ch and can_send(ch):
                mname, mlvl, mcls, alts, tz_raw, tz_norm = self.payload
                await ch.send(embed=_build_roster_embed(u, mname, mlvl, mcls, __alts_norm_list(alts), tz_raw, tz_norm))
            try: await interaction.response.edit_message(content="You're set. Welcome.", view=None)
            except Exception: pass
    RosterConfirmView.join_server = _join_server_norm
except Exception: pass
# Re-bind on ready
try:
    @bot.listen("on_ready")
    async def __alts_ready_bind():
        try:
            globals()["_build_roster_embed"] = _build_roster_embed
            if 'log' in globals(): log.info("[alt-level-fix] builder active")
        except Exception: pass
except Exception: pass
# ==================== END ALT LEVEL DISPLAY HOTFIX ====================

# ==================== MOBILE TIMER SELECT FIX (no dunder alias) ====================
# Resolves: NameError: name '_MobileCategorySelect__d_mtim' is not defined
try:
    import discord as dm
    from typing import List as _List
    if 'TimerToggleView' in globals():
        # fresh class without double-underscore names
        class MobileCategorySelect(dm.ui.Select):
            def __init__(self, parent_view):
                self.parent_view = parent_view
                opts = []
                for cat in CATEGORY_ORDER:
                    emoji = EMOJI_FOR_CAT.get(cat) if 'EMOJI_FOR_CAT' in globals() else None
                    opts.append(dm.SelectOption(label=cat, value=cat, emoji=emoji, default=(cat in parent_view.shown)))
                super().__init__(
                    placeholder="Select categories to show",
                    min_values=0,
                    max_values=len(opts),
                    options=opts,
                    row=0,
                )
            async def callback(self, interaction: dm.Interaction):
                self.parent_view.shown = list(self.values)
                await self.parent_view.refresh(interaction)

        # Replace TimerToggleView.__init__ to use the new select
        _orig_init = TimerToggleView.__init__
        def __compact_ttv_init_mobile(self, guild: dm.Guild, user_id: int, init_show: _List[str]):
            dm.ui.View.__init__(self, timeout=300)
            self.guild = guild
            self.user_id = user_id
            self.shown = [c for c in CATEGORY_ORDER if c in (init_show or [])] or CATEGORY_ORDER[:]
            self.add_item(MobileCategorySelect(self))
            try:
                self.add_item(self._make_all_button())
                self.add_item(self._make_none_button())
            except Exception:
                pass
            self.message = None
        TimerToggleView.__init__ = __compact_ttv_init_mobile
        if 'log' in globals():
            log.info("[mobile] compact select fix applied")
except Exception as _e_fix_sel:
    try:
        if 'log' in globals(): log.warning(f"[mobile] compact select fix failed: {_e_fix_sel}")
    except Exception:
        pass
# ==================== END MOBILE TIMER SELECT FIX ====================

# ==================== MOBILE TIMER UX: persist selection + compact embeds ====================
# Goals: 
# 1) Multi-select defaults to userâ€™s last selection (only those selected). 
# 2) Persist selection on change. 
# 3) Compact timer embeds for mobile without removing data.
try:
    import discord as dm
    from typing import List as _List
except Exception:
    dm = None

# --- 1+2) Persisted multi-select ---
if dm is not None and 'TimerToggleView' in globals():
    # Re-define the mobile select to save prefs and not auto-select all.
    class MobileCategorySelect(dm.ui.Select):
        def __init__(self, parent_view):
            self.parent_view = parent_view
            opts = []
            for cat in CATEGORY_ORDER:
                opts.append(dm.SelectOption(
                    label=cat, value=cat, 
                    emoji=(EMOJI_FOR_CAT.get(cat) if 'EMOJI_FOR_CAT' in globals() else None),
                    default=(cat in parent_view.shown)  # only previously shown are preselected
                ))
            super().__init__(
                placeholder="Select categories to show",
                min_values=0,
                max_values=len(opts),
                options=opts,
                row=0,
            )
        async def callback(self, interaction: dm.Interaction):
            # Persist and refresh
            self.parent_view.shown = [c for c in CATEGORY_ORDER if c in self.values]
            try:
                # Save to DB so next open restores the same selection
                await set_user_shown_categories(interaction.guild.id, interaction.user.id, self.parent_view.shown)
            except Exception as e:
                if 'log' in globals(): log.warning(f"[mobile] save shown failed: {e}")
            await self.parent_view.refresh(interaction)

    # Override constructor to avoid defaulting to ALL when saved empty
    try:
        _orig_init_ttv = TimerToggleView.__init__  # type: ignore
        def __compact_ttv_init_mobile(self, guild: dm.Guild, user_id: int, init_show: _List[str]):
            dm.ui.View.__init__(self, timeout=300)
            self.guild = guild
            self.user_id = user_id
            # Only what was previously toggled; if none, start empty for quick focus
            self.shown = [c for c in CATEGORY_ORDER if c in (init_show or [])]
            # compact selector
            self.add_item(MobileCategorySelect(self))
            # keep All/None if defined
            try:
                self.add_item(self._make_all_button())
                self.add_item(self._make_none_button())
            except Exception:
                pass
            self.message = None
        TimerToggleView.__init__ = __compact_ttv_init_mobile  # type: ignore
        if 'log' in globals():
            log.info("[mobile] TimerToggleView persists selection + no default-all")
    except Exception as e:
        try:
            if 'log' in globals(): log.warning(f"[mobile] TimerToggleView hook failed: {e}")
        except Exception:
            pass

# --- 3) Compact timer embeds ---
try:
    __orig_build_timers = build_timer_embeds_for_categories  # type: ignore
except Exception:
    __orig_build_timers = None

async def _build_timer_embeds_compact(guild: dm.Guild, categories: _List[str]):
    # Re-implement using project helpers for compact mobile output
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
    # Sort inside each category
    for cat in grouped:
        items = grouped[cat]
        items.sort(key=lambda x: (natural_key(x[0]), natural_key(x[1])))

    embeds: List[dm.Embed] = []
    for cat in categories:
        items = grouped.get(cat, [])
        if not items:
            em = dm.Embed(title=f"{category_emoji(cat)} {cat}", description="No timers.", color=await get_category_color(gid, cat))
            embeds.append(em)
            continue
        normal: List[tuple] = []; nada_list: List[tuple] = []
        for sk, nm, tts, win in items:
            delta = tts - now; t = fmt_delta_for_list(delta)
            (nada_list if t == "-Nada" else normal).append((sk, nm, t, tts, win))
        lines: List[str] = []
        # compact one-liners: Name — `t` · Window[ · ETA HH:MM]
        for _, nm, t, ts, win_m in normal:
            win_status = window_label(now, ts, win_m)
            seg = f"• **{nm}** — `{t}` · {win_status}"
            if show_eta and (ts - now) > 0:
                try:
                    from datetime import datetime, timezone
                    seg += f" · {datetime.fromtimestamp(ts, tz=timezone.utc).strftime('ETA %H:%M UTC')}"
                except Exception:
                    pass
            lines.append(seg)
        if nada_list:
            lines.append("*Lost (-Nada)*")
            for _, nm, t, *_ in nada_list:
                lines.append(f"  · **{nm}** — `{t}`")
        desc = "\n".join(lines)[:4096]  # extra guard if lines else "No timers."
        em = dm.Embed(
            title=f"{category_emoji(cat)} {cat}",
            description=desc[:4096],
            color=await get_category_color(gid, cat)
        )
        embeds.append(em)
    return embeds[:10]

# Swap in compact builder
try:
    build_timer_embeds_for_categories = _build_timer_embeds_compact  # type: ignore
    if 'log' in globals():
        log.info("[mobile] compact timer embed builder active")
except Exception:
    pass
# ==================== END MOBILE TIMER UX ====================

# ==================== MOBILE TIMER UX — persist-only defaults + compact list (additive) ====================
# Baseline preserved. Adds:
# 1) Multi-select that preselects ONLY previously toggled categories.
# 2) Selection saves immediately and refreshes view.
# 3) Compact one-line timer embeds for mobile.
try:
    import discord as _dm
    from typing import List as _List
except Exception:
    _dm = None

# Add select without removing existing controls
if _dm is not None and 'TimerToggleView' in globals():
    class _MobileCategorySelect(_dm.ui.Select):
        def __init__(self, parent_view):
            self._parent = parent_view
            opts = []
            base = globals().get('CATEGORY_ORDER', [])
            emo = globals().get('EMOJI_FOR_CAT', {})
            for cat in base:
                opts.append(_dm.SelectOption(
                    label=cat,
                    value=cat,
                    emoji=(emo.get(cat) if isinstance(emo, dict) else None),
                    default=(cat in parent_view.shown)
                ))
            super().__init__(
                placeholder="Select categories to show",
                min_values=0,
                max_values=len(opts) or 1,
                options=opts,
                row=0,
            )
        async def callback(self, interaction: _dm.Interaction):
            sel = list(self.values)
            # Keep ordering consistent with CATEGORY_ORDER
            base = globals().get('CATEGORY_ORDER', [])
            self._parent.shown = [c for c in base if c in sel]
            try:
                await set_user_shown_categories(interaction.guild.id, interaction.user.id, self._parent.shown)
            except Exception as e:
                if 'log' in globals():
                    log.warning(f"[mobile] persist selection failed: {e}")
            await self._parent.refresh(interaction)

    try:
        _orig_init = TimerToggleView.__init__  # type: ignore
        def __patched_init(self, guild: _dm.Guild, user_id: int, init_show: _List[str]):
            # Call original to keep buttons and behavior
            _orig_init(self, guild, user_id, init_show)
            # Enforce "only previously toggled" default. If none saved, start empty.
            if not (init_show or []):
                self.shown = []
            # Inject compact select
            try:
                self.add_item(_MobileCategorySelect(self))
            except Exception as e:
                if 'log' in globals():
                    log.warning(f"[mobile] add select failed: {e}")
        TimerToggleView.__init__ = __patched_init  # type: ignore
        if 'log' in globals(): log.info("[mobile] TimerToggleView patched with persisted-only defaults and select")
    except Exception as e:
        try:
            if 'log' in globals(): log.warning(f"[mobile] patch bind failed: {e}")
        except Exception:
            pass

# Compact timer embed builder. Fallback to original on failure.
try:
    __orig_timer_builder = build_timer_embeds_for_categories  # type: ignore
except Exception:
    __orig_timer_builder = None

async def _build_timer_embeds_compact(guild, categories):
    try:
        import aiosqlite as _asq
        gid = guild.id
        show_eta = await get_show_eta(gid) if 'get_show_eta' in globals() else 0
        if not categories:
            return []
        q = ",".join("?" for _ in categories)
        async with _asq.connect(DB_PATH) as db:
            c = await db.execute(
                f"SELECT name,next_spawn_ts,category,sort_key,window_minutes FROM bosses WHERE guild_id=? AND category IN ({q})",
                (gid, *[norm_cat(c) for c in categories])
            )
            rows = await c.fetchall()
        now = now_ts()
        grouped = {k: [] for k in categories}
        for name, ts, cat, sk, win in rows:
            nc = norm_cat(cat)
            if nc in grouped:
                grouped[nc].append((sk or "", name, int(ts), int(win)))
        for cat in grouped:
            grouped[cat].sort(key=lambda x: (natural_key(x[0]), natural_key(x[1])))
        embeds = []
        for cat in categories:
            items = grouped.get(cat, [])
            lines = []
            nada = []
            for sk, nm, tts, win in items:
                delta = tts - now
                t = fmt_delta_for_list(delta)
                if t == "-Nada":
                    nada.append(f"· **{nm}** — `{t}`")
                    continue
                stat = window_label(now, tts, win)
                seg = f"• **{nm}** — `{t}` · {stat}"
                if show_eta and delta > 0:
                    from datetime import datetime, timezone
                    seg += f" · {datetime.fromtimestamp(tts, tz=timezone.utc).strftime('ETA %H:%M UTC')}"
                lines.append(seg)
            if nada:
                lines.append("*Lost (-Nada)*")
                lines.extend(nada)
            em = _dm.Embed(
                title=f"{category_emoji(cat)} {cat}",
                description=("\n".join(lines) if lines else "No timers.")[:4096],
                color=await get_category_color(gid, cat)
            )
            embeds.append(em)
        return embeds[:10]
    except Exception as e:
        if 'log' in globals():
            log.warning(f"[mobile] compact builder failed, falling back: {e}")
        if __orig_timer_builder:
            return await __orig_timer_builder(guild, categories)
        return []

try:
    build_timer_embeds_for_categories = _build_timer_embeds_compact  # type: ignore
    if 'log' in globals(): log.info("[mobile] compact embed builder bound")
except Exception:
    pass
# ==================== END MOBILE TIMER UX patch ====================

# ==================== MOBILE TIMERS: count-only "Missing" (additive, non-destructive) ====================
# Keeps all baseline features and layout. Only changes the per-category embed body to replace
# long -Nada lists with a single "Missing: N" line. No buttons. No refresh override.
try:
    import discord as dm
    import aiosqlite
    from typing import List, Dict, Tuple
except Exception:
    dm = None

# Keep a pointer to the original in case of fallback
try:
    __orig_builder_for_mobile = build_timer_embeds_for_categories  # type: ignore
except Exception:
    __orig_builder_for_mobile = None

async def _build_timer_embeds_count_missing_only(guild: dm.Guild, categories: List[str]) -> List[dm.Embed]:
    try:
        gid = guild.id
        show_eta = await get_show_eta(gid) if 'get_show_eta' in globals() else 0
        if not categories:
            return []
        # fetch all bosses for the selected categories
        async with aiosqlite.connect(DB_PATH) as db:
            q = ",".join("?" for _ in categories)
            cur = await db.execute(
                f"SELECT name,next_spawn_ts,category,sort_key,window_minutes FROM bosses "
                f"WHERE guild_id=? AND category IN ({q})",
                (gid, *[norm_cat(c) for c in categories])
            )
            rows = await cur.fetchall()
        now = now_ts()
        # group by requested labels preserving order
        grouped: Dict[str, List[Tuple[str,str,int,int]]] = {c: [] for c in categories}
        for name, ts, cat, sk, win in rows:
            target = None
            nc = norm_cat(cat)
            for lbl in categories:
                if norm_cat(lbl) == nc:
                    target = lbl; break
            if target is None:
                continue
            grouped[target].append((sk or "", name, int(ts), int(win)))
        # sort groups
        for k in grouped:
            grouped[k].sort(key=lambda x: (natural_key(x[0]), natural_key(x[1])))

        embeds: List[dm.Embed] = []
        for cat in categories:
            items = grouped.get(cat, [])
            if not items:
                em = dm.Embed(
                    title=f"{category_emoji(cat)} {cat}",
                    description="No timers.",
                    color=await get_category_color(gid, cat)
                )
                embeds.append(em); continue

            lines: List[str] = []
            missing_count = 0
            for sk, nm, tts, win in items:
                delta = tts - now
                t = fmt_delta_for_list(delta)
                if t == "-Nada":
                    missing_count += 1
                    continue
                win_status = window_label(now, tts, win)
                seg = f"• **{nm}** `{t}` · {win_status}"
                if show_eta and delta > 0:
                    from datetime import datetime, timezone
                    seg += f" · {datetime.fromtimestamp(tts, tz=timezone.utc).strftime('ETA %H:%M UTC')}"
                lines.append(seg)

            if missing_count:
                # Only "Missing", no "-Nada" mention
                lines.append(f"*Missing:* **{missing_count}**")

            desc = "\n".join(lines)[:4096]  # extra guard if lines else "No timers."
            em = dm.Embed(
                title=f"{category_emoji(cat)} {cat}",
                description=desc[:4096],
                color=await get_category_color(gid, cat)
            )
            embeds.append(em)
        return embeds[:10]
    except Exception as e:
        if 'log' in globals(): log.warning(f"[mobile] count-missing-only failed: {e}")
        if __orig_builder_for_mobile:
            return await __orig_builder_for_mobile(guild, categories)
        return []

# Bind the builder without touching views or controls
try:
    build_timer_embeds_for_categories = _build_timer_embeds_count_missing_only  # type: ignore
    if 'log' in globals(): log.info("[mobile] -Nada list replaced with count-only 'Missing' line")
except Exception:
    pass
# ==================== END MOBILE TIMERS patch ====================






















if __name__ == "__main__":
    asyncio.run(main())
