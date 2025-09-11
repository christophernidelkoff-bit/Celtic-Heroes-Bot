# bot.py
"""
Celtic Heroes Boss Tracker Discord Bot
- Add custom boss timers (spawn intervals in minutes).
- Kill/Reset via:
    1) Normal command:    !boss killed <id_or_name>
    2) Shorthand command: !<BossName>   e.g., !Aggragoth  or  !"Forest Lord"
- Pre-spawn announcements (per-boss).
- Per-guild trusted role for who can report kills.
- Persistent storage via SQLite.
- Simple prefix commands (no slash registration needed).
"""

import os
import asyncio
import logging
import aiosqlite
from datetime import datetime, timezone
from typing import Optional, Tuple

import discord
from discord.ext import commands, tasks
from dotenv import load_dotenv

# ------------ Config ------------
load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN")
if not TOKEN:
    raise SystemExit("DISCORD_TOKEN environment variable required. See README.")

COMMAND_PREFIX = "!"
CHECK_INTERVAL_SECONDS = 15
DB_PATH = "bosses.db"
LOG_LEVEL = logging.INFO

intents = discord.Intents.default()
intents.message_content = True

logger = logging.getLogger("bossbot")
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")

bot = commands.Bot(command_prefix=COMMAND_PREFIX, intents=intents, help_command=None)

# Reserved roots so our shorthand handler doesn't intercept real commands
RESERVED_TRIGGERS = {
    "help", "boss", "seed_import"  # add any other top-level triggers here
}

# ------------ Utilities ------------
def now_ts() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp())

def format_timedelta_seconds(seconds: int) -> str:
    if seconds <= 0:
        return "0s"
    parts = []
    days, rem = divmod(seconds, 86400)
    if days:
        parts.append(f"{days}d")
    hours, rem = divmod(rem, 3600)
    if hours:
        parts.append(f"{hours}h")
    minutes, secs = divmod(rem, 60)
    if minutes:
        parts.append(f"{minutes}m")
    if secs and not parts:
        parts.append(f"{secs}s")
    return " ".join(parts)

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS bosses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                channel_id INTEGER,
                name TEXT NOT NULL,
                spawn_minutes INTEGER NOT NULL,
                next_spawn_ts INTEGER NOT NULL,
                pre_announce_min INTEGER DEFAULT 10,
                trusted_role_id INTEGER DEFAULT NULL,
                created_by INTEGER,
                notes TEXT DEFAULT ''
            );
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS guild_config (
                guild_id INTEGER PRIMARY KEY,
                default_channel INTEGER DEFAULT NULL
            );
            """
        )
        await db.commit()
    logger.info("Database initialized.")

# ------------ Boss Resolver (ID or Name) ------------
async def resolve_boss_by_identifier(ctx_or_msg, identifier: str) -> Tuple[Optional[tuple], Optional[str]]:
    """
    Resolve a boss in the current guild by numeric ID or by name (case-insensitive).
    Returns (row, error). Row = (id, name, spawn_minutes).
    """
    guild_id = ctx_or_msg.guild.id

    # Try numeric ID
    try:
        bid = int(identifier)
        async with aiosqlite.connect(DB_PATH) as db:
            c = await db.execute(
                "SELECT id, name, spawn_minutes FROM bosses WHERE id = ? AND guild_id = ?",
                (bid, guild_id),
            )
            r = await c.fetchone()
            if r:
                return r, None
    except ValueError:
        pass

    # Name matching: exact -> prefix -> contains
    name = identifier.strip().lower()
    like_prefix = f"{name}%"
    like_contains = f"%{name}%"
    async with aiosqlite.connect(DB_PATH) as db:
        # exact
        c = await db.execute(
            "SELECT id, name, spawn_minutes FROM bosses WHERE guild_id = ? AND LOWER(name) = ?",
            (guild_id, name),
        )
        exact = await c.fetchall()
        if len(exact) == 1:
            return exact[0], None
        if len(exact) > 1:
            opts = ", ".join(f'{row[1]}(ID {row[0]})' for row in exact[:10])
            return None, f"Multiple exact matches: {opts}. Please specify with the ID."

        # prefix
        c = await db.execute(
            "SELECT id, name, spawn_minutes FROM bosses WHERE guild_id = ? AND LOWER(name) LIKE ?",
            (guild_id, like_prefix),
        )
        pref = await c.fetchall()
        if len(pref) == 1:
            return pref[0], None
        if len(pref) > 1:
            opts = ", ".join(f'{row[1]}(ID {row[0]})' for row in pref[:10])
            return None, f"Multiple matches starting with '{identifier}': {opts}. Please be more specific or use the ID."

        # contains
        c = await db.execute(
            "SELECT id, name, spawn_minutes FROM bosses WHERE guild_id = ? AND LOWER(name) LIKE ?",
            (guild_id, like_contains),
        )
        cont = await c.fetchall()
        if len(cont) == 1:
            return cont[0], None
        if len(cont) > 1:
            opts = ", ".join(f'{row[1]}(ID {row[0]})' for row in cont[:10])
            return None, f"Found multiple bosses containing '{identifier}': {opts}. Please specify with the ID."

    return None, f"No boss found for '{identifier}'. Use `!boss list` to see IDs."

# ------------ Background task ------------
@tasks.loop(seconds=CHECK_INTERVAL_SECONDS)
async def check_timers():
    async with aiosqlite.connect(DB_PATH) as db:
        now = now_ts()
        # Spawn announces
        cursor = await db.execute(
            "SELECT id, guild_id, channel_id, name, spawn_minutes, next_spawn_ts, pre_announce_min FROM bosses WHERE next_spawn_ts <= ?",
            (now,),
        )
        for boss_id, guild_id, channel_id, name, spawn_minutes, next_spawn_ts, pre_announce_min in await cursor.fetchall():
            guild = bot.get_guild(guild_id)
            if guild:
                channel = guild.get_channel(channel_id) if channel_id else None
                if channel is None:
                    c = await db.execute("SELECT default_channel FROM guild_config WHERE guild_id = ?", (guild_id,))
                    cfg = await c.fetchone()
                    if cfg and cfg[0]:
                        channel = guild.get_channel(cfg[0])
                if channel:
                    try:
                        await channel.send(
                            f":skull_and_crossbones: **{name}** has spawned! (ID: {boss_id})\n"
                            f"Use `{COMMAND_PREFIX}boss killed {boss_id}` or `{COMMAND_PREFIX}{name}` to reset."
                        )
                    except Exception as e:
                        logger.exception(f"Failed to announce spawn: {e}")
            # roll timer forward
            new_ts = now + spawn_minutes * 60
            await db.execute("UPDATE bosses SET next_spawn_ts = ? WHERE id = ?", (new_ts, boss_id))

        # Pre-announces
        cursor = await db.execute(
            "SELECT id, guild_id, channel_id, name, spawn_minutes, next_spawn_ts, pre_announce_min FROM bosses WHERE next_spawn_ts > ?",
            (now,),
        )
        for boss_id, guild_id, channel_id, name, spawn_minutes, next_spawn_ts, pre_announce_min in await cursor.fetchall():
            pre_ts = next_spawn_ts - (pre_announce_min * 60)
            if pre_ts <= now < next_spawn_ts:
                key = f"{guild_id}:{boss_id}:pre"
                if not hasattr(bot, "_pre_announced"):
                    bot._pre_announced = set()
                if key in bot._pre_announced:
                    continue
                bot._pre_announced.add(key)

                guild = bot.get_guild(guild_id)
                if not guild:
                    continue
                channel = guild.get_channel(channel_id) if channel_id else None
                if channel is None:
                    c = await db.execute("SELECT default_channel FROM guild_config WHERE guild_id = ?", (guild_id,))
                    cfg = await c.fetchone()
                    if cfg and cfg[0]:
                        channel = guild.get_channel(cfg[0])
                if channel:
                    time_left = format_timedelta_seconds(next_spawn_ts - now)
                    try:
                        await channel.send(
                            f":alarm_clock: **{name}** spawning in {time_left}! (ID: {boss_id})\n"
                            f"Reset on kill with `{COMMAND_PREFIX}{name}`."
                        )
                    except Exception as e:
                        logger.exception(f"Failed to pre-announce: {e}")

    # Clean preannounce cache periodically
    if not hasattr(bot, "_pre_announced_cleanup_at"):
        bot._pre_announced_cleanup_at = now_ts() + 900
    if now_ts() > bot._pre_announced_cleanup_at:
        bot._pre_announced = set()
        bot._pre_announced_cleanup_at = now_ts() + 900

# ------------ Events ------------
@bot.event
async def on_ready():
    logger.info(f"Logged in as {bot.user} (ID: {bot.user.id})")
    await init_db()
    if not check_timers.is_running():
        check_timers.start()

@bot.event
async def on_message(message: discord.Message):
    """Shorthand handler: !<BossName> resets timer if permissions allow."""
    if message.author.bot or not message.guild:
        return

    content = (message.content or "").strip()
    prefix = COMMAND_PREFIX

    if content.startswith(prefix) and len(content) > len(prefix):
        shorthand = content[len(prefix):].strip()  # everything after "!"
        # if it starts with a known command root, let normal commands handle it
        first_token = shorthand.split(" ", 1)[0].lower()
        if first_token not in RESERVED_TRIGGERS:
            identifier = shorthand.strip().strip('"').strip("'")
            result, err = await resolve_boss_by_identifier(message, identifier)
            if result and not err:
                boss_id, name, spawn_minutes = result
                # permission check
                if await user_has_trusted_role(message.author, message.guild.id, boss_id=boss_id):
                    async with aiosqlite.connect(DB_PATH) as db:
                        new_ts = now_ts() + spawn_minutes * 60
                        await db.execute("UPDATE bosses SET next_spawn_ts = ? WHERE id = ?", (new_ts, boss_id))
                        await db.commit()
                    await message.channel.send(
                        f":crossed_swords: **{name}** killed (shorthand). "
                        f"Next spawn in {format_timedelta_seconds(spawn_minutes*60)}."
                    )
                    return
                else:
                    await message.channel.send(
                        ":no_entry: You don't have permission to reset this boss. "
                        f"Ask an admin to set a trusted role with `{COMMAND_PREFIX}boss setrole @Role`."
                    )
                    return
            # If not found or ambiguous, fall through to normal command handling

    await bot.process_commands(message)

# ------------ Permission helper ------------
async def user_has_trusted_role(member: discord.Member, guild_id: int, boss_id: Optional[int] = None) -> bool:
    if member.guild_permissions.administrator:
        return True
    async with aiosqlite.connect(DB_PATH) as db:
        if boss_id:
            c = await db.execute("SELECT trusted_role_id FROM bosses WHERE id = ? AND guild_id = ?", (boss_id, guild_id))
            r = await c.fetchone()
            if r and r[0]:
                role_id = r[0]
                return any(role.id == role_id for role in member.roles)
    return member.guild_permissions.manage_messages

# ------------ Commands ------------
@bot.command(name="help")
async def help_cmd(ctx):
    msg = f"""**Boss Tracker Help**

**Shorthand reset**
`{COMMAND_PREFIX}<BossName>` — Reset a boss by name quickly. Examples:
`{COMMAND_PREFIX}Aggragoth`, `{COMMAND_PREFIX}"Forest Lord"`.
(Requires trusted role or mod/admin perms.)

**Boss commands**
`{COMMAND_PREFIX}boss add "Boss Name" <spawn_minutes> [#channel_or_id] [pre_announce_minutes]`
`{COMMAND_PREFIX}boss list`
`{COMMAND_PREFIX}boss info <id>`
`{COMMAND_PREFIX}boss killed <id_or_name>`
`{COMMAND_PREFIX}boss delete <id>`  (Manage Server)
`{COMMAND_PREFIX}boss setrole @Role | none`  (Manage Server)
`{COMMAND_PREFIX}boss setchannel #channel`   (Manage Server)
`{COMMAND_PREFIX}boss edit <id> spawn_minutes|pre_announce_min|name <value>`
`{COMMAND_PREFIX}boss ping <id>`
`{COMMAND_PREFIX}seed_import <json_url>` (Admin)

Tip: Use `!boss list` to see IDs.
"""
    await ctx.send(msg)

@bot.group(name="boss", invoke_without_command=True)
async def boss_group(ctx):
    await ctx.send(f"Use `{COMMAND_PREFIX}help` to see boss commands.")

@boss_group.command(name="add")
async def boss_add(ctx, name: str, spawn_minutes: int, channel: Optional[str] = None, pre_announce_min: int = 10):
    channel_id = None
    if channel:
        if channel.startswith("<#") and channel.endswith(">"):
            channel_id = int(channel[2:-1])
        else:
            try:
                channel_id = int(channel)
            except ValueError:
                found = discord.utils.get(ctx.guild.channels, name=channel.strip("#"))
                if found:
                    channel_id = found.id

    next_spawn = now_ts() + spawn_minutes * 60
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO bosses (guild_id, channel_id, name, spawn_minutes, next_spawn_ts, pre_announce_min, created_by) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (ctx.guild.id, channel_id, name, spawn_minutes, next_spawn, pre_announce_min, ctx.author.id),
        )
        await db.commit()
        cursor = await db.execute("SELECT last_insert_rowid()")
        row = await cursor.fetchone()
        boss_id = row[0] if row else None
    await ctx.send(f":white_check_mark: Added **{name}** (ID: {boss_id}). Every {spawn_minutes}m. "
                   f"Next spawn in {format_timedelta_seconds(spawn_minutes*60)}.")

@boss_group.command(name="list")
async def boss_list(ctx):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT id, name, spawn_minutes, next_spawn_ts, channel_id, pre_announce_min FROM bosses WHERE guild_id = ? ORDER BY next_spawn_ts",
            (ctx.guild.id,),
        )
        rows = await cursor.fetchall()
    if not rows:
        await ctx.send("No bosses configured. Add one with `!boss add`.")
        return
    now = now_ts()
    lines = []
    for boss_id, name, spawn_minutes, next_spawn_ts, channel_id, pre_announce_min in rows:
        left = next_spawn_ts - now
        ch = f"<#{channel_id}>" if channel_id else "Default channel"
        lines.append(f"**{boss_id}** | {name} | every {spawn_minutes}m | next in {format_timedelta_seconds(left)} | preannounce {pre_announce_min}m | {ch}")
    await ctx.send("Bosses:\n" + "\n".join(lines))

@boss_group.command(name="info")
async def boss_info(ctx, boss_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT id, name, spawn_minutes, next_spawn_ts, channel_id, pre_announce_min, trusted_role_id, notes FROM bosses WHERE guild_id = ? AND id = ?",
            (ctx.guild.id, boss_id),
        )
        r = await cursor.fetchone()
    if not r:
        await ctx.send(f"No boss with ID {boss_id} found.")
        return
    id_, name, spawn_minutes, next_spawn_ts, channel_id, pre_announce_min, trusted_role_id, notes = r
    left = next_spawn_ts - now_ts()
    ch = f"<#{channel_id}>" if channel_id else "Default channel"
    role_text = f"<@&{trusted_role_id}>" if trusted_role_id else "None"
    msg = (
        f"**{id_}** | {name}\n"
        f"Spawn interval: {spawn_minutes} minutes\n"
        f"Next spawn: in {format_timedelta_seconds(left)} "
        f"({datetime.fromtimestamp(next_spawn_ts, tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')})\n"
        f"Pre-announce: {pre_announce_min} minutes\n"
        f"Announcement channel: {ch}\n"
        f"Trusted role for kill reporting: {role_text}\n"
        f"Notes: {notes or 'None'}"
    )
    await ctx.send(msg)

@boss_group.command(name="killed")
async def boss_killed_cmd(ctx, *, identifier: str):
    """Accepts ID or Name: !boss killed 12  |  !boss killed Aggragoth  |  !boss killed "Forest Lord" """
    result, err = await resolve_boss_by_identifier(ctx, identifier)
    if err:
        await ctx.send(f":no_entry: {err}")
        return
    boss_id, name, spawn_minutes = result
    if not await user_has_trusted_role(ctx.author, ctx.guild.id, boss_id=boss_id):
        await ctx.send(":no_entry: You don't have permission to report kills for this boss. "
                       f"Admins can set a trusted role with `{COMMAND_PREFIX}boss setrole @Role`.")
        return
    async with aiosqlite.connect(DB_PATH) as db:
        new_ts = now_ts() + spawn_minutes * 60
        await db.execute("UPDATE bosses SET next_spawn_ts = ? WHERE id = ?", (new_ts, boss_id))
        await db.commit()
    await ctx.send(f":crossed_swords: Reported **{name}** killed. "
                   f"Next spawn in {format_timedelta_seconds(spawn_minutes*60)}.")

@boss_group.command(name="delete")
@commands.has_permissions(manage_guild=True)
async def boss_delete(ctx, boss_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM bosses WHERE id = ? AND guild_id = ?", (boss_id, ctx.guild.id))
        await db.commit()
    await ctx.send(f":wastebasket: Deleted boss with ID {boss_id} if it existed.")

@boss_group.command(name="setrole")
@commands.has_permissions(manage_guild=True)
async def boss_setrole(ctx, role: str):
    if role.lower() in ("none", "clear"):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE bosses SET trusted_role_id = NULL WHERE guild_id = ?", (ctx.guild.id,))
            await db.commit()
        await ctx.send(":white_check_mark: Cleared trusted role for boss reports.")
        return
    r_obj = None
    if role.startswith("<@&") and role.endswith(">"):
        rid = int(role[3:-1])
        r_obj = ctx.guild.get_role(rid)
    else:
        r_obj = discord.utils.get(ctx.guild.roles, name=role)
    if r_obj is None:
        await ctx.send("Role not found. Mention it like `@Raid Lead` or use exact name.")
        return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE bosses SET trusted_role_id = ? WHERE guild_id = ?", (r_obj.id, ctx.guild.id))
        await db.commit()
    await ctx.send(f":white_check_mark: Set trusted role to **{r_obj.name}**.")

@boss_group.command(name="setchannel")
@commands.has_permissions(manage_guild=True)
async def boss_setchannel(ctx, channel: str):
    channel_id = None
    if channel.startswith("<#") and channel.endswith(">"):
        channel_id = int(channel[2:-1])
    else:
        try:
            channel_id = int(channel)
        except ValueError:
            found = discord.utils.get(ctx.guild.channels, name=channel.strip("#"))
            if found:
                channel_id = found.id
    if channel_id is None:
        await ctx.send("Channel not found. Mention a channel or provide its ID.")
        return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO guild_config (guild_id, default_channel) VALUES (?, ?)",
            (ctx.guild.id, channel_id),
        )
        await db.commit()
    await ctx.send(f":white_check_mark: Default announcements channel set to <#{channel_id}>.")

@boss_group.command(name="edit")
@commands.has_permissions(manage_guild=True)
async def boss_edit(ctx, boss_id: int, field: str, value: str):
    allowed = ("spawn_minutes", "pre_announce_min", "name")
    if field not in allowed:
        await ctx.send(f"Editable fields: {', '.join(allowed)}")
        return
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT id FROM bosses WHERE id = ? AND guild_id = ?", (boss_id, ctx.guild.id))
        if not await c.fetchone():
            await ctx.send("Boss not found.")
            return
        if field in ("spawn_minutes", "pre_announce_min"):
            try:
                v = int(value)
            except ValueError:
                await ctx.send("Value must be an integer for this field.")
                return
            await db.execute(f"UPDATE bosses SET {field} = ? WHERE id = ?", (v, boss_id))
        else:
            await db.execute(f"UPDATE bosses SET {field} = ? WHERE id = ?", (value, boss_id))
        await db.commit()
    await ctx.send(":white_check_mark: Boss updated. If you changed spawn_minutes and want it to take effect now, run `!boss killed {boss_id}`.")

@boss_group.command(name="ping")
async def boss_ping(ctx, boss_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT name FROM bosses WHERE id = ? AND guild_id = ?", (boss_id, ctx.guild.id))
        r = await c.fetchone()
    if not r:
        await ctx.send("Boss not found.")
        return
    name = r[0]
    await ctx.send(f":sound: Manual ping: **{name}** (ID: {boss_id}). Use `{COMMAND_PREFIX}boss killed {boss_id}` or `{COMMAND_PREFIX}{name}` to reset.")

# ------------ Seed import (Admin) ------------
@bot.command(name="seed_import")
@commands.has_permissions(administrator=True)
async def seed_import(ctx, json_url: str):
    import aiohttp
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(json_url) as resp:
                if resp.status != 200:
                    await ctx.send(f"Failed to fetch JSON (status {resp.status}).")
                    return
                data = await resp.json()
    except Exception as e:
        await ctx.send(f"Failed to fetch/parse JSON: {e}")
        return
    inserted = 0
    async with aiosqlite.connect(DB_PATH) as db:
        for item in data:
            name = item.get("name")
            if not name:
                continue
            spawn_minutes = int(item.get("spawn_minutes", 60))
            channel_id = item.get("channel_id")
            pre = int(item.get("pre_announce_min", 10))
            next_spawn = now_ts() + spawn_minutes * 60
            await db.execute(
                "INSERT INTO bosses (guild_id, channel_id, name, spawn_minutes, next_spawn_ts, pre_announce_min, created_by) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (ctx.guild.id, channel_id, name, spawn_minutes, next_spawn, pre, ctx.author.id),
            )
            inserted += 1
        await db.commit()
    await ctx.send(f":white_check_mark: Imported {inserted} bosses.")

# ------------ Error handlers ------------
@boss_delete.error
async def on_boss_delete_error(ctx, error):
    if isinstance(error, commands.MissingPermissions):
        await ctx.send(":no_entry: You need Manage Server permissions to delete bosses.")

@seed_import.error
async def on_seed_error(ctx, error):
    if isinstance(error, commands.MissingPermissions):
        await ctx.send(":no_entry: Only server admins can perform seed imports.")

# ------------ Run ------------
if __name__ == "__main__":
    bot.run(TOKEN)
