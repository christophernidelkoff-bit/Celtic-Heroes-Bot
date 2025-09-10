# bot.py
"""
Celtic Heroes Boss Tracker Discord Bot
Features:
- Add custom boss timers (spawn intervals in minutes).
- Kill/Reset command to restart a boss timer.
- Pre-spawn announcements (configurable per-boss).
- Per-guild trusted role for who can report kills.
- Persistent storage via SQLite.
- Simple prefix commands (no slash registration needed).
"""

import os
import asyncio
import logging
import aiosqlite
from datetime import datetime, timedelta, timezone
from typing import Optional

import discord
from discord.ext import commands, tasks
from dotenv import load_dotenv

# Load .env
load_dotenv()

TOKEN = os.getenv("DISCORD_TOKEN")
if not TOKEN:
    raise SystemExit("DISCORD_TOKEN environment variable required. See README.")

# Bot config
COMMAND_PREFIX = "!"
CHECK_INTERVAL_SECONDS = 15  # background check interval
DB_PATH = "bosses.db"
LOG_LEVEL = logging.INFO

intents = discord.Intents.default()
intents.message_content = True

logger = logging.getLogger("bossbot")
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")

bot = commands.Bot(command_prefix=COMMAND_PREFIX, intents=intents, help_command=None)


# ---------- Utilities ----------
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
        # show seconds only if <1min remaining or nothing else to show
        parts.append(f"{secs}s")
    return " ".join(parts)


# ---------- Database ----------
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


# ---------- Background task ----------
@tasks.loop(seconds=CHECK_INTERVAL_SECONDS)
async def check_timers():
    """Runs periodically to check for bosses that have spawned or need pre-announcement."""
    async with aiosqlite.connect(DB_PATH) as db:
        now = now_ts()
        # Get bosses that need announcements for spawn (next_spawn_ts <= now)
        cursor = await db.execute(
            "SELECT id, guild_id, channel_id, name, spawn_minutes, next_spawn_ts, pre_announce_min FROM bosses WHERE next_spawn_ts <= ?",
            (now,),
        )
        rows = await cursor.fetchall()
        for row in rows:
            boss_id, guild_id, channel_id, name, spawn_minutes, next_spawn_ts, pre_announce_min = row
            # announce spawn to channel (if channel exists)
            guild = bot.get_guild(guild_id)
            if guild is None:
                continue
            channel = None
            if channel_id:
                channel = guild.get_channel(channel_id)
            else:
                # try guild default config
                c = await db.execute("SELECT default_channel FROM guild_config WHERE guild_id = ?", (guild_id,))
                cfg = await c.fetchone()
                if cfg and cfg[0]:
                    channel = guild.get_channel(cfg[0])

            if channel is None:
                logger.warning(f"Boss {name} spawned but no channel configured for guild {guild_id}.")
            else:
                try:
                    await channel.send(f":skull_and_crossbones: **{name}** has spawned! (ID: {boss_id})\nUse `{COMMAND_PREFIX}boss killed {boss_id}` to reset the timer when killed.")
                except Exception as e:
                    logger.exception(f"Failed to announce spawn in guild {guild_id}: {e}")

            # update next_spawn_ts = now + spawn_minutes*60
            new_ts = now + spawn_minutes * 60
            await db.execute("UPDATE bosses SET next_spawn_ts = ? WHERE id = ?", (new_ts, boss_id))

        # Handle pre-announcements: find bosses where next_spawn - pre_announce <= now < next_spawn
        cursor = await db.execute(
            "SELECT id, guild_id, channel_id, name, spawn_minutes, next_spawn_ts, pre_announce_min FROM bosses WHERE next_spawn_ts > ?",
            (now,),
        )
        rows = await cursor.fetchall()
        for row in rows:
            boss_id, guild_id, channel_id, name, spawn_minutes, next_spawn_ts, pre_announce_min = row
            pre_ts = next_spawn_ts - (pre_announce_min * 60)
            # we want to announce if pre_ts <= now < next_spawn_ts
            if pre_ts <= now < next_spawn_ts:
                # but avoid duplicate pre-announces — we'll mark pre-announced by storing a very small negative pre_announce_min temporarily
                # Simpler approach: create a transient "announced_pre" cache per run.
                # We'll keep a set in bot._pre_announced to avoid multiple announces within same minute/window
                key = f"{guild_id}:{boss_id}:pre"
                if not hasattr(bot, "_pre_announced"):
                    bot._pre_announced = set()
                if key in bot._pre_announced:
                    continue
                bot._pre_announced.add(key)
                guild = bot.get_guild(guild_id)
                if guild is None:
                    continue
                channel = None
                if channel_id:
                    channel = guild.get_channel(channel_id)
                else:
                    c = await db.execute("SELECT default_channel FROM guild_config WHERE guild_id = ?", (guild_id,))
                    cfg = await c.fetchone()
                    if cfg and cfg[0]:
                        channel = guild.get_channel(cfg[0])
                if channel is not None:
                    time_left = format_timedelta_seconds(next_spawn_ts - now)
                    try:
                        await channel.send(f":alarm_clock: **{name}** spawning in {time_left}! (ID: {boss_id}). Prepare! Use `{COMMAND_PREFIX}boss killed {boss_id}` when it's down.")
                    except Exception as e:
                        logger.exception(f"Failed to send pre-announcement for {name}: {e}")

    # clean pre_announced set occasionally (older than 15 minutes)
    if hasattr(bot, "_pre_announced_cleanup_at"):
        if now_ts() > bot._pre_announced_cleanup_at:
            bot._pre_announced = set()
            bot._pre_announced_cleanup_at = now_ts() + 900
    else:
        bot._pre_announced_cleanup_at = now_ts() + 900


# ---------- Commands ----------
@bot.event
async def on_ready():
    logger.info(f"Logged in as {bot.user} (ID: {bot.user.id})")
    await init_db()
    if not check_timers.is_running():
        check_timers.start()


@bot.command(name="help")
async def help_cmd(ctx):
    msg = f"""**Boss Tracker Help**
Prefix commands (also use `{COMMAND_PREFIX}` before each):
`{COMMAND_PREFIX}boss add "Boss Name" <spawn_minutes> [#channel_or_channel_id] [pre_announce_minutes]` - Add a boss timer. Example: `{COMMAND_PREFIX}boss add "Forest Lord" 120 #boss-timers 10`
`{COMMAND_PREFIX}boss list` - List active bosses for this server.
`{COMMAND_PREFIX}boss info <id>` - Show details for boss ID.
`{COMMAND_PREFIX}boss killed <id>` - Report boss killed and reset its timer to now + spawn_minutes. (Requires trusted role if set)
`{COMMAND_PREFIX}boss delete <id>` - Remove a boss from the list.
`{COMMAND_PREFIX}boss setrole @Role` - Set the trusted role for who can report kills. Use `none` to clear.
`{COMMAND_PREFIX}boss setchannel #channel` - Set default channel for announcements if not set per-boss.
`{COMMAND_PREFIX}boss edit <id> spawn_minutes|pre_announce <value>` - Edit boss timing fields.
`{COMMAND_PREFIX}boss ping <id>` - Force announce (simulate spawn).
"""
    await ctx.send(msg)


@bot.group(name="boss", invoke_without_command=True)
async def boss_group(ctx):
    await ctx.send(f"Use `{COMMAND_PREFIX}help` to see boss commands.")


@boss_group.command(name="add")
async def boss_add(ctx, name: str, spawn_minutes: int, channel: Optional[str] = None, pre_announce_min: int = 10):
    """
    Usage: !boss add "Boss Name" 120 #channel 10
    channel optional can be the mention like <#id> or the literal channel id string or omitted.
    """
    # parse channel
    channel_id = None
    if channel:
        # If channel mention like <#123>
        if channel.startswith("<#") and channel.endswith(">"):
            channel_id = int(channel[2:-1])
        else:
            try:
                channel_id = int(channel)
            except ValueError:
                # maybe passed a name; attempt to find
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
    await ctx.send(f":white_check_mark: Added boss **{name}** (ID: {boss_id}). Spawns every {spawn_minutes} minutes. Next spawn in {format_timedelta_seconds(spawn_minutes*60)}.")


@boss_group.command(name="list")
async def boss_list(ctx):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT id, name, spawn_minutes, next_spawn_ts, channel_id, pre_announce_min FROM bosses WHERE guild_id = ? ORDER BY next_spawn_ts", (ctx.guild.id,))
        rows = await cursor.fetchall()
    if not rows:
        await ctx.send("No bosses configured for this server. Add one with `!boss add`.")
        return
    lines = []
    now = now_ts()
    for r in rows:
        boss_id, name, spawn_minutes, next_spawn_ts, channel_id, pre_announce_min = r
        left = next_spawn_ts - now
        ch = f"<#{channel_id}>" if channel_id else "Default channel"
        lines.append(f"**{boss_id}** | {name} | every {spawn_minutes}m | next in {format_timedelta_seconds(left)} | preannounce {pre_announce_min}m | {ch}")
    out = "Bosses:\n" + "\n".join(lines)
    await ctx.send(out)


@boss_group.command(name="info")
async def boss_info(ctx, boss_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT id, name, spawn_minutes, next_spawn_ts, channel_id, pre_announce_min, trusted_role_id, notes FROM bosses WHERE guild_id = ? AND id = ?", (ctx.guild.id, boss_id))
        r = await cursor.fetchone()
    if not r:
        await ctx.send(f"No boss with ID {boss_id} found in this server.")
        return
    id_, name, spawn_minutes, next_spawn_ts, channel_id, pre_announce_min, trusted_role_id, notes = r
    now = now_ts()
    left = next_spawn_ts - now
    ch = f"<#{channel_id}>" if channel_id else "Default channel"
    role_text = f"<@&{trusted_role_id}>" if trusted_role_id else "None"
    msg = (
        f"**{id_}** | {name}\n"
        f"Spawn interval: {spawn_minutes} minutes\n"
        f"Next spawn: in {format_timedelta_seconds(left)} ({datetime.fromtimestamp(next_spawn_ts, tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')})\n"
        f"Pre-announce: {pre_announce_min} minutes\n"
        f"Announcement channel: {ch}\n"
        f"Trusted role for kill reporting: {role_text}\n"
        f"Notes: {notes or 'None'}"
    )
    await ctx.send(msg)


async def user_has_trusted_role(member: discord.Member, guild_id: int, boss_id: Optional[int] = None) -> bool:
    # If boss has a trusted_role_id, require that role. Else if guild has a default role? For simplicity, if boss trusts role set enforce it; otherwise allow ops (manage_guild or manage_messages).
    if member.guild_permissions.administrator:
        return True
    async with aiosqlite.connect(DB_PATH) as db:
        if boss_id:
            c = await db.execute("SELECT trusted_role_id FROM bosses WHERE id = ? AND guild_id = ?", (boss_id, guild_id))
            r = await c.fetchone()
            if r and r[0]:
                role_id = r[0]
                return any(r.id == role_id for r in member.roles)
    # fallback: allow members with Manage Messages permission to report
    return member.guild_permissions.manage_messages


@boss_group.command(name="killed")
async def boss_killed(ctx, boss_id: int):
    # Check trust
    if not await user_has_trusted_role(ctx.author, ctx.guild.id, boss_id=boss_id):
        await ctx.send(":no_entry: You don't have permission to report kills for this boss. Server admins can set a trusted role with `!boss setrole @Role` or grant Manage Messages permission.")
        return
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT id, name, spawn_minutes FROM bosses WHERE id = ? AND guild_id = ?", (boss_id, ctx.guild.id))
        r = await c.fetchone()
        if not r:
            await ctx.send(f"No boss with ID {boss_id} found.")
            return
        _, name, spawn_minutes = r
        new_ts = now_ts() + spawn_minutes * 60
        await db.execute("UPDATE bosses SET next_spawn_ts = ? WHERE id = ?", (new_ts, boss_id))
        await db.commit()
    await ctx.send(f":crossed_swords: Reported **{name}** killed. Timer reset — next spawn in {format_timedelta_seconds(spawn_minutes*60)}.")


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
    """
    Usage:
    !boss setrole @TrustedRole   -> stores trusted role id on all bosses? We'll set a default for new bosses by updating existing bosses' trusted_role_id to this role.
    !boss setrole none          -> clears trusted role for the server (set to NULL for future bosses)
    """
    if role.lower() in ("none", "clear"):
        # clear role on all bosses for this guild
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE bosses SET trusted_role_id = NULL WHERE guild_id = ?", (ctx.guild.id,))
            await db.commit()
        await ctx.send(":white_check_mark: Cleared trusted role for boss reports in this server.")
        return

    # get role mention or name
    r_obj = None
    if role.startswith("<@&") and role.endswith(">"):
        rid = int(role[3:-1])
        r_obj = ctx.guild.get_role(rid)
    else:
        # try role by name
        r_obj = discord.utils.get(ctx.guild.roles, name=role)

    if r_obj is None:
        await ctx.send("Role not found. Use a role mention like `@Trusted` or the exact role name.")
        return

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE bosses SET trusted_role_id = ? WHERE guild_id = ?", (r_obj.id, ctx.guild.id))
        await db.commit()
    await ctx.send(f":white_check_mark: Set trusted role for reporting kills to **{r_obj.name}** for all bosses in this server.")


@boss_group.command(name="setchannel")
@commands.has_permissions(manage_guild=True)
async def boss_setchannel(ctx, channel: str):
    """
    Set default announcement channel for this guild.
    Usage: !boss setchannel #channel or channel id
    """
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
        await ctx.send("Channel not found. Provide a channel mention or ID.")
        return

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR REPLACE INTO guild_config (guild_id, default_channel) VALUES (?, ?)", (ctx.guild.id, channel_id))
        await db.commit()
    await ctx.send(f":white_check_mark: Set default announcements channel to <#{channel_id}>.")


@boss_group.command(name="edit")
@commands.has_permissions(manage_guild=True)
async def boss_edit(ctx, boss_id: int, field: str, value: str):
    # Only allow editing spawn_minutes or pre_announce_min or name
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
    await ctx.send(":white_check_mark: Boss updated.")


@boss_group.command(name="ping")
async def boss_ping(ctx, boss_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("SELECT name FROM bosses WHERE id = ? AND guild_id = ?", (boss_id, ctx.guild.id))
        r = await c.fetchone()
    if not r:
        await ctx.send("Boss not found.")
        return
    name = r[0]
    await ctx.send(f":sound: Manual ping: **{name}** (ID: {boss_id}). Use `{COMMAND_PREFIX}boss killed {boss_id}` to reset timer.")


# ---------- Convenience: seed import ----------
@bot.command(name="seed_import")
@commands.has_permissions(administrator=True)
async def seed_import(ctx, json_url: str):
    """
    Convenience to import from a publicly reachable JSON listing of bosses.
    Example: admin runs '!seed_import https://example.com/my_bosses.json'
    JSON format: [{ "name": "Forest Lord", "spawn_minutes": 120, "channel_id": null, "pre_announce_min": 10 }, ...]
    NOTE: This will only work if the environment where the bot runs has internet access.
    """
    import aiohttp
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(json_url) as resp:
                if resp.status != 200:
                    await ctx.send("Failed to fetch JSON (status {}).".format(resp.status))
                    return
                data = await resp.json()
    except Exception as e:
        await ctx.send(f"Failed to fetch/parse JSON: {e}")
        return

    inserted = 0
    async with aiosqlite.connect(DB_PATH) as db:
        for item in data:
            name = item.get("name")
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


# ---------- Error handlers ----------
@boss_delete.error
async def on_boss_delete_error(ctx, error):
    if isinstance(error, commands.MissingPermissions):
        await ctx.send(":no_entry: You need Manage Server permissions to delete bosses.")


@seed_import.error
async def on_seed_error(ctx, error):
    if isinstance(error, commands.MissingPermissions):
        await ctx.send(":no_entry: Only server admins can perform seed imports.")


# ---------- Run ----------
if __name__ == "__main__":
    bot.run(TOKEN)
