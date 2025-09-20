-- =========================
-- Timer DB: schema + seeds
-- =========================
PRAGMA journal_mode=WAL;

-- --- core tables ---
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
  notes TEXT DEFAULT '',
  category TEXT DEFAULT NULL,
  sort_key TEXT DEFAULT '',
  window_minutes INTEGER DEFAULT 0
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_boss_unique
ON bosses(guild_id, name, category);

CREATE TABLE IF NOT EXISTS boss_aliases (
  guild_id INTEGER NOT NULL,
  boss_id INTEGER NOT NULL,
  alias TEXT NOT NULL,
  UNIQUE (guild_id, alias)
);

-- subscriptions (optional; safe to exist)
CREATE TABLE IF NOT EXISTS subscription_panels (
  guild_id INTEGER NOT NULL,
  category TEXT NOT NULL,
  message_id INTEGER NOT NULL,
  channel_id INTEGER DEFAULT NULL,
  PRIMARY KEY (guild_id, category)
);
CREATE TABLE IF NOT EXISTS subscription_members (
  guild_id INTEGER NOT NULL,
  boss_id INTEGER NOT NULL,
  user_id INTEGER NOT NULL,
  PRIMARY KEY (guild_id, boss_id, user_id)
);
CREATE TABLE IF NOT EXISTS subscription_emojis (
  guild_id INTEGER NOT NULL,
  boss_id INTEGER NOT NULL,
  emoji TEXT NOT NULL,
  PRIMARY KEY (guild_id, boss_id)
);

-- category cosmetics/routing (optional)
CREATE TABLE IF NOT EXISTS category_colors (
  guild_id INTEGER NOT NULL,
  category TEXT NOT NULL,
  color_hex TEXT NOT NULL,
  PRIMARY KEY (guild_id, category)
);
CREATE TABLE IF NOT EXISTS category_channels (
  guild_id INTEGER NOT NULL,
  category TEXT NOT NULL,
  channel_id INTEGER NOT NULL,
  PRIMARY KEY (guild_id, category)
);

-- user prefs (optional)
CREATE TABLE IF NOT EXISTS user_timer_prefs (
  guild_id INTEGER NOT NULL,
  user_id INTEGER NOT NULL,
  categories TEXT NOT NULL,
  PRIMARY KEY (guild_id, user_id)
);

-- reaction-role panels (optional; some builds use these)
CREATE TABLE IF NOT EXISTS reaction_role_panels (
  guild_id INTEGER NOT NULL,
  message_id INTEGER NOT NULL,
  channel_id INTEGER NOT NULL,
  title TEXT DEFAULT '',
  description TEXT DEFAULT '',
  PRIMARY KEY (guild_id, message_id)
);
CREATE TABLE IF NOT EXISTS reaction_role_map (
  guild_id INTEGER NOT NULL,
  message_id INTEGER NOT NULL,
  emoji TEXT NOT NULL,
  role_id INTEGER NOT NULL,
  PRIMARY KEY (guild_id, message_id, emoji)
);
CREATE TABLE IF NOT EXISTS rr_panels (
  message_id INTEGER PRIMARY KEY,
  guild_id INTEGER NOT NULL,
  channel_id INTEGER NOT NULL,
  title TEXT DEFAULT ''
);
CREATE TABLE IF NOT EXISTS rr_map (
  panel_message_id INTEGER NOT NULL,
  emoji TEXT NOT NULL,
  role_id INTEGER NOT NULL,
  PRIMARY KEY (panel_message_id, emoji)
);

-- guild config/meta (used by the bot)
CREATE TABLE IF NOT EXISTS guild_config (
  guild_id INTEGER PRIMARY KEY,
  default_channel INTEGER DEFAULT NULL,
  prefix TEXT DEFAULT NULL,
  sub_channel_id INTEGER DEFAULT NULL,
  sub_message_id INTEGER DEFAULT NULL,
  uptime_minutes INTEGER DEFAULT NULL,
  heartbeat_channel_id INTEGER DEFAULT NULL,
  show_eta INTEGER DEFAULT 0,
  sub_ping_channel_id INTEGER DEFAULT NULL
);
CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT
);

-- ==== parameters ====
-- Set your guild id here if you know it; otherwise this falls back to 0.
-- In sqlite3 CLI you can also:  .param set @G 123456789012345678
WITH params(g) AS (SELECT COALESCE(@G, 0))
-- ==== seed bosses ====
INSERT OR IGNORE INTO bosses
(guild_id, channel_id, name, spawn_minutes, next_spawn_ts, pre_announce_min, notes, category, sort_key, window_minutes)
VALUES
-- METEORIC
((SELECT g FROM params), NULL, 'Doomclaw',   7,  CAST(strftime('%s','now') AS INTEGER), 10, '', 'Meteoric', 'do',  5),
((SELECT g FROM params), NULL, 'Bonehad',   15,  CAST(strftime('%s','now') AS INTEGER), 10, '', 'Meteoric', 'bo',  5),
((SELECT g FROM params), NULL, 'Rockbelly', 15,  CAST(strftime('%s','now') AS INTEGER), 10, '', 'Meteoric', 'ro',  5),
((SELECT g FROM params), NULL, 'Redbane',   20,  CAST(strftime('%s','now') AS INTEGER), 10, '', 'Meteoric', 're',  5),
((SELECT g FROM params), NULL, 'Coppinger', 20,  CAST(strftime('%s','now') AS INTEGER), 10, '', 'Meteoric', 'cp',  5),
((SELECT g FROM params), NULL, 'Goretusk',  20,  CAST(strftime('%s','now') AS INTEGER), 10, '', 'Meteoric', 'go',  5),

-- FROZEN
((SELECT g FROM params), NULL, 'Eye',       28,  CAST(strftime('%s','now') AS INTEGER), 10, '', 'Frozen',   'ey',  3),
((SELECT g FROM params), NULL, 'Swampie',   33,  CAST(strftime('%s','now') AS INTEGER), 10, '', 'Frozen',   'sw',  3),
((SELECT g FROM params), NULL, 'Woody',     38,  CAST(strftime('%s','now') AS INTEGER), 10, '', 'Frozen',   'wo',  3),
((SELECT g FROM params), NULL, 'Chained',   43,  CAST(strftime('%s','now') AS INTEGER), 10, '', 'Frozen',   'ch',  3),
((SELECT g FROM params), NULL, 'Grom',      48,  CAST(strftime('%s','now') AS INTEGER), 10, '', 'Frozen',   'gr',  3),
((SELECT g FROM params), NULL, 'Pyrus',     58,  CAST(strftime('%s','now') AS INTEGER), 10, '', 'Frozen',   'py',  3),

-- DL
((SELECT g FROM params), NULL, '155',       63,  CAST(strftime('%s','now') AS INTEGER), 10, '', 'DL',       'dl155', 3),
((SELECT g FROM params), NULL, '160',       68,  CAST(strftime('%s','now') AS INTEGER), 10, '', 'DL',       'dl160', 3),
((SELECT g FROM params), NULL, '165',       73,  CAST(strftime('%s','now') AS INTEGER), 10, '', 'DL',       'dl165', 3),
((SELECT g FROM params), NULL, '170',       78,  CAST(strftime('%s','now') AS INTEGER), 10, '', 'DL',       'dl170', 3),
((SELECT g FROM params), NULL, '180',       88,  CAST(strftime('%s','now') AS INTEGER), 10, '', 'DL',       'dl180', 3),

-- EDL
((SELECT g FROM params), NULL, '185',       72,  CAST(strftime('%s','now') AS INTEGER), 10, '', 'EDL',      'edl185', 3),
((SELECT g FROM params), NULL, '190',       81,  CAST(strftime('%s','now') AS INTEGER), 10, '', 'EDL',      'edl190', 3),
((SELECT g FROM params), NULL, '195',       89,  CAST(strftime('%s','now') AS INTEGER), 10, '', 'EDL',      'edl195', 4),
((SELECT g FROM params), NULL, '200',      108,  CAST(strftime('%s','now') AS INTEGER), 10, '', 'EDL',      'edl200', 5),
((SELECT g FROM params), NULL, '205',      117,  CAST(strftime('%s','now') AS INTEGER), 10, '', 'EDL',      'edl205', 4),
((SELECT g FROM params), NULL, '210',      125,  CAST(strftime('%s','now') AS INTEGER), 10, '', 'EDL',      'edl210', 5),
((SELECT g FROM params), NULL, '215',      134,  CAST(strftime('%s','now') AS INTEGER), 10, '', 'EDL',      'edl215', 5),

-- RINGS (3h35m respawn = 215 minutes)
((SELECT g FROM params), NULL, 'North Ring', 215, CAST(strftime('%s','now') AS INTEGER), 10, '', 'Rings', 'ring-n', 50),
((SELECT g FROM params), NULL, 'Center Ring',215, CAST(strftime('%s','now') AS INTEGER), 10, '', 'Rings', 'ring-c', 50),
((SELECT g FROM params), NULL, 'South Ring', 215, CAST(strftime('%s','now') AS INTEGER), 10, '', 'Rings', 'ring-s', 50),
((SELECT g FROM params), NULL, 'East Ring',  215, CAST(strftime('%s','now') AS INTEGER), 10, '', 'Rings', 'ring-e', 50),

-- EG
((SELECT g FROM params), NULL, 'Draig Liathphur', 240,  CAST(strftime('%s','now') AS INTEGER), 10, '', 'EG', 'eg-draig',   840),
((SELECT g FROM params), NULL, 'Sciathan Leathair',240,  CAST(strftime('%s','now') AS INTEGER), 10, '', 'EG', 'eg-bat',     300),
((SELECT g FROM params), NULL, 'Thymea Banebark',  240,  CAST(strftime('%s','now') AS INTEGER), 10, '', 'EG', 'eg-thymea',  840),
((SELECT g FROM params), NULL, 'Proteus',         1080,  CAST(strftime('%s','now') AS INTEGER), 10, '', 'EG', 'eg-prot',     15),
((SELECT g FROM params), NULL, 'Gelebron',        1920,  CAST(strftime('%s','now') AS INTEGER), 10, '', 'EG', 'eg-gele',   1680),
((SELECT g FROM params), NULL, 'Dhiothu',         2040,  CAST(strftime('%s','now') AS INTEGER), 10, '', 'EG', 'eg-dhio',   1680),
((SELECT g FROM params), NULL, 'Bloodthorn',      2040,  CAST(strftime('%s','now') AS INTEGER), 10, '', 'EG', 'eg-bt',     1680),
((SELECT g FROM params), NULL, 'Crom’s Manikin',  5760,  CAST(strftime('%s','now') AS INTEGER), 10, '', 'EG', 'eg-crom',   1440),

-- MIDRAIDS
((SELECT g FROM params), NULL, 'Aggorath',       1200,  CAST(strftime('%s','now') AS INTEGER), 10, '', 'Midraids', 'mr-aggy',  960),
((SELECT g FROM params), NULL, 'Mordris',        1200,  CAST(strftime('%s','now') AS INTEGER), 10, '', 'Midraids', 'mr-mord',  960),
((SELECT g FROM params), NULL, 'Necromancer',    1320,  CAST(strftime('%s','now') AS INTEGER), 10, '', 'Midraids', 'mr-necro', 960),
((SELECT g FROM params), NULL, 'Hrungnir',       1320,  CAST(strftime('%s','now') AS INTEGER), 10, '', 'Midraids', 'mr-hrung', 960)
;

-- ==== aliases ====
-- Helper CTE to fetch boss IDs by name for this guild
WITH params(g) AS (SELECT COALESCE(@G, 0))
INSERT OR IGNORE INTO boss_aliases(guild_id,boss_id,alias)
SELECT (SELECT g FROM params),
       b.id,
       a.alias
FROM bosses b
JOIN (
  -- Meteoric
  SELECT 'Coppinger' AS name, 'copp' AS alias UNION ALL
  -- Frozen
  SELECT 'Swampie','swampy' UNION ALL
  SELECT 'Swampie','swamplord' UNION ALL
  SELECT 'Chained','chain' UNION ALL
  SELECT 'Pyrus','py' UNION ALL
  -- DL
  SELECT '180','snorri' UNION ALL
  SELECT '180','snor' UNION ALL
  -- EDL
  SELECT '215','unox' UNION ALL
  -- Rings
  SELECT 'North Ring','northring' UNION ALL
  SELECT 'Center Ring','centre' UNION ALL
  SELECT 'Center Ring','centering' UNION ALL
  SELECT 'South Ring','southring' UNION ALL
  SELECT 'East Ring','eastring' UNION ALL
  -- EG
  SELECT 'Draig Liathphur','draig' UNION ALL
  SELECT 'Draig Liathphur','dragon' UNION ALL
  SELECT 'Draig Liathphur','riverdragon' UNION ALL
  SELECT 'Sciathan Leathair','sciathan' UNION ALL
  SELECT 'Sciathan Leathair','bat' UNION ALL
  SELECT 'Sciathan Leathair','northbat' UNION ALL
  SELECT 'Thymea Banebark','thymea' UNION ALL
  SELECT 'Thymea Banebark','tree' UNION ALL
  SELECT 'Thymea Banebark','ancienttree' UNION ALL
  SELECT 'Proteus','prot' UNION ALL
  SELECT 'Proteus','base' UNION ALL
  SELECT 'Proteus','prime' UNION ALL
  SELECT 'Gelebron','gele' UNION ALL
  SELECT 'Dhiothu','dino' UNION ALL
  SELECT 'Dhiothu','dhio' UNION ALL
  SELECT 'Dhiothu','d2' UNION ALL
  SELECT 'Bloodthorn','bt' UNION ALL
  SELECT 'Crom’s Manikin','manikin' UNION ALL
  SELECT 'Crom’s Manikin','crom' UNION ALL
  SELECT 'Crom’s Manikin','croms' UNION ALL
  -- Midraids
  SELECT 'Aggorath','aggy' UNION ALL
  SELECT 'Mordris','mord' UNION ALL
  SELECT 'Mordris','mordy' UNION ALL
  SELECT 'Necromancer','necro' UNION ALL
  SELECT 'Hrungnir','hrung' UNION ALL
  SELECT 'Hrungnir','muk'
) a ON a.name = b.name
WHERE b.guild_id = (SELECT g FROM params);

-- ==== meta for downtime roll-forward ====
-- Many builds use these to compute downtime and shift next_spawn_ts.
INSERT OR REPLACE INTO meta(key,value) VALUES
('last_tick_ts', CAST(strftime('%s','now') AS TEXT)),
('last_startup_ts', CAST(strftime('%s','now') AS TEXT)),
('offline_since', '0');

-- Done.
