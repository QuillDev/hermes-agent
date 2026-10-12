"""Explicit profile opt-in for NEW native Desktop conversations, without new actors.

Only native create admission enrolls a canonical key. A config epoch captured
before admission is the activation boundary, not a creation timestamp guess.
SQLite stores transport metadata only; task/dialog contents stay in the Hub.
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, contextmanager
import hashlib
import json
import logging
import os
from pathlib import Path
import secrets
import sqlite3
import sys

import hermes_yaml as yaml
import psutil

log = logging.getLogger(__name__)


def require_automatic_runtime():
    # aiohttp buffers headers and schedules the first body write on 3.11.
    # Only 3.12+ eagerly starts that writer before the submission gate returns.
    if sys.version_info < (3, 12):
        raise RuntimeError('Automatic Discord forum sync requires Python 3.12 or newer; no operation admitted')


def automatic_config(home):
    try:
        cfg = (yaml.safe_load((Path(home) / 'config.yaml').read_text(encoding='utf-8-sig')) or {}).get('session_clients', {})
        forum = cfg.get('discord_forum', {})
        if cfg.get('enabled') is not True or forum.get('automatic') is not True:
            return None
        if sys.version_info < (3, 12):
            log.warning('Automatic Discord forum sync requires Python 3.12 or newer; automatic mode is unavailable')
            return None
        epoch = forum.get('activation_id')
        guild = str(forum.get('guild_id') or '')
        fid = str(forum.get('forum_id') or '')
        name = forum.get('forum_name', 'hermes-sessions')
        if not isinstance(epoch, str) or not epoch.strip() or not guild.isdecimal() or not int(guild):
            return None
        if fid and (not fid.isdecimal() or not int(fid)):
            return None
        if not isinstance(name, str) or not 1 <= len(name) <= 100:
            return None
        result = {'activation_id': epoch, 'guild_id': guild, 'forum_id': fid, 'forum_name': name}
        result['scope'] = hashlib.sha256(json.dumps(result, sort_keys=True).encode()).hexdigest()
        result['forum_marker'] = 'Hermes profile sync: ' + hashlib.sha256(str(Path(home).resolve()).encode()).hexdigest()
        return result
    except (OSError, ValueError, AttributeError, TypeError, yaml.YAMLError):
        return None


@contextmanager
def _db(home):
    path = Path(home) / 'session_clients_gateway.db'
    db = sqlite3.connect(path, timeout=10)
    path.chmod(0o600)
    db.row_factory = sqlite3.Row
    try:
        with db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS forum_enrollments (
                    session_key TEXT PRIMARY KEY, scope TEXT NOT NULL, sent INTEGER NOT NULL DEFAULT 0,
                    chat_id TEXT, guild_id TEXT, forum_id TEXT, bot_id TEXT, user_id TEXT);
                CREATE TABLE IF NOT EXISTS forum_provisions (scope TEXT PRIMARY KEY, sent INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS forum_poll_claim (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1), token TEXT, pid INTEGER, born REAL);
            ''')
            yield db
    finally:
        db.close()


def record_native_creation(home, key, *, eligible, idempotency_key=None, fresh=True):
    """Freeze opt-in at native allocation, including durable negative decisions.

    A retry returning an unstamped pre-upgrade runtime is private. Logical create
    decisions survive runtime teardown/restart; config changes never upgrade them.
    """
    cfg = automatic_config(home) if eligible and fresh else None
    scope = cfg['scope'] if cfg else ''
    with _db(home) as db:
        db.execute('CREATE TABLE IF NOT EXISTS forum_create_decisions (identity TEXT PRIMARY KEY, scope TEXT NOT NULL)')
        db.execute('BEGIN IMMEDIATE')
        existing = db.execute('SELECT scope FROM forum_enrollments WHERE session_key=?', (key,)).fetchone()
        if existing:
            scope = existing['scope']
        if idempotency_key:
            identity = hashlib.sha256(idempotency_key.encode()).hexdigest()
            db.execute('INSERT OR IGNORE INTO forum_create_decisions VALUES (?,?)', (identity, scope))
            scope = db.execute('SELECT scope FROM forum_create_decisions WHERE identity=?', (identity,)).fetchone()['scope']
        db.execute('INSERT OR IGNORE INTO forum_enrollments(session_key,scope) VALUES (?,?)', (key, scope))


def automatic_destination(home, chat_id):
    path = Path(home) / 'session_clients_gateway.db'
    if not path.exists():
        return None
    with _db(home) as db:
        row = db.execute('SELECT * FROM forum_enrollments WHERE chat_id=?', (str(chat_id),)).fetchone()
        return dict(row) if row else None


def _claim(home):
    from tui_gateway.session_clients import _owner_alive
    with _db(home) as db:
        db.execute('BEGIN IMMEDIATE')
        row = db.execute('SELECT * FROM forum_poll_claim WHERE singleton=1').fetchone()
        if row and _owner_alive({**dict(row), 'online': 1, 'session_key': ''}):
            return None
        token = secrets.token_hex(16)
        db.execute('INSERT OR REPLACE INTO forum_poll_claim VALUES (1,?,?,?)',
                   (token, os.getpid(), psutil.Process().create_time()))
        return token


def _release(home, token):
    with _db(home) as db:
        db.execute('DELETE FROM forum_poll_claim WHERE singleton=1 AND token=?', (token,))


@asynccontextmanager
async def forum_poll_scope(home):
    # The claim spans provisioning AND cursor delivery. Concurrent watchers cannot
    # both read an old cursor and send the same first exchange.
    token = await asyncio.to_thread(_claim, home)
    try:
        yield token is not None
    finally:
        if token:
            await asyncio.to_thread(_release, home, token)


def _pending(home, scope):
    with _db(home) as db:
        return [dict(r) for r in db.execute('SELECT * FROM forum_enrollments WHERE scope=?', (scope,))]


def _sent(home, scope, key=None):
    with _db(home) as db:
        if key is None:
            row = db.execute('SELECT sent FROM forum_provisions WHERE scope=?', (scope,)).fetchone()
        else:
            row = db.execute('SELECT sent FROM forum_enrollments WHERE session_key=?', (key,)).fetchone()
        return bool(row and row['sent'])


def _mark_sent(home, scope, key=None):
    with _db(home) as db:
        if key is None:
            db.execute('INSERT OR REPLACE INTO forum_provisions VALUES (?,1)', (scope,))
        else:
            db.execute('UPDATE forum_enrollments SET sent=1 WHERE session_key=? AND scope=?', (key, scope))


def _persistent(home, key):
    from hermes_state import SessionDB
    path = Path(home) / 'state.db'
    if not path.exists():
        return False
    with SessionDB(path) as db:
        row = db.get_session(key)
        model_config = row.get('model_config') if row else None
        if isinstance(model_config, str):
            model_config = json.loads(model_config)
        return bool(row and row.get('source') == 'desktop' and not row.get('parent_session_id')
                    and not (model_config or {}).get('_branched_from'))


def _qualifies(hub, home, key):
    from tui_gateway.session_clients import _owner_alive
    owner = hub.lookup(key)
    if not owner or not _owner_alive(owner) or not _persistent(home, key):
        return False
    with hub._db() as db:
        return db.execute("SELECT 1 FROM events WHERE session_key=? AND "
                          "json_extract(frame,'$.params.type')='session.clients.input' LIMIT 1", (key,)).fetchone() is not None


def _post_marker(home, key):
    return 'Hermes session sync: ' + hashlib.sha256((str(Path(home).resolve()) + ':' + key).encode()).hexdigest()


def _bind(hub, home, key, chat_id, destination):
    from tui_gateway.session_clients import _owner_alive
    # Record the reconciled post BEFORE linking. A crash between DBs is recoverable
    # without creating another post, and cursor zero captures the first exchange.
    with _db(home) as db:
        db.execute('UPDATE forum_enrollments SET chat_id=?,guild_id=?,forum_id=?,bot_id=?,user_id=? WHERE session_key=?',
                   (chat_id, destination['guild_id'], destination['forum_id'], destination['bot_id'], destination['user_id'], key))
    with hub._db() as db:
        db.execute('BEGIN IMMEDIATE')
        owner = db.execute('SELECT * FROM owners WHERE session_key=?', (key,)).fetchone()
        if not owner or not _owner_alive(dict(owner)):
            raise RuntimeError('Execution owner is unavailable; automatic sync does not reopen or retry tasks')
        existing = db.execute("SELECT * FROM clients WHERE platform='discord' AND chat_id=?", (chat_id,)).fetchone()
        if existing and (existing['session_key'] != key or existing['user_id'] != destination['user_id']):
            raise RuntimeError('Forum post is already bound to a different canonical owner')
        db.execute("INSERT OR IGNORE INTO clients VALUES ('discord',?,?,?)", (chat_id, destination['user_id'], key))


async def prepare_automatic_forums(hub, home, adapter):
    cfg = await asyncio.to_thread(automatic_config, home)
    if cfg is None:
        return set()
    with adapter.automatic_forum_delivery(lambda: require_automatic_scope(home, cfg['scope'])):
        return await _prepare_automatic_forums(hub, home, adapter, cfg)


async def _prepare_automatic_forums(hub, home, adapter, cfg):
    rows = await asyncio.to_thread(_pending, home, cfg['scope'])
    rows = [r for r in rows if await asyncio.to_thread(_persistent, home, r['session_key'])
            and (r['chat_id'] or await asyncio.to_thread(_qualifies, hub, home, r['session_key']))]
    if not rows:
        return set()
    ready = set()
    try:
        destination = await adapter.ensure_sync_forum(cfg)
        if destination is None:
            if await asyncio.to_thread(_sent, home, cfg['scope']):
                raise RuntimeError('Forum creation outcome is uncertain; no matching forum found, no duplicate attempted')
            await asyncio.to_thread(_mark_sent, home, cfg['scope'])
            require_automatic_scope(home, cfg['scope'])
            destination = await adapter.ensure_sync_forum(cfg, create=True)
        for row in rows:
            try:
                require_automatic_scope(home, cfg['scope'])
                key = row['session_key']
                marker = await asyncio.to_thread(_post_marker, home, key)
                chat = await adapter.reconcile_sync_post(destination, marker)
                if not chat:
                    if row['sent'] or row['chat_id']:
                        raise RuntimeError('Forum post creation outcome is uncertain or post was deleted; no duplicate attempted')
                    await asyncio.to_thread(_mark_sent, home, cfg['scope'], key)
                    require_automatic_scope(home, cfg['scope'])
                    chat = await adapter.create_sync_post(destination, marker, f'Hermes {marker[-16:]}')
                # Config revocation during network I/O must not authorize delivery.
                current = await asyncio.to_thread(automatic_config, home)
                if current is None or current['scope'] != cfg['scope']:
                    break
                await asyncio.to_thread(_bind, hub, home, key, chat, destination)
                ready.add(chat)
            except Exception:
                log.exception('Automatic Discord post sync failed for canonical session %s', row['session_key'])
    except Exception:
        log.exception('Automatic Discord forum is unavailable; no task was reopened or retried')
    return ready


def require_automatic_scope(home, scope, *, key=None, chat_id=None):
    require_automatic_runtime()
    cfg = automatic_config(home)
    if cfg is None or cfg['scope'] != scope:
        raise RuntimeError('Automatic forum activation was revoked; no operation admitted')
    if chat_id is not None:
        record = automatic_destination(home, chat_id)
        if not record or record['scope'] != scope or record['session_key'] != key:
            raise RuntimeError('Automatic forum link changed; no operation admitted')


async def automatic_intake_allowed(runner, source, home, record):
    cfg = await asyncio.to_thread(automatic_config, home)
    if cfg is None or cfg['scope'] != record['scope']:
        return False
    adapter = runner._intake_adapter_for(source)
    client = getattr(adapter, '_client', None)
    if (client is None or str(getattr(client.user, 'id', '')) != record['bot_id']
            or str(getattr(source, 'guild_id', '')) != record['guild_id']
            or str(getattr(source, 'parent_chat_id', '')) != record['forum_id']):
        return False
    try:
        destination = await adapter.ensure_sync_forum(cfg)
        require_automatic_scope(home, record['scope'])
        return destination is not None and all(destination[k] == record[k] for k in ('guild_id', 'forum_id', 'bot_id', 'user_id'))
    except Exception:
        log.warning('Automatic forum reply refused: receiving bot/forum policy is unavailable', exc_info=True)
        return False
