"""Detached, profile-local execution for opt-in Desktop and Discord sessions.

This is not a failover scheduler: only an explicit create/resume can start the
host. A lost owner never retries an admitted prompt or manufactures a new run.
"""
from __future__ import annotations

if __name__ == '__main__':
    import hermes_bootstrap

import logging
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time

HOST_KEY = '__hermes_session_execution_host__'
_launch_lock = threading.RLock()


def ensure_host(home: Path | str):
    from tools.environments.local import served_profile_child_env
    from tui_gateway.session_clients import _owner_alive, get_hub

    home = Path(home).resolve()
    hub = get_hub(home)
    with _launch_lock:
        record = hub.lookup(HOST_KEY)
        if record and _owner_alive(record):
            return record
        env = served_profile_child_env(target_home=home, inherit_credentials=True)
        env['HERMES_RUNTIME_DIR'] = str(home / 'session-clients' / 'runtime')
        options = ({'creationflags': subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP}
                   if os.name == 'nt' else {'start_new_session': True})
        log_path = home / 'session-clients' / 'host.log'
        fd = os.open(log_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        with os.fdopen(fd, 'ab') as output:
            process = subprocess.Popen(
                [sys.executable, '-m', 'tui_gateway.session_client_host'],
                cwd=Path(__file__).resolve().parent.parent, env=env,
                stdin=subprocess.DEVNULL, stdout=output, stderr=output, **options)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            record = hub.lookup(HOST_KEY)
            if record and _owner_alive(record):
                return record
            if process.poll() is not None:
                raise RuntimeError(f'Execution host failed to start; inspect {log_path}')
            time.sleep(0.05)
        raise RuntimeError(f'Execution host did not become ready; inspect {log_path}')


def create_on_host(home: Path | str, params: dict | None = None, *, method='session.create') -> dict:
    from tui_gateway.session_clients import get_hub

    host = ensure_host(home)
    return get_hub(home).call(HOST_KEY, method, {'source': 'desktop', **(params or {})}, expected_owner=host['owner'])


def _stored_gateway_history(home, key):
    from hermes_state import SessionDB
    path = Path(home) / 'state.db'
    if not path.exists():
        return False
    with SessionDB(path) as db:
        row = db.get_session(key)
        if row and row.get('source') == 'discord':
            return True
        import json
        lineage = db._resume_lineage_ids(key)
        for route in db.list_gateway_routing_rows():
            entry = json.loads(route['entry_json'])
            platform = entry.get('platform') or (entry.get('origin') or {}).get('platform')
            if platform == 'discord' and entry.get('session_id') in lineage:
                return True
    return False


def probe_gateway_owner(home, key: str):
    """Positive incumbent discovery, before native cold-resume can touch rows."""
    from gateway.session_client_bridge import GATEWAY_KEY
    from tui_gateway.session_clients import get_hub
    hub = get_hub(home)
    if hub.lookup(key):
        return
    legacy = _stored_gateway_history(home, key)
    discovery = hub.lookup(GATEWAY_KEY)
    # Enabling the flag on a live gateway wakes its existing watcher. Wait only
    # for discovery registration, never re-admit or retry a model task.
    deadline = time.monotonic() + 2
    while legacy and discovery is None and time.monotonic() < deadline:
        time.sleep(0.03)
        discovery = hub.lookup(GATEWAY_KEY)
    if discovery is None:
        if legacy:
            raise RuntimeError('Legacy gateway incumbent discovery is unavailable; no replacement run was started')
        return
    response = hub.call(GATEWAY_KEY, 'session.resume', {'session_id': key},
                        expected_owner=discovery['owner'])
    if 'error' in response:
        raise RuntimeError(response['error']['message'])
    if legacy and not response.get('result', {}).get('legacy_owner'):
        raise RuntimeError('Legacy gateway route has no verified incumbent; no replacement run was started')


def route_hosted_session(method: str, params: dict, rid):
    """Admit new Desktop sessions before an agent exists in the viewing backend."""
    from tui_gateway import server
    from tui_gateway.session_clients import _owner_alive, enabled, get_hub

    if method not in {'session.create', 'session.branch_stored', 'session.resume'}:
        return None
    if method in {'session.create', 'session.branch_stored'} and (params.get('source') != 'desktop' or params.get('room_plumbing')):
        return None
    home = server._profile_home(params.get('profile')) or server._hermes_home
    if not enabled(home):
        return None
    hub = get_hub(home)
    request = dict(params)
    if method == 'session.resume':
        key = str(params.get('session_id') or '')
        try:
            probe_gateway_owner(home, key)
        except RuntimeError as exc:
            return server._err(rid, 4092, str(exc))
        owner = hub.lookup(key)
        if owner and (_owner_alive(owner) or owner['sid'].startswith('gateway:')
                      or not owner['owner'].startswith('host:')):
            return None
        # A pre-opt-in agent already live in this backend cannot be moved across
        # processes. Keep its existing ownership rather than constructing a twin.
        with server._sessions_lock:
            if any(s.get('session_key') == key for s in server._sessions.values()):
                return None
        if owner:
            request['session_id'] = owner['session_key']
    try:
        response = create_on_host(home, request, method=method)
        response['id'] = rid
        info = response.get('result', {}).get('info')
        if info is not None:
            info['profile_name'] = server._response_profile_name(params.get('profile'))
        return response
    except (RuntimeError, ValueError, OSError) as exc:
        return server._err(rid, 4092, str(exc))


def main():
    from hermes_constants import get_hermes_home
    from tui_gateway.session_clients import get_hub

    home = get_hermes_home()
    # Claim before accepting any work. Concurrent launchers can start processes,
    # but only the generation-fenced winner can create an agent.
    try:
        hub = get_hub(home, execution_host=True, start=False)
    except RuntimeError:
        return
    from tui_gateway import server
    from tui_gateway.entry import ensure_mcp_discovery_started
    logging.basicConfig(level=logging.INFO)
    server._start_backend_heartbeat_refresher()
    ensure_mcp_discovery_started()
    stopped = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda _sig, _frame: stopped.set())
    logging.getLogger(__name__).info('Shared execution host ready for %s', home)
    try:
        hub.start()
        stopped.wait()
    finally:
        server._shutdown_sessions()
        hub.close()


if __name__ == '__main__':
    main()
