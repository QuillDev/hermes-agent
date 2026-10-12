"""Shutdown keeps the actual database usable until native admission settles."""
import sqlite3
import time

import pytest

from gateway.platforms.event import MessageEvent
from gateway.run_shutdown import GatewayShutdownMixin
from gateway.session_client_admission import release_internal_admission
from tests.gateway.test_session_client_legacy_bridge_boundaries import _setup
from tests.gateway.test_session_client_legacy_ownership import _close_runner
from tests.tui_gateway import test_session_client_regressions as fixtures

local_host = fixtures.local_host


@pytest.mark.asyncio
async def test_shutdown_does_not_close_database_with_reserved_native_admission(local_host, monkeypatch):
    _home, hub, _provider, runner, adapter, source = _setup(local_host, monkeypatch)
    adapter.gateway_runner = runner
    event = MessageEvent(text='reserved native wake', source=source, internal=True,
                         allow_gateway_control=False)
    try:
        await runner._handle_message(MessageEvent(text='native seed', source=source))
        entry = runner.session_store.lookup_by_session_key(runner._session_key_for_source(source))
        db = runner.session_store._db_for_key(entry.session_key)
        cursor = db._conn.cursor()
        assert cursor.execute('SELECT COUNT(*) FROM sessions').fetchone()[0] > 0
        await runner._internal_notification_admission(event, adapter)
        ctx = GatewayShutdownMixin._StopContext(deferred_count=lambda: 0, started_at=time.monotonic())
        runner._stop_quiesce_and_close_session_dbs(1, ctx)
        # A live SQL resource must not be invalidated or silently reopened while
        # its reserved original-owner work has not settled.
        assert cursor.execute('SELECT COUNT(*) FROM sessions').fetchone()[0] > 0
        assert hub.lookup(entry.session_id)['online']
        release_internal_admission(event)
        assert not hub.lookup(entry.session_id)['online']
        runner._stop_quiesce_and_close_session_dbs(1, ctx)
        with pytest.raises(sqlite3.ProgrammingError, match='closed'):
            cursor.execute('SELECT COUNT(*) FROM sessions')
    finally:
        release_internal_admission(event)
        _close_runner(runner)
