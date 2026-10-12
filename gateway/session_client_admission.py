"""Lifetime fencing for incumbent native notification admission, not ownership transfer."""
from __future__ import annotations

import asyncio
import sqlite3
import threading
from pathlib import Path

from gateway.config import Platform
from gateway.wake import WakeNotAccepted


class _AdmissionLifetime:
    def __init__(self):
        self.lock = threading.RLock()
        self.closed = False
        self.leases = 0
        self.permits = set()
        self.retire = None

    def release(self):
        with self.lock:
            self.leases -= 1
            if not self.leases and self.retire is not None:
                callback, self.retire = self.retire, None
                callback()


class _NativeAdmission:
    def __init__(self, lifetime, event, session_key, adapter):
        self.lifetime = lifetime
        self.event = event
        self.adapter = adapter
        self.session_key = session_key
        self.released = False
        self.accepted = False
        lifetime.permits.add(self)

    def release(self):
        with self.lifetime.lock:
            if not self.released:
                self.released = True
                self.lifetime.permits.discard(self)
                self.lifetime.release()


def _lifetime(runner):
    # Native runners serialize installation/admission/retirement on their loop.
    lifetime = getattr(runner, '_legacy_notification_lifetime', None)
    if lifetime is None:
        lifetime = runner._legacy_notification_lifetime = _AdmissionLifetime()
    return lifetime


def retire_internal_admission(runner, retire_callback) -> bool:
    """Close intake now; retire the broker only after reserved native work settles.

    Called by retire_gateway_bridge on the incumbent loop. False means retirement
    is deferred, NOT permission to retire the owner independently. No new worker
    is created; pending work drains under the same native adapter/runner.
    """
    lifetime = _lifetime(runner)
    with lifetime.lock:
        lifetime.closed = True
        for permit in tuple(lifetime.permits):
            if permit.accepted:
                settle_native_admission(permit.adapter, permit.event, permit.session_key)
        if lifetime.leases:
            lifetime.retire = retire_callback
            return False
        retire_callback()
        return True


def retire_before_database_close(runner, ctx):
    """Admission coroutines can outlive a drained executor: never close beneath them."""
    import logging
    from gateway.session_client_bridge import retire_gateway_bridge
    if retire_gateway_bridge(runner):
        return True
    ctx.timed_out = True
    logging.getLogger('gateway.run').warning(
        'Shutdown phase: native notification admission remains reserved; '
        'skipping SessionDB close/checkpoint until original work settles')
    return False


def _owner_target(runner, event):
    source = event.source
    home = runner._resolve_profile_home_for_source(source)
    if not (Path(home) / 'session-clients' / 'broker.sqlite').exists():
        return None
    from gateway.session_clients import _unlinked_legacy_owner
    from tui_gateway.session_clients import get_hub
    hub = get_hub(home)
    owner = _unlinked_legacy_owner(runner, source, hub)
    link = hub.link('discord', str(source.chat_id))
    if link:
        linked_owner = hub.lookup(link['session_key'])
        if linked_owner is None or (owner is not None and linked_owner['session_key'] != owner['session_key']):
            raise WakeNotAccepted('Native notification route has a different shared binding')
        owner = linked_owner
    expected = (event.metadata or {}).get('gateway_session_id')
    expected_owner = hub.lookup(expected) if expected else None
    if expected_owner is not None:
        if owner is not None and expected_owner['session_key'] != owner['session_key']:
            raise WakeNotAccepted('Native notification lineage no longer owns its route')
        owner = expected_owner
    return (hub, owner)


def _reserve(runner, event, lifetime, target, adapter):
    if target is None:
        return None  # Feature-off routes retain native admission.
    hub, _owner = target
    with lifetime.lock, hub._admission_lock:
        refreshed = _owner_target(runner, event)
        if refreshed is None or refreshed[1] is None:
            return None  # No shared execution owner: keep ordinary native semantics.
        _hub, owner = refreshed
        if lifetime.closed or runner._draining or runner._executor_closing:
            raise WakeNotAccepted('Legacy notification owner is retiring')
        current = hub.lookup(owner['session_key'])
        consumer = hub._external_consumers.get(owner['session_key'])
        route = runner._session_key_for_source(event.source)
        native_owner = (getattr(runner, '_legacy_client_owners', None) or {}).get(route)
        if (not current or not current['online'] or current['owner'] != hub.owner
                or consumer is not native_owner or getattr(consumer, 'runner', None) is not runner):
            raise WakeNotAccepted('Legacy notification owner is unavailable')
        lifetime.leases += 1
        return _NativeAdmission(lifetime, event, route, adapter)


async def acquire_internal_admission(runner, event, adapter):
    """Resolve without mutation, then reserve the incumbent against retirement."""
    if not event.internal or event.source.platform != Platform.DISCORD:
        return None
    lifetime = _lifetime(runner)
    try:
        target = await asyncio.to_thread(_owner_target, runner, event)
        permit = _reserve(runner, event, lifetime, target, adapter)
    except (sqlite3.Error, OSError, RuntimeError) as exc:
        raise WakeNotAccepted('Native notification ownership could not be reserved') from exc
    event._legacy_notification_admission = permit
    return permit


def release_internal_admission(event):
    permit = getattr(event, '_legacy_notification_admission', None)
    if permit is not None:
        permit.release()


def settle_native_admission(adapter, event, session_key):
    runner = getattr(adapter, 'gateway_runner', None)
    lifetime = getattr(runner, '_legacy_notification_lifetime', None)
    if (lifetime is None or session_key in adapter._active_sessions
            or runner._is_session_running(session_key)):
        return
    # The runner may consume an entire FIFO inside one adapter task. Settle
    # every accepted item that left that native chain, not only its first event.
    queued = [adapter._pending_messages.get(session_key), *(runner._overflow_queue(session_key) or ())]
    with lifetime.lock:
        for permit in tuple(lifetime.permits):
            if (permit.session_key == session_key
                    and (permit.accepted or permit.event is event)
                    and not any(item is permit.event for item in queued)):
                permit.release()


def track_native_admission(adapter, event, session_key, task):
    if getattr(event, '_legacy_notification_admission', None) is not None:
        task.add_done_callback(lambda _task: settle_native_admission(adapter, event, session_key))


async def dispatch_native_admission(adapter, event, session_key):
    """Shared idle/FIFO boundary, with the real GatewayRunner as consumer."""
    runner = getattr(adapter, 'gateway_runner', None)
    validator = getattr(runner, '_internal_notification_admission', None)
    try:
        if event.internal and validator is not None:
            await validator(event, adapter)
        if session_key in adapter._active_sessions:
            adapter._heal_stale_session_lock(session_key)
        if session_key in adapter._active_sessions:
            await adapter._handle_message_while_active(event, session_key)
        else:
            event._gateway_accepted = adapter._start_session_processing(event, session_key)
    except WakeNotAccepted:
        event._gateway_accepted = False
    finally:
        permit = getattr(event, '_legacy_notification_admission', None)
        if permit is not None:
            permit.accepted = event._gateway_accepted is True
        if not event._gateway_accepted:
            release_internal_admission(event)
