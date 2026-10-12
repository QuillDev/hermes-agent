"""Server-free actual aiohttp non-eager submission outcomes."""
import asyncio
from types import SimpleNamespace

import aiohttp
import pytest
from yarl import URL

from plugins.platforms.discord import adapter_forum_sync as transport
from tests.gateway.test_session_forum_runtime import _non_eager


class RecordingTransport:
    def __init__(self):
        self.writes = []

    def is_closing(self):
        return False

    def write(self, data):
        self.writes.append(bytes(data))

    def writelines(self, data):
        self.writes.extend(bytes(chunk) for chunk in data)


def _connection():
    wire = RecordingTransport()
    protocol = SimpleNamespace(transport=wire, writing_paused=False,
                               connected=True, start_timeout=lambda: None)
    return wire, SimpleNamespace(protocol=protocol, _connector=SimpleNamespace(force_close=False))


@pytest.mark.asyncio
async def test_actual_non_eager_aiohttp_writer_submits_after_send_returns(monkeypatch):
    _non_eager(monkeypatch)
    wire, conn = _connection()
    request = aiohttp.ClientRequest('POST', URL('https://discord.invalid/messages'),
                                    data=b'PRIVATE_BODY', loop=asyncio.get_running_loop())
    response = await request.send(conn)
    assert wire.writes == []
    await request._writer
    assert b'PRIVATE_BODY' in b''.join(wire.writes)
    response.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('boundary', ['context', 'request'])
async def test_non_eager_automatic_transport_rejects_before_callback_or_wire(monkeypatch, boundary):
    _non_eager(monkeypatch)
    wire, conn = _connection()
    authorized = []
    request = transport._AuthorizedDiscordRequest('POST', URL('https://discord.invalid/messages'),
                                                  data=b'PRIVATE_BODY', loop=asyncio.get_running_loop())
    token = None
    try:
        with pytest.raises(RuntimeError, match='Python 3.12'):
            if boundary == 'context':
                adapter = transport.DiscordForumSyncMixin()
                session = SimpleNamespace(_request_class=aiohttp.ClientRequest)
                adapter._client = SimpleNamespace(http=SimpleNamespace(_HTTPClient__session=session))
                with adapter.automatic_forum_delivery(lambda: authorized.append(True)):
                    await request.send(conn)
            else:
                # An inherited automatic context must fail closed at send too.
                token = transport._submission_authorize.set(lambda: authorized.append(True))
                await request.send(conn)
    finally:
        if token is not None:
            transport._submission_authorize.reset(token)
        if request._writer is not None:
            await request._writer
        if request.response is not None:
            request.response.close()
    assert authorized == []
    assert wire.writes == []


@pytest.mark.asyncio
async def test_non_eager_manual_transport_still_writes(monkeypatch):
    _non_eager(monkeypatch)
    wire, conn = _connection()
    request = transport._AuthorizedDiscordRequest('POST', URL('https://discord.invalid/messages'),
                                                  data=b'MANUAL_BODY', loop=asyncio.get_running_loop())
    response = await request.send(conn)
    await request._writer
    assert b'MANUAL_BODY' in b''.join(wire.writes)
    response.close()
