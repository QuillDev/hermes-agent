"""Private hosted sessions do not mirror or change the agent's creator identity."""
import asyncio
import os

import psutil
import pytest

from tests.tui_gateway.test_session_clients_live import DesktopPeer, _until


def _request(peer, request):
    from tui_gateway import server
    from tui_gateway.transport import bind_transport, reset_transport
    token = bind_transport(peer)
    try:
        return server.handle_request(request)
    finally:
        reset_transport(token)


@pytest.mark.asyncio
async def test_private_hosted_sessions_keep_one_host_and_the_creator_identity(tmp_path, monkeypatch):
    from tests.fakes.fake_llm_provider import FakeLLMServer, Text, write_hermes_home
    from tui_gateway import server
    from tui_gateway.session_clients import get_hub
    from tui_gateway.session_client_host import HOST_KEY
    from hermes_state import SessionDB

    alice, guest = DesktopPeer(), DesktopPeer()
    alice.auth_identity = {'provider': 'basic', 'user_id': 'alice'}
    guest.auth_identity = {'provider': 'basic', 'user_id': 'guest'}
    with FakeLLMServer(lambda record: Text('private hosted answer')) as provider:
        home = write_hermes_home(tmp_path / 'profile', provider.base_url, extra_config=(
            'session_clients:\n  enabled: true\n'
            'memory:\n  memory_enabled: false\n  user_profile_enabled: false\n'))
        monkeypatch.setenv('HERMES_HOME', str(home))
        monkeypatch.setattr(server, '_hermes_home', home)
        hub = get_hub(home)
        try:
            results = await asyncio.gather(*[asyncio.to_thread(_request, alice, {
                'id': f'create-{i}', 'method': 'session.create', 'params': {'source': 'desktop'}})
                for i in range(2)])
            assert all('result' in result for result in results), results
            sessions = [r['result'] for r in results]
            owners = [hub.lookup(s['stored_session_id']) for s in sessions]
            assert owners[0]['pid'] == owners[1]['pid'] != os.getpid()
            assert owners[0]['session_key'] != owners[1]['session_key']
            sid, key = sessions[0]['session_id'], sessions[0]['stored_session_id']
            assert sid not in server._sessions
            assert hub.links('discord') == []
            with SessionDB(home / 'state.db') as db:
                assert db.get_session(key) is None  # Abandoned private drafts stay out of history.
            submitted = await asyncio.to_thread(_request, alice, {
                'id': 'private-prompt', 'method': 'prompt.submit',
                'params': {'session_id': sid, 'text': 'private hosted prompt'}})
            assert submitted['result']['status'] == 'streaming', submitted
            await _until(lambda: any(f.get('params', {}).get('type') == 'message.complete' for f in alice.frames))
            assert any(f.get('params', {}).get('payload', {}).get('text') == 'private hosted answer' for f in alice.frames)
            assert hub.links('discord') == []
            joined = await asyncio.to_thread(_request, guest, {
                'id': 'guest-join', 'method': 'session.resume', 'params': {'session_id': key}})
            assert joined['result']['session_id'] == sid, joined
            second = await asyncio.to_thread(_request, guest, {
                'id': 'guest-prompt', 'method': 'prompt.submit',
                'params': {'session_id': sid, 'text': 'same session, different viewing login'}})
            assert second['result']['status'] == 'streaming', second
            await _until(lambda: any(f.get('params', {}).get('type') == 'message.complete' for f in guest.frames))
            assert len(provider.main_requests()) == 2
            with SessionDB(home / 'state.db') as db:
                assert db.get_session(key)['user_id'] == 'basic:alice'
                messages = db.get_messages_as_conversation(key)
            assert sum(m.get('role') == 'user' for m in messages) == 2
            assert hub.links('discord') == []
            closed = await asyncio.to_thread(_request, alice, {
                'id': 'detach', 'method': 'session.close', 'params': {'session_id': sid}})
            assert closed['result']['closed'], closed
            rejoined = await asyncio.to_thread(_request, alice, {
                'id': 'rejoin', 'method': 'session.resume', 'params': {'session_id': key}})
            assert rejoined['result']['session_id'] == sid
            assert hub.lookup(key)['pid'] == owners[0]['pid']
            process = psutil.Process(owners[0]['pid'])
            process.terminate()
            await asyncio.to_thread(process.wait, 15)
            lost = await asyncio.to_thread(_request, alice, {
                'id': 'lost-owner', 'method': 'prompt.submit',
                'params': {'session_id': sid, 'text': 'must not retry in a replacement'}})
            assert lost.get('error', {}).get('code') == 4092, lost
            assert hub.lookup(HOST_KEY)['pid'] == owners[0]['pid']
            reopened = await asyncio.to_thread(_request, alice, {
                'id': 'explicit-reopen', 'method': 'session.resume', 'params': {'session_id': key}})
            assert 'result' in reopened, reopened
            assert reopened['result']['session_id'] != sid
            assert hub.lookup(key)['pid'] != owners[0]['pid']
            stale = await asyncio.to_thread(_request, alice, {
                'id': 'stale-view', 'method': 'prompt.submit',
                'params': {'session_id': sid, 'text': 'old view must not target new generation'}})
            assert stale.get('error', {}).get('code') == 4092, stale
            assert len(provider.main_requests()) == 2
        finally:
            host = hub.lookup(HOST_KEY)
            if host:
                process = psutil.Process(host['pid'])
                if process.create_time() == host['born']:
                    process.terminate()
                    try:
                        await asyncio.to_thread(process.wait, 15)
                    except psutil.TimeoutExpired:
                        process.kill()
                        await asyncio.to_thread(process.wait, 5)
            hub.close()
