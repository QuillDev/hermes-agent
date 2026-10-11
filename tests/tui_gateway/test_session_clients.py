"""Cross-process clients share one execution owner, not copies of its transcript."""
import multiprocessing
import threading
import time

import pytest


def _home(tmp_path):
    tmp_path.mkdir(exist_ok=True)
    (tmp_path / 'config.yaml').write_text('session_clients:\n  enabled: true\n')
    return tmp_path


def test_private_sessions_need_explicit_single_use_profile_bound_invite(tmp_path):
    from tui_gateway.session_clients import SessionClientHub
    hub = SessionClientHub(_home(tmp_path / 'a'), start=False)
    other = SessionClientHub(_home(tmp_path / 'b'), start=False)
    record = hub.claim('stored', 'runtime')
    assert hub.links('discord') == []
    invite = hub.invite(record['session_key'])
    with pytest.raises(ValueError):
        other.attach(invite['token'], 'discord', 'thread', 'owner')
    linked = hub.attach(invite['token'], 'discord', 'thread', 'owner')
    assert linked['session_key'] == 'stored'
    assert hub.link('discord', 'thread')['user_id'] == 'owner'
    with pytest.raises(ValueError):
        hub.attach(invite['token'], 'discord', 'other-thread', 'owner')
    assert len(hub.links('discord')) == 1
    hub.close(); other.close()


def _reader(home, output):
    from tui_gateway.session_clients import SessionClientHub
    hub = SessionClientHub(home, start=False)
    record = hub.lookup('stored')
    frames = hub.events('stored', 0)
    output.put((record['sid'], [(e['seq'], e['frame']) for e in frames]))
    hub.close()


def test_other_process_reads_same_owner_and_replay_without_republishing(tmp_path):
    from tui_gateway.session_clients import SessionClientHub
    hub = SessionClientHub(_home(tmp_path), start=False)
    hub.claim('stored', 'runtime')
    frame = {'method': 'event', 'params': {'type': 'message.complete', 'session_id': 'runtime',
                                          'payload': {'text': 'one answer'}}}
    seq = hub.publish('stored', frame)
    ctx = multiprocessing.get_context('spawn')
    output = ctx.Queue()
    proc = ctx.Process(target=_reader, args=(str(tmp_path), output))
    proc.start(); proc.join(20)
    assert proc.exitcode == 0
    sid, replay = output.get(timeout=5)
    assert sid == 'runtime'
    assert replay == [(seq, frame)]
    assert hub.events('stored', seq) == []
    assert hub.event_head('stored') == seq
    hub.close()


def test_second_live_owner_cannot_claim_same_session(tmp_path):
    from tui_gateway.session_clients import SessionClientHub
    a = SessionClientHub(_home(tmp_path), start=False)
    b = SessionClientHub(tmp_path, start=False)
    a.claim('stored', 'first')
    with pytest.raises(RuntimeError):
        b.claim('stored', 'second')
    assert b.lookup('stored')['sid'] == 'first'
    a.close()
    with pytest.raises(RuntimeError, match='unavailable'):
        b.call('stored', 'prompt.submit', {'text': 'must not start a replacement'})
    dead = b.lookup('stored')
    assert dead is not None and dead['sid'] == 'first'
    b.close()


def test_expired_invitation_cannot_publish_a_private_session(tmp_path, monkeypatch):
    from tui_gateway import session_clients
    hub = session_clients.SessionClientHub(_home(tmp_path), start=False)
    hub.claim('stored', 'runtime')
    invitation = hub.invite('stored')
    monkeypatch.setattr(session_clients.time, 'time', lambda: invitation['expires_at'] + 1)
    try:
        with pytest.raises(ValueError, match='expired'):
            hub.attach(invitation['token'], 'discord', 'thread', 'owner')
        assert hub.links('discord') == []
    finally:
        hub.close()


def test_late_client_does_not_replay_and_disabling_keeps_private_events_off_mailbox(tmp_path):
    from tui_gateway.session_clients import SessionClientHub
    hub = SessionClientHub(_home(tmp_path), start=False)
    hub.claim('stored', 'runtime')
    class Peer:
        def __init__(self):
            self.frames = []
        def write(self, value):
            self.frames.append(value)
            return True
    early, late = Peer(), Peer()
    hub.subscribe('stored', early, after=0)
    one = {'method': 'event', 'params': {'type': 'message.complete', 'payload': {'text': 'one'}}}
    two = {'method': 'event', 'params': {'type': 'message.complete', 'payload': {'text': 'two'}}}
    try:
        hub.publish('stored', one)
        hub.subscribe('stored', late)
        hub._deliver()
        assert early.frames == [one]
        assert late.frames == []
        (tmp_path / 'config.yaml').write_text('session_clients:\n  enabled: false\n')
        hub.publish('stored', {'method': 'event', 'params': {'type': 'message.complete',
                                                          'payload': {'text': 'now private'}}})
        assert len(hub.events('stored', 0)) == 1
        (tmp_path / 'config.yaml').write_text('session_clients:\n  enabled: true\n')
        hub.publish('stored', two)
        hub._deliver()
        assert early.frames == [one, two]
        assert late.frames == [two]
    finally:
        hub.close()


def test_rpc_steers_and_stops_original_owner_not_another_agent(tmp_path, monkeypatch):
    from tui_gateway import server
    from tui_gateway.session_clients import SessionClientHub
    from tui_gateway.transport import bind_transport, reset_transport
    from hermes_state import SessionDB

    home = _home(tmp_path)
    db = SessionDB(home / 'state.db')
    db.create_session('stored', source='desktop')
    monkeypatch.setattr(server, '_hermes_home', home)
    monkeypatch.setattr(server, '_get_db', lambda: db)

    class Agent:
        def __init__(self):
            self.steering = []
            self.stopped = False
        def steer(self, text):
            self.steering.append(text)
        def interrupt(self, *a, **kw):
            self.stopped = True
    agent = Agent()
    session = {'session_key': 'stored', 'profile_home': str(home), 'agent': agent,
               'running': True, 'history_lock': threading.RLock(), 'history': [],
               'created_at': time.time(), 'last_active': time.time(), 'transport': None}
    monkeypatch.setitem(server._sessions, 'runtime', session)
    owner = SessionClientHub(home)
    owner.claim('stored', 'runtime')
    caller = SessionClientHub(home, start=False)
    session['session_key'] = 'new-compression-tip'
    owner.publish('stored', {'method': 'event', 'params': {'type': 'session.compressed'}})
    canonical = caller.lookup('new-compression-tip')
    assert canonical is not None and canonical['session_key'] == 'stored'
    invite = owner.invite('new-compression-tip')
    assert caller.attach(invite['token'], 'discord', 'thread', 'owner')['session_key'] == 'stored'
    with pytest.raises(RuntimeError):
        caller.claim('new-compression-tip', 'conflicting-runtime')
    reply = caller.call('new-compression-tip', 'session.steer', {'text': 'keep this same run'})
    assert 'error' not in reply, reply
    assert agent.steering == ['keep this same run']
    reply = caller.call('stored', 'session.interrupt', {})
    assert 'error' not in reply, reply
    assert agent.stopped
    assert session['agent'] is agent
    owner.close(); caller.close(); db.close()
