"""Opt-in Discord transport for a shared TUI runtime, without a second agent."""
from __future__ import annotations

import asyncio
import logging
import sqlite3
import time
from pathlib import Path

from gateway.config import Platform

logger = logging.getLogger(__name__)


def _cursor(home, session_key, chat_id, seq=None):
    # Keep delivery state separate from the broker's schema. A restart must not replay finals.
    with sqlite3.connect(Path(home) / "session_clients_gateway.db") as db:
        db.execute("CREATE TABLE IF NOT EXISTS cursors (session_key TEXT, chat_id TEXT, seq INTEGER, "
                   "PRIMARY KEY(session_key, chat_id))")
        if seq is not None:
            db.execute("INSERT INTO cursors VALUES (?, ?, ?) ON CONFLICT(session_key, chat_id) "
                       "DO UPDATE SET seq = excluded.seq", (session_key, chat_id, seq))
            return seq
        row = db.execute("SELECT seq FROM cursors WHERE session_key = ? AND chat_id = ?",
                         (session_key, chat_id)).fetchone()
        return row[0] if row else 0


def _rpc(hub, link, method, **params):
    origin = params.pop("origin", None) or (f"discord:{link['chat_id']}" if link.get('chat_id') else None)
    response = hub.call(link["session_key"], method, {"session_id": link["sid"], **params},
                        origin=origin, expected_owner=link.get('owner'))
    if response.get("error"):
        raise RuntimeError(response["error"].get("message", "Shared session request failed"))
    return response.get("result", {})


def _is_bot(event):
    author = getattr(event.raw_message, "author", None)
    return bool(getattr(author, "bot", False) or event.metadata.get("is_bot") or getattr(event.source, "is_bot", False))


async def _bind(runner, event, hub, home, command):
    source = event.source
    adapter = runner._delivery_adapter_for(source)
    if adapter is None:
        return "Discord is unavailable for this conversation."
    if command == "attach" and not event.get_command_args().strip():
        return "Usage: /attach TOKEN"
    # Tokens always get their own thread; never bind a desktop session to a public parent channel.
    if getattr(source, "auto_thread_created", False):
        # Discord's normal @mention ingress already created a fresh thread.
        # Threads cannot host child threads; bind that fresh, empty destination.
        thread = str(source.chat_id)
    else:
        parent_id = getattr(source, "parent_chat_id", None) or source.chat_id
        thread = await adapter.create_handoff_thread(str(parent_id), "Shared Hermes session")
    if not thread and command == "attach":
        return "Could not create a shared session thread. Try /attach in a server channel."
    chat_id, user_id = str(thread or source.chat_id), str(source.user_id)
    if command == "attach":
        link = await asyncio.to_thread(hub.attach, event.get_command_args().strip(), "discord", chat_id, user_id)
    else:
        from tui_gateway.session_client_host import create_on_host
        created = await asyncio.to_thread(create_on_host, home)
        if created.get('error'):
            raise RuntimeError(created['error']['message'])
        link = await asyncio.to_thread(hub.lookup, created['result']['stored_session_id'])
        invitation = await asyncio.to_thread(_rpc, hub, link, 'session.clients.share')
        link = await asyncio.to_thread(hub.attach, invitation['token'], 'discord', chat_id, user_id)
    head = await asyncio.to_thread(hub.event_head, link["session_key"])
    await asyncio.to_thread(_cursor, home, link["session_key"], chat_id, head)
    await adapter.send(chat_id, "Shared session connected. Messages here use the same session as Desktop.")
    return f"Shared session connected: <#{chat_id}>" if thread else "Shared session connected in this chat."


async def _submit(event, hub, link):
    command = event.get_command()
    if command in {"share", "attach"}:
        return f"This thread is already attached to shared session: {link['sid']}"
    if command == "stop":
        await asyncio.to_thread(_rpc, hub, link, "session.interrupt")
        return "Stop requested."
    if command == "steer":
        text = event.get_command_args().strip()
        if not text:
            return "Usage: /steer TEXT"
        await asyncio.to_thread(_rpc, hub, link, "session.steer", text=text, origin=f"discord:{event.source.chat_id}")
        return None
    if command == "status":
        return f"Shared session: {link['sid']}"
    refs = []
    origin = f"discord:{event.source.chat_id}"
    for index, path in enumerate(event.media_urls):
        mime = event.media_types[index] if index < len(event.media_types) else ""
        method = "image.attach" if mime.startswith("image/") else "file.attach"
        result = await asyncio.to_thread(_rpc, hub, link, method, path=path, origin=origin)
        if result.get("ref_text"):
            refs.append(result["ref_text"])
    text = "\n".join([*refs, event.text or ""])
    await asyncio.to_thread(_rpc, hub, link, "prompt.submit", text=text, origin=origin)
    return None


async def handle_session_client_message(runner, event) -> tuple[bool, str | None]:
    """Called only after the gateway's admission and emergency-stop gates."""
    if event.source.platform != Platform.DISCORD or event.internal:
        return False, None
    from tui_gateway.session_clients import enabled, get_hub
    home = runner._resolve_profile_home_for_source(event.source)
    sharing_enabled = await asyncio.to_thread(enabled, home)
    if not sharing_enabled and not (Path(home) / 'session-clients' / 'broker.sqlite').exists():
        return False, None
    from gateway.run import _profile_runtime_scope
    with _profile_runtime_scope(home):
        hub = await asyncio.to_thread(get_hub, home)
        link = await asyncio.to_thread(hub.link, "discord", str(event.source.chat_id))
        if not sharing_enabled:
            return (True, "Shared clients are disabled; no replacement run was started.") if link else (False, None)
        command = event.get_command()
        if not link and command not in {"attach", "share"}:
            return False, None
        if _is_bot(event):
            return True, None
        if link and str(event.source.user_id) != str(link["user_id"]):
            return True, "This shared session belongs to another Discord user."
        try:
            if link:
                return True, await _submit(event, hub, link)
            return True, await _bind(runner, event, hub, home, command)
        except (RuntimeError, ValueError, TimeoutError, OSError) as exc:
            logger.warning("Shared session request failed: %s", exc)
            return True, f"Shared session request failed: {exc}"


def _event_content(frame, chat_id):
    params = frame.get("params", {})
    event = params.get("type", params.get("event", frame.get("method")))
    data = params.get("payload", params.get("data", params))
    origin = data.get("origin")
    if origin == f"discord:{chat_id}":
        return event, {}, ""
    if event in {"message.complete", "message.interim", "session.clients.input"}:
        return event, data, str(data.get("text") or "")
    if event == "session.clients.attachment":
        return event, {"attachments": [{"path": data["path"]}]}, ""
    if event == "tool.start":
        return event, data, f"Working: {data.get('name', 'tool')}"
    return event, {}, ""


def _outbound_media(text, data):
    from gateway.platforms.base import BasePlatformAdapter
    media, text = BasePlatformAdapter.extract_media(text)
    images, text = BasePlatformAdapter.extract_images(text)
    attachments = list(data.get("attachments", []))
    for path, _voice in BasePlatformAdapter.filter_media_delivery_paths(media):
        attachments.append({"path": path})
    for url, caption in images:
        attachments.append({"url": url, "mime_type": "image/", "caption": caption})
    return text, attachments


async def _send_attachment(adapter, chat_id, attachment):
    import mimetypes
    from gateway.platforms.base import BasePlatformAdapter
    path = attachment.get("path") or attachment.get("url")
    if not path:
        return
    if attachment.get("path") and not BasePlatformAdapter.filter_media_delivery_paths([(path, False)]):
        return
    mime = attachment.get("mime_type") or mimetypes.guess_type(path)[0] or ""
    if mime.startswith("image/") and attachment.get("path"):
        result = await adapter.send_image_file(chat_id, path, caption=attachment.get("caption"))
    elif mime.startswith("image/"):
        result = await adapter.send_image(chat_id, path, caption=attachment.get("caption"))
    else:
        result = await adapter.send_document(chat_id, path, caption=attachment.get("caption"))
    if not result.success:
        raise RuntimeError("Shared session attachment delivery failed")


async def _send_frame(adapter, chat_id, frame, last_progress):
    event, data, text = _event_content(frame, chat_id)
    progress = event in {"message.interim", "tool.start"}
    if progress and time.monotonic() - last_progress < 3:
        return last_progress
    text, attachments = _outbound_media(text, data)
    if text:
        result = await adapter.send(chat_id, text, metadata={"_interim_send": True} if progress else None)
        if not result.success:
            raise RuntimeError("Shared session message delivery failed")
        if progress:
            last_progress = time.monotonic()
    for attachment in attachments:
        await _send_attachment(adapter, chat_id, attachment)
    return last_progress


async def _poll_link(hub, home, adapter, link, progress):
    key, chat_id = link["session_key"], str(link["chat_id"])
    after = await asyncio.to_thread(_cursor, home, key, chat_id)
    frames = await asyncio.to_thread(hub.events, key, after)
    for row in frames:
        slot = (str(home), key, chat_id)
        progress[slot] = await _send_frame(adapter, chat_id, row["frame"], progress.get(slot, 0))
        await asyncio.to_thread(_cursor, home, key, chat_id, row["seq"])


async def poll_session_clients(runner, home, profile, progress):
    """Deliver one profile's durable broker events through that profile's Discord adapter."""
    from tui_gateway.session_clients import enabled, get_hub
    if not await asyncio.to_thread(enabled, home):
        return
    adapter = runner._adapters_for_profile(profile).get(Platform.DISCORD)
    if adapter is None:
        return
    from gateway.run import _profile_runtime_scope
    with _profile_runtime_scope(home):
        hub = await asyncio.to_thread(get_hub, home)
        links = await asyncio.to_thread(hub.links, "discord")
        for link in links:
            try:
                await _poll_link(hub, home, adapter, link, progress)
            except Exception:
                # A deleted or inaccessible thread must not starve the profile's other clients.
                logger.exception("Shared session delivery failed for Discord chat %s", link["chat_id"])



async def session_client_watcher(runner):
    from gateway.run import _multiplex_profile_homes
    from hermes_constants import get_hermes_home
    # Capture launch identity before the first await; never borrow a routed turn's environment.
    launch_home = get_hermes_home()
    launch_profile = runner._primary_profile_name
    progress = {}
    while runner._running:
        homes = (_multiplex_profile_homes(runner.config) if runner.config.multiplex_profiles
                 else [(launch_profile, launch_home)])
        for profile, home in homes:
            try:
                await poll_session_clients(runner, home, profile, progress)
            except Exception:
                logger.exception("Shared session event delivery failed for profile %s", profile)
        await asyncio.sleep(1)
