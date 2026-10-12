---
sidebar_position: 8
title: "Shared session clients"
description: "Continue a live session between Hermes Desktop and Discord on the same host and profile"
---

# Shared session clients

Session clients let Desktop and a Discord thread work with the same live Hermes session. With sharing enabled, new Desktop sessions and Discord `/share` sessions execute in a detached, profile-local host. Desktop and Discord are viewing/control clients, not execution owners. Sharing does not start a second agent or transfer a running agent between clients.

## Enable sharing

Sharing is **off by default**. In Desktop, open **Settings → Advanced → Desktop** and turn on **Enable session sharing** for the profile you want to use. You can also set it from that profile's terminal:

```bash
hermes config set session_clients.enabled true
```

Both Desktop and the Discord gateway must use the **same host and Hermes profile**. They communicate through a shared mailbox in that profile. This feature does not connect separate hosts or bridge separate profiles. Configure Discord and its authorized users/channels before attaching a session; a share token does not bypass Discord authorization.

## Share Desktop to Discord

1. Enable sharing, then open a new conversation in Desktop (or reopen stored history after finishing and closing its previous run).
2. Select **Share to Discord** beside the conversation title.
3. Copy the `/attach TOKEN` command shown in the dialog.
4. Paste it in an authorized Discord channel served by the gateway for the same profile. Mention the bot if that channel requires mentions (for example, `@Hermes /attach TOKEN`).

The token is **single use** and expires after **10 minutes**. Treat it as a temporary access credential. If it expires or has already been used, select **Share to Discord** again to create another token.

`/attach TOKEN` creates a **new Discord thread** in that channel and links it to the existing session. Continue the conversation in that thread. It does not attach to or replace an existing channel conversation.

## Start from Discord

Use `/share` in an authorized Discord channel to create a shared session. You can then open that session from Desktop's session history on the same host and profile. Both clients use the same live owner for prompts, steering, and stop requests.

## Monitor and control

Desktop receives live assistant output, tool progress, shared prompts, and attachment notifications. Discord receives the final reply and rate-limited progress, without echoing its own incoming prompts or uploads back into the originating thread. Received attachments in Desktop are display-only: they are not staged for retransmission.

In the linked Discord thread, send ordinary prompts or use `/steer TEXT` to guide the active agent and `/stop` to interrupt it. Desktop's existing prompt, steering, and stop controls target that same agent. File and image uploads pass through the owner's existing attachment handlers.

Disconnecting or closing a viewing client, switching conversations, and stopping its viewing backend do not stop the active run. Desktop `session.close` detaches that view; use the stop control to interrupt execution. Reconnecting Desktop resumes the same host runtime and history; Discord delivery uses a durable cursor so normal gateway reconnects do not replay completed replies. Turning sharing off disables new client controls and mirrored events without starting a replacement run. The current clients are native Desktop and Discord; the legacy browser-chat backend is not connected to this transport.

## Privacy and lifetime

Private chats are **never automatically mirrored** into Discord. Linking requires an explicit share and attach action. Choose the destination channel with its members and permissions in mind.

For new detached-host sessions, **neither Desktop nor the Discord gateway needs to remain open for execution to continue**; the execution host must remain alive. Discord delivery pauses while its gateway is offline and catches up from its durable cursor when it returns. For bridged legacy sessions, the original gateway remains the execution owner and must stay running. The detached host uses the profile's credentials and private mailbox; it has no public network listener. Its logs live in `<profile home>/session-clients/host.log`.

If the execution host itself exits, controls fail closed: an admitted prompt is never automatically retried in a replacement process. Explicitly reopening stored history can start a fresh host and resume that conversation, but cannot recover the lost live Python agent or automatically restart its interrupted task.

A session already running before sharing was enabled remains owned by its original runtime. Sharing cannot move that live Python agent into the detached host. Private hosted drafts remain out of saved history until a prompt or explicit sharing action.

## Existing conversations and rollback

**An in-place legacy GatewayRunner bridge is implemented; full migration is not.** With sharing enabled on the same host/profile, Desktop resumes a registered existing Discord conversation on its original gateway owner before native cold-resume can repair history or retire queued rows. The gateway's actual agent, pinned prompt, queues, source identity, authorization and existing Discord destination stay in that process. Desktop can observe streamed output/progress and use plain-text prompts, steering and stop without `/share`, `/attach`, reopening the conversation or creating another Discord thread. Attached views protect the incumbent cache from passive TTL/LRU/pressure eviction; explicit native conversation boundaries still control teardown.

Known incumbent owners and verified compression aliases are generation-fenced. Branch/reset/delegate children do not inherit an old view's authority. Native completion notifications remain on the original gateway path; unavailable ownership is refused before notification admission. Private Desktop sessions are not auto-mirrored. An undiscovered, retired, dead or unresponsive legacy owner fails closed rather than constructing another writer or replaying admitted work. The bridge does not move an agent between processes. Its initial control surface excludes voice, attachments, rewind/hidden prompts and client queue replay; interactive dialogs keep their original gateway answer transport.

Turning sharing off is not a reverse migration. It does not rewrite the original routing index, import JSONL transcripts, or restore a database snapshot. A route with an existing shared owner remains fenced rather than starting another writer. There is currently no supported procedure that hands legacy ownership back while reconciling the latest canonical history and compression aliases; do not use this setting as a legacy migration/rollback mechanism.

This is same-host, same-profile access to a live session, not remote-host synchronization or process failover.

Cross-process interactive approval/clarification dialogs are not bridged by this initial transport. Disable session sharing **before creating** a session for workflows requiring those dialogs. Delivery is replay-safe across normal reconnects, but a crash between an acknowledged Discord send and its cursor write can duplicate that delivery. The detached lifecycle is verified on Linux; native Windows process-tree/update termination behavior still requires platform testing.
