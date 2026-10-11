---
sidebar_position: 8
title: "Shared session clients"
description: "Continue a live session between Hermes Desktop and Discord on the same host and profile"
---

# Shared session clients

Session clients let Desktop and a Discord thread work with the same live Hermes session. Prompts, steering messages, and stop requests from either client go to the process that owns the session. Sharing does not start a second agent or transfer the running agent to another process.

## Enable sharing

Sharing is **off by default**. In Desktop, open **Settings → Advanced → Desktop** and turn on **Enable session sharing** for the profile you want to use. You can also set it from that profile's terminal:

```bash
hermes config set session_clients.enabled true
```

Both Desktop and the Discord gateway must use the **same host and Hermes profile**. They communicate through a shared mailbox in that profile. This feature does not connect separate hosts or bridge separate profiles. Configure Discord and its authorized users/channels before attaching a session; a share token does not bypass Discord authorization.

## Share Desktop to Discord

1. Open a live conversation in Desktop.
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

Disconnecting a viewer does not stop the owner's active run. Reconnecting Desktop resumes the owner's runtime and history; Discord delivery uses a durable cursor so normal gateway reconnects do not replay completed replies. Turning sharing off stops new mirrored events. The current clients are native Desktop and Discord; the legacy browser-chat backend is not connected to this transport.

## Privacy and lifetime

Private chats are **never automatically mirrored** into Discord. Linking requires an explicit share and attach action. Choose the destination channel with its members and permissions in mind.

The process that owns the session must remain alive. If Desktop owns it, keep Desktop and its backend running. If the Discord gateway owns it, keep that gateway running. Closing the owner app or stopping its backend does **not** migrate the live session to the other client. Stored history remains available, but the live agent cannot continue through an owner that has exited.

This is same-host, same-profile access to a live session, not remote-host synchronization or process failover.

Cross-process interactive approval/clarification dialogs are not bridged by this initial transport. Use ordinary, unshared sessions for workflows requiring those dialogs. Delivery is replay-safe across normal reconnects, but a crash between an acknowledged Discord send and its cursor write can duplicate that delivery.
