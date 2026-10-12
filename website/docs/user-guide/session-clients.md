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

## Automatically mirror new Desktop conversations to a Discord forum

This is a separate, **default-off, profile-local opt-in**. Automatic forum sync requires **Python 3.12–3.14** in both the native execution host and Discord gateway. On Python 3.11 it fails closed: no automatic enrollment, controls, provisioning, or private HTTP submission. The automatic transport rejects that runtime before invoking its authorization callback because aiohttp schedules its first write rather than starting it eagerly. Existing automatic links remain paused, not treated as manual links. Conversations created on that runtime remain private even after an upgrade and idempotent create retry. This limit applies only to automatic forum sync; manual sharing and ordinary Discord remain available on Python 3.11.

It is configured through the CLI in this first slice (there is no new Desktop setting yet):

```bash
hermes config set session_clients.enabled true
hermes config set session_clients.discord_forum.guild_id 1479890967405527113
hermes config set session_clients.discord_forum.forum_name hermes-sessions
hermes config set session_clients.discord_forum.activation_id first-enable
hermes config set session_clients.discord_forum.automatic true
```

`guild_id` is configuration, not a built-in destination. Set these keys only in the intended profile. An optional `session_clients.discord_forum.forum_id` selects an existing forum instead of resolving its unique name. Blank/invalid guild or activation values fail closed. Keep `activation_id` stable across gateway restarts; change it deliberately for a new activation boundary. Changing the activation or destination does not enroll old conversations.

Create a **new, empty Desktop conversation after enabling automatic mode**. Its first accepted prompt persists the canonical conversation and enrolls a forum post; the gateway catches up the first prompt and answer even if they finish before the post exists. Future Desktop activity and forum replies use that same execution owner and canonical history. No share token or attach step is needed. Abandoned drafts, pre-enable drafts/history, resumed existing conversations, branches, and conversations created with imported messages are not automatically published. There is no historical bulk publishing.

The receiving bot must be in the configured guild. The audience is the **guild owner authorized by that bot's current gateway policy**, plus the bot; guild administrators retain Discord's inherent administrative access. A new dedicated forum denies `@everyone` View Channel and allows only those two members. An existing forum must already have that restricted policy: Hermes validates it and does not change its permissions. Unknown-member and role visibility grants are refused. Intake verifies guild, parent forum, bot provenance, the linked user and current policy, in addition to normal gateway admission.

Provisioning requires bot Manage Channels/Manage Roles permissions. Discord's August 8, 2025 [official changelog](https://discord.com/blog/discord-update-august-8-2025) made forums available in all server types without enabling Community; its older October 24, 2024 Forum FAQ still states the previous restriction. Hermes therefore does not reject a guild merely because its `COMMUNITY` feature is absent. An API capability rejection still fails closed without changing guild policy. Posting requires View Channel, Send Messages, Send Messages in Threads and Read Message History. Hermes **does not enable Community, change guild safety requirements, or widen access**. Missing prerequisites leave sync unavailable with an error in the gateway log; Desktop execution remains on its original owner and admitted tasks are not retried. A pre-existing accessible, correctly restricted forum can be used without provisioning privileges.

Forum creation uses `POST /guilds/{guild_id}/channels` with channel type `15`; posts use `POST /channels/{forum_id}/threads` with an initial `message`. These are forum posts, not text-channel handoff threads. Each post has a stable canonical marker. Creation intents and PID/birth-fenced polling claims are durable; active and archived posts are reconciled before linking. Create POSTs are single-attempt (no SDK 5xx retry). An ambiguous response or a deleted/unobservable post never authorizes a duplicate create: polling reconciles, or stays blocked for operator investigation. Normal mirroring still uses the existing origin suppression and durable delivery cursor; an acknowledged message send followed by a crash before its cursor write retains the existing possible delivery-duplicate limitation.

Automatic eligibility is frozen at native creation, including a durable private decision when automatic mode is disabled, history is imported, or parent/branch lineage is present—even with an empty message list. Retrying the same logical create—even after restart—never upgrades that original decision; an unstamped existing runtime is private. Provisioning also excludes branch lineage in the durable conversation row.

Turning automatic mode off pauses these automatic links and controls without stopping or replacing their owner. Activation and destination changes are rechecked at control admission, again by the execution owner, and at each native Discord HTTP write submission after channel/file/media and connection preparation. That task-local transport gate covers every text chunk, upload, fallback and SDK retry after rate-limit waits; entering an async adapter call is not submission. Only a REST request already submitted cannot be recalled. Suppressed or partially delivered frames do not advance their delivery cursor; re-enabling the same activation can therefore duplicate an already delivered chunk of a partial frame (the existing per-frame cursor limitation). Other profiles and unconfigured guilds remain private. Do not use automatic mode to migrate existing histories: `/share` and `/attach` canonical-history migration integration is a **subsequent slice**, not implemented by this option.

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

In the linked Discord thread, send ordinary prompts or use `/steer TEXT` to guide the active agent and `/stop` to interrupt it. Both automatic and manually linked controls obey the receiving bot's command-specific slash policy: being the authorized guild owner does not confer slash-admin access. Registered aliases use their canonical command's policy; unsupported commands are not forwarded as prompts. Desktop's existing prompt, steering, and stop controls target that same agent. File and image uploads pass through the owner's existing attachment handlers.

Disconnecting or closing a viewing client, switching conversations, and stopping its viewing backend do not stop the active run. Desktop `session.close` detaches that view; use the stop control to interrupt execution. Reconnecting Desktop resumes the same host runtime and history; Discord delivery uses a durable cursor so normal gateway reconnects do not replay completed replies. Turning sharing off disables new client controls and mirrored events without starting a replacement run. The current clients are native Desktop and Discord; the legacy browser-chat backend is not connected to this transport.

## Privacy and lifetime

Without the separate automatic-forum opt-in, private chats are **never automatically mirrored** into Discord. Manual linking requires an explicit share and attach action. Choose the destination channel with its members and permissions in mind.

For new detached-host sessions, **neither Desktop nor the Discord gateway needs to remain open for execution to continue**; the execution host must remain alive. Discord delivery pauses while its gateway is offline and catches up from its durable cursor when it returns. For bridged legacy sessions, the original gateway remains the execution owner and must stay running. The detached host uses the profile's credentials and private mailbox; it has no public network listener. Its logs live in `<profile home>/session-clients/host.log`.

If the execution host itself exits, controls fail closed: an admitted prompt is never automatically retried in a replacement process. Explicitly reopening stored history can start a fresh host and resume that conversation, but cannot recover the lost live Python agent or automatically restart its interrupted task.

During the host-generation rollout, new unkeyed Desktop creates use the updated host; existing live owners are not moved. Caller-supplied create keys must be non-empty strings without leading or trailing whitespace: malformed keys are rejected, not trimmed or rewritten. Missing or null keys retain unkeyed behavior. The `hermes-host-v2:` prefix is for a fresh logical create only; retain the exact key on retries. A durable create route fences the selected host generation and owner, but **does not persist the native create result**: native result deduplication retains its existing 300-second TTL and requires the original runtime to remain available. Do not treat the route or the durable forum-eligibility decision as an unlimited exactly-once create guarantee.

A session already running before sharing was enabled remains owned by its original runtime. Sharing cannot move that live Python agent into the detached host. Private hosted drafts remain out of saved history until a prompt or explicit sharing action.

## Existing conversations and rollback

**An in-place legacy GatewayRunner bridge is implemented; full migration is not.** With sharing enabled on the same host/profile, Desktop resumes a registered existing Discord conversation on its original gateway owner before native cold-resume can repair history or retire queued rows. The gateway's actual agent, pinned prompt, queues, source identity, authorization and existing Discord destination stay in that process. Desktop can observe streamed output/progress and use plain-text prompts, steering and stop without `/share`, `/attach`, reopening the conversation or creating another Discord thread. Attached views protect the incumbent cache from passive TTL/LRU/pressure eviction; explicit native conversation boundaries still control teardown.

Known incumbent owners and verified compression aliases are generation-fenced. Branch/reset/delegate children do not inherit an old view's authority. Native completion notifications remain on the original gateway path; unavailable ownership is refused before notification admission. Existing private Desktop sessions are not auto-mirrored; only qualifying new conversations in the separately enabled automatic-forum mode are published. An undiscovered, retired, dead or unresponsive legacy owner fails closed rather than constructing another writer or replaying admitted work. The bridge does not move an agent between processes. Its initial control surface excludes voice, attachments, rewind/hidden prompts and client queue replay; interactive dialogs keep their original gateway answer transport.

Turning sharing off is not a reverse migration. It does not rewrite the original routing index, import JSONL transcripts, or restore a database snapshot. A route with an existing shared owner remains fenced rather than starting another writer. There is currently no supported procedure that hands legacy ownership back while reconciling the latest canonical history and compression aliases; do not use this setting as a legacy migration/rollback mechanism.

This is same-host, same-profile access to a live session, not remote-host synchronization or process failover.

Cross-process interactive approval/clarification dialogs are not bridged by this initial transport. Disable session sharing **before creating** a session for workflows requiring those dialogs. Delivery is replay-safe across normal reconnects, but a crash between an acknowledged Discord send and its cursor write can duplicate that delivery. The detached lifecycle is verified on Linux; native Windows process-tree/update termination behavior still requires platform testing.
