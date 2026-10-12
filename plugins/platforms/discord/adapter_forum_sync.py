"""Native Discord forum transport. No guild-policy changes or blind POST retries."""
from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar

import aiohttp

from gateway.session_forum_sync import require_automatic_runtime

_submission_authorize = ContextVar('discord_forum_submission_authorize', default=None)


class _AuthorizedDiscordRequest(aiohttp.ClientRequest):
    async def send(self, conn):
        # Connector/DNS, payload preparation and SDK rate-limit waits are over.
        # The automatic gate is valid only on the eager-writer runtime path.
        authorize = _submission_authorize.get()
        if authorize is not None:
            require_automatic_runtime()
            if self.method not in {'GET', 'HEAD', 'OPTIONS'}:
                authorize()
        return await super().send(conn)

try:
    import discord
    from discord.http import Route
except ImportError:  # The native adapter cannot connect without the optional SDK.
    discord = None
    Route = None


class DiscordForumSyncMixin:
    @contextmanager
    def automatic_forum_delivery(self, authorize):
        """Task-local, non-wire gate for every native HTTP attempt, including retries."""
        require_automatic_runtime()
        session = self._client.http._HTTPClient__session
        if session._request_class not in {aiohttp.ClientRequest, _AuthorizedDiscordRequest}:
            raise RuntimeError('Automatic forum delivery requires the native Discord HTTP transport')
        session._request_class = _AuthorizedDiscordRequest
        token = _submission_authorize.set(authorize)
        try:
            yield
        finally:
            _submission_authorize.reset(token)

    async def _forum_post_once(self, route, payload):
        # discord.py's HTTPClient retries 5xx POSTs. This endpoint has no nonce or
        # idempotency key: a lost response must be reconciled, never retried by SDK.
        token = self._client.http.token
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30),
                                         request_class=_AuthorizedDiscordRequest) as http:
            async with http.post(route.url, json=payload, headers={'Authorization': f'Bot {token}'}) as response:
                if response.status not in {200, 201}:
                    raise RuntimeError(f'Discord forum POST returned HTTP {response.status}; reconcile before retry')
                return await response.json()

    async def _sync_guild(self, cfg):
        if not self._client or not self._client.user:
            raise RuntimeError('Discord receiving bot is unavailable')
        guild = self._client.get_guild(int(cfg['guild_id']))
        if guild is None:
            raise RuntimeError('Configured sync guild is not served by this receiving bot')
        # Fetch current features/roles/owner and members; cache is not an auth policy.
        guild = await self._client.fetch_guild(guild.id)
        bot = await guild.fetch_member(self._client.user.id)
        owner = await guild.fetch_member(guild.owner_id)
        if self._is_sender_authorized(str(owner.id), 'group', cfg.get('forum_id') or None) is not True:
            raise RuntimeError('Sync guild owner is not authorized by the receiving bot policy')
        return guild, bot, owner

    def _validate_sync_forum(self, forum, guild, bot, owner):
        if not isinstance(forum, discord.ForumChannel) or forum.guild.id != guild.id:
            raise RuntimeError('Configured sync destination is not a forum in the configured guild')
        if forum.overwrites_for(guild.default_role).view_channel is not False:
            raise RuntimeError('Sync forum must deny @everyone View Channel')
        allowed = {bot.id, owner.id}
        view_bit = discord.Permissions(view_channel=True).value
        # Public .overwrites drops uncached members. Inspect every REST overwrite,
        # including unknown principals, before publishing private session content.
        for overwrite in forum._overwrites:
            if overwrite.allow & view_bit and (overwrite.is_role() or overwrite.id not in allowed):
                raise RuntimeError('Sync forum exposes private sessions to another member or role')
        if forum.overwrites_for(bot).view_channel is not True or forum.overwrites_for(owner).view_channel is not True:
            raise RuntimeError('Sync forum must explicitly allow the receiving bot and authorized guild owner')
        permissions = forum.permissions_for(bot)
        if not all(getattr(permissions, name) for name in ('view_channel', 'send_messages', 'send_messages_in_threads', 'read_message_history')):
            raise RuntimeError('Receiving bot lacks forum posting/history permissions')
        if forum.flags.require_tag:
            raise RuntimeError('Sync forum requires tags; configure a dedicated forum without required tags')
        if self._is_sender_authorized(str(owner.id), 'group', str(forum.id)) is not True:
            raise RuntimeError('Sync forum is outside receiving bot authorization policy')

    async def ensure_sync_forum(self, cfg, *, create=False):
        """Resolve by explicit ID or unique name; provision ONLY after durable intent."""
        guild, bot, owner = await self._sync_guild(cfg)
        channels = await guild.fetch_channels()
        if cfg.get('forum_id'):
            candidates = [c for c in channels if str(c.id) == cfg['forum_id']]
            if not candidates:
                raise RuntimeError('Configured sync forum does not exist in the configured guild')
        else:
            candidates = [c for c in channels if c.name == cfg['forum_name']]
        if len(candidates) > 1:
            raise RuntimeError('Sync forum name is ambiguous; configure forum_id')
        if candidates:
            forum = candidates[0]
            self._validate_sync_forum(forum, guild, bot, owner)
            return {'forum_id': str(forum.id), 'guild_id': str(guild.id),
                    'bot_id': str(bot.id), 'user_id': str(owner.id)}
        # Discord's Aug 8 2025 changelog removed the Community requirement.
        # Let the API reject unsupported guild capabilities; never change guild policy.
        required = ('manage_channels', 'manage_roles', 'view_channel', 'send_messages',
                    'send_messages_in_threads', 'read_message_history')
        if not all(getattr(bot.guild_permissions, name) for name in required):
            raise RuntimeError('Forum provisioning requires Manage Channels/Manage Roles and forum posting/history permissions')
        if not create:
            return None
        overwrites = {guild.default_role: discord.PermissionOverwrite(view_channel=False),
                      bot: discord.PermissionOverwrite(view_channel=True, send_messages=True,
                           send_messages_in_threads=True, read_message_history=True),
                      owner: discord.PermissionOverwrite(view_channel=True, send_messages=True,
                             send_messages_in_threads=True, read_message_history=True)}
        wire = []
        for target, overwrite in overwrites.items():
            allow, deny = overwrite.pair()
            wire.append({'id': str(target.id), 'type': 0 if isinstance(target, discord.Role) else 1,
                         'allow': str(allow.value), 'deny': str(deny.value)})
        await self._forum_post_once(Route('POST', '/guilds/{guild_id}/channels', guild_id=guild.id),
                                    {'name': cfg['forum_name'], 'type': 15,
                                     'permission_overwrites': wire, 'topic': cfg['forum_marker']})
        # Read the exact configured destination back; an HTTP success isn't proof.
        return await self.ensure_sync_forum(cfg)

    async def reconcile_sync_post(self, destination, marker):
        forum = await self._client.fetch_channel(int(destination['forum_id']))
        active = await forum.guild.active_threads()
        archived = [t async for t in forum.archived_threads(limit=None)]
        found = []
        for thread in [*active, *archived]:
            if thread.parent_id != forum.id or str(thread.owner_id) != destination['bot_id']:
                continue
            starter = await thread.fetch_message(thread.id)
            if str(starter.author.id) == destination['bot_id'] and starter.content == marker:
                found.append(str(thread.id))
        found = list(dict.fromkeys(found))
        if len(found) > 1:
            raise RuntimeError('Multiple sync posts have the canonical marker; refusing ambiguous binding')
        if found:
            await self._threads.mark_async(found[0])
            return found[0]
        return None

    async def create_sync_post(self, destination, marker, name):
        await self._forum_post_once(Route('POST', '/channels/{channel_id}/threads',
                                         channel_id=destination['forum_id']),
                                    {'name': name[:100], 'auto_archive_duration': 1440,
                                     'message': {'content': marker, 'allowed_mentions': {'parse': []}}})
        post = await self.reconcile_sync_post(destination, marker)
        if not post:
            raise RuntimeError('Discord did not expose the created post; awaiting reconciliation')
        return post
