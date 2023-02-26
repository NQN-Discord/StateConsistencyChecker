from typing import Set, Tuple, Dict, Optional, List
import asyncio
import aioredis
import argparse
from sys import stderr
from discord import Client, Guild, Forbidden
from logging import basicConfig, INFO, getLogger
import json


basicConfig(stream=stderr, level=INFO, format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
log = getLogger(__name__)

global_attrs = ("id", "owner_id", "icon", "name", "_system_channel_id", "premium_tier")
array_attrs = {
    "roles": ("id", "position", "_permissions", "name", "mentionable", "managed", "hoist", "_colour"),
    "channels": ("id", "type", "topic", "rate_limit_per_user", "position", "parent_id", "nsfw", "name", "permissions_overwrites"),
    "emojis": ("id", "_roles", "require_colons", "managed", "available", "animated")
}


def parse_id_dict(d):
    return [json.loads(v) for v in d.values()]


def deepgetattr(v, k: str):
    key, *rest = k.split(".", 1)

    if isinstance(v, (list, tuple)):
        new_v = next((i for i in v if str(i.id) == key), None)
        if new_v is None:
            return float("nan")
    elif isinstance(v, dict):
        new_v = v[key]
    else:
        new_v = getattr(v, key)

    if rest:
        return deepgetattr(new_v, rest[0])
    return new_v


class ConsistencyChecker:
    def __init__(self, redis: aioredis.Redis, client: Client):
        self.redis = redis
        self.client = client

    async def check_guild(self, guild_id: int, existing_guild: Guild) -> Dict[str, Tuple[str, str]]:
        try:
            guild_discord = await self.client.fetch_guild(guild_id)
        except Forbidden:
            return {}
        global_equalities = self.get_equalities(guild_discord, existing_guild, global_attrs)

        for array_type, attrs in array_attrs.items():
            discord_objects = sorted(getattr(guild_discord, array_type), key=lambda x: x.id)
            other_objects = sorted(getattr(existing_guild, array_type), key=lambda x: x.id)

            for i, j in zip(discord_objects, other_objects):
                for attr, value in self.get_equalities(i, j, attrs).items():
                    global_equalities[f"{array_type}.{i.id}.{attr}"] = value

        return {k: (repr(deepgetattr(guild_discord, k)), repr(deepgetattr(existing_guild, k))) for k, v in global_equalities.items() if not v}

    def get_guild_attrs(self, guild: Guild) -> Dict[str, str]:
        guild_attrs: List[str] = list(global_attrs)
        for array_type, attrs in array_attrs.items():
            discord_objects = sorted(getattr(guild, array_type), key=lambda x: x.id)
            guild_attrs.extend(f"{array_type}.{i.id}.{attr}" for attr in attrs for i in discord_objects)
        return {k: (repr(deepgetattr(guild, k))) for k in guild_attrs}

    def get_equalities(self, a, b, attrs: Tuple) -> Dict[str, bool]:
        return {attr: getattr(a, attr) == getattr(b, attr) for attr in attrs}