from typing import Set, Tuple, Dict, Optional
import yaml
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

    if isinstance(v, list):
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
    def __init__(self, redis_uri: str, discord_token: str):
        self.redis_uri = redis_uri
        self.discord_token = discord_token

    async def connect(self):
        self.redis = await aioredis.create_redis_pool(self.redis_uri)
        self.client = Client()
        await self.client.login(self.discord_token)
        log.info("Connected to Discord")

    async def guild_count(self) -> int:
        return await self.redis.scard("guilds")

    async def fetch_random_guild_ids(self, count: int) -> Set[int]:
        guild_ids = await self.redis.srandmember("guilds", count)
        return set(int(i) for i in guild_ids)

    async def fetch_all_guilds(self) -> Set[int]:
        guild_ids = await self.redis.smembers("guilds")
        return set(int(i) for i in guild_ids)

    async def fetch_guild_redis(self, guild_id: int) -> Guild:
        user = json.loads(await self.redis.get("user"))

        tr = self.redis.multi_exec()
        futures = {
            "channels": tr.hgetall(f"channels-{guild_id}"),
            "roles": tr.hgetall(f"roles-{guild_id}"),
            "emojis": tr.hgetall(f"emojis-{guild_id}"),
            "guild": tr.hgetall(f"guild-{guild_id}"),
            "me": tr.smembers(f"me-{guild_id}"),
            "nick": tr.get(f"nick-{guild_id}"),
        }
        await tr.execute()
        nick = await futures["nick"]
        if nick:
            nick = nick.decode()
        guild = {
            "channels": parse_id_dict(await futures["channels"]),
            "roles": parse_id_dict(await futures["roles"]),
            "emojis": parse_id_dict(await futures["emojis"]),
            "members": [{
                "user": user,
                "roles": [int(r) for r in await futures["me"]],
                "nick": nick
            }],
            "id": guild_id,
            **{k.decode(): v.decode() for k, v in (await futures["guild"]).items()},
        }
        guild["member_count"] = int(guild["member_count"])
        guild["system_channel_id"] = guild["system_channel_id"] or None
        guild["premium_tier"] = int(guild.get("premium_tier", "0") or "0")
        guild["icon"] = guild["icon"] or None
        guild["members"][0]["joined_at"] = guild["joined_at"]
        return self.client._connection._add_guild_from_data(guild)

    async def check_guild(self, guild_id: int, existing_guild: Optional[Guild] = None) -> Dict[str, Tuple[str, str]]:
        try:
            guild_discord = await self.client.fetch_guild(guild_id)
        except Forbidden:
            return {}
        if existing_guild is None:
            guild_other = await self.fetch_guild_redis(guild_id)
        else:
            guild_other = existing_guild
        global_equalities = self.get_equalities(guild_discord, guild_other, global_attrs)

        for array_type, attrs in array_attrs.items():
            discord_objects = sorted(getattr(guild_discord, array_type), key=lambda x: x.id)
            other_objects = sorted(getattr(guild_other, array_type), key=lambda x: x.id)

            for i, j in zip(discord_objects, other_objects):
                for attr, value in self.get_equalities(i, j, attrs).items():
                    global_equalities[f"{array_type}.{i.id}.{attr}"] = value

        return {k: (repr(deepgetattr(guild_discord, k)), repr(deepgetattr(guild_other, k))) for k, v in global_equalities.items() if not v}

    def get_equalities(self, a, b, attrs: Tuple) -> Dict[str, bool]:
        return {attr: getattr(a, attr) == getattr(b, attr) for attr in attrs}


async def main(config, args):
    checker = ConsistencyChecker(config["redis_uri"], config["token"])
    await checker.connect()

    guild_count = args.absolute
    total_guilds = 0
    if guild_count is None:
        total_guilds = await checker.guild_count()
        guild_count = int(args.percent * total_guilds / 100)
    if guild_count == total_guilds:
        guild_ids = await checker.fetch_all_guilds()
    else:
        guild_ids = await checker.fetch_random_guild_ids(guild_count)
    print(f"Starting to check {len(guild_ids)} guilds")
    for i, guild_id in enumerate(guild_ids, 1):
        if i % 100 == 0:
            print(f"Checking guild {i}")
        incorrect = await checker.check_guild(guild_id)
        if incorrect:
            print(guild_id, incorrect)
    await checker.client.logout()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gateway State Consistency Checker")
    parser.add_argument("--percent", type=int)
    parser.add_argument("--absolute", type=int)

    args = parser.parse_args()

    with open("config.yaml") as conf_file:
        config = yaml.load(conf_file, Loader=yaml.SafeLoader)

    asyncio.run(main(config, args))
