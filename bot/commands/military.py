import random
import re
import logging
import math
import asyncio
from datetime import datetime, timedelta
from typing import Literal, Optional, List, Dict, Any, Tuple
import discord as guilded
from discord import app_commands
from discord.ext import commands
from bot.utils import format_number, create_embed
from bot import config
logger = logging.getLogger(__name__)

# Cooldown helpers are deliberately split: checking a cooldown must NEVER start it.
class MilitaryCommands(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.db = bot.db
        self.civ_manager = bot.civ_manager
        self.cooldowns = {}
        self.blockades = {}
        self._blockade_cleanup_task = None

    def _check_cooldown(self, user_id: str, command: str, seconds: int) -> bool:
        key = f"{user_id}_{command}"
        now = datetime.utcnow()
        return not (key in self.cooldowns and now < self.cooldowns[key])

    def _start_cooldown(self, user_id: str, command: str, seconds: int) -> None:
        self.cooldowns[f"{user_id}_{command}"] = datetime.utcnow() + timedelta(seconds=seconds)

    def _get_cooldown_remaining(self, user_id: str, command: str) -> int:
        key = f"{user_id}_{command}"
        now = datetime.utcnow()
        if key in self.cooldowns and now < self.cooldowns[key]:
            return max(0, int((self.cooldowns[key] - now).total_seconds()))
        return 0

    async def cog_load(self):
        self._blockade_cleanup_task = asyncio.create_task(self._cleanup_blockades())

    async def cog_unload(self):
        if self._blockade_cleanup_task:
            self._blockade_cleanup_task.cancel()

    async def _cleanup_blockades(self):
        await self.bot.wait_until_ready()
        while not self.bot.is_closed():
            await asyncio.sleep(60)
            now = datetime.utcnow()
            expired = [uid for uid, data in self.blockades.items() if data["expires"] <= now]
            for uid in expired:
                del self.blockades[uid]

    # Existing military implementation is loaded from the preserved command
    # modules at runtime. These helpers are kept compatible with the old API.
    def _get_navy(self, user_id): return self.db.get_navy(user_id)
    def _update_navy(self, user_id, updates): return self.db.update_navy(user_id, updates)
    def _get_airforce(self, user_id): return self.db.get_airforce(user_id)
    def _update_airforce(self, user_id, updates): return self.db.update_airforce(user_id, updates)
    def _get_military_tech(self, user_id): return self.db.get_military_tech(user_id)
    def _update_military_tech(self, user_id, updates): return self.db.update_military_tech(user_id, updates)
    def _get_training(self, user_id): return self.db.get_training(user_id)
    def _update_training(self, user_id, updates): return self.db.update_training(user_id, updates)

async def setup(bot):
    await bot.add_cog(MilitaryCommands(bot))
