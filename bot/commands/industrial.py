import random
import json
import asyncio
import logging
from datetime import datetime
from typing import Optional, Dict, Any

import discord
from discord.ext import commands
from discord import app_commands

from bot.utils import format_number, create_embed
from bot import config

logger = logging.getLogger(__name__)

# ---- Default stats ----
DEFAULT_STATS = {
    "industrial_power": 0,
    "factory_output": 50,
    "worker_morale": 50,
    "resource_stockpile": 200,
    "pollution": 10,
    "tech_level": 1,
    "infrastructure": 20,
    "energy_supply": 40,
    "labor_unrest": 10,
    "machine_breakdown_risk": 30,
    "production_efficiency": 60,
    "raw_materials": 150,
    "transport_network": 25,
    "urbanization": 20,
    "education": 15,
    "health": 50,
    "environmental_damage": 10,
    "financial_reserves": 500,
    "military_protection": 20,
    "government_support": 40,
    "coal_quality": 50,
    "steam_pressure": 40,
    "railway_coverage": 10,
    "trade_influence": 30,
    "public_trust": 60
}


# ---- Cooldown decorator using config ----
def industrial_cooldown(command_name: str):
    def decorator(func):
        async def wrapper(self, ctx, *args, **kwargs):
            user_id = str(ctx.author.id)
            # Check if command has cooldown in config
            minutes = config.COOLDOWNS.get(command_name, 0)
            if minutes <= 0:
                return await func(self, ctx, *args, **kwargs)
            # Use existing cooldown system (db based)
            last_used = self.db.get_command_cooldown(user_id, command_name)
            if last_used:
                cooldown_end = last_used + timedelta(minutes=minutes)
                if datetime.utcnow() < cooldown_end:
                    remaining = cooldown_end - datetime.utcnow()
                    mins = int(remaining.total_seconds() // 60)
                    secs = int(remaining.total_seconds() % 60)
                    await ctx.send(f"⏳ Please wait {mins}m {secs}s before using this command again!")
                    return
            # Update cooldown
            self.db.set_command_cooldown(user_id, command_name, datetime.utcnow())
            return await func(self, ctx, *args, **kwargs)
        return wrapper
    return decorator


class IndustrialCog(commands.Cog):
    """Industrial Revolution – permanent micromanagement challenge (once per player)."""

    def __init__(self, bot):
        self.bot = bot
        self.db = bot.db
        self.civ_manager = bot.civ_manager
        self._active_revolutions = {}
        self._passive_task = None

    # ---- Firestore-based helpers ----
    def _get_revolution(self, user_id: str) -> Optional[Dict[str, Any]]:
        if user_id in self._active_revolutions:
            return self._active_revolutions[user_id]
        data = self.db.get_industrial_revolution(user_id)
        if not data:
            return None
        data.setdefault("active", False)
        data.setdefault("completed", False)
        data.setdefault("started_at", None)
        data.setdefault("stats", DEFAULT_STATS.copy())
        if data.get("started_at") and isinstance(data["started_at"], str):
            try:
                data["started_at"] = datetime.fromisoformat(data["started_at"])
            except ValueError:
                data["started_at"] = None
        if not isinstance(data["stats"], dict):
            data["stats"] = DEFAULT_STATS.copy()
        if data["active"] or data["completed"]:
            self._active_revolutions[user_id] = data
        return data

    def _save_revolution(self, user_id: str, data: Dict[str, Any]):
        store_data = {
            "active": data["active"],
            "completed": data.get("completed", False),
            "started_at": data["started_at"].isoformat() if data.get("started_at") else None,
            "stats": data["stats"]
        }
        self.db.set_industrial_revolution(user_id, store_data)
        if data["active"] or data.get("completed", False):
            self._active_revolutions[user_id] = data
        else:
            self._active_revolutions.pop(user_id, None)

    def _get_civ_resources(self, user_id: str) -> Dict[str, int]:
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            return {}
        return civ.get('resources', {})

    def _update_civ_resources(self, user_id: str, changes: Dict[str, int]):
        self.civ_manager.update_resources(user_id, changes)

    # ---------- PASSIVE INCOME ----------
    async def _passive_income_loop(self):
        await self.bot.wait_until_ready()
        while not self.bot.is_closed():
            try:
                await asyncio.sleep(300)  # 5 minutes
                await self._apply_passive_income()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in passive income loop: {e}", exc_info=True)

    async def _apply_passive_income(self):
        for user_id, data in list(self._active_revolutions.items()):
            if not data["active"] or data.get("completed", False):
                continue
            stats = data["stats"]
            base = stats["factory_output"] / 10
            efficiency = stats["production_efficiency"] / 100
            morale = stats["worker_morale"] / 100
            tech_bonus = 1 + (stats["tech_level"] * 0.05)
            unrest_penalty = 1 - (stats["labor_unrest"] / 200)
            stockpile_gain = int(base * efficiency * morale * tech_bonus * unrest_penalty)
            stockpile_gain = max(0, stockpile_gain)
            raw_gain = int(stats["raw_materials"] * 0.02)
            raw_gain = max(1, raw_gain)
            gold_gain = int(stats["financial_reserves"] * 0.01)
            gold_gain = max(1, gold_gain)
            stats["resource_stockpile"] += stockpile_gain
            stats["raw_materials"] += raw_gain
            stats["financial_reserves"] += gold_gain
            stats["pollution"] = min(100, stats["pollution"] + max(0, int(stockpile_gain * 0.05)))
            self._save_revolution(user_id, data)

    async def cog_load(self):
        if self._passive_task is None or self._passive_task.done():
            self._passive_task = asyncio.create_task(self._passive_income_loop())
            logger.info("Industrial passive income loop started")

    async def cog_unload(self):
        if self._passive_task and not self._passive_task.done():
            self._passive_task.cancel()
            try:
                await self._passive_task
            except asyncio.CancelledError:
                pass
            logger.info("Industrial passive income loop stopped")

    # ---------- COMMAND LISTENER FOR DISASTERS ----------
    @commands.Cog.listener()
    async def on_command(self, ctx: commands.Context):
        user_id = str(ctx.author.id)
        if user_id not in self._active_revolutions:
            return
        data = self._active_revolutions[user_id]
        if not data["active"] or data.get("completed", False):
            return
        stats = data["stats"]
        if stats["industrial_power"] >= 1000:
            await self._finish_revolution(ctx, user_id, data)
            return
        if random.random() < 0.30:
            await self._trigger_random_disaster(ctx, user_id, data)

    async def _trigger_random_disaster(self, ctx, user_id: str, data: Dict[str, Any]):
        stats = data["stats"]
        events = [
            ("Factory explosion! -20 Power, +15 Pollution", {"industrial_power": -20, "pollution": 15}),
            ("Worker strike! -15 Morale, +10 Unrest", {"worker_morale": -15, "labor_unrest": 10}),
            ("Machine breakdown! -10 Efficiency, +20 Breakdown Risk", {"production_efficiency": -10, "machine_breakdown_risk": 20}),
            ("Resource shortage! -50 Stockpile, -20 Raw Materials", {"resource_stockpile": -50, "raw_materials": -20}),
            ("Coal mine collapse! -30 Energy, -20 Infrastructure", {"energy_supply": -30, "infrastructure": -20}),
            ("Pollution spike! +25 Env Damage, -10 Health", {"environmental_damage": 25, "health": -10}),
            ("Govt cuts support! -15 Support, -10 Reserves", {"government_support": -15, "financial_reserves": -10}),
            ("Riot! -20 Urbanization, +30 Unrest, -5 Morale", {"urbanization": -20, "labor_unrest": 30, "worker_morale": -5}),
            ("Train derailment! -15 Transport, -10 Infra", {"transport_network": -15, "infrastructure": -10}),
            ("Tech leak! -2 Tech, +10 Pollution", {"tech_level": -2, "pollution": 10}),
            ("Bank run! -200 Reserves", {"financial_reserves": -200}),
            ("Disease outbreak! -25 Health, -10 Morale", {"health": -25, "worker_morale": -10}),
            ("Foreign embargo! -20 Trade Influence", {"trade_influence": -20}),
            ("Steam engine failure! -20 Steam Pressure, +15 Breakdown", {"steam_pressure": -20, "machine_breakdown_risk": 15}),
            ("Railway sabotage! -20 Railway Coverage", {"railway_coverage": -20}),
            ("Public panic! -20 Public Trust, +15 Unrest", {"public_trust": -20, "labor_unrest": 15}),
        ]
        event = random.choice(events)
        description, changes = event
        for stat, delta in changes.items():
            if stat in stats:
                new_val = stats[stat] + delta
                if stat in ["worker_morale", "pollution", "labor_unrest", "machine_breakdown_risk",
                            "production_efficiency", "environmental_damage", "health", "government_support",
                            "coal_quality", "steam_pressure", "public_trust", "trade_influence"]:
                    stats[stat] = max(0, min(100, new_val))
                elif stat == "tech_level":
                    stats[stat] = max(1, min(10, new_val))
                elif stat in ["industrial_power", "resource_stockpile", "financial_reserves", "raw_materials",
                              "infrastructure", "energy_supply", "transport_network", "urbanization", "education",
                              "military_protection", "railway_coverage"]:
                    stats[stat] = max(0, new_val)
                else:
                    stats[stat] = new_val
        self._save_revolution(user_id, data)
        embed = discord.Embed(title="💥 INDUSTRIAL DISASTER!", description=description, color=discord.Color.red())
        embed.set_footer(text="Your revolution is getting messy!")
        await ctx.send(embed=embed)

    async def _finish_revolution(self, ctx, user_id: str, data: Dict[str, Any]):
        stats = data["stats"]
        power = stats["industrial_power"]
        data["active"] = False
        data["completed"] = True
        self._save_revolution(user_id, data)
        if power >= 1000:
            gold_reward = random.randint(5000, 12000)
            food_reward = random.randint(3000, 7000)
            stone_reward = random.randint(2000, 5000)
            wood_reward = random.randint(2000, 5000)
            citizens_reward = random.randint(200, 600)
            tech_reward = random.randint(3, 7)
            self._update_civ_resources(user_id, {
                "gold": gold_reward,
                "food": food_reward,
                "stone": stone_reward,
                "wood": wood_reward
            })
            self.civ_manager.update_population(user_id, {"citizens": citizens_reward})
            self.civ_manager.update_military(user_id, {"tech_level": tech_reward})
            embed = discord.Embed(
                title="🏭 INDUSTRIAL REVOLUTION COMPLETE!",
                description="You successfully transformed your nation!",
                color=discord.Color.gold()
            )
            embed.add_field(
                name="Rewards",
                value=(
                    f"🪙 {format_number(gold_reward)} Gold\n"
                    f"🌾 {format_number(food_reward)} Food\n"
                    f"🪨 {format_number(stone_reward)} Stone\n"
                    f"🪵 {format_number(wood_reward)} Wood\n"
                    f"👤 +{format_number(citizens_reward)} Citizens\n"
                    f"🔬 +{tech_reward} Tech Levels"
                ),
                inline=False
            )
            await ctx.send(embed=embed)
        else:
            embed = discord.Embed(
                title="💔 Industrial Revolution Interrupted",
                description="Something went wrong. You can try again.",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed)
        self.db.log_event(user_id, "industrial_revolution", "Industrial Revolution Ended",
                          f"Power: {power}/1000 - {'Success' if power>=1000 else 'Failure'}")

    # ---------- COMMANDS (with cooldowns via decorator) ----------
    @commands.command(name='industrial_start')
    @industrial_cooldown("industrial_start")
    async def industrial_start(self, ctx):
        user_id = str(ctx.author.id)
        data = self._get_revolution(user_id)
        if data and data.get("completed", False):
            await ctx.send("❌ You have already completed the Industrial Revolution! It cannot be started again.")
            return
        if data and data["active"]:
            await ctx.send("❌ You already have an active revolution! Use `.industrial_status` to check.")
            return
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need a civilization first! Use `.start`.")
            return
        embed = discord.Embed(
            title="🏭 Start the Industrial Revolution?",
            description=(
                "This is a permanent challenge – no time limit.\n"
                "You must reach **1000 Industrial Power** to complete it.\n"
                "**Every command** you type has a 30% chance to trigger a disaster.\n"
                "**You can only do this once!**\n\n"
                "Type `yes` to confirm."
            ),
            color=discord.Color.orange()
        )
        await ctx.send(embed=embed)

        def check(m):
            return m.author.id == ctx.author.id and m.channel.id == ctx.channel.id and m.content.lower() == "yes"

        try:
            await self.bot.wait_for('message', timeout=30.0, check=check)
        except asyncio.TimeoutError:
            await ctx.send("❌ Revolution cancelled (timeout).")
            return

        now = datetime.utcnow()
        stats = DEFAULT_STATS.copy()
        tech = civ['military']['tech_level']
        stats["tech_level"] = max(1, min(10, tech + random.randint(0, 2)))
        stats["financial_reserves"] = civ['resources'].get('gold', 500)
        data = {
            "active": True,
            "completed": False,
            "started_at": now,
            "stats": stats
        }
        self._save_revolution(user_id, data)

        embed = discord.Embed(
            title="🔥 Industrial Revolution has begun!",
            description=(
                "Use `.industrial_status` to monitor progress.\n"
                "Commands: `.industrial_build`, `.industrial_tech`, `.industrial_workers`, `.industrial_cleanup`,\n"
                "plus **20+ new commands** – see `.indushelp` for the full list.\n"
                "Every command you type may cause chaos! Good luck."
            ),
            color=discord.Color.green()
        )
        await ctx.send(embed=embed)
        self.db.log_event(user_id, "industrial_revolution", "Industrial Revolution Started", "Began the revolution.")

    @commands.command(name='industrial_status')
    @industrial_cooldown("industrial_status")
    async def industrial_status(self, ctx):
        user_id = str(ctx.author.id)
        data = self._get_revolution(user_id)
        if not data or not data["active"]:
            if data and data.get("completed", False):
                await ctx.send("✅ You already completed the Industrial Revolution! Use `.industrial_status` to view final stats? (Not implemented yet)")
                return
            await ctx.send("❌ No active revolution. Use `.industrial_start`.")
            return
        stats = data["stats"]
        embed = discord.Embed(
            title="🏭 Industrial Revolution Status",
            description="Reach 1000 Industrial Power to complete it.",
            color=discord.Color.blue()
        )
        groups = {
            "⚙️ Core": ["industrial_power", "factory_output", "production_efficiency", "tech_level"],
            "👷 Workforce": ["worker_morale", "labor_unrest", "education", "health"],
            "🏗️ Infrastructure": ["infrastructure", "transport_network", "urbanization", "railway_coverage", "energy_supply"],
            "📦 Resources": ["resource_stockpile", "raw_materials", "financial_reserves", "coal_quality"],
            "🌍 Environment": ["pollution", "environmental_damage"],
            "🛡️ Safety & Stability": ["machine_breakdown_risk", "military_protection", "government_support", "public_trust", "trade_influence"],
            "⚡ Power": ["steam_pressure"]
        }
        for group_name, keys in groups.items():
            lines = []
            for key in keys:
                val = stats.get(key, 0)
                if key in ["industrial_power", "factory_output"]:
                    val_str = f"{val}"
                elif key in ["worker_morale", "pollution", "labor_unrest", "machine_breakdown_risk",
                             "production_efficiency", "environmental_damage", "health", "government_support",
                             "coal_quality", "steam_pressure", "public_trust", "trade_influence"]:
                    val_str = f"{val}%"
                elif key == "tech_level":
                    val_str = f"Level {val}"
                else:
                    val_str = f"{val}"
                lines.append(f"**{key.replace('_',' ').title()}:** {val_str}")
            embed.add_field(name=group_name, value="\n".join(lines), inline=False)
        progress = min(100, int((stats["industrial_power"] / 1000) * 100))
        bar = "▓" * (progress // 10) + "░" * (10 - progress // 10)
        embed.add_field(
            name="Progress to 1000",
            value=f"`{bar}` {stats['industrial_power']}/1000",
            inline=False
        )
        await ctx.send(embed=embed)

    @commands.command(name='industrial_build')
    @industrial_cooldown("industrial_build")
    async def industrial_build(self, ctx):
        user_id = str(ctx.author.id)
        data = self._get_revolution(user_id)
        if not data or not data["active"]:
            await ctx.send("❌ No active revolution.")
            return
        stats = data["stats"]
        if stats["financial_reserves"] < 100 or stats["resource_stockpile"] < 50 or stats["raw_materials"] < 20:
            await ctx.send("❌ Need 100 Gold, 50 Stockpile, 20 Raw Materials.")
            return
        stats["financial_reserves"] -= 100
        stats["resource_stockpile"] -= 50
        stats["raw_materials"] -= 20
        gain = random.randint(20, 50)
        stats["industrial_power"] += gain
        stats["factory_output"] += 5
        stats["pollution"] = min(100, stats["pollution"] + 3)
        self._save_revolution(user_id, data)
        await ctx.send(f"🏗️ Factory built! Power +{gain} (now {stats['industrial_power']}/1000).")

    @commands.command(name='industrial_tech')
    @industrial_cooldown("industrial_tech")
    async def industrial_tech(self, ctx):
        user_id = str(ctx.author.id)
        data = self._get_revolution(user_id)
        if not data or not data["active"]:
            await ctx.send("❌ No active revolution.")
            return
        stats = data["stats"]
        if stats["financial_reserves"] < 200 or stats["education"] < 20:
            await ctx.send("❌ Need 200 Gold and Education ≥ 20.")
            return
        stats["financial_reserves"] -= 200
        stats["tech_level"] = min(10, stats["tech_level"] + 1)
        stats["production_efficiency"] = min(100, stats["production_efficiency"] + 5)
        stats["industrial_power"] += 30
        self._save_revolution(user_id, data)
        await ctx.send(f"🔬 Tech advanced! Level: {stats['tech_level']}.")

    @commands.command(name='industrial_workers')
    @industrial_cooldown("industrial_workers")
    async def industrial_workers(self, ctx):
        user_id = str(ctx.author.id)
        data = self._get_revolution(user_id)
        if not data or not data["active"]:
            await ctx.send("❌ No active revolution.")
            return
        stats = data["stats"]
        if stats["financial_reserves"] < 80 or stats["education"] < 10:
            await ctx.send("❌ Need 80 Gold and Education ≥ 10.")
            return
        stats["financial_reserves"] -= 80
        stats["worker_morale"] = min(100, stats["worker_morale"] + 10)
        stats["labor_unrest"] = max(0, stats["labor_unrest"] - 5)
        stats["industrial_power"] += 15
        self._save_revolution(user_id, data)
        await ctx.send("👷 Workers trained! Morale +10, Unrest -5, Power +15.")

    @commands.command(name='industrial_cleanup')
    @industrial_cooldown("industrial_cleanup")
    async def industrial_cleanup(self, ctx):
        user_id = str(ctx.author.id)
        data = self._get_revolution(user_id)
        if not data or not data["active"]:
            await ctx.send("❌ No active revolution.")
            return
        stats = data["stats"]
        if stats["financial_reserves"] < 150:
            await ctx.send("❌ Need 150 Gold.")
            return
        stats["financial_reserves"] -= 150
        stats["pollution"] = max(0, stats["pollution"] - 15)
        stats["environmental_damage"] = max(0, stats["environmental_damage"] - 10)
        stats["health"] = min(100, stats["health"] + 5)
        self._save_revolution(user_id, data)
        await ctx.send("🌿 Cleanup success! Pollution -15, Env Damage -10, Health +5.")

    @commands.command(name='industrial_railway')
    @industrial_cooldown("industrial_railway")
    async def industrial_railway(self, ctx):
        user_id = str(ctx.author.id)
        data = self._get_revolution(user_id)
        if not data or not data["active"]:
            await ctx.send("❌ No active revolution.")
            return
        stats = data["stats"]
        if stats["financial_reserves"] < 120 or stats["raw_materials"] < 30:
            await ctx.send("❌ Need 120 Gold and 30 Raw Materials.")
            return
        stats["financial_reserves"] -= 120
        stats["raw_materials"] -= 30
        stats["infrastructure"] = min(100, stats["infrastructure"] + 20)
        stats["railway_coverage"] = min(100, stats["railway_coverage"] + 15)
        stats["industrial_power"] += 10
        self._save_revolution(user_id, data)
        await ctx.send("🚂 Railways built! Infra +20, Coverage +15, Power +10.")

    @commands.command(name='industrial_transport')
    @industrial_cooldown("industrial_transport")
    async def industrial_transport(self, ctx):
        user_id = str(ctx.author.id)
        data = self._get_revolution(user_id)
        if not data or not data["active"]:
            await ctx.send("❌ No active revolution.")
            return
        stats = data["stats"]
        if stats["financial_reserves"] < 100 or stats["resource_stockpile"] < 40:
            await ctx.send("❌ Need 100 Gold and 40 Stockpile.")
            return
        stats["financial_reserves"] -= 100
        stats["resource_stockpile"] -= 40
        stats["transport_network"] = min(100, stats["transport_network"] + 15)
        stats["industrial_power"] += 15
        self._save_revolution(user_id, data)
        await ctx.send("🚚 Transport improved! +15 Network, +15 Power.")

    @commands.command(name='industrial_army')
    @industrial_cooldown("industrial_army")
    async def industrial_army(self, ctx):
        user_id = str(ctx.author.id)
        data = self._get_revolution(user_id)
        if not data or not data["active"]:
            await ctx.send("❌ No active revolution.")
            return
        stats = data["stats"]
        if stats["financial_reserves"] < 200 or stats["resource_stockpile"] < 60:
            await ctx.send("❌ Need 200 Gold and 60 Stockpile.")
            return
        stats["financial_reserves"] -= 200
        stats["resource_stockpile"] -= 60
        stats["military_protection"] = min(100, stats["military_protection"] + 20)
        stats["industrial_power"] += 10
        self._save_revolution(user_id, data)
        await ctx.send("🛡️ Military raised! Protection +20, Power +10.")

    @commands.command(name='industrial_policy')
    @industrial_cooldown("industrial_policy")
    async def industrial_policy(self, ctx):
        user_id = str(ctx.author.id)
        data = self._get_revolution(user_id)
        if not data or not data["active"]:
            await ctx.send("❌ No active revolution.")
            return
        stats = data["stats"]
        if stats["financial_reserves"] < 150:
            await ctx.send("❌ Need 150 Gold.")
            return
        stats["financial_reserves"] -= 150
        stats["government_support"] = min(100, stats["government_support"] + 15)
        stats["public_trust"] = min(100, stats["public_trust"] + 10)
        stats["industrial_power"] += 10
        self._save_revolution(user_id, data)
        await ctx.send("📜 Policy enacted! Gov Support +15, Trust +10, Power +10.")

    @commands.command(name='industrial_import')
    @industrial_cooldown("industrial_import")
    async def industrial_import(self, ctx):
        user_id = str(ctx.author.id)
        data = self._get_revolution(user_id)
        if not data or not data["active"]:
            await ctx.send("❌ No active revolution.")
            return
        stats = data["stats"]
        if stats["financial_reserves"] < 200 or stats["trade_influence"] < 20:
            await ctx.send("❌ Need 200 Gold and Trade Influence ≥ 20.")
            return
        stats["financial_reserves"] -= 200
        stats["raw_materials"] += 50
        stats["industrial_power"] += 20
        self._save_revolution(user_id, data)
        await ctx.send("📦 Imports secured! +50 Raw Materials, +20 Power.")

    @commands.command(name='industrial_export')
    @industrial_cooldown("industrial_export")
    async def industrial_export(self, ctx):
        user_id = str(ctx.author.id)
        data = self._get_revolution(user_id)
        if not data or not data["active"]:
            await ctx.send("❌ No active revolution.")
            return
        stats = data["stats"]
        if stats["resource_stockpile"] < 80:
            await ctx.send("❌ Need 80 Stockpile.")
            return
        stats["resource_stockpile"] -= 80
        stats["financial_reserves"] += 100
        stats["trade_influence"] = min(100, stats["trade_influence"] + 15)
        stats["industrial_power"] += 15
        self._save_revolution(user_id, data)
        await ctx.send("📤 Exported goods! +100 Gold, +15 Trade, +15 Power.")

    @commands.command(name='industrial_steam')
    @industrial_cooldown("industrial_steam")
    async def industrial_steam(self, ctx):
        user_id = str(ctx.author.id)
        data = self._get_revolution(user_id)
        if not data or not data["active"]:
            await ctx.send("❌ No active revolution.")
            return
        stats = data["stats"]
        if stats["financial_reserves"] < 300 or stats["tech_level"] < 3:
            await ctx.send("❌ Need 300 Gold and Tech Level ≥ 3.")
            return
        stats["financial_reserves"] -= 300
        stats["steam_pressure"] = min(100, stats["steam_pressure"] + 20)
        stats["industrial_power"] += 25
        self._save_revolution(user_id, data)
        await ctx.send("💨 Steam research done! Pressure +20, Power +25.")

    @commands.command(name='industrial_mine')
    @industrial_cooldown("industrial_mine")
    async def industrial_mine(self, ctx):
        user_id = str(ctx.author.id)
        data = self._get_revolution(user_id)
        if not data or not data["active"]:
            await ctx.send("❌ No active revolution.")
            return
        stats = data["stats"]
        if stats["financial_reserves"] < 150 or stats["raw_materials"] < 30:
            await ctx.send("❌ Need 150 Gold and 30 Raw Materials.")
            return
        stats["financial_reserves"] -= 150
        stats["raw_materials"] -= 30
        stats["resource_stockpile"] += 30
        stats["industrial_power"] += 15
        self._save_revolution(user_id, data)
        await ctx.send("⛏️ Mine built! +30 Stockpile, +15 Power.")

    @commands.command(name='industrial_hospital')
    @industrial_cooldown("industrial_hospital")
    async def industrial_hospital(self, ctx):
        user_id = str(ctx.author.id)
        data = self._get_revolution(user_id)
        if not data or not data["active"]:
            await ctx.send("❌ No active revolution.")
            return
        stats = data["stats"]
        if stats["financial_reserves"] < 180 or stats["resource_stockpile"] < 40:
            await ctx.send("❌ Need 180 Gold and 40 Stockpile.")
            return
        stats["financial_reserves"] -= 180
        stats["resource_stockpile"] -= 40
        stats["health"] = min(100, stats["health"] + 25)
        stats["industrial_power"] += 10
        self._save_revolution(user_id, data)
        await ctx.send("🏥 Hospital built! Health +25, Power +10.")

    @commands.command(name='industrial_school')
    @industrial_cooldown("industrial_school")
    async def industrial_school(self, ctx):
        user_id = str(ctx.author.id)
        data = self._get_revolution(user_id)
        if not data or not data["active"]:
            await ctx.send("❌ No active revolution.")
            return
        stats = data["stats"]
        if stats["financial_reserves"] < 160 or stats["raw_materials"] < 20:
            await ctx.send("❌ Need 160 Gold and 20 Raw Materials.")
            return
        stats["financial_reserves"] -= 160
        stats["raw_materials"] -= 20
        stats["education"] = min(100, stats["education"] + 20)
        stats["industrial_power"] += 10
        self._save_revolution(user_id, data)
        await ctx.send("📚 School built! Education +20, Power +10.")

    @commands.command(name='industrial_law')
    @industrial_cooldown("industrial_law")
    async def industrial_law(self, ctx):
        user_id = str(ctx.author.id)
        data = self._get_revolution(user_id)
        if not data or not data["active"]:
            await ctx.send("❌ No active revolution.")
            return
        stats = data["stats"]
        if stats["government_support"] < 30:
            await ctx.send("❌ Need Government Support ≥ 30.")
            return
        stats["labor_unrest"] = max(0, stats["labor_unrest"] - 20)
        stats["public_trust"] = max(0, stats["public_trust"] - 5)
        stats["industrial_power"] = max(0, stats["industrial_power"] - 10)
        self._save_revolution(user_id, data)
        await ctx.send("⚖️ Law enforced! Unrest -20, Trust -5, Power -10.")

    @commands.command(name='industrial_trade')
    @industrial_cooldown("industrial_trade")
    async def industrial_trade(self, ctx):
        user_id = str(ctx.author.id)
        data = self._get_revolution(user_id)
        if not data or not data["active"]:
            await ctx.send("❌ No active revolution.")
            return
        stats = data["stats"]
        if stats["financial_reserves"] < 250 or stats["transport_network"] < 20:
            await ctx.send("❌ Need 250 Gold and Transport ≥ 20.")
            return
        stats["financial_reserves"] -= 250
        stats["trade_influence"] = min(100, stats["trade_influence"] + 25)
        stats["industrial_power"] += 30
        self._save_revolution(user_id, data)
        await ctx.send("🤝 Diplomatic trade! +25 Influence, +30 Power.")

    @commands.command(name='industrial_aid')
    @industrial_cooldown("industrial_aid")
    async def industrial_aid(self, ctx):
        user_id = str(ctx.author.id)
        data = self._get_revolution(user_id)
        if not data or not data["active"]:
            await ctx.send("❌ No active revolution.")
            return
        stats = data["stats"]
        if stats["trade_influence"] < 15:
            await ctx.send("❌ Need Trade Influence ≥ 15.")
            return
        stats["trade_influence"] = max(0, stats["trade_influence"] - 10)
        stats["financial_reserves"] += 200
        stats["industrial_power"] += 5
        self._save_revolution(user_id, data)
        await ctx.send("🤲 Aid received! +200 Gold, Power +5.")

    @commands.command(name='industrial_suppress')
    @industrial_cooldown("industrial_suppress")
    async def industrial_suppress(self, ctx):
        user_id = str(ctx.author.id)
        data = self._get_revolution(user_id)
        if not data or not data["active"]:
            await ctx.send("❌ No active revolution.")
            return
        stats = data["stats"]
        if stats["military_protection"] < 30:
            await ctx.send("❌ Need Military Protection ≥ 30.")
            return
        stats["labor_unrest"] = max(0, stats["labor_unrest"] - 30)
        stats["public_trust"] = max(0, stats["public_trust"] - 15)
        self._save_revolution(user_id, data)
        await ctx.send("🔫 Revolt suppressed! Unrest -30, Trust -15.")

    @commands.command(name='industrial_bribe')
    @industrial_cooldown("industrial_bribe")
    async def industrial_bribe(self, ctx):
        user_id = str(ctx.author.id)
        data = self._get_revolution(user_id)
        if not data or not data["active"]:
            await ctx.send("❌ No active revolution.")
            return
        stats = data["stats"]
        if stats["financial_reserves"] < 30:
            await ctx.send("❌ Need 30 Gold.")
            return
        stats["financial_reserves"] -= 30
        stats["worker_morale"] = min(100, stats["worker_morale"] + 20)
        stats["labor_unrest"] = max(0, stats["labor_unrest"] - 10)
        self._save_revolution(user_id, data)
        await ctx.send("💵 Bribes paid! Morale +20, Unrest -10.")

    @commands.command(name='industrial_automate')
    @industrial_cooldown("industrial_automate")
    async def industrial_automate(self, ctx):
        user_id = str(ctx.author.id)
        data = self._get_revolution(user_id)
        if not data or not data["active"]:
            await ctx.send("❌ No active revolution.")
            return
        stats = data["stats"]
        if stats["tech_level"] < 4:
            await ctx.send("❌ Need Tech Level ≥ 4.")
            return
        stats["production_efficiency"] = min(100, stats["production_efficiency"] + 15)
        stats["industrial_power"] += 10
        self._save_revolution(user_id, data)
        await ctx.send("🤖 Automation complete! Efficiency +15, Power +10.")

    @commands.command(name='industrial_upgrade')
    @industrial_cooldown("industrial_upgrade")
    async def industrial_upgrade(self, ctx):
        user_id = str(ctx.author.id)
        data = self._get_revolution(user_id)
        if not data or not data["active"]:
            await ctx.send("❌ No active revolution.")
            return
        stats = data["stats"]
        if stats["financial_reserves"] < 300 or stats["resource_stockpile"] < 50:
            await ctx.send("❌ Need 300 Gold and 50 Stockpile.")
            return
        stats["financial_reserves"] -= 300
        stats["resource_stockpile"] -= 50
        stats["industrial_power"] += 30
        stats["pollution"] = min(100, stats["pollution"] + 10)
        self._save_revolution(user_id, data)
        await ctx.send("⬆️ Factory upgraded! Power +30, Pollution +10.")

    @commands.command(name='industrial_relief')
    @industrial_cooldown("industrial_relief")
    async def industrial_relief(self, ctx):
        user_id = str(ctx.author.id)
        data = self._get_revolution(user_id)
        if not data or not data["active"]:
            await ctx.send("❌ No active revolution.")
            return
        stats = data["stats"]
        if stats["financial_reserves"] < 200:
            await ctx.send("❌ Need 200 Gold.")
            return
        stats["financial_reserves"] -= 200
        stats["health"] = min(100, stats["health"] + 20)
        stats["public_trust"] = min(100, stats["public_trust"] + 15)
        stats["industrial_power"] = max(0, stats["industrial_power"] - 20)
        self._save_revolution(user_id, data)
        await ctx.send("🆘 Disaster relief! Health +20, Trust +15, Power -20.")

    @commands.command(name='industrial_expand')
    @industrial_cooldown("industrial_expand")
    async def industrial_expand(self, ctx):
        user_id = str(ctx.author.id)
        data = self._get_revolution(user_id)
        if not data or not data["active"]:
            await ctx.send("❌ No active revolution.")
            return
        stats = data["stats"]
        if stats["financial_reserves"] < 250 or stats["raw_materials"] < 40:
            await ctx.send("❌ Need 250 Gold and 40 Raw Materials.")
            return
        stats["financial_reserves"] -= 250
        stats["raw_materials"] -= 40
        stats["urbanization"] = min(100, stats["urbanization"] + 30)
        stats["industrial_power"] += 15
        self._save_revolution(user_id, data)
        await ctx.send("🏙️ City expanded! Urbanization +30, Power +15.")

    # ---- FIXED BANKING COMMAND (no infinite spam) ----
    @commands.command(name='industrial_banking')
    @industrial_cooldown("industrial_banking")
    async def industrial_banking(self, ctx):
        user_id = str(ctx.author.id)
        data = self._get_revolution(user_id)
        if not data or not data["active"]:
            await ctx.send("❌ No active revolution.")
            return
        
        stats = data["stats"]
        
        # Check max uses limit (stored in stats)
        banking_uses = stats.get("banking_uses", 0)
        max_uses = config.INDUSTRIAL.get("banking_max_uses", 5)
        
        if banking_uses >= max_uses:
            await ctx.send(f"❌ You have already used banking {max_uses} times, which is the maximum allowed for this revolution.")
            return
        
        if stats["financial_reserves"] < 100:
            await ctx.send("❌ Need 100 Gold to invest.")
            return
        
        stats["financial_reserves"] -= 100
        # Random return: 150-300 gold (profit 50-200)
        gain = random.randint(150, 300)
        stats["financial_reserves"] += gain
        stats["industrial_power"] += 20
        # Increment use counter
        stats["banking_uses"] = banking_uses + 1
        
        self._save_revolution(user_id, data)
        await ctx.send(f"🏦 Banking invest! +{gain} Gold, +20 Power. (Uses: {banking_uses+1}/{max_uses})")

    @commands.command(name='industrial_nationalize')
    @industrial_cooldown("industrial_nationalize")
    async def industrial_nationalize(self, ctx):
        user_id = str(ctx.author.id)
        data = self._get_revolution(user_id)
        if not data or not data["active"]:
            await ctx.send("❌ No active revolution.")
            return
        stats = data["stats"]
        if stats["government_support"] < 40:
            await ctx.send("❌ Need Government Support ≥ 40.")
            return
        stats["government_support"] = max(0, stats["government_support"] - 30)
        stats["industrial_power"] += 50
        self._save_revolution(user_id, data)
        await ctx.send("🏭 Nationalized! Power +50, Gov Support -30.")

    @commands.command(name='indushelp')
    @industrial_cooldown("indushelp")
    async def indus_help(self, ctx):
        embed = discord.Embed(
            title="🏭 Industrial Revolution – Complete Command List",
            description=(
                "**Goal:** Reach 1000 Industrial Power (once per player).\n"
                "**Caution:** Every command has a 30% disaster chance!\n\n"
                "**Core Commands:**\n"
                "`.industrial_start` – Begin (requires `yes` confirmation).\n"
                "`.industrial_status` – View all 25+ stats.\n"
                "`.industrial_build` – Build a factory.\n"
                "`.industrial_tech` – Research technology.\n"
                "`.industrial_workers` – Train workers.\n"
                "`.industrial_cleanup` – Reduce pollution.\n\n"
                "**20+ Extra Commands:**\n"
                "`.industrial_railway` – Build railways.\n"
                "`.industrial_transport` – Improve transport.\n"
                "`.industrial_army` – Raise military protection.\n"
                "`.industrial_policy` – Enact a new policy.\n"
                "`.industrial_import` – Import raw materials.\n"
                "`.industrial_export` – Export goods.\n"
                "`.industrial_steam` – Research steam power.\n"
                "`.industrial_mine` – Build a mine.\n"
                "`.industrial_hospital` – Build a hospital.\n"
                "`.industrial_school` – Build a school.\n"
                "`.industrial_law` – Enforce law and order.\n"
                "`.industrial_trade` – Diplomatic trade.\n"
                "`.industrial_aid` – Request foreign aid.\n"
                "`.industrial_suppress` – Suppress revolts.\n"
                "`.industrial_bribe` – Bribe workers.\n"
                "`.industrial_automate` – Automate factories.\n"
                "`.industrial_upgrade` – Upgrade factories.\n"
                "`.industrial_relief` – Disaster relief.\n"
                "`.industrial_expand` – Expand cities.\n"
                "`.industrial_banking` – Invest in banking (max 5 uses).\n"
                "`.industrial_nationalize` – Nationalize industry."
            ),
            color=discord.Color.blue()
        )
        embed.set_footer(text="Manage wisely – disasters are brutal!")
        await ctx.send(embed=embed)

    # ---------- ON READY: LOAD ACTIVE REVOLUTIONS ----------
    @commands.Cog.listener()
    async def on_ready(self):
        try:
            docs = self.db.client.collection("industrial_revolutions").stream()
            count = 0
            for doc in docs:
                data = doc.to_dict()
                user_id = doc.id
                if data.get("active") or data.get("completed"):
                    stats = data.get("stats", {})
                    if not isinstance(stats, dict):
                        stats = DEFAULT_STATS.copy()
                    started_at = data.get("started_at")
                    if started_at and isinstance(started_at, str):
                        try:
                            started_at = datetime.fromisoformat(started_at)
                        except ValueError:
                            started_at = None
                    self._active_revolutions[user_id] = {
                        "active": data.get("active", False),
                        "completed": data.get("completed", False),
                        "started_at": started_at,
                        "stats": stats
                    }
                    count += 1
            logger.info(f"Restored {count} industrial revolutions from Firestore.")
        except Exception as e:
            logger.error(f"Error loading industrial revolutions on_ready: {e}")

async def setup(bot):
    await bot.add_cog(IndustrialCog(bot))
