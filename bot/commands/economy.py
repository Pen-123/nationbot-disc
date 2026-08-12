import random
import asyncio
import math
import discord as guilded
from discord import app_commands
from discord.ext import commands
from datetime import datetime, timedelta
import logging
from bot.utils import format_number, create_embed, get_territory_modifier
from bot import config
from functools import wraps
from typing import List, Literal, Optional

logger = logging.getLogger(__name__)

# ---- Cooldown decorator with testing mode skip ----
def check_cooldown_decorator(command_name: str):
    def decorator(func):
        @wraps(func)
        async def wrapper(self, ctx, *args, **kwargs):
            # If testing mode is on, skip cooldown
            if self.db.get_testing_mode():
                return await func(self, ctx, *args, **kwargs)
            minutes = config.COOLDOWNS.get(command_name, 0)
            if minutes <= 0:
                return await func(self, ctx, *args, **kwargs)
            user_id = str(ctx.author.id)
            last_used = self.db.get_command_cooldown(user_id, command_name)
            if last_used:
                cooldown_end = last_used + timedelta(minutes=minutes)
                if datetime.utcnow() < cooldown_end:
                    remaining = cooldown_end - datetime.utcnow()
                    mins = int(remaining.total_seconds() // 60)
                    secs = int(remaining.total_seconds() % 60)
                    await ctx.send(f"⏳ Please wait {mins}m {secs}s before using this command again!")
                    return
            self.db.set_command_cooldown(user_id, command_name, datetime.utcnow())
            return await func(self, ctx, *args, **kwargs)
        return wrapper
    return decorator

class EconomyCommands(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.db = bot.db
        self.civ_manager = bot.civ_manager
        self._tasks = []

        # ---- Megaproject definitions ----
        self.megaprojects = {
            "great_wall": {
                "name": "Great Wall",
                "cost": {"gold": 5000000, "stone": 2000000},
                "effect": {"defense_strength": 50},
                "description": "Permanent +50% defense bonus",
                "tech_required": 5,
            },
            "space_program": {
                "name": "Space Program",
                "cost": {"gold": 10000000, "wood": 500000, "stone": 500000},
                "effect": {"tech_speed": 100},
                "description": "Permanent +100% tech research speed",
                "tech_required": 7,
            },
            "global_bank": {
                "name": "Global Bank",
                "cost": {"gold": 8000000, "food": 1000000},
                "effect": {"tax_bonus": 50},
                "description": "Permanent +50% tax income",
                "tech_required": 6,
            },
            "ai_network": {
                "name": "AI Network",
                "cost": {"gold": 15000000, "wood": 1000000, "stone": 1000000},
                "effect": {"resource_production": 100},
                "description": "Permanent +100% all resource production",
                "tech_required": 8,
            },
            "green_energy": {
                "name": "Green Energy Grid",
                "cost": {"gold": 6000000, "wood": 300000, "stone": 300000},
                "effect": {"happiness_boost": 20},
                "description": "Permanent +20 happiness",
                "tech_required": 4,
            }
        }

        # ---- Policy definitions ----
        self.policies = {
            "military_service": {
                "name": "Military Service",
                "levels": {
                    1: {"gold_cost": 50000, "effect": {"soldier_training_speed": 5}, "desc": "+5% soldier training"},
                    2: {"gold_cost": 150000, "effect": {"soldier_training_speed": 10}, "desc": "+10% soldier training"},
                    3: {"gold_cost": 300000, "effect": {"soldier_training_speed": 20}, "desc": "+20% soldier training"},
                },
                "max_level": 3,
                "base_desc": "Boosts soldier training speed.",
            },
            "agricultural_subsidies": {
                "name": "Agricultural Subsidies",
                "levels": {
                    1: {"gold_cost": 40000, "food_cost": 20000, "effect": {"farm_bonus": 10}, "desc": "+10% farm yield"},
                    2: {"gold_cost": 120000, "food_cost": 60000, "effect": {"farm_bonus": 20}, "desc": "+20% farm yield"},
                    3: {"gold_cost": 250000, "food_cost": 120000, "effect": {"farm_bonus": 35}, "desc": "+35% farm yield"},
                },
                "max_level": 3,
                "base_desc": "Increases food production from farming.",
            },
            "trade_agreements": {
                "name": "Trade Agreements",
                "levels": {
                    1: {"gold_cost": 60000, "effect": {"trade_profit": 5}, "desc": "+5% trade profit"},
                    2: {"gold_cost": 180000, "effect": {"trade_profit": 12}, "desc": "+12% trade profit"},
                    3: {"gold_cost": 400000, "effect": {"trade_profit": 25}, "desc": "+25% trade profit"},
                },
                "max_level": 3,
                "base_desc": "Increases profit from trades.",
            },
            "public_education": {
                "name": "Public Education",
                "levels": {
                    1: {"gold_cost": 80000, "effect": {"tech_speed": 5}, "desc": "+5% tech research speed"},
                    2: {"gold_cost": 200000, "effect": {"tech_speed": 12}, "desc": "+12% tech research speed"},
                    3: {"gold_cost": 500000, "effect": {"tech_speed": 25}, "desc": "+25% tech research speed"},
                },
                "max_level": 3,
                "base_desc": "Accelerates technology research.",
            },
            "environmental_protection": {
                "name": "Environmental Protection",
                "levels": {
                    1: {"gold_cost": 70000, "stone_cost": 30000, "effect": {"happiness_boost": 5, "resource_production": -5}, "desc": "+5% happiness, -5% resource production"},
                    2: {"gold_cost": 180000, "stone_cost": 80000, "effect": {"happiness_boost": 10, "resource_production": -3}, "desc": "+10% happiness, -3% resource production"},
                    3: {"gold_cost": 350000, "stone_cost": 150000, "effect": {"happiness_boost": 15, "resource_production": 0}, "desc": "+15% happiness"},
                },
                "max_level": 3,
                "base_desc": "Boosts happiness but may reduce production initially.",
            },
            "industrial_innovation": {
                "name": "Industrial Innovation",
                "levels": {
                    1: {"gold_cost": 90000, "effect": {"resource_production": 5}, "desc": "+5% all resource production"},
                    2: {"gold_cost": 220000, "effect": {"resource_production": 12}, "desc": "+12% all resource production"},
                    3: {"gold_cost": 500000, "effect": {"resource_production": 25}, "desc": "+25% all resource production"},
                },
                "max_level": 3,
                "base_desc": "Increases all resource production.",
            },
        }

    async def cog_load(self):
        self._tasks.append(asyncio.create_task(self._corporation_loop()))
        self._tasks.append(asyncio.create_task(self._policy_loop()))

    async def cog_unload(self):
        for t in self._tasks:
            try:
                t.cancel()
            except:
                pass

    # ---- Corporation passive income loop ----
    async def _corporation_loop(self):
        await self.bot.wait_until_ready()
        while not self.bot.is_closed():
            await asyncio.sleep(3600)  # every hour
            try:
                for civ in self.db.get_all_civilizations():
                    corps = civ.get('corporations', [])
                    if corps:
                        total_income = 0
                        for corp in corps:
                            level = corp.get('level', 1)
                            total_income += self._get_corp_income(civ, level)
                        if total_income > 0:
                            self.civ_manager.update_resources(civ['user_id'], {"gold": total_income})
                            logger.info(f"Corporation paid {total_income} gold to {civ['user_id']}")
            except Exception as e:
                logger.error(f"Corporation loop error: {e}")

    def _get_corp_income(self, civ: dict, level: int) -> int:
        tech = civ['military']['tech_level']
        base = level * 500
        if tech >= 5:
            return base
        else:
            return base // 2

    # ---- Policy passive loop (no action needed) ----
    async def _policy_loop(self):
        pass

    # ---- Helper: autocomplete for .sell ----
    async def _sell_item_autocomplete(self, interaction: guilded.Interaction, current: str) -> List[app_commands.Choice[str]]:
        user_id = str(interaction.user.id)
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            return []
        items = civ.get("hyper_items", [])
        choices = []
        for item in items:
            if current.lower() in item.lower():
                choices.append(app_commands.Choice(name=item[:100], value=item))
        return choices[:25]

    # ---- Helper: civil war check ----
    async def check_civil_war_and_proceed(self, ctx, user_id: str) -> bool:
        try:
            if self.civ_manager.check_civil_war_risk(user_id):
                civ = self.civ_manager.get_civilization(user_id)
                if civ:
                    embed = create_embed(
                        "💥 CIVIL WAR!",
                        "Your civilization has been torn apart by internal conflict! Check your events with `.events` to see the damage.",
                        guilded.Color.red()
                    )
                    await ctx.send(embed=embed)
                return False
            return True
        except Exception as e:
            logger.error(f"Error checking civil war for {user_id}: {e}")
            return True

    # ================================================================
    # EARLY-GAME COMMANDS
    # ================================================================

    @commands.command(name='gather')
    @check_cooldown_decorator("gather")
    async def gather_resources(self, ctx):
        user_id = str(ctx.author.id)
        if not await self.check_civil_war_and_proceed(ctx, user_id):
            return
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need to start a civilization first! Use `.start <name>`")
            return

        if self.db.get_testing_mode():
            gathered = {
                "gold": config.TESTING_GAIN,
                "wood": config.TESTING_GAIN,
                "stone": config.TESTING_GAIN,
                "food": config.TESTING_GAIN,
            }
            self.civ_manager.update_resources(user_id, gathered)
            embed = create_embed("🧪 TESTING MODE", f"Gained {config.TESTING_GAIN} of each resource!", guilded.Color.gold())
            await ctx.send(embed=embed)
            return

        possible_resources = ['gold', 'wood', 'stone', 'food']
        gathered = {}
        employment_rate = self.civ_manager.get_employment_rate(user_id) / 100
        employment_factor = 1 + employment_rate * config.ECONOMY["gather_employment_coeff"]
        territory_factor = get_territory_modifier(civ['territory']['land_size'])
        for resource in possible_resources:
            if random.random() < config.ECONOMY["gather_chance"]:
                base = random.randint(config.ECONOMY["gather_base_min"], config.ECONOMY["gather_base_max"])
                amount = int(base * employment_factor * territory_factor)
                amount = min(amount, config.CAPS["gather"])
                gathered[resource] = amount

        if not gathered:
            await ctx.send("🔍 Your scouts searched but found nothing of value this time.")
            return

        luck_modifier = self.civ_manager.calculate_total_modifier(user_id, "luck")
        if luck_modifier > 1.0:
            for resource in gathered:
                gathered[resource] = int(gathered[resource] * luck_modifier)
                gathered[resource] = min(gathered[resource], config.CAPS["gather"])

        self.civ_manager.update_resources(user_id, gathered)
        embed = create_embed("🔍 Resource Gathering", "Your scouts return with valuable resources!", guilded.Color.green())
        resource_icons = {"gold": "🪙", "wood": "🪵", "stone": "🪨", "food": "🌾"}
        resource_text = "\n".join([f"{resource_icons[res]} {format_number(amt)} {res.capitalize()}" for res, amt in gathered.items()])
        embed.add_field(name="Resources Gathered", value=resource_text, inline=False)
        embed.add_field(name="Employment Factor", value=f"{employment_factor:.2f}x", inline=True)
        embed.add_field(name="Territory Factor", value=f"{territory_factor:.2f}x", inline=True)
        await ctx.send(embed=embed)

    @commands.command(name='work')
    @check_cooldown_decorator("work")
    async def work(self, ctx, amount: int = None):
        if amount is None or amount < 1:
            await ctx.send("💼 **Work Command**\nUsage: `.work <amount>`\nEmploy <amount> citizens to gain gold.")
            return
        user_id = str(ctx.author.id)
        if not await self.check_civil_war_and_proceed(ctx, user_id):
            return
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need to start a civilization first! Use `.start <name>`")
            return

        population = civ['population']
        current_employed = population.get('employed', 0)
        unemployed = population['citizens'] - current_employed
        if amount > unemployed:
            await ctx.send(f"❌ Only {unemployed} unemployed citizens available!")
            return

        self.civ_manager.update_employment(user_id, amount)

        gold_gain = amount * random.randint(config.ECONOMY["work_gold_per_citizen_min"], config.ECONOMY["work_gold_per_citizen_max"])
        ideology = civ.get('ideology', '')
        if ideology == 'communism':
            gold_gain = int(gold_gain * 1.15)
        gold_gain = min(gold_gain, config.CAPS["work"])

        self.civ_manager.update_resources(user_id, {"gold": gold_gain})
        new_rate = self.civ_manager.get_employment_rate(user_id)

        embed = create_embed("💼 Citizens Employed", f"Successfully employed {format_number(amount)} citizens and gained {format_number(gold_gain)} gold!", guilded.Color.green())
        embed.add_field(name="New Employment Rate", value=f"{new_rate:.1f}%", inline=True)
        await ctx.send(embed=embed)

    @commands.command(name='farm')
    @check_cooldown_decorator("farm")
    async def farm_food(self, ctx):
        user_id = str(ctx.author.id)
        if not await self.check_civil_war_and_proceed(ctx, user_id):
            return
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need to start a civilization first! Use `.start <name>`")
            return

        base_food = random.randint(config.ECONOMY["farm_base_min"], config.ECONOMY["farm_base_max"])
        citizen_bonus = civ['population']['citizens'] // config.ECONOMY["farm_citizen_divisor"]
        employment_rate = self.civ_manager.get_employment_rate(user_id) / 100
        employment_factor = 1 + employment_rate * config.ECONOMY["farm_employment_coeff"]
        territory_factor = get_territory_modifier(civ['territory']['land_size'])

        total_food = int((base_food + citizen_bonus) * employment_factor * territory_factor)
        if civ.get('ideology') == 'communism':
            total_food = int(total_food * 1.1)
        total_food = min(total_food, config.CAPS["farm"])

        event_text = ""
        if random.random() < 0.1:
            event_multiplier = random.choice([0.5, 1.5, 2.0])
            total_food = int(total_food * event_multiplier)
            if event_multiplier < 1:
                event_text = "🦗 Locust swarm damaged some crops!"
            else:
                event_text = "🌈 Perfect weather blessed your harvest!"

        self.civ_manager.update_resources(user_id, {"food": total_food})
        embed = create_embed("🌾 Farming", f"Your farmers worked the fields and produced {format_number(total_food)} food!", guilded.Color.green())
        if event_text:
            embed.add_field(name="Special Event", value=event_text, inline=False)
        await ctx.send(embed=embed)

    @commands.command(name='mine')
    @check_cooldown_decorator("mine")
    async def mine_resources(self, ctx):
        user_id = str(ctx.author.id)
        if not await self.check_civil_war_and_proceed(ctx, user_id):
            return
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need to start a civilization first! Use `.start <name>`")
            return

        stone_yield = random.randint(config.ECONOMY["mine_stone_base_min"], config.ECONOMY["mine_stone_base_max"])
        wood_yield = random.randint(config.ECONOMY["mine_wood_base_min"], config.ECONOMY["mine_wood_base_max"])

        employment_rate = self.civ_manager.get_employment_rate(user_id) / 100
        employment_factor = 1 + employment_rate * config.ECONOMY["mine_employment_coeff"]
        territory_factor = get_territory_modifier(civ['territory']['land_size'])

        stone_yield = int(stone_yield * employment_factor * territory_factor)
        wood_yield = int(wood_yield * employment_factor * territory_factor)
        tech_bonus = 1 + (civ['military']['tech_level'] * 0.1)
        stone_yield = int(stone_yield * tech_bonus)
        wood_yield = int(wood_yield * tech_bonus)

        stone_yield = min(stone_yield, config.CAPS["mine_stone"])
        wood_yield = min(wood_yield, config.CAPS["mine_wood"])

        bonus_gold = 0
        if random.random() < config.ECONOMY["mine_bonus_gold_chance"]:
            bonus_gold = random.randint(config.ECONOMY["mine_bonus_gold_min"], config.ECONOMY["mine_bonus_gold_max"])
            bonus_gold = min(bonus_gold, config.ECONOMY["mine_bonus_gold_cap"])

        updates = {"stone": stone_yield, "wood": wood_yield}
        if bonus_gold > 0:
            updates["gold"] = bonus_gold
        self.civ_manager.update_resources(user_id, updates)

        embed = create_embed("⛏️ Mining Operation", "Your miners have extracted resources from the earth!", guilded.Color.blue())
        result_text = f"🪨 {format_number(stone_yield)} Stone\n🪵 {format_number(wood_yield)} Wood"
        if bonus_gold > 0:
            result_text += f"\n🪙 {format_number(bonus_gold)} Gold (Lucky find!)"
        embed.add_field(name="Resources Extracted", value=result_text, inline=False)
        embed.add_field(name="Employment Factor", value=f"{employment_factor:.2f}x", inline=True)
        embed.add_field(name="Territory Factor", value=f"{territory_factor:.2f}x", inline=True)
        await ctx.send(embed=embed)

    # ================================================================
    # BUFFED MID-GAME COMMANDS
    # ================================================================

    @commands.command(name='harvest')
    @check_cooldown_decorator("harvest")
    async def harvest_food(self, ctx):
        user_id = str(ctx.author.id)
        if not await self.check_civil_war_and_proceed(ctx, user_id):
            return
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need to start a civilization first! Use `.start <name>`")
            return

        pop = civ['population']['citizens']
        happiness = civ['population']['happiness']
        territory_factor = get_territory_modifier(civ['territory']['land_size'])

        total_harvest = int((pop * 2 + happiness * 5) * territory_factor * 0.5)
        total_harvest = min(total_harvest, 1_500_000)

        self.civ_manager.update_resources(user_id, {"food": total_harvest})
        self.civ_manager.update_population(user_id, {"happiness": 3})

        embed = create_embed("🌽 Great Harvest", f"A bountiful harvest brings {format_number(total_harvest)} food!", guilded.Color.gold())
        embed.add_field(name="Morale Boost", value="Citizens are happy! (+3 happiness)", inline=False)
        await ctx.send(embed=embed)

    @commands.command(name='drill')
    @check_cooldown_decorator("drill")
    async def drill_minerals(self, ctx):
        user_id = str(ctx.author.id)
        if not await self.check_civil_war_and_proceed(ctx, user_id):
            return
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need to start a civilization first! Use `.start <name>`")
            return
        if civ['military']['tech_level'] < 2:
            await ctx.send("❌ You need Tech Level 2 to drill!")
            return

        tech = civ['military']['tech_level']
        pop = civ['population']['citizens']
        territory_factor = get_territory_modifier(civ['territory']['land_size'])

        base = 200 + (tech * 50) + (pop // 20)
        gold_gain = int(base * 2 * territory_factor)
        stone_gain = int(base * 1.5 * territory_factor)

        gold_gain = min(gold_gain, 2_000_000)
        stone_gain = min(stone_gain, 1_000_000)

        self.civ_manager.update_resources(user_id, {"gold": gold_gain, "stone": stone_gain})
        embed = create_embed("⛏️ Deep Drilling", f"Extracted {format_number(gold_gain)} gold and {format_number(stone_gain)} stone!", guilded.Color.purple())
        await ctx.send(embed=embed)

    @commands.command(name='labor')
    @check_cooldown_decorator("labor")
    async def forced_labor(self, ctx):
        user_id = str(ctx.author.id)
        if not await self.check_civil_war_and_proceed(ctx, user_id):
            return
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need to start a civilization first! Use `.start <name>`")
            return
        if civ['military']['tech_level'] < 3:
            await ctx.send("❌ You need Tech Level 3 for forced labor!")
            return

        pop = civ['population']['citizens']
        tech = civ['military']['tech_level']
        territory_factor = get_territory_modifier(civ['territory']['land_size'])

        base = 50 + (pop // 10) + (tech * 20)
        loot = {
            "gold": int(base * 1.2 * territory_factor),
            "food": int(base * 1.0 * territory_factor),
            "wood": int(base * 1.5 * territory_factor),
            "stone": int(base * 1.5 * territory_factor)
        }
        for key in loot:
            loot[key] = min(loot[key], 800_000)

        self.civ_manager.update_resources(user_id, loot)
        self.civ_manager.update_population(user_id, {"happiness": -10})

        embed = create_embed("⛏️ Forced Labor", "Your citizens worked tirelessly!", guilded.Color.orange())
        loot_text = "\n".join([f"{'🪙' if res == 'gold' else '🌾' if res == 'food' else '🪵' if res == 'wood' else '🪨'} {format_number(amt)} {res.capitalize()}" for res, amt in loot.items()])
        embed.add_field(name="Resources", value=loot_text, inline=False)
        await ctx.send(embed=embed)

    @commands.command(name='raidcaravan')
    @check_cooldown_decorator("raidcaravan")
    async def raid_caravan(self, ctx):
        user_id = str(ctx.author.id)
        if not await self.check_civil_war_and_proceed(ctx, user_id):
            return
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need to start a civilization first! Use `.start <name>`")
            return

        military = civ['military']
        if military['soldiers'] < config.ECONOMY["raid_min_soldiers"]:
            await ctx.send(f"❌ You need at least {config.ECONOMY['raid_min_soldiers']} soldiers to raid caravans!")
            return

        base_success = 0.6
        soldier_bonus = min(0.3, military['soldiers'] / 100)
        spy_bonus = min(0.1, military['spies'] / 50)
        success_chance = base_success + soldier_bonus + spy_bonus
        if civ.get('ideology') == 'anarchy':
            success_chance += 0.1

        if random.random() < success_chance:
            soldier_power = military['soldiers'] * 2
            spy_power = military['spies'] * 5
            territory_factor = get_territory_modifier(civ['territory']['land_size']) ** 1.3

            loot = {
                "gold": int(random.randint(200, 600) * territory_factor * (1 + soldier_power/500)),
                "food": int(random.randint(100, 250) * territory_factor * (1 + soldier_power/500)),
                "wood": int(random.randint(60, 180) * territory_factor * (1 + soldier_power/500)),
                "stone": int(random.randint(50, 150) * territory_factor * (1 + soldier_power/500))
            }

            if random.random() < 0.1:
                bonus_gold = random.randint(200, 500)
                loot["gold"] += bonus_gold

            for key in loot:
                loot[key] = min(loot[key], 2_000_000)

            self.civ_manager.update_resources(user_id, loot)

            embed = create_embed("🏴‍☠️ Caravan Raid - Success!", "Your raiders ambushed a wealthy merchant caravan!", guilded.Color.green())
            loot_text = "\n".join([f"{'🪙' if res == 'gold' else '🌾' if res == 'food' else '🪵' if res == 'wood' else '🪨'} {format_number(amt)} {res.capitalize()}" for res, amt in loot.items() if amt > 0])
            embed.add_field(name="Loot Acquired", value=loot_text, inline=False)
        else:
            soldier_loss = random.randint(1, 3)
            self.civ_manager.update_military(user_id, {"soldiers": -soldier_loss})
            embed = create_embed("🏴‍☠️ Caravan Raid - Failed!", f"The caravan's guards were too strong! You lost {soldier_loss} soldiers in the failed attack.", guilded.Color.red())
        await ctx.send(embed=embed)

    # ================================================================
    # NEW COMMANDS: CHEERUP & BUYTECH
    # ================================================================

    @commands.command(name='cheerup')
    @check_cooldown_decorator("cheerup")
    async def cheer_up(self, ctx):
        user_id = str(ctx.author.id)
        if not await self.check_civil_war_and_proceed(ctx, user_id):
            return
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need to start a civilization first! Use `.start <name>`")
            return

        if civ['resources']['gold'] < 2000:
            await ctx.send("❌ You need 2000 gold to cheer up your citizens!")
            return

        self.civ_manager.spend_resources(user_id, {"gold": 2000})
        current = civ['population']['happiness']
        boost = int((100 - current) * 0.5)
        self.civ_manager.update_population(user_id, {"happiness": boost})

        embed = create_embed("😊 Cheer Up!", f"Citizens are much happier! (+{boost} happiness)", guilded.Color.green())
        await ctx.send(embed=embed)

    @commands.command(name='buytech', aliases=['buylevel'])
    @check_cooldown_decorator("buytech")
    async def buy_tech_level(self, ctx):
        user_id = str(ctx.author.id)
        if not await self.check_civil_war_and_proceed(ctx, user_id):
            return
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need to start a civilization first! Use `.start <name>`")
            return

        current_level = civ['military']['tech_level']
        if current_level >= 10:
            await ctx.send("❌ You already have the maximum tech level (10)!")
            return

        if civ['resources']['gold'] < 2000:
            await ctx.send("❌ You need 2000 gold to purchase a tech level!")
            return

        self.civ_manager.spend_resources(user_id, {"gold": 2000})
        self.civ_manager.update_military(user_id, {"tech_level": 1})
        new_level = current_level + 1

        embed = create_embed("🔬 Technology Purchased!", f"Tech level increased from **{current_level}** to **{new_level}**!", guilded.Color.blue())
        embed.add_field(name="Cost", value="🪙 2,000 Gold", inline=True)
        await ctx.send(embed=embed)

    # ================================================================
    # CORPORATION SYSTEM
    # ================================================================

    @commands.group(name='corporation', invoke_without_command=True)
    async def corporation(self, ctx):
        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need to start a civilization first! Use `.start <name>`")
            return
        corps = civ.get('corporations', [])
        if not corps:
            await ctx.send("📭 You have no corporations. Use `.corporation build` to start one.")
            return
        embed = create_embed("🏢 Your Corporations", "", guilded.Color.blue())
        total_income = 0
        for i, corp in enumerate(corps, 1):
            level = corp.get('level', 1)
            income = self._get_corp_income(civ, level)
            total_income += income
            embed.add_field(name=f"Corp #{i}", value=f"Level: {level}\nIncome: {format_number(income)} gold/hour", inline=True)
        embed.add_field(name="Total Passive Income", value=f"{format_number(total_income)} gold/hour", inline=False)
        await ctx.send(embed=embed)

    @corporation.command(name='build')
    async def corp_build(self, ctx):
        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need to start a civilization first! Use `.start <name>`")
            return
        corps = civ.get('corporations', [])
        if len(corps) >= 5:
            await ctx.send("❌ You already have the maximum of 5 corporations!")
            return
        if civ['resources']['gold'] < 100000:
            await ctx.send("❌ You need 100,000 gold to build a corporation!")
            return
        self.civ_manager.spend_resources(user_id, {"gold": 100000})
        corps.append({"level": 1})
        self.db.update_civilization(user_id, {"corporations": corps})
        await ctx.send("🏢 Corporation built! It will generate passive income every hour. Use `.corporation list` to view.")

    @corporation.command(name='upgrade')
    async def corp_upgrade(self, ctx, corp_number: int = 1):
        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need to start a civilization first! Use `.start <name>`")
            return
        corps = civ.get('corporations', [])
        if not corps:
            await ctx.send("❌ You don't have any corporations to upgrade!")
            return
        if corp_number < 1 or corp_number > len(corps):
            await ctx.send(f"❌ Invalid corporation number! Choose 1 to {len(corps)}.")
            return
        if civ['resources']['gold'] < 200000:
            await ctx.send("❌ You need 200,000 gold to upgrade a corporation!")
            return
        self.civ_manager.spend_resources(user_id, {"gold": 200000})
        corps[corp_number - 1]['level'] += 1
        self.db.update_civilization(user_id, {"corporations": corps})
        await ctx.send(f"⬆️ Corporation #{corp_number} upgraded to level {corps[corp_number - 1]['level']}!")

    @corporation.command(name='list', aliases=['view'])
    async def corp_list(self, ctx):
        await self.corporation(ctx)

    # ================================================================
    # MEGAPROJECT SYSTEM
    # ================================================================

    @commands.group(name='megaproject', invoke_without_command=True)
    async def megaproject(self, ctx):
        embed = create_embed("🏗️ Megaprojects", "Build world-changing projects! Use `.megaproject build <name>`", guilded.Color.gold())
        for key, data in self.megaprojects.items():
            cost_str = ", ".join([f"{amt} {res}" for res, amt in data['cost'].items()])
            embed.add_field(
                name=f"{data['name']} (Tech {data['tech_required']})",
                value=f"Cost: {cost_str}\nEffect: {data['description']}",
                inline=False
            )
        await ctx.send(embed=embed)

    @megaproject.command(name='build')
    async def megaproject_build(self, ctx, project_name: str):
        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need to start a civilization first! Use `.start <name>`")
            return

        project = None
        for key, data in self.megaprojects.items():
            if key.lower() == project_name.lower() or data['name'].lower() == project_name.lower():
                project = (key, data)
                break
        if not project:
            await ctx.send(f"❌ Unknown project. Use `.megaproject` to see available projects.")
            return

        key, data = project
        if civ['military']['tech_level'] < data['tech_required']:
            await ctx.send(f"❌ You need Tech Level {data['tech_required']} to build {data['name']}!")
            return

        built = civ.get('megaprojects', [])
        if key in built:
            await ctx.send(f"❌ You already built {data['name']}!")
            return

        if not self.civ_manager.can_afford(user_id, data['cost']):
            cost_str = ", ".join([f"{amt} {res}" for res, amt in data['cost'].items()])
            await ctx.send(f"❌ Cannot afford {data['name']}! Requires: {cost_str}")
            return

        self.civ_manager.spend_resources(user_id, data['cost'])
        built.append(key)
        bonuses = civ.get('bonuses', {})
        for effect_key, effect_value in data['effect'].items():
            bonuses[effect_key] = bonuses.get(effect_key, 0) + effect_value
        self.db.update_civilization(user_id, {"megaprojects": built, "bonuses": bonuses})

        embed = create_embed(f"🏗️ {data['name']} Complete!", f"You built {data['name']}! {data['description']}", guilded.Color.gold())
        await ctx.send(embed=embed)

    @megaproject.command(name='list')
    async def megaproject_list(self, ctx):
        await self.megaproject(ctx)

    # ================================================================
    # POLICY SYSTEM
    # ================================================================

    @commands.group(name='policy', invoke_without_command=True)
    async def policy(self, ctx):
        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need to start a civilization first! Use `.start <name>`")
            return
        active = civ.get('policies', {})
        if not active:
            await ctx.send("📭 You have no active policies. Use `.policy enable <name>` to enable one.")
            return

        embed = create_embed("📜 Active Policies", "", guilded.Color.blue())
        for policy_key, level in active.items():
            if policy_key in self.policies:
                pdata = self.policies[policy_key]
                level_data = pdata['levels'].get(level)
                if level_data:
                    embed.add_field(
                        name=f"{pdata['name']} (Level {level})",
                        value=level_data['desc'],
                        inline=False
                    )
        await ctx.send(embed=embed)

    @policy.command(name='enable')
    async def policy_enable(self, ctx, policy_name: str):
        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need to start a civilization first! Use `.start <name>`")
            return

        policy_key = None
        for key, data in self.policies.items():
            if key.lower() == policy_name.lower() or data['name'].lower() == policy_name.lower():
                policy_key = key
                break
        if not policy_key:
            await ctx.send(f"❌ Unknown policy. Use `.policieshelp` to see available policies.")
            return

        active = civ.get('policies', {})
        if policy_key in active:
            await ctx.send(f"❌ Policy '{self.policies[policy_key]['name']}' is already active.")
            return

        pdata = self.policies[policy_key]
        level_data = pdata['levels'][1]
        cost = {}
        for res, amt in level_data.items():
            if res.endswith('_cost'):
                resource = res.replace('_cost', '')
                cost[resource] = amt
        if not self.civ_manager.can_afford(user_id, cost):
            cost_str = ", ".join([f"{amt} {res}" for res, amt in cost.items()])
            await ctx.send(f"❌ Cannot afford to enable {pdata['name']}! Requires: {cost_str}")
            return

        self.civ_manager.spend_resources(user_id, cost)
        active[policy_key] = 1
        self.db.update_civilization(user_id, {"policies": active})
        self._apply_policy_effects(user_id, policy_key, 1, active)

        await ctx.send(f"✅ Enabled **{pdata['name']}** at Level 1! Use `.policy upgrade <name>` to improve it.")

    @policy.command(name='upgrade')
    async def policy_upgrade(self, ctx, policy_name: str):
        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need to start a civilization first! Use `.start <name>`")
            return

        policy_key = None
        for key, data in self.policies.items():
            if key.lower() == policy_name.lower() or data['name'].lower() == policy_name.lower():
                policy_key = key
                break
        if not policy_key:
            await ctx.send(f"❌ Unknown policy. Use `.policieshelp` to see available policies.")
            return

        active = civ.get('policies', {})
        if policy_key not in active:
            await ctx.send(f"❌ Policy '{self.policies[policy_key]['name']}' is not active. Enable it first.")
            return

        pdata = self.policies[policy_key]
        current_level = active[policy_key]
        if current_level >= pdata['max_level']:
            await ctx.send(f"❌ {pdata['name']} is already at maximum level ({pdata['max_level']}).")
            return

        next_level = current_level + 1
        level_data = pdata['levels'][next_level]
        cost = {}
        for res, amt in level_data.items():
            if res.endswith('_cost'):
                resource = res.replace('_cost', '')
                cost[resource] = amt
        if not self.civ_manager.can_afford(user_id, cost):
            cost_str = ", ".join([f"{amt} {res}" for res, amt in cost.items()])
            await ctx.send(f"❌ Cannot afford to upgrade {pdata['name']} to Level {next_level}! Requires: {cost_str}")
            return

        self.civ_manager.spend_resources(user_id, cost)
        active[policy_key] = next_level
        self.db.update_civilization(user_id, {"policies": active})
        self._apply_policy_effects(user_id, policy_key, next_level, active)

        await ctx.send(f"⬆️ Upgraded **{pdata['name']}** to Level {next_level}!")

    @policy.command(name='disable')
    async def policy_disable(self, ctx, policy_name: str):
        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need to start a civilization first! Use `.start <name>`")
            return

        policy_key = None
        for key, data in self.policies.items():
            if key.lower() == policy_name.lower() or data['name'].lower() == policy_name.lower():
                policy_key = key
                break
        if not policy_key:
            await ctx.send(f"❌ Unknown policy. Use `.policieshelp` to see available policies.")
            return

        active = civ.get('policies', {})
        if policy_key not in active:
            await ctx.send(f"❌ Policy '{self.policies[policy_key]['name']}' is not active.")
            return

        del active[policy_key]
        self.db.update_civilization(user_id, {"policies": active})
        self._apply_all_policies(user_id)

        await ctx.send(f"❌ Disabled **{self.policies[policy_key]['name']}**.")

    @policy.command(name='list')
    async def policy_list(self, ctx):
        embed = create_embed("📜 Available Policies", "Use `.policy enable <name>` to start one.", guilded.Color.blue())
        for key, data in self.policies.items():
            embed.add_field(
                name=f"{data['name']} (Max Level {data['max_level']})",
                value=f"{data['base_desc']}\n"
                      f"Level 1: {data['levels'][1]['desc']} (Cost: {self._format_cost(data['levels'][1])})",
                inline=False
            )
        embed.add_field(
            name="Commands",
            value="`.policy enable <name>` – Enable at Level 1\n"
                  "`.policy upgrade <name>` – Upgrade to next level\n"
                  "`.policy disable <name>` – Disable policy\n"
                  "`.policy` – View your active policies",
            inline=False
        )
        await ctx.send(embed=embed)

    def _format_cost(self, level_data: dict) -> str:
        cost_items = []
        for key, val in level_data.items():
            if key.endswith('_cost'):
                resource = key.replace('_cost', '')
                cost_items.append(f"{val} {resource}")
        return ", ".join(cost_items) if cost_items else "Free"

    def _apply_policy_effects(self, user_id: str, policy_key: str, level: int, active_policies: dict):
        pdata = self.policies[policy_key]
        level_data = pdata['levels'][level]
        effect = level_data.get('effect', {})
        if not effect:
            return

        self._remove_policy_effect(user_id, policy_key)

        bonuses = self.civ_manager.get_civilization(user_id).get('bonuses', {})
        for effect_key, effect_value in effect.items():
            bonus_key = f"policy_{policy_key}_{effect_key}"
            bonuses[bonus_key] = effect_value
        self.db.update_civilization(user_id, {"bonuses": bonuses})

    def _remove_policy_effect(self, user_id: str, policy_key: str):
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            return
        bonuses = civ.get('bonuses', {})
        to_remove = [k for k in bonuses if k.startswith(f"policy_{policy_key}_")]
        for k in to_remove:
            del bonuses[k]
        self.db.update_civilization(user_id, {"bonuses": bonuses})

    def _apply_all_policies(self, user_id: str):
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            return
        active = civ.get('policies', {})
        bonuses = civ.get('bonuses', {})
        to_remove = [k for k in bonuses if k.startswith("policy_")]
        for k in to_remove:
            del bonuses[k]
        self.db.update_civilization(user_id, {"bonuses": bonuses})
        for policy_key, level in active.items():
            self._apply_policy_effects(user_id, policy_key, level, active)

    @commands.command(name='policieshelp')
    async def policies_help(self, ctx):
        await self.policy_list(ctx)

    # ================================================================
    # REMAINING COMMANDS (tax, lottery, invest, etc.)
    # ================================================================

    @commands.command(name='tax')
    @check_cooldown_decorator("tax")
    async def collect_taxes(self, ctx):
        user_id = str(ctx.author.id)
        if not await self.check_civil_war_and_proceed(ctx, user_id):
            return
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need to start a civilization first! Use `.start <name>`")
            return

        population = civ['population']
        base_tax = population['citizens'] * config.ECONOMY["tax_base_per_citizen"]
        happiness_modifier = population['happiness'] / 100
        employment_rate = self.civ_manager.get_employment_rate(user_id) / 100
        employment_factor = 1 + employment_rate * config.ECONOMY["tax_employment_coeff"]
        territory_factor = get_territory_modifier(civ['territory']['land_size'])

        total_tax = int(base_tax * happiness_modifier * employment_factor * territory_factor)

        ideology = civ.get('ideology', '')
        if ideology == 'democracy':
            total_tax = int(total_tax * 1.05)
        elif ideology == 'fascism':
            total_tax = int(total_tax * 1.1)
            self.civ_manager.update_population(user_id, {"happiness": config.ECONOMY["tax_fascism_extra_penalty"]})
        elif ideology == 'communism':
            total_tax = int(total_tax * 0.8)

        total_tax = min(total_tax, config.CAPS["tax"])

        self.civ_manager.update_resources(user_id, {"gold": total_tax})
        self.civ_manager.update_population(user_id, {"happiness": config.ECONOMY["tax_happiness_penalty"]})

        population_loss = 0
        if population['happiness'] < config.ECONOMY["tax_population_loss_threshold"] and random.random() < config.ECONOMY["tax_population_loss_chance"]:
            population_loss = random.randint(config.ECONOMY["tax_population_loss_min"], config.ECONOMY["tax_population_loss_max"])
            self.civ_manager.update_population(user_id, {"citizens": -population_loss})

        embed = create_embed("💰 Tax Collection", f"Collected {format_number(total_tax)} gold in taxes from your citizens.", guilded.Color.gold())
        if ideology == 'fascism':
            embed.add_field(name="Regime Effect", value=f"Forced taxation decreased happiness by {abs(config.ECONOMY['tax_fascism_extra_penalty'])}!", inline=False)
        if population_loss > 0:
            embed.add_field(name="⚠️ Population Loss", value=f"{population_loss} citizens emigrated in protest against high taxes!", inline=False)
        embed.set_footer(text="Tax is now less rewarding due to public dissatisfaction.")
        await ctx.send(embed=embed)

    @commands.command(name='lottery')
    @check_cooldown_decorator("lottery")
    async def play_lottery(self, ctx, bet: int = None):
        if bet is None:
            await ctx.send("💸 **Lottery** - Risk it all for glory!\nUsage: `.lottery <gold_amount>`\nMinimum bet: 50 gold")
            return
        if bet < 50:
            await ctx.send("❌ Minimum lottery bet is 50 gold!")
            return
        user_id = str(ctx.author.id)
        if not await self.check_civil_war_and_proceed(ctx, user_id):
            return
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need to start a civilization first! Use `.start <name>`")
            return
        if not self.civ_manager.can_afford(user_id, {"gold": bet}):
            await ctx.send(f"❌ You don't have {format_number(bet)} gold to bet!")
            return

        self.civ_manager.spend_resources(user_id, {"gold": bet})

        roll = random.random()
        if roll < 0.01:
            winnings = bet * 50
            result = f"🎰 **MEGA JACKPOT!** You won {format_number(winnings)} gold!"
            color = guilded.Color.gold()
        elif roll < 0.05:
            winnings = bet * 10
            result = f"🎰 **Big Win!** You won {format_number(winnings)} gold!"
            color = guilded.Color.green()
        elif roll < 0.20:
            winnings = bet * 2
            result = f"🎰 **Winner!** You won {format_number(winnings)} gold!"
            color = guilded.Color.green()
        elif roll < 0.40:
            winnings = bet
            result = f"🎰 **Break Even** - You got your {format_number(bet)} gold back."
            color = guilded.Color.blue()
        else:
            winnings = 0
            result = f"🎰 **No Luck** - Better luck next time!"
            color = guilded.Color.red()

        if winnings > 0:
            winnings = min(winnings, 10000000)
            self.civ_manager.update_resources(user_id, {"gold": winnings})

        embed = create_embed("🎰 Lottery Results", result, color)
        embed.add_field(name="Bet Amount", value=f"{format_number(bet)} gold", inline=True)
        if winnings > 0:
            embed.add_field(name="Winnings", value=f"{format_number(winnings)} gold", inline=True)
        await ctx.send(embed=embed)

    @commands.command(name='invest')
    @check_cooldown_decorator("invest")
    async def invest_gold(self, ctx, amount: int = None):
        if amount is None:
            await ctx.send("💼 **Investment Banking**\nUsage: `.invest <gold_amount>`\nReturns profit after 2 hours with 80% success rate.")
            return
        if amount < 100:
            await ctx.send("❌ Minimum investment is 100 gold!")
            return
        user_id = str(ctx.author.id)
        if not await self.check_civil_war_and_proceed(ctx, user_id):
            return
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need to start a civilization first! Use `.start <name>`")
            return
        if not self.civ_manager.can_afford(user_id, {"gold": amount}):
            await ctx.send(f"❌ You don't have {format_number(amount)} gold to invest!")
            return

        self.civ_manager.spend_resources(user_id, {"gold": amount})

        embed = create_embed("💼 Investment Made", f"Invested {format_number(amount)} gold in the market.\nCheck back in 2 hours to see your returns!", guilded.Color.blue())
        await ctx.send(embed=embed)

        async def investment_return():
            await asyncio.sleep(7200)
            if random.random() < 0.8:
                profit_multiplier = random.uniform(1.2, 1.8)
                returns = int(amount * profit_multiplier)
                returns = min(returns, 10000000)
                self.civ_manager.update_resources(user_id, {"gold": returns})
                try:
                    user = await self.bot.fetch_user(int(user_id))
                    await user.send(f"💰 **Investment Return**: Your investment of {format_number(amount)} gold has returned {format_number(returns)} gold! (Profit: {format_number(returns - amount)})")
                except:
                    pass
            else:
                loss_multiplier = random.uniform(0.3, 0.7)
                returns = int(amount * loss_multiplier)
                self.civ_manager.update_resources(user_id, {"gold": returns})
                try:
                    user = await self.bot.fetch_user(int(user_id))
                    await user.send(f"📉 **Investment Loss**: Market crash! Your investment of {format_number(amount)} gold only returned {format_number(returns)} gold. (Loss: {format_number(amount - returns)})")
                except:
                    pass

        asyncio.create_task(investment_return())

    @commands.command(name='drive')
    async def drive_citizens(self, ctx, amount: int = None):
        if amount is None or amount < 1:
            await ctx.send("🚗 **Drive Command**\nUsage: `.drive <amount>`\nUnemploy <amount> citizens to reduce employment rate.")
            return
        user_id = str(ctx.author.id)
        if not await self.check_civil_war_and_proceed(ctx, user_id):
            return
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need to start a civilization first! Use `.start <name>`")
            return

        population = civ['population']
        current_employed = population.get('employed', 0)
        if amount > current_employed:
            await ctx.send(f"❌ Only {current_employed} employed citizens available to unemploy!")
            return

        self.civ_manager.update_employment(user_id, -amount)
        self.civ_manager.update_population(user_id, {"happiness": -2})
        new_rate = self.civ_manager.get_employment_rate(user_id)

        embed = create_embed("🚗 Citizens Unemployed", f"Successfully unemployed {format_number(amount)} citizens.", guilded.Color.red())
        embed.add_field(name="New Employment Rate", value=f"{new_rate:.1f}%", inline=True)
        embed.add_field(name="Morale Impact", value="Unemployment has caused unrest. (-2 happiness)", inline=False)
        await ctx.send(embed=embed)

    @commands.command(name='festival')
    @check_cooldown_decorator("festival")
    async def hold_festival(self, ctx):
        user_id = str(ctx.author.id)
        if not await self.check_civil_war_and_proceed(ctx, user_id):
            return
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need to start a civilization first! Use `.start <name>`")
            return

        festival_cost = {"gold": 200, "food": 100}
        if not self.civ_manager.can_afford(user_id, festival_cost):
            await ctx.send("❌ You need 200 gold and 100 food to hold a festival!")
            return

        self.civ_manager.spend_resources(user_id, festival_cost)
        happiness_boost = 10
        self.civ_manager.update_population(user_id, {"happiness": happiness_boost})

        ideology = civ.get('ideology', '')
        if ideology == 'theocracy':
            happiness_boost = int(happiness_boost * 1.2)

        embed = create_embed("🎉 Grand Festival", "Your civilization celebrates with a grand festival, boosting morale!", guilded.Color.gold())
        embed.add_field(name="Morale Boost", value=f"Citizens are overjoyed! (+{happiness_boost} happiness)", inline=False)
        embed.add_field(name="Cost", value="🪙 200 Gold\n🌾 100 Food", inline=True)
        if ideology == 'theocracy':
            embed.add_field(name="Ideology Bonus", value="Theocratic celebrations enhanced happiness!", inline=False)
        await ctx.send(embed=embed)

    @commands.command(name='cheer')
    @check_cooldown_decorator("cheer")
    async def cheer_citizens(self, ctx):
        user_id = str(ctx.author.id)
        if not await self.check_civil_war_and_proceed(ctx, user_id):
            return
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need to start a civilization first! Use `.start <name>`")
            return

        cheer_cost = {"gold": 50}
        if not self.civ_manager.can_afford(user_id, cheer_cost):
            await ctx.send("❌ You need 50 gold to spread cheer!")
            return

        self.civ_manager.spend_resources(user_id, cheer_cost)
        happiness_boost = 5
        self.civ_manager.update_population(user_id, {"happiness": happiness_boost})

        ideology = civ.get('ideology', '')
        if ideology == 'democracy':
            happiness_boost = int(happiness_boost * 1.1)

        embed = create_embed("😊 Spreading Cheer", "Your leaders spread cheer, uplifting your citizens!", guilded.Color.green())
        embed.add_field(name="Morale Boost", value=f"Citizens are happier! (+{happiness_boost} happiness)", inline=False)
        embed.add_field(name="Cost", value="🪙 50 Gold", inline=True)
        if ideology == 'democracy':
            embed.add_field(name="Ideology Bonus", value="Democratic unity enhanced happiness!", inline=False)
        await ctx.send(embed=embed)

    @commands.command(name='sell')
    @app_commands.describe(item_name="Hyper item name to sell")
    @app_commands.autocomplete(item_name=_sell_item_autocomplete)
    async def sell_hyper_item(self, ctx, item_name: str = None):
        if not item_name:
            await ctx.send("💰 **Sell Hyper Items**\nUsage: `.sell <item-name>`\nSell specific hyper items to wandering merchants for gold.")
            return
        user_id = str(ctx.author.id)
        if not await self.check_civil_war_and_proceed(ctx, user_id):
            return
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need to start a civilization first! Use `.start <name>`")
            return

        hyper_items = civ['hyper_items']
        if item_name not in hyper_items:
            await ctx.send(f"❌ You don't have the '{item_name}' hyper item!")
            return

        prices = {
            "Lucky-Charm": random.randint(config.ECONOMY["sell_common_min"], config.ECONOMY["sell_common_max"]),
            "Ancient-Relic": random.randint(config.ECONOMY["sell_rare_min"], config.ECONOMY["sell_rare_max"]),
            "Crystal-Heart": random.randint(config.ECONOMY["sell_rare_min"], config.ECONOMY["sell_rare_max"]),
            "Dragon-Scale": random.randint(config.ECONOMY["sell_rare_min"] + 50, config.ECONOMY["sell_rare_max"] + 50),
            "Phoenix-Feather": random.randint(config.ECONOMY["sell_legendary_min"], config.ECONOMY["sell_legendary_max"])
        }
        gold_value = prices.get(item_name, random.randint(50, 150))
        gold_value = min(gold_value, config.CAPS["sell"])

        self.civ_manager.use_hyper_item(user_id, item_name)
        self.civ_manager.update_resources(user_id, {"gold": gold_value})

        embed = create_embed("💰 Item Sold!", f"You sold the '{item_name}' to a wandering merchant for {format_number(gold_value)} gold!", guilded.Color.gold())
        await ctx.send(embed=embed)

    @commands.command(name='advertise')
    @check_cooldown_decorator("advertise")
    async def advertise_civilization(self, ctx):
        user_id = str(ctx.author.id)
        if not await self.check_civil_war_and_proceed(ctx, user_id):
            return
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need to start a civilization first! Use `.start <name>`")
            return

        ad_cost = config.ECONOMY["advertise_cost"]
        if not self.civ_manager.can_afford(user_id, {"gold": ad_cost}):
            await ctx.send(f"❌ You need {ad_cost} gold to run advertising campaigns!")
            return

        self.civ_manager.spend_resources(user_id, {"gold": ad_cost})

        base_new_citizens = random.randint(config.ECONOMY["advertise_citizen_min"], config.ECONOMY["advertise_citizen_max"])
        happiness_bonus = civ['population']['happiness'] // 10
        employment_rate = self.civ_manager.get_employment_rate(user_id) / 100
        employment_factor = 1 + employment_rate * config.ECONOMY["advertise_employment_coeff"]
        territory_factor = get_territory_modifier(civ['territory']['land_size'])

        total_new_citizens = int((base_new_citizens + happiness_bonus) * employment_factor * territory_factor)
        total_new_citizens = min(total_new_citizens, config.CAPS["advertise"])

        ideology = civ.get('ideology', '')
        if ideology == 'democracy':
            total_new_citizens = int(total_new_citizens * 1.2)
        elif ideology == 'fascism':
            total_new_citizens = int(total_new_citizens * 0.8)

        self.civ_manager.update_population(user_id, {"citizens": total_new_citizens})

        embed = create_embed("📢 Advertising Campaign", f"You advertised for free passports and {format_number(total_new_citizens)} people became citizens of your country!", guilded.Color.green())
        embed.add_field(name="Cost", value=f"🪙 {ad_cost} Gold", inline=True)
        if ideology == 'democracy':
            embed.add_field(name="Ideology Bonus", value="Democratic values attracted more immigrants!", inline=False)
        elif ideology == 'fascism':
            embed.add_field(name="Ideology Penalty", value="Authoritarian regime discouraged some potential immigrants.", inline=False)
        await ctx.send(embed=embed)

    @commands.command(name='census')
    async def show_census(self, ctx):
        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need to start a civilization first! Use `.start <name>`")
            return

        resources = civ['resources']
        population = civ['population']
        employment_rate = self.civ_manager.get_employment_rate(user_id)

        embed = create_embed("📊 National Census Report", f"Current status of {civ['name']}", guilded.Color.blue())
        resource_text = (f"🪙 **Gold**: {format_number(resources['gold'])}\n"
                         f"🌾 **Food**: {format_number(resources['food'])}\n"
                         f"🪵 **Wood**: {format_number(resources['wood'])}\n"
                         f"🪨 **Stone**: {format_number(resources['stone'])}")
        embed.add_field(name="💰 Resources", value=resource_text, inline=True)
        population_text = (f"👥 **Total Citizens**: {format_number(population['citizens'])}\n"
                           f"💼 **Employed**: {format_number(population.get('employed', 0))}\n"
                           f"📈 **Employment Rate**: {employment_rate:.1f}%\n"
                           f"😊 **Happiness**: {population['happiness']}%\n"
                           f"🍽️ **Hunger**: {population['hunger']}%")
        embed.add_field(name="👥 Population", value=population_text, inline=True)
        await ctx.send(embed=embed)

    @commands.command(name='recruit')
    async def recruit_soldiers(self, ctx, number: int = None):
        if number is None or number < 1:
            await ctx.send("🎖️ **Recruitment Drive**\nUsage: `.recruit <number>`\nAttempt to convert citizens into soldiers. Higher numbers risk population loss if recruitment fails.")
            return
        user_id = str(ctx.author.id)
        if not await self.check_civil_war_and_proceed(ctx, user_id):
            return
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need to start a civilization first! Use `.start <name>`")
            return

        population = civ['population']
        current_citizens = population['citizens']
        if number > current_citizens:
            await ctx.send(f"❌ You only have {format_number(current_citizens)} citizens available for recruitment!")
            return

        base_success = 0.7
        happiness_modifier = population['happiness'] / 100
        population_ratio = number / current_citizens
        ratio_penalty = 0
        if population_ratio > 0.1:
            ratio_penalty = (population_ratio - 0.1) * 2
        success_chance = base_success * happiness_modifier - ratio_penalty
        success_chance = max(0.1, min(0.9, success_chance))

        if random.random() < success_chance:
            self.civ_manager.update_population(user_id, {"citizens": -number})
            self.civ_manager.update_military(user_id, {"soldiers": number})
            embed = create_embed("🎖️ Recruitment Success!", f"{format_number(number)} loyal citizens have enlisted as soldiers! Your military grows stronger.", guilded.Color.green())
            embed.add_field(name="New Military Strength", value=f"🛡️ {format_number(civ['military']['soldiers'] + number)} Soldiers", inline=True)
        else:
            citizens_lost = min(number * 2, current_citizens // 2)
            self.civ_manager.update_population(user_id, {"citizens": -citizens_lost, "happiness": -5})
            embed = create_embed("🎖️ Recruitment Failed!", f"Your recruitment drive failed. {format_number(citizens_lost)} people, fearing conscription, have fled the country.", guilded.Color.red())
            embed.add_field(name="Morale Impact", value="Citizens are fearful of forced conscription. (-5 happiness)", inline=False)
        await ctx.send(embed=embed)

    @commands.command(name='buysoldiers')
    async def buy_soldiers(self, ctx, amount: int = None):
        if amount is None or amount < 1:
            await ctx.send(f"⚔️ **Buy Soldiers**\nUsage: `.buysoldiers <amount>`\nCost: {config.MILITARY['soldier_buy_cost']} gold per soldier.")
            return
        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need to start a civilization first! Use `.start <name>`")
            return
        cost = amount * config.MILITARY['soldier_buy_cost']
        if not self.civ_manager.can_afford(user_id, {"gold": cost}):
            await ctx.send(f"❌ You need {format_number(cost)} gold to buy {format_number(amount)} soldiers! You have {format_number(civ['resources']['gold'])} gold.")
            return
        self.civ_manager.spend_resources(user_id, {"gold": cost})
        self.civ_manager.update_military(user_id, {"soldiers": amount})
        embed = create_embed("⚔️ Soldiers Bought!", f"You bought {format_number(amount)} soldiers for {format_number(cost)} gold!", guilded.Color.green())
        await ctx.send(embed=embed)

    @commands.command(name='burn')
    async def burn_resources(self, ctx):
        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need to start a civilization first! Use `.start <name>`")
            return

        resources = civ['resources']
        changes = {}
        for res in ['gold', 'food', 'wood', 'stone']:
            current = resources.get(res, 0)
            if current > 1000:
                changes[res] = 1000 - current

        if not changes:
            await ctx.send("✅ All your resources are already at or below 1000. Nothing to burn.")
            return

        self.civ_manager.update_resources(user_id, changes)

        embed = create_embed("🔥 Resources Burned!", "Excess resources have been reduced to 1000 each.", guilded.Color.orange())
        for res, change in changes.items():
            old = resources[res]
            embed.add_field(name=res.capitalize(), value=f"{format_number(old)} → 1000 (-{format_number(-change)})", inline=True)
        await ctx.send(embed=embed)

    @commands.command(name='immigration')
    @check_cooldown_decorator("immigration")
    async def open_immigration(self, ctx):
        user_id = str(ctx.author.id)
        if not await self.check_civil_war_and_proceed(ctx, user_id):
            return
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need to start a civilization first! Use `.start <name>`")
            return

        employment_rate = self.civ_manager.get_employment_rate(user_id) / 100
        employment_factor = 1 + employment_rate * config.ECONOMY["immigration_employment_coeff"]
        territory_factor = get_territory_modifier(civ['territory']['land_size'])
        base_gain = random.randint(config.ECONOMY["immigration_citizen_min"], config.ECONOMY["immigration_citizen_max"])
        gained = int(base_gain * employment_factor * territory_factor)
        gained = min(gained, config.CAPS["immigration"])

        self.civ_manager.update_population(user_id, {"citizens": gained})

        happiness_loss = random.randint(config.ECONOMY["immigration_happiness_loss_min"], config.ECONOMY["immigration_happiness_loss_max"])
        self.civ_manager.update_population(user_id, {"happiness": -happiness_loss})

        riot_triggered = False
        if random.random() < config.ECONOMY["immigration_riot_chance"]:
            riot_triggered = True
            extra_happiness_loss = random.randint(config.ECONOMY["immigration_riot_happiness_loss_min"], config.ECONOMY["immigration_riot_happiness_loss_max"])
            soldier_loss = random.randint(config.ECONOMY["immigration_riot_soldier_loss_min"], config.ECONOMY["immigration_riot_soldier_loss_max"])
            self.civ_manager.update_population(user_id, {"happiness": -extra_happiness_loss})
            self.civ_manager.update_military(user_id, {"soldiers": -soldier_loss})
            self.db.log_event(user_id, "immigration_riot", "Immigration Riot!",
                              f"Anti-immigration protests turned violent! Lost {soldier_loss} soldiers and {extra_happiness_loss} happiness.")

        self.db.log_event(user_id, "immigration", "Immigration Opened",
                          f"Gained {gained} citizens but lost {happiness_loss} happiness. Riot: {riot_triggered}")

        embed = create_embed("🛂 Immigration Open!", f"Your borders are now open to immigrants! {gained} new citizens have arrived.", guilded.Color.blue())
        embed.add_field(name="👥 Citizens Gained", value=f"+{format_number(gained)}", inline=True)
        embed.add_field(name="😡 Happiness Change", value=f"-{happiness_loss}", inline=True)

        if riot_triggered:
            embed.add_field(name="💥 PROTEST RIOT!", value=f"Anti-immigration protests turned violent! Lost {soldier_loss} soldiers and an additional {extra_happiness_loss} happiness!", inline=False)
            embed.color = guilded.Color.red()
        else:
            embed.add_field(name="⚠️ Tensions Rising", value="Protests are simmering – be careful!", inline=False)

        embed.set_footer(text="Cooldown: 10 minutes. Immigration is controversial!")
        await ctx.send(embed=embed)

    @commands.command(name='buycard')
    async def buy_card(self, ctx):
        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need to start a civilization first! Use `.start <name>`")
            return
        if not self.civ_manager.can_afford(user_id, {"gold": 500}):
            await ctx.send("❌ You need 500 gold to buy a card!")
            return
        self.civ_manager.spend_resources(user_id, {"gold": 500})
        card = random.choice(config.CARD_POOL)
        purchased = civ.get('purchased_cards', [])
        purchased.append(card)
        self.db.update_civilization(user_id, {"purchased_cards": purchased})
        embed = create_embed("🎴 Card Purchased!", f"You spent 500 gold and received:\n**{card['name']}** – {card['description']}", guilded.Color.gold())
        embed.add_field(name="How to use", value="Use `.cards use \"Card Name\"` to activate it.", inline=False)
        await ctx.send(embed=embed)

async def setup(bot):
    await bot.add_cog(EconomyCommands(bot))
