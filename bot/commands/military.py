import random
import re
import logging
import math
from datetime import datetime, timedelta
from typing import Literal, Optional, List, Dict, Any, Tuple

import discord as guilded
from discord import app_commands
from discord.ext import commands

from bot.utils import format_number, create_embed
from bot import config

logger = logging.getLogger(__name__)

# ---- CONSTANTS ----
SHIP_TYPES = {
    "frigate": {
        "name": "Frigate",
        "cost_gold": config.MILITARY["ship_costs"]["frigate"]["gold"],
        "cost_wood": config.MILITARY["ship_costs"]["frigate"]["wood"],
        "cost_stone": config.MILITARY["ship_costs"]["frigate"]["stone"],
        "strength": 10,
        "counters": [],
        "weak_against": ["destroyer", "aircraft_carrier"],
        "description": "Light escort ship, good against submarines and patrol."
    },
    "destroyer": {
        "name": "Destroyer",
        "cost_gold": config.MILITARY["ship_costs"]["destroyer"]["gold"],
        "cost_wood": config.MILITARY["ship_costs"]["destroyer"]["wood"],
        "cost_stone": config.MILITARY["ship_costs"]["destroyer"]["stone"],
        "strength": 20,
        "counters": ["frigate", "submarine"],
        "weak_against": ["battleship"],
        "description": "Anti-submarine and anti-aircraft, counters frigates and subs."
    },
    "battleship": {
        "name": "Battleship",
        "cost_gold": config.MILITARY["ship_costs"]["battleship"]["gold"],
        "cost_wood": config.MILITARY["ship_costs"]["battleship"]["wood"],
        "cost_stone": config.MILITARY["ship_costs"]["battleship"]["stone"],
        "strength": 40,
        "counters": ["destroyer", "submarine"],
        "weak_against": ["aircraft_carrier"],
        "description": "Heavily armed, counters destroyers and subs, but vulnerable to carriers."
    },
    "aircraft_carrier": {
        "name": "Aircraft Carrier",
        "cost_gold": config.MILITARY["ship_costs"]["aircraft_carrier"]["gold"],
        "cost_wood": config.MILITARY["ship_costs"]["aircraft_carrier"]["wood"],
        "cost_stone": config.MILITARY["ship_costs"]["aircraft_carrier"]["stone"],
        "strength": 50,
        "counters": ["frigate", "destroyer", "submarine"],
        "weak_against": ["battleship"],
        "description": "Project power, counters all small ships, but weak against battleships."
    },
    "submarine": {
        "name": "Submarine",
        "cost_gold": config.MILITARY["ship_costs"]["submarine"]["gold"],
        "cost_wood": config.MILITARY["ship_costs"]["submarine"]["wood"],
        "cost_stone": config.MILITARY["ship_costs"]["submarine"]["stone"],
        "strength": 15,
        "counters": ["battleship"],
        "weak_against": ["destroyer", "aircraft_carrier"],
        "description": "Stealth, counters battleships, prevents naval invasions."
    }
}

PLANE_TYPES = {
    "fighter": {
        "name": "Fighter",
        "cost_gold": config.MILITARY["plane_costs"]["fighter"]["gold"],
        "cost_wood": config.MILITARY["plane_costs"]["fighter"]["wood"],
        "cost_stone": config.MILITARY["plane_costs"]["fighter"]["stone"],
        "range": 0,
        "strength": 15,
        "description": "Short-range air superiority, defensive only."
    },
    "attacker": {
        "name": "Attacker",
        "cost_gold": config.MILITARY["plane_costs"]["attacker"]["gold"],
        "cost_wood": config.MILITARY["plane_costs"]["attacker"]["wood"],
        "cost_stone": config.MILITARY["plane_costs"]["attacker"]["stone"],
        "range": 1,
        "strength": 25,
        "description": "Medium-range ground attack, can strike one subregion away."
    },
    "bomber": {
        "name": "Bomber",
        "cost_gold": config.MILITARY["plane_costs"]["bomber"]["gold"],
        "cost_wood": config.MILITARY["plane_costs"]["bomber"]["wood"],
        "cost_stone": config.MILITARY["plane_costs"]["bomber"]["stone"],
        "range": 2,
        "strength": 40,
        "description": "Long-range heavy bomber, can strike two subregions away."
    }
}

TRAINING_LEVELS = [1.0, 1.5, 2.0, 3.0]   # multipliers for training level 0,1,2,3
MAX_BOOSTED_SOLDIERS = 300

# ---- MAIN COG ----
class MilitaryCommands(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.db = bot.db
        self.civ_manager = bot.civ_manager
        self.cooldowns = {}

    # ---- HELPERS (Firestore-based) ----
    def _get_navy(self, user_id: str) -> Dict[str, int]:
        return self.db.get_navy(user_id)

    def _update_navy(self, user_id: str, updates: Dict[str, int]) -> bool:
        return self.db.update_navy(user_id, updates)

    def _get_airforce(self, user_id: str) -> Dict[str, int]:
        return self.db.get_airforce(user_id)

    def _update_airforce(self, user_id: str, updates: Dict[str, int]) -> bool:
        return self.db.update_airforce(user_id, updates)

    def _get_military_tech(self, user_id: str) -> Dict[str, int]:
        return self.db.get_military_tech(user_id)

    def _update_military_tech(self, user_id: str, updates: Dict[str, int]) -> bool:
        return self.db.update_military_tech(user_id, updates)

    def _get_training(self, user_id: str) -> Dict[str, int]:
        return self.db.get_training(user_id)

    def _update_training(self, user_id: str, updates: Dict[str, int]) -> bool:
        return self.db.update_training(user_id, updates)

    def _get_border_info(self, user_id: str) -> Dict[str, Any]:
        doc = self.db.client.collection("borders").document(user_id).get()
        if doc.exists:
            data = doc.to_dict()
            return {
                "has_border": data.get("has_border", False),
                "border_strength": data.get("border_strength", 0),
                "border_soldiers": data.get("border_soldiers", 0),
            }
        return {"has_border": False, "border_strength": 0, "border_soldiers": 0}

    def _update_border(self, user_id: str, updates: Dict[str, Any]) -> bool:
        try:
            doc_ref = self.db.client.collection("borders").document(user_id)
            doc = doc_ref.get()
            if doc.exists:
                current = doc.to_dict()
                for k, v in updates.items():
                    current[k] = v
                doc_ref.set(current)
            else:
                doc_ref.set(updates)
            return True
        except Exception as e:
            logger.error(f"_update_border error for {user_id}: {e}")
            return False

    def _check_war(self, attacker_id: str, defender_id: str) -> bool:
        wars = self.db.get_wars(status="ongoing")
        for war in wars:
            a = war.get("attacker_id")
            d = war.get("defender_id")
            if (a == attacker_id and d == defender_id) or (a == defender_id and d == attacker_id):
                return True
        return False

    def _do_attackers_border_defender(self, attacker_id: str, defender_id: str) -> bool:
        territory_cog = self.bot.get_cog("TerritoryCog")
        if not territory_cog:
            return False
        attacker_provinces = territory_cog._get_owned_provinces(attacker_id)
        defender_provinces = territory_cog._get_owned_provinces(defender_id)
        if not attacker_provinces or not defender_provinces:
            return False
        from bot.commands.territory import PROVINCE_TO_SUBREGION, SUBREGION_DATA
        attacker_subregions = set()
        for p in attacker_provinces:
            sub = PROVINCE_TO_SUBREGION.get(p)
            if sub:
                attacker_subregions.add(sub)
        defender_subregions = set()
        for p in defender_provinces:
            sub = PROVINCE_TO_SUBREGION.get(p)
            if sub:
                defender_subregions.add(sub)
        for att_sub in attacker_subregions:
            for def_sub in defender_subregions:
                if def_sub in SUBREGION_DATA.get(att_sub, {}).get("neighbours", []):
                    return True
        return False

    def _has_completed_industrial(self, user_id: str) -> bool:
        data = self.db.get_industrial_revolution(user_id)
        return data and data.get("completed", False) == 1

    def _calculate_military_strength(self, civ: dict, navy_counts: dict = None,
                                     air_counts: dict = None, training: dict = None) -> float:
        tech = self._get_military_tech(civ['user_id'])
        ground_tech = tech.get("ground_tech", 1)
        naval_tech = tech.get("naval_tech", 1)
        air_tech = tech.get("air_tech", 1)

        if training is None:
            training = self._get_training(civ['user_id'])
        training_level = training.get("level", 0)
        multiplier = TRAINING_LEVELS[training_level] if training_level < len(TRAINING_LEVELS) else 1.0

        soldiers = civ['military']['soldiers']
        boosted_count = min(soldiers, MAX_BOOSTED_SOLDIERS)
        normal_count = soldiers - boosted_count
        effective_soldiers = normal_count + (boosted_count * multiplier)
        ground_power = effective_soldiers * 10

        spies = civ['military']['spies']
        spy_power = spies * 5

        if navy_counts is None:
            navy_counts = self._get_navy(civ['user_id'])
        navy_power = 0
        for ship_type, count in navy_counts.items():
            stats = SHIP_TYPES.get(ship_type)
            if stats:
                navy_power += count * stats["strength"] * naval_tech

        if air_counts is None:
            air_counts = self._get_airforce(civ['user_id'])
        air_power = 0
        for plane_type, count in air_counts.items():
            stats = PLANE_TYPES.get(plane_type)
            if stats:
                air_power += count * stats["strength"] * air_tech

        territory_bonus = civ['territory']['land_size'] / 10000
        return ground_power + spy_power + navy_power + air_power + territory_bonus

    def _extract_user_id(self, input_str: str) -> str:
        if not input_str:
            return None
        if input_str.startswith('<@') and input_str.endswith('>'):
            inner = input_str[2:-1]
            inner = inner.lstrip('!').strip()
            if inner:
                return inner
        if input_str.isalnum() and len(input_str) >= 6:
            return input_str
        m = re.search(r'[A-Za-z0-9]{6,}', input_str)
        return m.group(0) if m else None

    async def _get_member_from_mention(self, ctx, mention: str):
        if mention is None:
            return None
        if hasattr(mention, "id"):
            return mention
        try:
            if ctx.message.mentions:
                for m in ctx.message.mentions:
                    if str(m.id) == self._extract_user_id(mention):
                        return m
                return ctx.message.mentions[0]
        except:
            pass
        try:
            converter = commands.MemberConverter()
            return await converter.convert(ctx, mention)
        except:
            pass
        user_id = self._extract_user_id(mention)
        if user_id:
            try:
                return await ctx.guild.fetch_member(user_id)
            except:
                pass
        return None

    def _check_cooldown(self, user_id: str, command: str, seconds: int) -> bool:
        key = f"{user_id}_{command}"
        now = datetime.utcnow()
        if key in self.cooldowns and now < self.cooldowns[key]:
            return False
        self.cooldowns[key] = now + timedelta(seconds=seconds)
        return True

    def _get_cooldown_remaining(self, user_id: str, command: str) -> int:
        key = f"{user_id}_{command}"
        now = datetime.utcnow()
        if key in self.cooldowns and now < self.cooldowns[key]:
            return int((self.cooldowns[key] - now).total_seconds())
        return 0

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

    # ---- COMMANDS ----

    @commands.command(name='train')
    @app_commands.describe(unit_type="Type of unit to train", amount="How many units to train")
    @app_commands.choices(unit_type=[
        app_commands.Choice(name="soldiers", value="soldiers"),
        app_commands.Choice(name="spies", value="spies"),
    ])
    async def train_soldiers(self, ctx, unit_type: Optional[Literal["soldiers", "spies"]] = None, amount: int = None):
        try:
            user_id = str(ctx.author.id)
            cooldown_seconds = config.COOLDOWNS.get("train", 2) * 60
            if not self._check_cooldown(user_id, 'train', cooldown_seconds):
                remaining = self._get_cooldown_remaining(user_id, 'train')
                mins = remaining // 60
                secs = remaining % 60
                await ctx.send(f"⏳ Please wait {mins}m {secs}s before training again!")
                return

            if not unit_type:
                embed = create_embed("⚔️ Military Training", "Train units to strengthen your army!", guilded.Color.blue())
                embed.add_field(name="Available Units",
                                value=f"`soldiers` - Basic infantry ({config.MILITARY['train_cost_soldier_gold']} gold, {config.MILITARY['train_cost_soldier_food']} food each)\n"
                                      f"`spies` - Intelligence operatives ({config.MILITARY['train_cost_spy_gold']} gold, {config.MILITARY['train_cost_spy_food']} food each)",
                                inline=False)
                embed.add_field(name="Usage", value="`.train <unit_type> <amount>`", inline=False)
                await ctx.send(embed=embed)
                return

            if not await self.check_civil_war_and_proceed(ctx, user_id):
                return
            civ = self.civ_manager.get_civilization(user_id)
            if not civ:
                await ctx.send("❌ You need to start a civilization first! Use `.start`")
                return

            unit_type = unit_type.lower()
            if unit_type not in ['soldiers', 'spies']:
                await ctx.send("❌ Invalid unit type! Choose 'soldiers' or 'spies'.")
                return

            if amount is None or amount < 1:
                await ctx.send("❌ Please specify a valid amount to train!")
                return

            if unit_type == 'soldiers':
                gold_cost = amount * config.MILITARY['train_cost_soldier_gold']
                food_cost = amount * config.MILITARY['train_cost_soldier_food']
            else:
                gold_cost = amount * config.MILITARY['train_cost_spy_gold']
                food_cost = amount * config.MILITARY['train_cost_spy_food']

            costs = {"gold": gold_cost, "food": food_cost}
            if not self.civ_manager.can_afford(user_id, costs):
                await ctx.send(f"❌ Not enough resources! Need {format_number(gold_cost)} gold and {format_number(food_cost)} food.")
                return

            training_modifier = self.civ_manager.get_ideology_modifier(user_id, "soldier_training_speed")
            bonus_units = 0
            penalty_units = 0
            if training_modifier > 1.0:
                bonus_chance = (training_modifier - 1.0) * 0.5
                if random.random() < bonus_chance:
                    bonus_units = max(1, amount // 10)
                    amount += bonus_units
            elif training_modifier < 1.0:
                penalty_chance = (1.0 - training_modifier) * 0.5
                if random.random() < penalty_chance:
                    penalty_units = max(1, amount // 10)
                    amount = max(1, amount - penalty_units)

            self.civ_manager.spend_resources(user_id, costs)
            self.civ_manager.update_military(user_id, {unit_type: amount})

            embed = create_embed(f"⚔️ Training Complete", f"Successfully trained {format_number(amount)} {unit_type}!", guilded.Color.green())
            embed.add_field(name="Cost", value=f"🪙 {format_number(gold_cost)} Gold\n🌾 {format_number(food_cost)} Food", inline=True)
            if bonus_units > 0:
                embed.add_field(name="Bonus Units", value=f"🎉 Ideology bonus added {bonus_units} extra units!", inline=True)
            if penalty_units > 0:
                embed.add_field(name="Training Issues", value=f"⚠️ Ideology penalty lost {penalty_units} units during training", inline=True)
            await ctx.send(embed=embed)

        except Exception as e:
            logger.error(f"Error in train command: {e}", exc_info=True)

    @commands.command(name='declare')
    @app_commands.describe(target="Civilization leader to declare war on")
    async def declare_war(self, ctx, target: Optional[guilded.Member] = None):
        try:
            if not target:
                await ctx.send("⚔️ **Declaration of War**\nUsage: `.declare <user>`\nNote: War must be declared before attacking!")
                return

            user_id = str(ctx.author.id)
            if not await self.check_civil_war_and_proceed(ctx, user_id):
                return
            civ = self.civ_manager.get_civilization(user_id)
            if not civ:
                await ctx.send("❌ You need to start a civilization first! Use `.start`")
                return

            target_id = str(target.id)
            if target_id == user_id:
                await ctx.send("❌ You cannot declare war on yourself!")
                return
            target_civ = self.civ_manager.get_civilization(target_id)
            if not target_civ:
                await ctx.send("❌ Target user doesn't have a civilization!")
                return

            if self._check_war(user_id, target_id):
                await ctx.send("❌ You're already at war with this civilization!")
                return

            self.db.declare_war(user_id, target_id, "declared")
            self.db.log_event(user_id, "war_declaration", "War Declared",
                              f"{civ['name']} has declared war on {target_civ['name']}!")

            embed = create_embed("⚔️ War Declared!", f"**{civ['name']}** has officially declared war on **{target_civ['name']}**!", guilded.Color.red())
            embed.add_field(name="Next Steps", value="You can now use `.attack <target> <level>` (1-10).", inline=False)
            await ctx.send(embed=embed)
            try:
                await ctx.send(f"{target.mention} ⚔️ **WAR DECLARED!** {civ['name']} (led by {ctx.author.display_name}) has declared war on your civilization!")
            except Exception:
                await ctx.send(f"⚔️ **WAR DECLARED!** {civ['name']} (led by {ctx.author.display_name}) has declared war on **{target_civ['name']}**!")

        except Exception as e:
            logger.error(f"Error declaring war: {e}", exc_info=True)

    @commands.command(name='attack')
    @app_commands.describe(target="Civilization leader to attack", level="Attack intensity (1-10, higher = more damage but more cost)")
    async def attack_civilization(self, ctx, target: Optional[guilded.Member] = None, level: int = 5):
        try:
            user_id = str(ctx.author.id)
            cooldown_seconds = config.COOLDOWNS.get("attack", 3) * 60
            if not self._check_cooldown(user_id, 'attack', cooldown_seconds):
                remaining = self._get_cooldown_remaining(user_id, 'attack')
                mins = remaining // 60
                secs = remaining % 60
                await ctx.send(f"⏳ Please wait {mins}m {secs}s before attacking again!")
                return

            if not target:
                await ctx.send("⚔️ **Direct Attack**\nUsage: `.attack <user> <level>`\nLevel: 1-10 (higher = more damage, higher cost)\nNote: War must be declared first!")
                return
            if level < 1 or level > 10:
                await ctx.send("❌ Attack level must be between 1 and 10!")
                return

            if not await self.check_civil_war_and_proceed(ctx, user_id):
                return
            civ = self.civ_manager.get_civilization(user_id)
            if not civ:
                await ctx.send("❌ You need to start a civilization first! Use `.start`")
                return
            if civ['military']['soldiers'] < 10:
                await ctx.send("❌ You need at least 10 soldiers to launch an attack!")
                return

            target_id = str(target.id)
            if target_id == user_id:
                await ctx.send("❌ You cannot attack yourself!")
                return
            target_civ = self.civ_manager.get_civilization(target_id)
            if not target_civ:
                await ctx.send("❌ Target user doesn't have a civilization!")
                return

            if not self._check_war(user_id, target_id):
                await ctx.send("❌ You must declare war first! Use `.declare @user`")
                return

            shares_border = self._do_attackers_border_defender(user_id, target_id)
            attacker_strength = self._calculate_military_strength(civ)
            defender_strength = self._calculate_military_strength(target_civ)

            level_multiplier = 0.5 + (level - 1) * (1.5 / 9)
            attacker_strength *= level_multiplier

            attacker_roll = random.uniform(0.8, 1.2)
            defender_roll = random.uniform(0.8, 1.2)

            if not shares_border:
                defender_roll *= 1.5
                await ctx.send("🛡️ **DEFENSIVE ADVANTAGE!** The defender does not share a border with you, making invasion much harder! (+50% defense)")

            if civ.get('ideology') == 'fascism':
                attacker_roll *= 1.1
            if target_civ.get('ideology') == 'fascism':
                defender_roll *= 1.1
            if civ.get('ideology') == 'destruction':
                attacker_roll *= 1.15
                defender_roll *= 0.9
            if target_civ.get('ideology') == 'pacifist':
                defender_roll *= 0.85

            strength_ratio = defender_strength / max(1, attacker_strength)
            if strength_ratio < 0.5:
                underdog_bonus = (0.5 - strength_ratio) * 0.8
                defender_roll *= (1 + underdog_bonus)
                if strength_ratio < 0.25 and random.random() < 0.15:
                    defender_roll *= 1.5
                    await ctx.send("🎯 **UNDERDOG SPIRIT!** The defenders fight with incredible determination against overwhelming odds!")

            final_attacker = attacker_strength * attacker_roll
            final_defender = defender_strength * defender_roll

            soldier_cost_multiplier = 1 + (level - 1) * 0.1
            gold_cost_multiplier = 1 + (level - 1) * 0.15
            soldier_cost = int(10 * soldier_cost_multiplier)
            gold_cost = int(200 * gold_cost_multiplier)

            if civ['military']['soldiers'] < soldier_cost:
                await ctx.send(f"❌ You need at least {soldier_cost} soldiers for a level {level} attack!")
                return
            if civ['resources']['gold'] < gold_cost:
                await ctx.send(f"❌ You need at least {gold_cost} gold for a level {level} attack!")
                return

            self.civ_manager.update_military(user_id, {"soldiers": -soldier_cost})
            self.civ_manager.update_resources(user_id, {"gold": -gold_cost})

            if final_attacker > final_defender:
                victory_margin = final_attacker / max(1, final_defender)
                await self._process_attack_victory(ctx, user_id, target_id, civ, target_civ, victory_margin, level)
            else:
                defeat_margin = final_defender / max(1, final_attacker)
                await self._process_attack_defeat(ctx, user_id, target_id, civ, target_civ, defeat_margin, level)

        except Exception as e:
            logger.error(f"Error in attack command: {e}", exc_info=True)

    async def _process_attack_victory(self, ctx, attacker_id, defender_id, attacker_civ, defender_civ, margin, level):
        try:
            damage_multiplier = 0.5 + (level - 1) * (1.5 / 9)
            attacker_losses = min(random.randint(2, 8), attacker_civ['military']['soldiers'])
            defender_losses = min(int(attacker_losses * margin * damage_multiplier), defender_civ['military']['soldiers'])

            spoils = {
                "gold": min(int(defender_civ['resources']['gold'] * 0.15 * damage_multiplier), defender_civ['resources']['gold']),
                "food": min(int(defender_civ['resources']['food'] * 0.10 * damage_multiplier), defender_civ['resources']['food']),
                "stone": min(int(defender_civ['resources']['stone'] * 0.10 * damage_multiplier), defender_civ['resources']['stone']),
                "wood": min(int(defender_civ['resources']['wood'] * 0.10 * damage_multiplier), defender_civ['resources']['wood'])
            }
            territory_gained = min(int(defender_civ['territory']['land_size'] * 0.05 * damage_multiplier), defender_civ['territory']['land_size'])

            self.civ_manager.update_military(attacker_id, {"soldiers": -attacker_losses})
            self.civ_manager.update_military(defender_id, {"soldiers": -defender_losses})
            self.civ_manager.update_resources(attacker_id, spoils)
            negative_spoils = {res: -amt for res, amt in spoils.items()}
            self.civ_manager.update_resources(defender_id, negative_spoils)
            self.civ_manager.update_territory(attacker_id, {"land_size": territory_gained})
            self.civ_manager.update_territory(defender_id, {"land_size": -territory_gained})

            embed = create_embed("⚔️ Victory!", f"**{attacker_civ['name']}** has defeated **{defender_civ['name']}** in battle! (Level {level} attack)", guilded.Color.green())
            embed.add_field(name="Battle Results", value=f"Your Losses: {attacker_losses} soldiers\nEnemy Losses: {defender_losses} soldiers", inline=True)
            spoils_text = "\n".join([f"{'🪙' if res == 'gold' else '🌾' if res == 'food' else '🪨' if res == 'stone' else '🪵'} {format_number(amt)} {res.capitalize()}" for res, amt in spoils.items() if amt > 0])
            embed.add_field(name="Spoils of War", value=spoils_text or "None", inline=True)
            embed.add_field(name="Territory Gained", value=f"🏞️ {format_number(territory_gained)} km²", inline=True)

            if attacker_civ.get('ideology') == 'destruction':
                extra_damage = min(int(defender_civ['resources']['gold'] * 0.05 * damage_multiplier), defender_civ['resources']['gold'])
                self.civ_manager.update_resources(defender_id, {"gold": -extra_damage})
                embed.add_field(name="Destruction Bonus", value=f"Your destructive forces caused extra damage! (-{format_number(extra_damage)} enemy gold)", inline=False)

            await ctx.send(embed=embed)
            self.db.log_event(attacker_id, "victory", "Battle Victory", f"Defeated {defender_civ['name']} in battle (Level {level})!")
            self.db.log_event(defender_id, "defeat", "Battle Defeat", f"Defeated by {attacker_civ['name']} in battle (Level {level}).")
            try:
                member = await ctx.guild.fetch_member(defender_id)
                if member:
                    await ctx.send(f"{member.mention} ⚔️ Your civilization **{defender_civ['name']}** was defeated by **{attacker_civ['name']}** in battle! (Level {level})")
                else:
                    await ctx.send(f"⚔️ The civilization **{defender_civ['name']}** was defeated by **{attacker_civ['name']}** in battle! (Level {level})")
            except Exception:
                await ctx.send(f"⚔️ The civilization **{defender_civ['name']}** was defeated by **{attacker_civ['name']}** in battle! (Level {level})")
        except Exception as e:
            logger.error(f"Error processing attack victory: {e}", exc_info=True)

    async def _process_attack_defeat(self, ctx, attacker_id, defender_id, attacker_civ, defender_civ, margin, level):
        try:
            damage_multiplier = 0.5 + (level - 1) * (1.5 / 9)
            attacker_losses = min(int(random.randint(5, 15) * margin * damage_multiplier), attacker_civ['military']['soldiers'])
            defender_losses = min(random.randint(2, 5), defender_civ['military']['soldiers'])

            self.civ_manager.update_military(attacker_id, {"soldiers": -attacker_losses})
            self.civ_manager.update_military(defender_id, {"soldiers": -defender_losses})

            strength_ratio = defender_civ['military']['soldiers'] / max(1, attacker_civ['military']['soldiers'])
            if strength_ratio < 0.5:
                bonus_gold = min(int(attacker_civ['resources']['gold'] * 0.1), attacker_civ['resources']['gold'])
                bonus_morale = 20
                self.civ_manager.update_resources(defender_id, {"gold": bonus_gold})
                self.civ_manager.update_population(defender_id, {"happiness": bonus_morale})
                await ctx.send(f"🏆 **UNDERDOG VICTORY!** {defender_civ['name']} gains {format_number(bonus_gold)} gold and +{bonus_morale} happiness for their heroic defense!")

            self.civ_manager.update_population(attacker_id, {"happiness": -10})

            embed = create_embed("⚔️ Defeat!", f"**{attacker_civ['name']}** was defeated by **{defender_civ['name']}**! (Level {level} attack)", guilded.Color.red())
            embed.add_field(name="Battle Results", value=f"Your Losses: {attacker_losses} soldiers\nEnemy Losses: {defender_losses} soldiers", inline=True)
            embed.add_field(name="Consequences", value="Your people are demoralized! (-10 happiness)", inline=False)

            if defender_civ.get('ideology') == 'pacifist':
                peace_chance = random.random()
                if peace_chance > 0.7:
                    embed.add_field(name="Pacifist Appeal", value="The defenders have offered a chance for peace through diplomacy! Use `.peace @user` to propose peace.", inline=False)

            await ctx.send(embed=embed)
            self.db.log_event(attacker_id, "defeat", "Battle Defeat", f"Defeated by {defender_civ['name']} in battle (Level {level}).")
            self.db.log_event(defender_id, "victory", "Battle Victory", f"Successfully defended against {attacker_civ['name']} (Level {level})!")
            try:
                member = await ctx.guild.fetch_member(defender_id)
                if member:
                    await ctx.send(f"{member.mention} ⚔️ Your civilization **{defender_civ['name']}** successfully defended against **{attacker_civ['name']}**! (Level {level})")
                else:
                    await ctx.send(f"⚔️ The civilization **{defender_civ['name']}** successfully defended against **{attacker_civ['name']}**! (Level {level})")
            except Exception:
                await ctx.send(f"⚔️ The civilization **{defender_civ['name']}** successfully defended against **{attacker_civ['name']}**! (Level {level})")
        except Exception as e:
            logger.error(f"Error processing attack defeat: {e}", exc_info=True)

    @commands.command(name='stealthbattle')
    @app_commands.describe(target="Civilization leader to target")
    async def stealth_battle(self, ctx, target: Optional[guilded.Member] = None):
        try:
            user_id = str(ctx.author.id)
            cooldown_seconds = config.COOLDOWNS.get("stealthbattle", 4) * 60
            if not self._check_cooldown(user_id, 'stealthbattle', cooldown_seconds):
                remaining = self._get_cooldown_remaining(user_id, 'stealthbattle')
                mins = remaining // 60
                secs = remaining % 60
                await ctx.send(f"⏳ Please wait {mins}m {secs}s before using stealth battle again!")
                return

            if not target:
                await ctx.send("🕵️ **Stealth Battle**\nUsage: `.stealthbattle <user>`\nUses spies instead of soldiers for covert operations.")
                return

            if not await self.check_civil_war_and_proceed(ctx, user_id):
                return
            civ = self.civ_manager.get_civilization(user_id)
            if not civ:
                await ctx.send("❌ You need to start a civilization first! Use `.start`")
                return
            if civ['military']['spies'] < 3:
                await ctx.send("❌ You need at least 3 spies to conduct stealth operations!")
                return

            target_id = str(target.id)
            target_civ = self.civ_manager.get_civilization(target_id)
            if not target_civ:
                await ctx.send("❌ Target user doesn't have a civilization!")
                return

            tech = self._get_military_tech(user_id)
            target_tech = self._get_military_tech(target_id)
            attacker_spy_power = civ['military']['spies'] * tech.get("ground_tech", 1)
            defender_spy_power = target_civ['military']['spies'] * target_tech.get("ground_tech", 1)

            success_chance = 0.6 + (attacker_spy_power - defender_spy_power) / 100
            success_chance = max(0.2, min(0.9, success_chance))

            if civ.get('ideology') == 'anarchy':
                success_chance *= 0.8
            elif civ.get('ideology') == 'destruction':
                success_chance *= 1.2
                if random.random() < 0.1:
                    success_chance += 0.15
            if target_civ.get('ideology') == 'fascism':
                success_chance *= 0.9
            elif target_civ.get('ideology') == 'pacifist':
                success_chance *= 1.1

            if random.random() < success_chance:
                spy_losses = random.randint(0, 2)
                operation_type = random.choice(['sabotage', 'theft', 'intel'])
                result_text = ""
                if operation_type == 'sabotage':
                    damage = {"stone": -random.randint(50, 200), "wood": -random.randint(30, 150)}
                    self.civ_manager.update_resources(target_id, damage)
                    result_text = "Your spies sabotaged enemy infrastructure!"
                    if civ.get('ideology') == 'destruction':
                        extra = {"gold": -random.randint(20, 100), "food": -random.randint(30, 120)}
                        self.civ_manager.update_resources(target_id, extra)
                        result_text += " Your destructive spies caused extra chaos!"
                elif operation_type == 'theft':
                    stolen = min(int(target_civ['resources']['gold'] * random.uniform(0.05, 0.15)), target_civ['resources']['gold'])
                    self.civ_manager.update_resources(target_id, {"gold": -stolen})
                    self.civ_manager.update_resources(user_id, {"gold": stolen})
                    result_text = f"Your spies stole {format_number(stolen)} gold!"
                else:
                    tech_gain = 1 if random.random() < 0.3 else 0
                    if tech_gain:
                        self._update_military_tech(user_id, {"ground_tech": tech_gain})
                    result_text = "Your spies gathered valuable intelligence!" + (f" (+{tech_gain} ground tech level)" if tech_gain else "")

                if spy_losses > 0:
                    self.civ_manager.update_military(user_id, {"spies": -spy_losses})

                embed = create_embed("🕵️ Stealth Operation Success!", result_text, guilded.Color.purple())
                if spy_losses > 0:
                    embed.add_field(name="Casualties", value=f"Lost {spy_losses} spies during the operation", inline=False)
                await ctx.send(embed=embed)

                try:
                    await ctx.send(f"{target.mention} 🕵️ Your civilization **{target_civ['name']}** was hit by a successful stealth operation from **{civ['name']}**!")
                except Exception:
                    await ctx.send(f"🕵️ The civilization **{target_civ['name']}** was hit by a successful stealth operation from **{civ['name']}**!")
            else:
                spy_losses = random.randint(1, 4)
                self.civ_manager.update_military(user_id, {"spies": -spy_losses})
                embed = create_embed("🕵️ Stealth Operation Failed!", f"Your stealth mission was detected! Lost {spy_losses} spies.", guilded.Color.red())
                await ctx.send(embed=embed)
                try:
                    await ctx.send(f"{target.mention} 🔍 Your intelligence network detected and thwarted a stealth attack from **{civ['name']}**!")
                except Exception:
                    await ctx.send(f"🔍 The intelligence network of **{target_civ['name']}** detected and thwarted a stealth attack from **{civ['name']}**!")

        except Exception as e:
            logger.error(f"Error in stealthbattle command: {e}", exc_info=True)

    @commands.command(name='siege')
    @app_commands.describe(target="Civilization leader to siege")
    async def siege_city(self, ctx, target: Optional[guilded.Member] = None):
        try:
            user_id = str(ctx.author.id)
            cooldown_seconds = config.COOLDOWNS.get("siege", 10) * 60
            if not self._check_cooldown(user_id, 'siege', cooldown_seconds):
                remaining = self._get_cooldown_remaining(user_id, 'siege')
                mins = remaining // 60
                secs = remaining % 60
                await ctx.send(f"⏳ Please wait {mins}m {secs}s before sieging again!")
                return

            if not target:
                await ctx.send("🏰 **Siege Warfare**\nUsage: `.siege <user>`\nDrains enemy resources over time but requires large army.")
                return

            if not await self.check_civil_war_and_proceed(ctx, user_id):
                return
            civ = self.civ_manager.get_civilization(user_id)
            if not civ:
                await ctx.send("❌ You need to start a civilization first! Use `.start`")
                return
            if civ['military']['soldiers'] < 50:
                await ctx.send("❌ You need at least 50 soldiers to lay siege!")
                return

            target_id = str(target.id)
            target_civ = self.civ_manager.get_civilization(target_id)
            if not target_civ:
                await ctx.send("❌ Target user doesn't have a civilization!")
                return

            if not self._check_war(user_id, target_id):
                await ctx.send("❌ You must declare war first! Use `.declare @user`")
                return

            tech = self._get_military_tech(user_id)
            siege_power = civ['military']['soldiers'] + tech.get("ground_tech", 1) * 10
            defender_resistance = target_civ['military']['soldiers'] + target_civ['territory']['land_size'] / 100
            siege_effectiveness = siege_power / (siege_power + defender_resistance)

            strength_ratio = target_civ['military']['soldiers'] / max(1, civ['military']['soldiers'])
            if strength_ratio < 0.5:
                underdog_resistance = (0.5 - strength_ratio) * 0.3
                siege_effectiveness *= (1 - underdog_resistance)
                await ctx.send("🛡️ **UNDERDOG DEFENSE!** The defenders use clever tactics to resist the siege more effectively!")

            resource_drain = {
                "gold": min(int(target_civ['resources']['gold'] * siege_effectiveness * 0.1), target_civ['resources']['gold']),
                "food": min(int(target_civ['resources']['food'] * siege_effectiveness * 0.2), target_civ['resources']['food']),
                "wood": min(int(target_civ['resources']['wood'] * siege_effectiveness * 0.15), target_civ['resources']['wood']),
                "stone": min(int(target_civ['resources']['stone'] * siege_effectiveness * 0.15), target_civ['resources']['stone'])
            }

            maintenance_cost = {"gold": civ['military']['soldiers'] * 2, "food": civ['military']['soldiers'] * 3}
            if not self.civ_manager.can_afford(user_id, maintenance_cost):
                await ctx.send("❌ You cannot afford to maintain the siege! Need more gold and food.")
                return

            self.civ_manager.spend_resources(user_id, maintenance_cost)
            negative_drain = {res: -amt for res, amt in resource_drain.items()}
            self.civ_manager.update_resources(target_id, negative_drain)
            self.civ_manager.update_population(target_id, {"happiness": -15})
            self.civ_manager.update_population(user_id, {"happiness": -5})

            embed = create_embed("🏰 Siege in Progress", f"**{civ['name']}** has laid siege to **{target_civ['name']}**!", guilded.Color.orange())
            drain_text = "\n".join([f"{'🪙' if res == 'gold' else '🌾' if res == 'food' else '🪨' if res == 'stone' else '🪵'} {format_number(amt)} {res.capitalize()}" for res, amt in resource_drain.items() if amt > 0])
            embed.add_field(name="Enemy Resources Drained", value=drain_text or "None", inline=True)
            cost_text = f"🪙 {format_number(maintenance_cost['gold'])} Gold\n🌾 {format_number(maintenance_cost['food'])} Food"
            embed.add_field(name="Siege Maintenance Cost", value=cost_text, inline=True)

            if civ.get('ideology') == 'destruction':
                extra_damage = {
                    "gold": min(int(target_civ['resources']['gold'] * 0.05), target_civ['resources']['gold']),
                    "food": min(int(target_civ['resources']['food'] * 0.05), target_civ['resources']['food'])
                }
                self.civ_manager.update_resources(target_id, {k: -v for k, v in extra_damage.items()})
                embed.add_field(name="Destruction Bonus", value=f"Your destructive siege caused extra damage!\n🪙 {format_number(extra_damage['gold'])} Gold\n🌾 {format_number(extra_damage['food'])} Food", inline=False)

            await ctx.send(embed=embed)
            self.db.log_event(user_id, "siege", "Siege Initiated", f"Laying siege to {target_civ['name']}")
            self.db.log_event(target_id, "besieged", "Under Siege", f"Being sieged by {civ['name']}")
            try:
                await ctx.send(f"{target.mention} 🏰 Your civilization **{target_civ['name']}** is under siege by **{civ['name']}**!")
            except Exception:
                await ctx.send(f"🏰 The civilization **{target_civ['name']}** is under siege by **{civ['name']}**!")

        except Exception as e:
            logger.error(f"Error in siege command: {e}", exc_info=True)

    @commands.command(name='find')
    async def find_soldiers(self, ctx):
        try:
            user_id = str(ctx.author.id)
            cooldown_seconds = config.COOLDOWNS.get("find", 1) * 60
            if not self._check_cooldown(user_id, 'find', cooldown_seconds):
                remaining = self._get_cooldown_remaining(user_id, 'find')
                mins = remaining // 60
                secs = remaining % 60
                await ctx.send(f"⏳ Please wait {mins}m {secs}s before searching for soldiers again!")
                return

            if not await self.check_civil_war_and_proceed(ctx, user_id):
                return
            civ = self.civ_manager.get_civilization(user_id)
            if not civ:
                await ctx.send("❌ You need to start a civilization first! Use `.start`")
                return

            base_chance = 0.5
            min_soldiers = 5
            max_soldiers = 20

            if civ.get('ideology') == 'pacifist':
                base_chance *= 1.9
                max_soldiers = 15
            elif civ.get('ideology') == 'destruction':
                base_chance *= 0.75
                max_soldiers = 30
                min_soldiers = 10

            happiness_mod = 1 + (civ['population']['happiness'] / 100)
            final_chance = min(0.9, base_chance * happiness_mod)

            if random.random() < final_chance:
                soldiers_found = random.randint(min_soldiers, max_soldiers)
                bonus = 0
                if civ.get('ideology') == 'destruction' and random.random() < 0.2:
                    bonus = soldiers_found // 2
                    soldiers_found += bonus
                self.civ_manager.update_military(user_id, {"soldiers": soldiers_found})
                embed = create_embed("🔍 Soldiers Found!", f"You've discovered {soldiers_found} wandering soldiers who have joined your army!" +
                                     (f" (including {bonus} coerced by your destructive reputation)" if bonus else ""), guilded.Color.green())
                if civ.get('ideology') == 'pacifist':
                    embed.add_field(name="Pacifist Note", value="These soldiers joined reluctantly, drawn by your peaceful ideals.", inline=False)
            else:
                embed = create_embed("🔍 Search Unsuccessful", "You couldn't find any willing soldiers to join your cause.", guilded.Color.blue())
                if civ.get('ideology') == 'destruction':
                    embed.add_field(name="Destruction Backfire", value="Your reputation scared away potential recruits.", inline=False)

            await ctx.send(embed=embed)

        except Exception as e:
            logger.error(f"Error in find command: {e}", exc_info=True)

    @commands.command(name='peace')
    @app_commands.describe(target="Civilization leader to offer peace to")
    async def make_peace(self, ctx, target: Optional[guilded.Member] = None):
        try:
            if not target:
                await ctx.send("🕊️ **Peace Offering**\nUsage: `.peace <user>`\nSend a peace offer to end a war. They can accept with `.accept_peace <you>`.")
                return

            user_id = str(ctx.author.id)
            if not await self.check_civil_war_and_proceed(ctx, user_id):
                return
            civ = self.civ_manager.get_civilization(user_id)
            if not civ:
                await ctx.send("❌ You need to start a civilization first! Use `.start`")
                return

            target_id = str(target.id)
            if target_id == user_id:
                await ctx.send("❌ You're already at peace with yourself!")
                return

            target_civ = self.civ_manager.get_civilization(target_id)
            if not target_civ:
                await ctx.send("❌ Target user doesn't have a civilization!")
                return

            if not self._check_war(user_id, target_id):
                await ctx.send("❌ You're not at war with this civilization!")
                return

            offers = self.db.get_peace_offers()
            for offer in offers:
                if offer.get("offerer_id") == user_id and offer.get("receiver_id") == target_id:
                    await ctx.send("❌ You already have a pending peace offer to this civilization!")
                    return

            self.db.create_peace_offer(user_id, target_id)

            embed = create_embed("🕊️ Peace Offer Sent!", f"**{civ['name']}** has offered peace to **{target_civ['name']}**! They can accept with `.accept_peace @{ctx.author.display_name}`.", guilded.Color.green())
            await ctx.send(embed=embed)

            try:
                await ctx.send(f"{target.mention} 🕊️ **Peace Offer Received!** {civ['name']} (led by {ctx.author.display_name}) has offered peace to end the war. Use `.accept_peace @{ctx.author.display_name}` to accept!")
            except Exception:
                await ctx.send(f"🕊️ **Peace Offer Received!** {civ['name']} (led by {ctx.author.display_name}) has offered peace to end the war. Use `.accept_peace @{ctx.author.display_name}` to accept!")

        except Exception as e:
            logger.error(f"Error in peace command: {e}", exc_info=True)

    @commands.command(name='accept_peace')
    @app_commands.describe(target="Civilization leader who sent peace offer")
    async def accept_peace(self, ctx, target: Optional[guilded.Member] = None):
        try:
            if not target:
                await ctx.send("🕊️ **Accept Peace**\nUsage: `.accept_peace <user>`\nAccept a pending peace offer to end the war.")
                return

            user_id = str(ctx.author.id)
            if not await self.check_civil_war_and_proceed(ctx, user_id):
                return
            civ = self.civ_manager.get_civilization(user_id)
            if not civ:
                await ctx.send("❌ You need to start a civilization first! Use `.start`")
                return

            offerer_id = str(target.id)
            if offerer_id == user_id:
                await ctx.send("❌ You can't accept your own peace offer!")
                return

            offerer_civ = self.civ_manager.get_civilization(offerer_id)
            if not offerer_civ:
                await ctx.send("❌ That user doesn't have a civilization!")
                return

            if not self._check_war(user_id, offerer_id):
                await ctx.send("❌ You're not at war with this civilization!")
                return

            offers = self.db.get_peace_offers()
            offer_id = None
            for offer in offers:
                if offer.get("offerer_id") == offerer_id and offer.get("receiver_id") == user_id:
                    offer_id = offer.get("id")
                    break

            if not offer_id:
                await ctx.send("❌ No pending peace offer from this civilization!")
                return

            self.db.end_war(user_id, offerer_id, "peace")
            self.db.update_peace_offer(offer_id, "accepted")

            self.civ_manager.update_population(user_id, {"happiness": 15})
            self.civ_manager.update_population(offerer_id, {"happiness": 15})

            embed = create_embed("🕊️ Peace Achieved!", f"**{civ['name']}** has accepted peace from **{offerer_civ['name']}**! The war is over.", guilded.Color.green())
            if civ.get('ideology') == 'pacifist' or offerer_civ.get('ideology') == 'pacifist':
                embed.add_field(name="Pacifist Influence", value="The peace movement was strengthened by pacifist ideals!", inline=False)

            await ctx.send(embed=embed)

            try:
                await ctx.send(f"{target.mention} 🕊️ **Peace Accepted!** {civ['name']} (led by {ctx.author.display_name}) has accepted your peace offer! The war is over.")
            except Exception:
                await ctx.send(f"🕊️ **Peace Accepted!** {civ['name']} (led by {ctx.author.display_name}) has accepted the peace offer! The war is over.")

            self.db.log_event(user_id, "peace_accepted", "Peace Accepted", f"Accepted peace with {offerer_civ['name']}")
            self.db.log_event(offerer_id, "peace_accepted", "Peace Accepted", f"Peace accepted by {civ['name']}")

        except Exception as e:
            logger.error(f"Error in accept_peace command: {e}", exc_info=True)

    # ---- CARDS ----
    @commands.command(name='cards')
    async def manage_cards(self, ctx, action: str = None, *, card_name: str = None):
        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need to start a civilization first!")
            return

        if action is None or action.lower() == 'view':
            purchased = civ.get('purchased_cards', [])
            if not purchased:
                await ctx.send("📭 You have no cards. Buy one with `.buycard` or earn them from military commands!")
                return

            embed = create_embed("🎴 Your Cards", "", guilded.Color.blue())
            for i, card in enumerate(purchased, 1):
                embed.add_field(
                    name=f"{i}. {card['name']}",
                    value=f"Type: {card['type']}\n{card['description']}\nUse: `.cards use \"{card['name']}\"`",
                    inline=False
                )
            await ctx.send(embed=embed)

        elif action.lower() == 'use':
            if not card_name:
                await ctx.send("❌ Please specify a card name: `.cards use \"Card Name\"`")
                return

            purchased = civ.get('purchased_cards', [])
            card = next((c for c in purchased if c['name'].lower() == card_name.lower()), None)
            if not card:
                await ctx.send(f"❌ You don't have a card named '{card_name}'.")
                return

            effect = card['effect']
            if card['type'] == 'bonus':
                bonuses = civ.get('bonuses', {})
                for key, value in effect.items():
                    bonuses[key] = bonuses.get(key, 0) + value
                self.db.update_civilization(user_id, {"bonuses": bonuses})
            else:
                if "gold" in effect:
                    self.civ_manager.update_resources(user_id, {"gold": effect["gold"]})
                if "food" in effect:
                    self.civ_manager.update_resources(user_id, {"food": effect["food"]})
                if "stone" in effect:
                    self.civ_manager.update_resources(user_id, {"stone": effect["stone"]})
                if "wood" in effect:
                    self.civ_manager.update_resources(user_id, {"wood": effect["wood"]})
                if "soldiers" in effect:
                    self.civ_manager.update_military(user_id, {"soldiers": effect["soldiers"]})
                if "spies" in effect:
                    self.civ_manager.update_military(user_id, {"spies": effect["spies"]})
                if "tech_level" in effect:
                    self.civ_manager.update_military(user_id, {"tech_level": effect["tech_level"]})
                if "citizens" in effect:
                    self.civ_manager.update_population(user_id, {"citizens": effect["citizens"]})
                if "happiness" in effect:
                    self.civ_manager.update_population(user_id, {"happiness": effect["happiness"]})

            purchased.remove(card)
            self.db.update_civilization(user_id, {"purchased_cards": purchased})
            await ctx.send(f"✅ Used **{card['name']}**! Effect applied.")

        else:
            await ctx.send("❌ Invalid action. Use `.cards view` or `.cards use \"Card Name\"`.")

    @commands.command(name='buycard')
    async def buy_card(self, ctx):
        """Purchase a random card for 500 gold."""
        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need to start a civilization first! Use `.start <name>`")
            return

        # Check if they can afford 500 gold
        if not self.civ_manager.can_afford(user_id, {"gold": 500}):
            await ctx.send("❌ You need 500 gold to buy a card!")
            return

        # Deduct gold
        self.civ_manager.spend_resources(user_id, {"gold": 500})

        # Pick a random card from config.CARD_POOL
        card = random.choice(config.CARD_POOL)

        # Store in civilization's purchased_cards list
        purchased_cards = civ.get('purchased_cards', [])
        purchased_cards.append(card)
        self.db.update_civilization(user_id, {"purchased_cards": purchased_cards})

        embed = create_embed(
            "🎴 Card Purchased!",
            f"You spent 500 gold and received:\n**{card['name']}** – {card['description']}",
            guilded.Color.gold()
        )
        embed.add_field(name="How to use", value="Use `.cards use \"Card Name\"` to activate it.", inline=False)
        await ctx.send(embed=embed)
    
    # ---- BORDERS ----
    @commands.command(name='addborder')
    async def add_border(self, ctx):
        try:
            user_id = str(ctx.author.id)
            cooldown_seconds = config.COOLDOWNS.get("addborder", 5) * 60
            if not self._check_cooldown(user_id, 'addborder', cooldown_seconds):
                remaining = self._get_cooldown_remaining(user_id, 'addborder')
                mins = remaining // 60
                secs = remaining % 60
                await ctx.send(f"⏳ Please wait {mins}m {secs}s before adding another border!")
                return

            if not await self.check_civil_war_and_proceed(ctx, user_id):
                return
            civ = self.civ_manager.get_civilization(user_id)
            if not civ:
                await ctx.send("❌ You need to start a civilization first! Use `.start`")
                return

            border_cost = config.MILITARY["border_cost"]
            if not self.civ_manager.can_afford(user_id, border_cost):
                await ctx.send(f"❌ Not enough resources! Need {format_number(border_cost['gold'])} gold, {format_number(border_cost['stone'])} stone, and {format_number(border_cost['wood'])} wood.")
                return

            border_info = self._get_border_info(user_id)
            if border_info.get("has_border", False):
                await ctx.send("❌ You already have a border! Use `.removeborder` to remove it first.")
                return

            self.civ_manager.spend_resources(user_id, border_cost)
            self._update_border(user_id, {"has_border": True, "border_strength": 100, "border_soldiers": 0})

            embed = create_embed("🛡️ Border Established!", f"**{civ['name']}** has built a defensive border around their territory!", guilded.Color.green())
            embed.add_field(name="Border Strength", value="100/100", inline=True)
            embed.add_field(name="Cost", value=f"🪙 {format_number(border_cost['gold'])} Gold\n🪨 {format_number(border_cost['stone'])} Stone\n🪵 {format_number(border_cost['wood'])} Wood", inline=True)
            embed.add_field(name="Next Steps", value="Use `.rectract <percentage>` to assign soldiers to your border for extra defense!", inline=False)
            await ctx.send(embed=embed)

        except Exception as e:
            logger.error(f"Error in addborder command: {e}", exc_info=True)

    @commands.command(name='removeborder')
    async def remove_border(self, ctx):
        try:
            user_id = str(ctx.author.id)
            cooldown_seconds = config.COOLDOWNS.get("removeborder", 2) * 60
            if not self._check_cooldown(user_id, 'removeborder', cooldown_seconds):
                remaining = self._get_cooldown_remaining(user_id, 'removeborder')
                mins = remaining // 60
                secs = remaining % 60
                await ctx.send(f"⏳ Please wait {mins}m {secs}s before removing border again!")
                return

            if not await self.check_civil_war_and_proceed(ctx, user_id):
                return
            civ = self.civ_manager.get_civilization(user_id)
            if not civ:
                await ctx.send("❌ You need to start a civilization first! Use `.start`")
                return

            border_info = self._get_border_info(user_id)
            if not border_info.get("has_border", False):
                await ctx.send("❌ You don't have a border to remove!")
                return

            soldiers_to_return = border_info.get("border_soldiers", 0)
            if soldiers_to_return > 0:
                self.civ_manager.update_military(user_id, {"soldiers": soldiers_to_return})

            self._update_border(user_id, {"has_border": False, "border_strength": 0, "border_soldiers": 0})

            embed = create_embed("🛡️ Border Removed!", f"**{civ['name']}** has dismantled their defensive border.", guilded.Color.blue())
            if soldiers_to_return > 0:
                embed.add_field(name="Soldiers Returned", value=f"⚔️ {format_number(soldiers_to_return)} soldiers have returned to your main army.", inline=False)
            await ctx.send(embed=embed)

        except Exception as e:
            logger.error(f"Error in removeborder command: {e}", exc_info=True)

    @commands.command(name='rectract', aliases=['retract'])
    async def rectract_soldiers(self, ctx, percentage: int = None):
        try:
            user_id = str(ctx.author.id)
            cooldown_seconds = config.COOLDOWNS.get("rectract", 1) * 60
            if not self._check_cooldown(user_id, 'rectract', cooldown_seconds):
                remaining = self._get_cooldown_remaining(user_id, 'rectract')
                mins = remaining // 60
                secs = remaining % 60
                await ctx.send(f"⏳ Please wait {mins}m {secs}s before rectracting soldiers again!")
                return

            if percentage is None or percentage < 1 or percentage > 100:
                await ctx.send("❌ Please specify a percentage between 1-100! Usage: `.rectract <percentage>`")
                return

            if not await self.check_civil_war_and_proceed(ctx, user_id):
                return
            civ = self.civ_manager.get_civilization(user_id)
            if not civ:
                await ctx.send("❌ You need to start a civilization first! Use `.start`")
                return

            border_info = self._get_border_info(user_id)
            if not border_info.get("has_border", False):
                await ctx.send("❌ You need to build a border first! Use `.addborder`")
                return

            current_border_strength = border_info.get("border_strength", 0)
            current_border_soldiers = border_info.get("border_soldiers", 0)
            available_soldiers = civ['military']['soldiers']

            if available_soldiers == 0:
                await ctx.send("❌ You don't have any soldiers to assign to the border!")
                return

            soldiers_to_assign = min((available_soldiers * percentage) // 100, available_soldiers)
            if soldiers_to_assign == 0:
                await ctx.send("❌ That percentage would assign 0 soldiers. Try a higher percentage or train more soldiers.")
                return

            new_border_soldiers = current_border_soldiers + soldiers_to_assign
            border_strength_increase = soldiers_to_assign * 2

            self._update_border(user_id, {
                "border_soldiers": new_border_soldiers,
                "border_strength": current_border_strength + border_strength_increase
            })
            self.civ_manager.update_military(user_id, {"soldiers": -soldiers_to_assign})

            embed = create_embed("🛡️ Soldiers Assigned to Border!", f"**{civ['name']}** has assigned {format_number(soldiers_to_assign)} soldiers to reinforce the border.", guilded.Color.green())
            embed.add_field(name="Border Soldiers", value=f"⚔️ {format_number(new_border_soldiers)} total", inline=True)
            embed.add_field(name="Border Strength", value=f"🛡️ {format_number(current_border_strength + border_strength_increase)}", inline=True)
            embed.add_field(name="Main Army", value=f"⚔️ {format_number(available_soldiers - soldiers_to_assign)} soldiers remaining", inline=True)
            await ctx.send(embed=embed)

        except Exception as e:
            logger.error(f"Error in rectract command: {e}", exc_info=True)

    @commands.command(name='retrieve')
    async def retrieve_soldiers(self, ctx, percentage: int = None):
        try:
            user_id = str(ctx.author.id)
            cooldown_seconds = config.COOLDOWNS.get("retrieve", 1) * 60
            if not self._check_cooldown(user_id, 'retrieve', cooldown_seconds):
                remaining = self._get_cooldown_remaining(user_id, 'retrieve')
                mins = remaining // 60
                secs = remaining % 60
                await ctx.send(f"⏳ Please wait {mins}m {secs}s before retrieving soldiers again!")
                return

            if percentage is None or percentage < 1 or percentage > 100:
                await ctx.send("❌ Please specify a percentage between 1-100! Usage: `.retrieve <percentage>`")
                return

            if not await self.check_civil_war_and_proceed(ctx, user_id):
                return
            civ = self.civ_manager.get_civilization(user_id)
            if not civ:
                await ctx.send("❌ You need to start a civilization first! Use `.start`")
                return

            border_info = self._get_border_info(user_id)
            if not border_info.get("has_border", False):
                await ctx.send("❌ You need to build a border first! Use `.addborder`")
                return

            current_border_strength = border_info.get("border_strength", 0)
            current_border_soldiers = border_info.get("border_soldiers", 0)

            if current_border_soldiers == 0:
                await ctx.send("❌ You don't have any soldiers assigned to your border!")
                return

            soldiers_to_retrieve = min((current_border_soldiers * percentage) // 100, current_border_soldiers)
            strength_loss = (current_border_strength * soldiers_to_retrieve) // current_border_soldiers
            new_border_strength = max(1, current_border_strength - strength_loss)
            new_border_soldiers = current_border_soldiers - soldiers_to_retrieve

            self._update_border(user_id, {
                "border_soldiers": new_border_soldiers,
                "border_strength": new_border_strength
            })
            self.civ_manager.update_military(user_id, {"soldiers": soldiers_to_retrieve})

            embed = create_embed("🛡️ Soldiers Retrieved from Border!", f"**{civ['name']}** has retrieved {format_number(soldiers_to_retrieve)} soldiers from the border.", guilded.Color.blue())
            embed.add_field(name="Border Soldiers", value=f"⚔️ {format_number(new_border_soldiers)} remaining", inline=True)
            embed.add_field(name="Border Strength", value=f"🛡️ {format_number(new_border_strength)}", inline=True)
            embed.add_field(name="Main Army", value=f"⚔️ +{format_number(soldiers_to_retrieve)} soldiers", inline=True)

            if new_border_strength < 50:
                embed.add_field(name="⚠️ Warning", value="Your border strength is low! Consider reinforcing it.", inline=False)
            await ctx.send(embed=embed)

        except Exception as e:
            logger.error(f"Error in retrieve command: {e}", exc_info=True)

    @commands.command(name='borderinfo')
    async def border_info(self, ctx):
        try:
            user_id = str(ctx.author.id)
            cooldown_seconds = config.COOLDOWNS.get("borderinfo", 1) * 60
            if not self._check_cooldown(user_id, 'borderinfo', cooldown_seconds):
                remaining = self._get_cooldown_remaining(user_id, 'borderinfo')
                mins = remaining // 60
                secs = remaining % 60
                await ctx.send(f"⏳ Please wait {mins}m {secs}s before checking border info again!")
                return

            if not await self.check_civil_war_and_proceed(ctx, user_id):
                return
            civ = self.civ_manager.get_civilization(user_id)
            if not civ:
                await ctx.send("❌ You need to start a civilization first! Use `.start`")
                return

            border_info = self._get_border_info(user_id)
            if not border_info.get("has_border", False):
                embed = create_embed("🛡️ Border Status", "You don't have a defensive border yet!", guilded.Color.blue())
                embed.add_field(name="How to Build", value="Use `.addborder` to build a defensive border (costs resources).", inline=False)
            else:
                border_strength = border_info.get("border_strength", 0)
                border_soldiers = border_info.get("border_soldiers", 0)
                embed = create_embed("🛡️ Border Status", f"**{civ['name']}**'s defensive border", guilded.Color.green())
                embed.add_field(name="Border Strength", value=f"🛡️ {format_number(border_strength)}", inline=True)
                embed.add_field(name="Border Soldiers", value=f"⚔️ {format_number(border_soldiers)}", inline=True)
                embed.add_field(name="Main Army", value=f"⚔️ {format_number(civ['military']['soldiers'])}", inline=True)
                defense_bonus = min(50, border_strength // 10)
                embed.add_field(name="Defense Bonus", value=f"🛡️ +{defense_bonus}% in defensive battles", inline=False)
                embed.add_field(name="Management", value="Use `.rectract <percentage>` to assign soldiers\nUse `.retrieve <percentage>` to retrieve soldiers", inline=False)

            await ctx.send(embed=embed)

        except Exception as e:
            logger.error(f"Error in borderinfo command: {e}", exc_info=True)

    # ---- NAVY, AIRFORCE, TECH, TRAINING (no cooldowns) ----
    @commands.command(name='buildship')
    @app_commands.describe(ship_type="Type of ship to build", amount="How many ships to build")
    @app_commands.choices(ship_type=[
        app_commands.Choice(name="frigate", value="frigate"),
        app_commands.Choice(name="destroyer", value="destroyer"),
        app_commands.Choice(name="battleship", value="battleship"),
        app_commands.Choice(name="aircraft_carrier", value="aircraft_carrier"),
        app_commands.Choice(name="submarine", value="submarine"),
    ])
    async def build_ship(self, ctx, ship_type: Optional[Literal["frigate", "destroyer", "battleship", "aircraft_carrier", "submarine"]] = None, amount: int = 1):
        try:
            user_id = str(ctx.author.id)

            if not ship_type:
                embed = create_embed("🚢 Build Ships", "Build navy ships to strengthen your fleet!", guilded.Color.blue())
                ship_list = []
                for key, data in SHIP_TYPES.items():
                    ship_list.append(f"**{data['name']}** (`{key}`) – 🪙{data['cost_gold']} 🪵{data['cost_wood']} 🪨{data['cost_stone']}\n{data['description']}")
                embed.add_field(name="Available Ships", value="\n\n".join(ship_list), inline=False)
                embed.add_field(name="Usage", value="`.buildship <type> <amount>` (e.g., `.buildship frigate 5`)", inline=False)
                await ctx.send(embed=embed)
                return

            if not await self.check_civil_war_and_proceed(ctx, user_id):
                return
            civ = self.civ_manager.get_civilization(user_id)
            if not civ:
                await ctx.send("❌ You need to start a civilization first! Use `.start`")
                return

            ship_type = ship_type.lower()
            if ship_type not in SHIP_TYPES:
                await ctx.send(f"❌ Invalid ship type! Choose from: {', '.join(SHIP_TYPES.keys())}")
                return

            if amount < 1:
                await ctx.send("❌ Amount must be at least 1!")
                return

            ship_data = SHIP_TYPES[ship_type]
            total_cost = {
                "gold": ship_data["cost_gold"] * amount,
                "wood": ship_data["cost_wood"] * amount,
                "stone": ship_data["cost_stone"] * amount
            }

            if not self.civ_manager.can_afford(user_id, total_cost):
                await ctx.send(f"❌ Not enough resources! Need {format_number(total_cost['gold'])} gold, {format_number(total_cost['wood'])} wood, and {format_number(total_cost['stone'])} stone.")
                return

            self.civ_manager.spend_resources(user_id, total_cost)
            self._update_navy(user_id, {ship_type: amount})

            embed = create_embed("🚢 Ship Build Complete!", f"Built **{amount} {ship_data['name']}(s)** for **{civ['name']}**!", guilded.Color.green())
            embed.add_field(name="Cost", value=f"🪙 {format_number(total_cost['gold'])} Gold\n🪵 {format_number(total_cost['wood'])} Wood\n🪨 {format_number(total_cost['stone'])} Stone", inline=True)
            embed.add_field(name="Total Navy", value="Check with `.navy`", inline=True)
            await ctx.send(embed=embed)

        except Exception as e:
            logger.error(f"Error in buildship command: {e}", exc_info=True)

    @commands.command(name='buildplane')
    @app_commands.describe(plane_type="Type of plane to build", amount="How many planes to build")
    @app_commands.choices(plane_type=[
        app_commands.Choice(name="fighter", value="fighter"),
        app_commands.Choice(name="attacker", value="attacker"),
        app_commands.Choice(name="bomber", value="bomber"),
    ])
    async def build_plane(self, ctx, plane_type: Optional[Literal["fighter", "attacker", "bomber"]] = None, amount: int = 1):
        try:
            user_id = str(ctx.author.id)

            if not plane_type:
                embed = create_embed("✈️ Build Planes", "Build airforce planes to dominate the skies!", guilded.Color.blue())
                plane_list = []
                for key, data in PLANE_TYPES.items():
                    reqs = ""
                    if key in ["attacker", "bomber"]:
                        reqs = " (Requires Industrial Revolution + Air Tech 3)"
                    plane_list.append(f"**{data['name']}** (`{key}`) – 🪙{data['cost_gold']} 🪵{data['cost_wood']} 🪨{data['cost_stone']}\nRange: {data['range']} subregions{reqs}\n{data['description']}")
                embed.add_field(name="Available Planes", value="\n\n".join(plane_list), inline=False)
                embed.add_field(name="Usage", value="`.buildplane <type> <amount>` (e.g., `.buildplane fighter 3`)", inline=False)
                await ctx.send(embed=embed)
                return

            if not await self.check_civil_war_and_proceed(ctx, user_id):
                return
            civ = self.civ_manager.get_civilization(user_id)
            if not civ:
                await ctx.send("❌ You need to start a civilization first! Use `.start`")
                return

            plane_type = plane_type.lower()
            if plane_type not in PLANE_TYPES:
                await ctx.send(f"❌ Invalid plane type! Choose from: {', '.join(PLANE_TYPES.keys())}")
                return

            if plane_type in ["attacker", "bomber"]:
                if not self._has_completed_industrial(user_id):
                    await ctx.send("❌ You must complete the Industrial Revolution before building attackers or bombers!")
                    return
                tech = self._get_military_tech(user_id)
                if tech.get("air_tech", 1) < 3:
                    await ctx.send("❌ You need Air Tech level 3 or higher to build attackers or bombers! Use `.tech air` to upgrade.")
                    return

            if amount < 1:
                await ctx.send("❌ Amount must be at least 1!")
                return

            plane_data = PLANE_TYPES[plane_type]
            total_cost = {
                "gold": plane_data["cost_gold"] * amount,
                "wood": plane_data["cost_wood"] * amount,
                "stone": plane_data["cost_stone"] * amount
            }

            if not self.civ_manager.can_afford(user_id, total_cost):
                await ctx.send(f"❌ Not enough resources! Need {format_number(total_cost['gold'])} gold, {format_number(total_cost['wood'])} wood, and {format_number(total_cost['stone'])} stone.")
                return

            self.civ_manager.spend_resources(user_id, total_cost)
            self._update_airforce(user_id, {plane_type: amount})

            embed = create_embed("✈️ Plane Build Complete!", f"Built **{amount} {plane_data['name']}(s)** for **{civ['name']}**!", guilded.Color.green())
            embed.add_field(name="Cost", value=f"🪙 {format_number(total_cost['gold'])} Gold\n🪵 {format_number(total_cost['wood'])} Wood\n🪨 {format_number(total_cost['stone'])} Stone", inline=True)
            embed.add_field(name="Total Airforce", value="Check with `.airforce`", inline=True)
            await ctx.send(embed=embed)

        except Exception as e:
            logger.error(f"Error in buildplane command: {e}", exc_info=True)

    @commands.command(name='tech')
    @app_commands.describe(branch="Tech branch to upgrade", amount="How many levels to upgrade (default 1)")
    @app_commands.choices(branch=[
        app_commands.Choice(name="ground", value="ground"),
        app_commands.Choice(name="naval", value="naval"),
        app_commands.Choice(name="air", value="air"),
    ])
    async def upgrade_tech(self, ctx, branch: Optional[Literal["ground", "naval", "air"]] = None, amount: int = 1):
        try:
            if not branch:
                embed = create_embed(
                    "🔬 Military Tech Upgrade",
                    f"Upgrade your military technology for {config.MILITARY['tech_upgrade_cost']} gold per level.",
                    guilded.Color.blue()
                )
                embed.add_field(name="Branches", value="`ground` – improves soldiers and ground combat\n`naval` – improves ship strength\n`air` – improves plane strength", inline=False)
                embed.add_field(name="Usage", value="`.tech <branch> [amount]` (e.g., `.tech ground 2`)", inline=False)
                embed.add_field(name="Current Tech", value="Check with `.tech status`", inline=False)
                await ctx.send(embed=embed)
                return

            user_id = str(ctx.author.id)
            civ = self.civ_manager.get_civilization(user_id)
            if not civ:
                await ctx.send("❌ You need to start a civilization first! Use `.start`")
                return

            if branch == "status":
                tech = self._get_military_tech(user_id)
                embed = create_embed("🔬 Current Military Tech", f"**{civ['name']}**'s technology levels:", guilded.Color.blue())
                embed.add_field(name="Ground Tech", value=f"Level {tech.get('ground_tech', 1)}", inline=True)
                embed.add_field(name="Naval Tech", value=f"Level {tech.get('naval_tech', 1)}", inline=True)
                embed.add_field(name="Air Tech", value=f"Level {tech.get('air_tech', 1)}", inline=True)
                await ctx.send(embed=embed)
                return

            if branch not in ["ground", "naval", "air"]:
                await ctx.send("❌ Invalid branch! Choose from: `ground`, `naval`, `air`.")
                return

            if amount < 1:
                await ctx.send("❌ Amount must be at least 1!")
                return

            tech = self._get_military_tech(user_id)
            current_level = tech.get(f"{branch}_tech", 1)
            if current_level + amount > 10:
                await ctx.send(f"❌ Tech level cannot exceed 10! Current level: {current_level}")
                return

            cost = config.MILITARY['tech_upgrade_cost'] * amount
            if not self.civ_manager.can_afford(user_id, {"gold": cost}):
                await ctx.send(f"❌ Not enough gold! Need {format_number(cost)} gold for {amount} level(s).")
                return

            self.civ_manager.spend_resources(user_id, {"gold": cost})
            self._update_military_tech(user_id, {f"{branch}_tech": amount})

            new_level = current_level + amount
            embed = create_embed("🔬 Tech Upgrade Complete!", f"**{branch.capitalize()} Tech** increased from **{current_level}** to **{new_level}**!", guilded.Color.green())
            embed.add_field(name="Cost", value=f"🪙 {format_number(cost)} Gold", inline=True)
            await ctx.send(embed=embed)

        except Exception as e:
            logger.error(f"Error in tech command: {e}", exc_info=True)

    @commands.command(name='trainboost')
    @app_commands.describe(amount="How many training levels to increase (default 1)")
    async def train_boost(self, ctx, amount: int = 1):
        try:
            user_id = str(ctx.author.id)
            civ = self.civ_manager.get_civilization(user_id)
            if not civ:
                await ctx.send("❌ You need to start a civilization first! Use `.start`")
                return

            training = self._get_training(user_id)
            current_level = training.get("level", 0)
            if current_level >= 3:
                await ctx.send("❌ Training level is already at maximum (level 3)!")
                return

            if amount < 1:
                await ctx.send("❌ Amount must be at least 1!")
                return

            new_level = min(current_level + amount, 3)
            actual_increase = new_level - current_level
            if actual_increase == 0:
                await ctx.send("❌ Already at max level!")
                return

            cost = config.MILITARY['tech_upgrade_cost'] * actual_increase
            if not self.civ_manager.can_afford(user_id, {"gold": cost}):
                await ctx.send(f"❌ Not enough gold! Need {format_number(cost)} gold to increase training by {actual_increase} level(s).")
                return

            self.civ_manager.spend_resources(user_id, {"gold": cost})
            self._update_training(user_id, {"level": actual_increase})

            embed = create_embed(
                "⚔️ Training Level Up!",
                f"Training level increased from **{current_level}** to **{new_level}**!\n"
                f"Multiplier: {TRAINING_LEVELS[current_level]}x → {TRAINING_LEVELS[new_level]}x (up to {MAX_BOOSTED_SOLDIERS} soldiers)",
                guilded.Color.gold()
            )
            embed.add_field(name="Cost", value=f"🪙 {format_number(cost)} Gold", inline=True)
            await ctx.send(embed=embed)

        except Exception as e:
            logger.error(f"Error in trainboost command: {e}", exc_info=True)

    @commands.command(name='navy')
    async def show_navy(self, ctx):
        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need to start a civilization first! Use `.start`")
            return

        navy = self._get_navy(user_id)
        tech = self._get_military_tech(user_id)
        naval_tech = tech.get("naval_tech", 1)

        embed = create_embed("🚢 Navy Fleet", f"**{civ['name']}**'s naval forces (Naval Tech: {naval_tech})", guilded.Color.blue())
        total_ships = 0
        total_strength = 0
        for ship_type, count in navy.items():
            if count > 0:
                ship_data = SHIP_TYPES.get(ship_type)
                strength = count * ship_data["strength"] * naval_tech
                total_ships += count
                total_strength += strength
                embed.add_field(
                    name=ship_data["name"],
                    value=f"Count: {count}\nStrength: {format_number(strength)}",
                    inline=True
                )
        if total_ships == 0:
            embed.description += "\n\n**No ships built yet!** Use `.buildship` to start your navy."
        else:
            embed.add_field(name="Total Ships", value=format_number(total_ships), inline=True)
            embed.add_field(name="Total Naval Strength", value=format_number(total_strength), inline=True)

        await ctx.send(embed=embed)

    @commands.command(name='airforce')
    async def show_airforce(self, ctx):
        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need to start a civilization first! Use `.start`")
            return

        air = self._get_airforce(user_id)
        tech = self._get_military_tech(user_id)
        air_tech = tech.get("air_tech", 1)

        embed = create_embed("✈️ Airforce Fleet", f"**{civ['name']}**'s air forces (Air Tech: {air_tech})", guilded.Color.blue())
        total_planes = 0
        total_strength = 0
        for plane_type, count in air.items():
            if count > 0:
                plane_data = PLANE_TYPES.get(plane_type)
                strength = count * plane_data["strength"] * air_tech
                total_planes += count
                total_strength += strength
                embed.add_field(
                    name=plane_data["name"],
                    value=f"Count: {count}\nStrength: {format_number(strength)}\nRange: {plane_data['range']} subregion(s)",
                    inline=True
                )
        if total_planes == 0:
            embed.description += "\n\n**No planes built yet!** Use `.buildplane` to start your airforce."
        else:
            embed.add_field(name="Total Planes", value=format_number(total_planes), inline=True)
            embed.add_field(name="Total Air Strength", value=format_number(total_strength), inline=True)

        await ctx.send(embed=embed)

async def setup(bot):
    await bot.add_cog(MilitaryCommands(bot))
