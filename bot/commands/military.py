import random
import re
import logging
import math
from datetime import datetime, timedelta
from typing import Literal, Optional

import discord as guilded
from discord import app_commands
from discord.ext import commands

from bot.utils import format_number, create_embed

logger = logging.getLogger(__name__)

# Simple in-memory cooldown decorator
def cooldown(seconds=60):
    def decorator(func):
        cooldowns = {}
        
        async def wrapper(self, ctx, *args, **kwargs):
            user_id = str(ctx.author.id)
            now = datetime.utcnow()
            
            # Check if user is on cooldown
            if user_id in cooldowns:
                remaining = cooldowns[user_id] - now
                if remaining.total_seconds() > 0:
                    mins = int(remaining.total_seconds() // 60)
                    secs = int(remaining.total_seconds() % 60)
                    await ctx.send(f"⏳ Please wait {mins}m {secs}s before using this command again!")
                    return
            
            # Execute command
            result = await func(self, ctx, *args, **kwargs)
            
            # Set cooldown
            cooldowns[user_id] = now + timedelta(seconds=seconds)
            return result
            
        return wrapper
    return decorator

class MilitaryCommands(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.db = bot.db
        self.civ_manager = bot.civ_manager
        self.create_tables()
        
        # Track cooldowns for commands that need them
        self.cooldowns = {}

    def create_tables(self):
        """Create necessary database tables"""
        try:
            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS wars (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        attacker_id TEXT NOT NULL,
                        defender_id TEXT NOT NULL,
                        war_type TEXT NOT NULL,
                        result TEXT NOT NULL DEFAULT 'ongoing',
                        declared_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        ended_at TIMESTAMP
                    )
                ''')
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS peace_offers (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        offerer_id TEXT NOT NULL,
                        receiver_id TEXT NOT NULL,
                        status TEXT NOT NULL DEFAULT 'pending',
                        offered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        responded_at TIMESTAMP
                    )
                ''')
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS borders (
                        user_id TEXT PRIMARY KEY,
                        has_border BOOLEAN DEFAULT FALSE,
                        border_strength INTEGER DEFAULT 0,
                        border_soldiers INTEGER DEFAULT 0,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS unlocked_cards (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id TEXT NOT NULL,
                        card_name TEXT NOT NULL,
                        unlocked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        used BOOLEAN DEFAULT FALSE,
                        UNIQUE(user_id, card_name)
                    )
                ''')
                conn.commit()
        except Exception as e:
            logger.error(f"Error creating tables: {e}", exc_info=True)

    async def check_civil_war_and_proceed(self, ctx, user_id: str) -> bool:
        """Check for civil war risk and proceed if safe"""
        try:
            if self.civ_manager.check_civil_war_risk(user_id):
                # Civil war occurred - send message and stop command execution
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

    def _extract_user_id(self, input_str: str) -> str:
        """
        Extract user ID from a mention string like <@id> (or <@!id>) or return
        the input if it looks like an ID (alphanumeric, length >= 6).
        Returns None if extraction fails.
        """
        if not input_str:
            return None

        # If it's a mention like <@ac5egiu8e> or <@!ac5egiu8e>
        if input_str.startswith('<@') and input_str.endswith('>'):
            inner = input_str[2:-1]
            inner = inner.lstrip('!')
            inner = inner.strip()
            if inner:
                return inner

        # If it's a raw ID (alphanumeric)
        if input_str.isalnum() and len(input_str) >= 6:
            return input_str

        # Try to find an alphanumeric token inside the string (fallback)
        m = re.search(r'[A-Za-z0-9]{6,}', input_str)
        if m:
            return m.group(0)

        return None

    async def _get_member_from_mention(self, ctx, mention: str):
        """
        Robustly resolve a mention string (or usage where user typed/displayed name)
        to a Member object.
        """
        if mention is None:
            return None

        # If the caller already passed a Member object by accident
        if hasattr(mention, "id") and hasattr(mention, "display_name"):
            return mention

        # 1) Use ctx.mentions if present (this is the most reliable)
        try:
            mentions = getattr(ctx, "mentions", None)
            if mentions:
                # If mention string contains an ID, try to match that exact mention in ctx.mentions
                user_id = self._extract_user_id(mention)
                if user_id:
                    for m in mentions:
                        if str(getattr(m, "id", "")).lower() == user_id.lower():
                            return m
                # Otherwise return the first mentioned member
                return mentions[0]
        except Exception:
            pass

        # 2) Try Guilded's MemberConverter (handles many common formats)
        try:
            converter = commands.MemberConverter()
            member = await converter.convert(ctx, mention)
            if member:
                return member
        except Exception:
            pass

        # 3) Try extracting an ID and fetching by it
        user_id = self._extract_user_id(mention)
        if user_id:
            try:
                member = await ctx.guild.fetch_member(user_id)
                if member:
                    return member
            except Exception:
                pass

        # 4) Fallback: search guild members by name/display_name (case-insensitive)
        try:
            guild_members = getattr(ctx.guild, "members", None)
            if guild_members:
                lowered = mention.lower()
                for m in guild_members:
                    try:
                        if getattr(m, "name", "").lower() == lowered or getattr(m, "display_name", "").lower() == lowered:
                            return m
                    except Exception:
                        continue
        except Exception:
            pass

        return None

    def _check_cooldown(self, user_id: str, command: str, seconds: int) -> bool:
        """Check if user is on cooldown for a command"""
        key = f"{user_id}_{command}"
        now = datetime.utcnow()
        
        if key in self.cooldowns:
            if now < self.cooldowns[key]:
                return False
                
        # Set new cooldown
        self.cooldowns[key] = now + timedelta(seconds=seconds)
        return True

    def _get_cooldown_remaining(self, user_id: str, command: str) -> int:
        """Get remaining cooldown seconds"""
        key = f"{user_id}_{command}"
        now = datetime.utcnow()
        
        if key in self.cooldowns and now < self.cooldowns[key]:
            remaining = self.cooldowns[key] - now
            return int(remaining.total_seconds())
        return 0

    def _get_territory_cog(self):
        """Get the TerritoryCog instance."""
        return self.bot.get_cog("TerritoryCog")

    def _do_attackers_border_defender(self, attacker_id: str, defender_id: str) -> bool:
        """
        Check if the attacker shares a border with the defender.
        Returns True if any province owned by attacker is in a subregion
        that borders any subregion owned by defender.
        """
        territory_cog = self._get_territory_cog()
        if not territory_cog:
            return False
        
        # Get provinces owned by each player
        attacker_provinces = territory_cog._get_owned_provinces(attacker_id)
        defender_provinces = territory_cog._get_owned_provinces(defender_id)
        
        if not attacker_provinces or not defender_provinces:
            return False
        
        # Get subregions for each player
        from bot.commands.territory import PROVINCE_TO_SUBREGION, SUBREGION_DATA
        
        attacker_subregions = set()
        for province in attacker_provinces:
            sub = PROVINCE_TO_SUBREGION.get(province)
            if sub:
                attacker_subregions.add(sub)
        
        defender_subregions = set()
        for province in defender_provinces:
            sub = PROVINCE_TO_SUBREGION.get(province)
            if sub:
                defender_subregions.add(sub)
        
        # Check if any attacker subregion borders any defender subregion
        for att_sub in attacker_subregions:
            neighbours = SUBREGION_DATA.get(att_sub, {}).get("neighbours", [])
            for def_sub in defender_subregions:
                if def_sub in neighbours:
                    return True
        
        return False

    # ---------- COMMANDS ----------

    @commands.command(name='train')
    @app_commands.describe(unit_type="Type of unit to train", amount="How many units to train")
    @app_commands.choices(unit_type=[
        app_commands.Choice(name="soldiers", value="soldiers"),
        app_commands.Choice(name="spies", value="spies"),
    ])
    async def train_soldiers(self, ctx, unit_type: Optional[Literal["soldiers", "spies"]] = None, amount: int = None):
        """Train military units (2min cooldown)"""
        try:
            user_id = str(ctx.author.id)
            
            if not self._check_cooldown(user_id, 'train', 120):
                remaining = self._get_cooldown_remaining(user_id, 'train')
                mins = remaining // 60
                secs = remaining % 60
                await ctx.send(f"⏳ Please wait {mins}m {secs}s before training again!")
                return
            
            if not unit_type:
                embed = create_embed(
                    "⚔️ Military Training",
                    "Train units to strengthen your army!",
                    guilded.Color.blue()
                )
                embed.add_field(name="Available Units", value="`soldiers` - Basic infantry (50 gold, 10 food each)\n`spies` - Intelligence operatives (100 gold, 5 food each)", inline=False)
                embed.add_field(name="Usage", value="`/train <unit_type> <amount>` (or `.train`)", inline=False)
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
                gold_cost = amount * 50
                food_cost = amount * 10
            else:
                gold_cost = amount * 100
                food_cost = amount * 5

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

            embed = create_embed(
                f"⚔️ Training Complete",
                f"Successfully trained {format_number(amount)} {unit_type}!",
                guilded.Color.green()
            )
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
        """Declare war on another civilization (No cooldown)"""
        try:
            if not target:
                await ctx.send("⚔️ **Declaration of War**\nUsage: `/declare <user>`\nNote: War must be declared before attacking!")
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

            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT id FROM wars 
                    WHERE ((attacker_id = ? AND defender_id = ?) OR (attacker_id = ? AND defender_id = ?))
                    AND result = 'ongoing'
                ''', (user_id, target_id, target_id, user_id))

                if cursor.fetchone():
                    await ctx.send("❌ You're already at war with this civilization!")
                    return

                # ---- FIX: Explicitly set result to 'ongoing' ----
                cursor.execute('''
                    INSERT INTO wars (attacker_id, defender_id, war_type, declared_at, result)
                    VALUES (?, ?, ?, ?, ?)
                ''', (user_id, target_id, 'declared', datetime.utcnow(), 'ongoing'))
                conn.commit()

            self.db.log_event(user_id, "war_declaration", "War Declared",
                              f"{civ['name']} has declared war on {target_civ['name']}!")

            embed = create_embed(
                "⚔️ War Declared!",
                f"**{civ['name']}** has officially declared war on **{target_civ['name']}**!",
                guilded.Color.red()
            )
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
        """Launch a direct attack with intensity level 1-10 (3min cooldown)"""
        try:
            user_id = str(ctx.author.id)
            
            if not self._check_cooldown(user_id, 'attack', 180):
                remaining = self._get_cooldown_remaining(user_id, 'attack')
                mins = remaining // 60
                secs = remaining % 60
                await ctx.send(f"⏳ Please wait {mins}m {secs}s before attacking again!")
                return
            
            if not target:
                await ctx.send("⚔️ **Direct Attack**\nUsage: `.attack <user> <level>`\nLevel: 1-10 (higher = more damage, higher cost)\nNote: War must be declared first!")
                return
            
            # Validate level
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

            # ---- Debug: Check if war exists ----
            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT id, result FROM wars 
                    WHERE ((attacker_id = ? AND defender_id = ?) OR (attacker_id = ? AND defender_id = ?))
                    AND result = 'ongoing'
                ''', (user_id, target_id, target_id, user_id))

                war = cursor.fetchone()
                logger.info(f"War check: user_id={user_id}, target_id={target_id}, war found: {war}")

                if not war:
                    # Check if there is any war at all (maybe ended or not ongoing)
                    cursor.execute('''
                        SELECT id, result FROM wars 
                        WHERE ((attacker_id = ? AND defender_id = ?) OR (attacker_id = ? AND defender_id = ?))
                    ''', (user_id, target_id, target_id, user_id))
                    any_war = cursor.fetchone()
                    if any_war:
                        await ctx.send(f"❌ You have a war with this civilization, but it is not 'ongoing' (status: {any_war['result']}). Use `.declare` again if needed.")
                    else:
                        await ctx.send("❌ You must declare war first! Use `.declare @user`")
                    return

            # ---- BORDER CHECK FOR DEFENSIVE MODIFIER ----
            shares_border = self._do_attackers_border_defender(user_id, target_id)
            
            # Calculate battle strength
            attacker_strength = self._calculate_military_strength(civ)
            defender_strength = self._calculate_military_strength(target_civ)

            # Apply attack level multiplier to attacker (level 1 = 0.5x, level 5 = 1x, level 10 = 2x)
            level_multiplier = 0.5 + (level - 1) * (1.5 / 9)  # 0.5 -> 2.0
            attacker_strength *= level_multiplier

            # Apply random factors
            attacker_roll = random.uniform(0.8, 1.2)
            defender_roll = random.uniform(0.8, 1.2)

            # ---- DEFENSIVE MODIFIER FOR NON-BORDERING ATTACKERS ----
            if not shares_border:
                defender_roll *= 1.5  # +50% defense bonus
                await ctx.send("🛡️ **DEFENSIVE ADVANTAGE!** The defender does not share a border with you, making invasion much harder! (+50% defense)")

            # Ideology modifiers
            if civ.get('ideology') == 'fascism':
                attacker_roll *= 1.1
            if target_civ.get('ideology') == 'fascism':
                defender_roll *= 1.1

            if civ.get('ideology') == 'destruction':
                attacker_roll *= 1.15
                defender_roll *= 0.9
            if target_civ.get('ideology') == 'pacifist':
                defender_roll *= 0.85

            # Underdog victory system
            strength_ratio = defender_strength / max(1, attacker_strength)
            if strength_ratio < 0.5:
                underdog_bonus = (0.5 - strength_ratio) * 0.8
                defender_roll *= (1 + underdog_bonus)
                if strength_ratio < 0.25 and random.random() < 0.15:
                    defender_roll *= 1.5
                    await ctx.send("🎯 **UNDERDOG SPIRIT!** The defenders fight with incredible determination against overwhelming odds!")

            final_attacker_strength = attacker_strength * attacker_roll
            final_defender_strength = defender_strength * defender_roll

            # Level-based cost scaling
            soldier_cost_multiplier = 1 + (level - 1) * 0.1  # 1x at level 1, 1.9x at level 10
            gold_cost_multiplier = 1 + (level - 1) * 0.15   # 1x at level 1, 2.35x at level 10
            
            soldier_cost = int(10 * soldier_cost_multiplier)  # minimum 10 soldiers
            gold_cost = int(200 * gold_cost_multiplier)       # minimum 200 gold

            if civ['military']['soldiers'] < soldier_cost:
                await ctx.send(f"❌ You need at least {soldier_cost} soldiers for a level {level} attack!")
                return
            if civ['resources']['gold'] < gold_cost:
                await ctx.send(f"❌ You need at least {gold_cost} gold for a level {level} attack!")
                return

            # Spend costs
            self.civ_manager.update_military(user_id, {"soldiers": -soldier_cost})
            self.civ_manager.update_resources(user_id, {"gold": -gold_cost})

            if final_attacker_strength > final_defender_strength:
                victory_margin = final_attacker_strength / max(1, final_defender_strength)
                await self._process_attack_victory(ctx, user_id, target_id, civ, target_civ, victory_margin, level)
            else:
                defeat_margin = final_defender_strength / max(1, final_attacker_strength)
                await self._process_attack_defeat(ctx, user_id, target_id, civ, target_civ, defeat_margin, level)

        except Exception as e:
            logger.error(f"Error in attack command: {e}", exc_info=True)

    async def _process_attack_victory(self, ctx, attacker_id, defender_id, attacker_civ, defender_civ, margin, level):
        """Process successful attack with level scaling"""
        try:
            # Damage scales with level
            damage_multiplier = 0.5 + (level - 1) * (1.5 / 9)  # 0.5 -> 2.0
            
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

            embed = create_embed(
                "⚔️ Victory!",
                f"**{attacker_civ['name']}** has defeated **{defender_civ['name']}** in battle! (Level {level} attack)",
                guilded.Color.green()
            )
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
        """Process failed attack with level scaling"""
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

            embed = create_embed(
                "⚔️ Defeat!",
                f"**{attacker_civ['name']}** was defeated by **{defender_civ['name']}**! (Level {level} attack)",
                guilded.Color.red()
            )
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

    # ---- The rest of the commands (stealthbattle, siege, find, peace, etc.) are unchanged ----
    # I'll omit them here for brevity, but they are the same as in the original file.
    # The full file should include all methods.


    @commands.command(name='stealthbattle')
    @app_commands.describe(target="Civilization leader to target")
    async def stealth_battle(self, ctx, target: Optional[guilded.Member] = None):
        """Conduct a spy-based stealth attack (4min cooldown)"""
        try:
            user_id = str(ctx.author.id)
            
            if not self._check_cooldown(user_id, 'stealthbattle', 240):
                remaining = self._get_cooldown_remaining(user_id, 'stealthbattle')
                mins = remaining // 60
                secs = remaining % 60
                await ctx.send(f"⏳ Please wait {mins}m {secs}s before using stealth battle again!")
                return
            
            if not target:
                await ctx.send("🕵️ **Stealth Battle**\nUsage: `/stealthbattle <user>`\nUses spies instead of soldiers for covert operations.")
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

            attacker_spy_power = civ['military']['spies'] * civ['military']['tech_level']
            defender_spy_power = target_civ['military']['spies'] * target_civ['military']['tech_level']

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
                    damage = {
                        "stone": -random.randint(50, 200),
                        "wood": -random.randint(30, 150)
                    }
                    self.civ_manager.update_resources(target_id, damage)
                    result_text = "Your spies sabotaged enemy infrastructure!"
                    if civ.get('ideology') == 'destruction':
                        extra_damage = {
                            "gold": -random.randint(20, 100),
                            "food": -random.randint(30, 120)
                        }
                        self.civ_manager.update_resources(target_id, extra_damage)
                        result_text += f" Your destructive spies caused extra chaos!"
                elif operation_type == 'theft':
                    stolen = min(int(target_civ['resources']['gold'] * random.uniform(0.05, 0.15)), target_civ['resources']['gold'])
                    self.civ_manager.update_resources(target_id, {"gold": -stolen})
                    self.civ_manager.update_resources(user_id, {"gold": stolen})
                    result_text = f"Your spies stole {format_number(stolen)} gold!"
                else:
                    tech_gain = 1 if random.random() < 0.3 else 0
                    if tech_gain:
                        self.civ_manager.update_military(user_id, {"tech_level": tech_gain})
                    result_text = "Your spies gathered valuable intelligence!" + (f" (+{tech_gain} tech level)" if tech_gain else "")

                if spy_losses > 0:
                    self.civ_manager.update_military(user_id, {"spies": -spy_losses})

                embed = create_embed(
                    "🕵️ Stealth Operation Success!",
                    result_text,
                    guilded.Color.purple()
                )
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
                embed = create_embed(
                    "🕵️ Stealth Operation Failed!",
                    f"Your stealth mission was detected! Lost {spy_losses} spies.",
                    guilded.Color.red()
                )
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
        """Lay siege to an enemy civilization (10min cooldown)"""
        try:
            user_id = str(ctx.author.id)
            
            if not self._check_cooldown(user_id, 'siege', 600):
                remaining = self._get_cooldown_remaining(user_id, 'siege')
                mins = remaining // 60
                secs = remaining % 60
                await ctx.send(f"⏳ Please wait {mins}m {secs}s before sieging again!")
                return
            
            if not target:
                await ctx.send("🏰 **Siege Warfare**\nUsage: `/siege <user>`\nDrains enemy resources over time but requires large army.")
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

            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT id FROM wars 
                    WHERE ((attacker_id = ? AND defender_id = ?) OR (attacker_id = ? AND defender_id = ?))
                    AND result = 'ongoing'
                ''', (user_id, target_id, target_id, user_id))
                war = cursor.fetchone()
                if not war:
                    await ctx.send("❌ You must declare war first! Use `.declare @user`")
                    return

            siege_power = civ['military']['soldiers'] + civ['military']['tech_level'] * 10
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

            maintenance_cost = {
                "gold": civ['military']['soldiers'] * 2,
                "food": civ['military']['soldiers'] * 3
            }

            if not self.civ_manager.can_afford(user_id, maintenance_cost):
                await ctx.send("❌ You cannot afford to maintain the siege! Need more gold and food.")
                return

            self.civ_manager.spend_resources(user_id, maintenance_cost)
            negative_drain = {res: -amt for res, amt in resource_drain.items()}
            self.civ_manager.update_resources(target_id, negative_drain)
            self.civ_manager.update_population(target_id, {"happiness": -15})
            self.civ_manager.update_population(user_id, {"happiness": -5})

            embed = create_embed(
                "🏰 Siege in Progress",
                f"**{civ['name']}** has laid siege to **{target_civ['name']}**!",
                guilded.Color.orange()
            )
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
        """Search for wandering soldiers to recruit (1min cooldown)"""
        try:
            user_id = str(ctx.author.id)
            
            if not self._check_cooldown(user_id, 'find', 60):
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
                embed = create_embed(
                    "🔍 Soldiers Found!",
                    f"You've discovered {soldiers_found} wandering soldiers who have joined your army!" +
                    (f" (including {bonus} coerced by your destructive reputation)" if bonus else ""),
                    guilded.Color.green()
                )
                if civ.get('ideology') == 'pacifist':
                    embed.add_field(name="Pacifist Note", value="These soldiers joined reluctantly, drawn by your peaceful ideals.", inline=False)
            else:
                embed = create_embed(
                    "🔍 Search Unsuccessful",
                    "You couldn't find any willing soldiers to join your cause.",
                    guilded.Color.blue()
                )
                if civ.get('ideology') == 'destruction':
                    embed.add_field(name="Destruction Backfire", value="Your reputation scared away potential recruits.", inline=False)

            await ctx.send(embed=embed)

        except Exception as e:
            logger.error(f"Error in find command: {e}", exc_info=True)

    @commands.command(name='peace')
    @app_commands.describe(target="Civilization leader to offer peace to")
    async def make_peace(self, ctx, target: Optional[guilded.Member] = None):
        """Offer peace to an enemy civilization (No cooldown)"""
        try:
            if not target:
                await ctx.send("🕊️ **Peace Offering**\nUsage: `/peace <user>`\nSend a peace offer to end a war. They can accept with `/accept_peace <you>`.")
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

            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT id FROM wars 
                    WHERE ((attacker_id = ? AND defender_id = ?) OR (attacker_id = ? AND defender_id = ?))
                    AND result = 'ongoing'
                ''', (user_id, target_id, target_id, user_id))
                war = cursor.fetchone()
                if not war:
                    await ctx.send("❌ You're not at war with this civilization!")
                    return

                cursor.execute('''
                    SELECT COUNT(*) FROM peace_offers 
                    WHERE offerer_id = ? AND receiver_id = ? AND status = 'pending'
                ''', (user_id, target_id))
                if cursor.fetchone()[0] > 0:
                    await ctx.send("❌ You already have a pending peace offer to this civilization!")
                    return

                cursor.execute('''
                    INSERT INTO peace_offers (offerer_id, receiver_id)
                    VALUES (?, ?)
                ''', (user_id, target_id))
                conn.commit()

            embed = create_embed(
                "🕊️ Peace Offer Sent!",
                f"**{civ['name']}** has offered peace to **{target_civ['name']}**! They can accept with `.accept_peace @{ctx.author.display_name}`.",
                guilded.Color.green()
            )
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
        """Accept a peace offer from another civilization (No cooldown)"""
        try:
            if not target:
                await ctx.send("🕊️ **Accept Peace**\nUsage: `/accept_peace <user>`\nAccept a pending peace offer to end the war.")
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

            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT id FROM wars 
                    WHERE ((attacker_id = ? AND defender_id = ?) OR (attacker_id = ? AND defender_id = ?))
                    AND result = 'ongoing'
                ''', (user_id, offerer_id, offerer_id, user_id))
                war = cursor.fetchone()
                if not war:
                    await ctx.send("❌ You're not at war with this civilization!")
                    return

                cursor.execute('''
                    SELECT id FROM peace_offers 
                    WHERE offerer_id = ? AND receiver_id = ? AND status = 'pending'
                ''', (offerer_id, user_id))
                offer = cursor.fetchone()
                if not offer:
                    await ctx.send("❌ No pending peace offer from this civilization!")
                    return

                war_id = war[0]
                cursor.execute('''
                    UPDATE wars SET result = 'peace', ended_at = ?
                    WHERE id = ?
                ''', (datetime.utcnow(), war_id))
                cursor.execute('''
                    UPDATE peace_offers SET status = 'accepted', responded_at = ?
                    WHERE id = ?
                ''', (datetime.utcnow(), offer[0]))
                conn.commit()

            self.civ_manager.update_population(user_id, {"happiness": 15})
            self.civ_manager.update_population(offerer_id, {"happiness": 15})

            embed = create_embed(
                "🕊️ Peace Achieved!",
                f"**{civ['name']}** has accepted peace from **{offerer_civ['name']}**! The war is over.",
                guilded.Color.green()
            )
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

    @commands.command(name='cards')
    @app_commands.describe(
        action="Choose whether to view or use cards",
        card_name="Card name (required when action=use)",
        target="Optional target for cards that affect other players"
    )
    @app_commands.choices(
        action=[
            app_commands.Choice(name="view", value="view"),
            app_commands.Choice(name="use", value="use"),
        ],
        card_name=[
            app_commands.Choice(name="Gamble Card", value="Gamble Card"),
            app_commands.Choice(name="Resource Heist", value="Resource Heist"),
            app_commands.Choice(name="Military Coup", value="Military Coup"),
            app_commands.Choice(name="Territory Gambit", value="Territory Gambit"),
            app_commands.Choice(name="Population Swap", value="Population Swap"),
        ]
    )
    async def manage_cards(
        self,
        ctx,
        action: Optional[Literal["view", "use"]] = None,
        card_name: str = None,
        target: Optional[guilded.Member] = None
    ):
        """View or use your unlocked cards (No cooldown)"""
        try:
            user_id = str(ctx.author.id)
            
            if not await self.check_civil_war_and_proceed(ctx, user_id):
                return
                
            civ = self.civ_manager.get_civilization(user_id)

            if not civ:
                await ctx.send("❌ You need to start a civilization first! Use `.start`")
                return

            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                
                if action is None or action.lower() == 'view':
                    cursor.execute('''
                        SELECT card_name, used FROM unlocked_cards 
                        WHERE user_id = ? ORDER BY unlocked_at DESC
                    ''', (user_id,))
                    cards = cursor.fetchall()
                    
                    if not cards:
                        embed = create_embed(
                            "🎴 Your Cards",
                            "You haven't unlocked any cards yet! Cards are unlocked when you reach new tech levels.",
                            guilded.Color.blue()
                        )
                    else:
                        embed = create_embed(
                            "🎴 Your Cards",
                            f"You have {len(cards)} unlocked card(s):",
                            guilded.Color.blue()
                        )
                        for card_name, used in cards:
                            status = "✅ Used" if used else "🟢 Available"
                            embed.add_field(name=card_name, value=status, inline=True)
                        embed.add_field(
                            name="How to Use", 
                            value="Use `/cards use <card_name> [target]` to use a card. Example: `/cards use Resource Heist @username`", 
                            inline=False
                        )
                    await ctx.send(embed=embed)
                    return
                
                elif action.lower() == 'use':
                    if not card_name:
                        await ctx.send("❌ Please specify a card name! Usage: `/cards use <card_name> [target]`")
                        return
                    
                    cursor.execute('''
                        SELECT card_name, used FROM unlocked_cards 
                        WHERE user_id = ? AND card_name = ?
                    ''', (user_id, card_name))
                    card_data = cursor.fetchone()
                    
                    if not card_data:
                        await ctx.send(f"❌ You don't have the card '{card_name}' or it doesn't exist!")
                        return
                    
                    if card_data[1]:
                        await ctx.send("❌ You've already used this card!")
                        return
                    
                    result = await self._process_card_effect(ctx, user_id, card_name, target)
                    
                    if result:
                        cursor.execute('''
                            UPDATE unlocked_cards SET used = 1 
                            WHERE user_id = ? AND card_name = ?
                        ''', (user_id, card_name))
                        conn.commit()
                        self.db.log_event(user_id, "card_used", f"Card Used: {card_name}", result)
                        await ctx.send(result)
                    else:
                        await ctx.send("❌ Failed to process card effect!")
                    
                else:
                    await ctx.send("❌ Invalid action! Use `/cards view` or `/cards use <card_name> [target]`.")

        except Exception as e:
            logger.error(f"Error in cards command: {e}", exc_info=True)

    async def _process_card_effect(self, ctx, user_id, card_name, target_member: Optional[guilded.Member]):
        """Process card effects"""
        try:
            civ = self.civ_manager.get_civilization(user_id)
            
            if card_name == "Gamble Card":
                if not target_member:
                    return "❌ Gamble Card requires a target! Usage: `/cards use \"Gamble Card\" <target>`"
                
                target = target_member
                target_id = str(target.id)
                target_civ = self.civ_manager.get_civilization(target_id)
                
                if not target_civ:
                    return "❌ Target doesn't have a civilization!"
                
                population_loss = civ['population']['citizens'] // 2
                self.civ_manager.update_population(user_id, {"citizens": -population_loss})
                
                if random.random() < 0.001:
                    self.civ_manager.reset_civilization(target_id)
                    return f"🎰 **JACKPOT!** You sacrificed {format_number(population_loss)} people and **COMPLETELY DESTROYED {target_civ['name'].upper()}!** 🎰"
                else:
                    return f"💀 You sacrificed {format_number(population_loss)} people for nothing... The gamble failed."
                
            elif card_name == "Resource Heist":
                if not target_member:
                    return "❌ Resource Heist requires a target! Usage: `/cards use \"Resource Heist\" <target>`"
                
                target = target_member
                target_id = str(target.id)
                target_civ = self.civ_manager.get_civilization(target_id)
                
                if not target_civ:
                    return "❌ Target doesn't have a civilization!"
                
                if random.random() < 0.1:
                    stolen = {
                        "gold": max(1, civ['resources']['gold'] // 4),
                        "food": max(1, civ['resources']['food'] // 4),
                        "stone": max(1, civ['resources']['stone'] // 4),
                        "wood": max(1, civ['resources']['wood'] // 4)
                    }
                    self.civ_manager.update_resources(user_id, {k: -v for k, v in stolen.items()})
                    self.civ_manager.update_resources(target_id, stolen)
                    return f"😱 **HEIST BACKFIRED!** {target_civ['name']} stole your resources instead!"
                else:
                    stolen = {
                        "gold": max(1, target_civ['resources']['gold'] // 4),
                        "food": max(1, target_civ['resources']['food'] // 4),
                        "stone": max(1, target_civ['resources']['stone'] // 4),
                        "wood": max(1, target_civ['resources']['wood'] // 4)
                    }
                    self.civ_manager.update_resources(user_id, stolen)
                    self.civ_manager.update_resources(target_id, {k: -v for k, v in stolen.items()})
                    return f"💰 **Successful Heist!** Stole 25% of {target_civ['name']}'s resources!"
            
            elif card_name == "Military Coup":
                if random.random() < 0.5:
                    self.civ_manager.update_military(user_id, {
                        "soldiers": civ['military']['soldiers'],
                        "spies": civ['military']['spies']
                    })
                    return "🎖️ **Successful Coup!** Your military has doubled in size!"
                else:
                    self.civ_manager.update_military(user_id, {
                        "soldiers": -civ['military']['soldiers'],
                        "spies": -civ['military']['spies']
                    })
                    return "💥 **Coup Failed!** Your military has been disbanded!"
            
            elif card_name == "Territory Gambit":
                current_territory = civ['territory']['land_size']
                if random.random() < 0.3:
                    new_territory = current_territory * 3
                    self.civ_manager.update_territory(user_id, {"land_size": new_territory - current_territory})
                    return f"🎯 **Gambit Success!** Your territory tripled from {format_number(current_territory)} to {format_number(new_territory)} km²!"
                else:
                    lost_territory = max(1, current_territory // 2)
                    self.civ_manager.update_territory(user_id, {"land_size": -lost_territory})
                    return f"💸 **Gambit Failed!** You lost half your territory, now at {format_number(current_territory - lost_territory)} km²."
            
            elif card_name == "Population Swap":
                if not target_member:
                    return "❌ Population Swap requires a target! Usage: `/cards use \"Population Swap\" <target>`"
                
                target = target_member
                target_id = str(target.id)
                target_civ = self.civ_manager.get_civilization(target_id)
                
                if not target_civ:
                    return "❌ Target doesn't have a civilization!"
                
                your_pop = civ['population']['citizens']
                their_pop = target_civ['population']['citizens']
                
                self.civ_manager.update_population(user_id, {"citizens": their_pop - your_pop})
                self.civ_manager.update_population(target_id, {"citizens": your_pop - their_pop})
                
                return f"🔄 **Population Swap!** You now have {format_number(their_pop)} people, they have {format_number(your_pop)}!"
            
            else:
                return f"❌ Card '{card_name}' effect not implemented yet!"
                
        except Exception as e:
            logger.error(f"Error processing card effect: {e}", exc_info=True)
            return f"❌ Error processing card effect: {str(e)}"

    # ---------- BORDER COMMANDS ----------
    @commands.command(name='addborder')
    async def add_border(self, ctx):
        """Add a defensive border to your territory (5min cooldown)"""
        try:
            user_id = str(ctx.author.id)
            
            if not self._check_cooldown(user_id, 'addborder', 300):
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

            border_cost = {
                "gold": 1000,
                "stone": 500,
                "wood": 300
            }

            if not self.civ_manager.can_afford(user_id, border_cost):
                await ctx.send(f"❌ Not enough resources! Need {format_number(border_cost['gold'])} gold, {format_number(border_cost['stone'])} stone, and {format_number(border_cost['wood'])} wood.")
                return

            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT has_border FROM borders WHERE user_id = ?', (user_id,))
                existing_border = cursor.fetchone()

                if existing_border and existing_border[0]:
                    await ctx.send("❌ You already have a border! Use `.removeborder` to remove it first.")
                    return

                self.civ_manager.spend_resources(user_id, border_cost)
                cursor.execute('''
                    INSERT OR REPLACE INTO borders (user_id, has_border, border_strength, border_soldiers)
                    VALUES (?, TRUE, 100, 0)
                ''', (user_id,))
                conn.commit()

            embed = create_embed(
                "🛡️ Border Established!",
                f"**{civ['name']}** has built a defensive border around their territory!",
                guilded.Color.green()
            )
            embed.add_field(name="Border Strength", value="100/100", inline=True)
            embed.add_field(name="Cost", value=f"🪙 1,000 Gold\n🪨 500 Stone\n🪵 300 Wood", inline=True)
            embed.add_field(name="Next Steps", value="Use `.rectract <percentage>` to assign soldiers to your border for extra defense!", inline=False)

            await ctx.send(embed=embed)

        except Exception as e:
            logger.error(f"Error in addborder command: {e}", exc_info=True)

    @commands.command(name='removeborder')
    async def remove_border(self, ctx):
        """Remove your defensive border and retrieve all soldiers (2min cooldown)"""
        try:
            user_id = str(ctx.author.id)
            
            if not self._check_cooldown(user_id, 'removeborder', 120):
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

            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT border_soldiers FROM borders WHERE user_id = ? AND has_border = TRUE', (user_id,))
                border_data = cursor.fetchone()

                if not border_data:
                    await ctx.send("❌ You don't have a border to remove!")
                    return

                soldiers_to_return = border_data[0]
                if soldiers_to_return > 0:
                    self.civ_manager.update_military(user_id, {"soldiers": soldiers_to_return})

                cursor.execute('DELETE FROM borders WHERE user_id = ?', (user_id,))
                conn.commit()

            embed = create_embed(
                "🛡️ Border Removed!",
                f"**{civ['name']}** has dismantled their defensive border.",
                guilded.Color.blue()
            )
            if soldiers_to_return > 0:
                embed.add_field(name="Soldiers Returned", value=f"⚔️ {format_number(soldiers_to_return)} soldiers have returned to your main army.", inline=False)

            await ctx.send(embed=embed)

        except Exception as e:
            logger.error(f"Error in removeborder command: {e}", exc_info=True)

    @commands.command(name='rectract', aliases=['retract'])
    async def rectract_soldiers(self, ctx, percentage: int = None):
        """Assign a percentage of your soldiers to the border (1min cooldown)"""
        try:
            user_id = str(ctx.author.id)
            
            if not self._check_cooldown(user_id, 'rectract', 60):
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

            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT border_strength, border_soldiers FROM borders WHERE user_id = ? AND has_border = TRUE', (user_id,))
                border_data = cursor.fetchone()

                if not border_data:
                    await ctx.send("❌ You need to build a border first! Use `.addborder`")
                    return

            current_border_strength, current_border_soldiers = border_data
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
            
            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    UPDATE borders 
                    SET border_soldiers = ?, border_strength = ?, updated_at = ?
                    WHERE user_id = ?
                ''', (new_border_soldiers, current_border_strength + border_strength_increase, datetime.utcnow(), user_id))
                conn.commit()

            self.civ_manager.update_military(user_id, {"soldiers": -soldiers_to_assign})

            embed = create_embed(
                "🛡️ Soldiers Assigned to Border!",
                f"**{civ['name']}** has assigned {format_number(soldiers_to_assign)} soldiers to reinforce the border.",
                guilded.Color.green()
            )
            embed.add_field(name="Border Soldiers", value=f"⚔️ {format_number(new_border_soldiers)} total", inline=True)
            embed.add_field(name="Border Strength", value=f"🛡️ {format_number(current_border_strength + border_strength_increase)}", inline=True)
            embed.add_field(name="Main Army", value=f"⚔️ {format_number(available_soldiers - soldiers_to_assign)} soldiers remaining", inline=True)

            await ctx.send(embed=embed)

        except Exception as e:
            logger.error(f"Error in rectract command: {e}", exc_info=True)

    @commands.command(name='retrieve')
    async def retrieve_soldiers(self, ctx, percentage: int = None):
        """Retrieve a percentage of soldiers from the border (1min cooldown)"""
        try:
            user_id = str(ctx.author.id)
            
            if not self._check_cooldown(user_id, 'retrieve', 60):
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

            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT border_strength, border_soldiers FROM borders WHERE user_id = ? AND has_border = TRUE', (user_id,))
                border_data = cursor.fetchone()

                if not border_data:
                    await ctx.send("❌ You need to build a border first! Use `.addborder`")
                    return

            current_border_strength, current_border_soldiers = border_data
            
            if current_border_soldiers == 0:
                await ctx.send("❌ You don't have any soldiers assigned to your border!")
                return

            soldiers_to_retrieve = min((current_border_soldiers * percentage) // 100, current_border_soldiers)
            strength_loss = (current_border_strength * soldiers_to_retrieve) // current_border_soldiers
            new_border_strength = max(1, current_border_strength - strength_loss)
            new_border_soldiers = current_border_soldiers - soldiers_to_retrieve

            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    UPDATE borders 
                    SET border_soldiers = ?, border_strength = ?, updated_at = ?
                    WHERE user_id = ?
                ''', (new_border_soldiers, new_border_strength, datetime.utcnow(), user_id))
                conn.commit()

            self.civ_manager.update_military(user_id, {"soldiers": soldiers_to_retrieve})

            embed = create_embed(
                "🛡️ Soldiers Retrieved from Border!",
                f"**{civ['name']}** has retrieved {format_number(soldiers_to_retrieve)} soldiers from the border.",
                guilded.Color.blue()
            )
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
        """Check your border status (1min cooldown)"""
        try:
            user_id = str(ctx.author.id)
            
            if not self._check_cooldown(user_id, 'borderinfo', 60):
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

            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT border_strength, border_soldiers, created_at FROM borders WHERE user_id = ? AND has_border = TRUE', (user_id,))
                border_data = cursor.fetchone()

            if not border_data:
                embed = create_embed(
                    "🛡️ Border Status",
                    "You don't have a defensive border yet!",
                    guilded.Color.blue()
                )
                embed.add_field(name="How to Build", value="Use `.addborder` to build a defensive border (costs resources).", inline=False)
            else:
                border_strength, border_soldiers, created_at = border_data
                embed = create_embed(
                    "🛡️ Border Status",
                    f"**{civ['name']}**'s defensive border",
                    guilded.Color.green()
                )
                embed.add_field(name="Border Strength", value=f"🛡️ {format_number(border_strength)}", inline=True)
                embed.add_field(name="Border Soldiers", value=f"⚔️ {format_number(border_soldiers)}", inline=True)
                embed.add_field(name="Main Army", value=f"⚔️ {format_number(civ['military']['soldiers'])}", inline=True)
                defense_bonus = min(50, border_strength // 10)
                embed.add_field(name="Defense Bonus", value=f"🛡️ +{defense_bonus}% in defensive battles", inline=False)
                embed.add_field(name="Management", value="Use `.rectract <percentage>` to assign soldiers\nUse `.retrieve <percentage>` to retrieve soldiers", inline=False)

            await ctx.send(embed=embed)

        except Exception as e:
            logger.error(f"Error in borderinfo command: {e}", exc_info=True)

    # ---------- HELPER ----------
    def _calculate_military_strength(self, civ):
        """Calculate total military strength of a civilization"""
        try:
            soldiers = civ['military']['soldiers']
            spies = civ['military']['spies']
            tech_level = civ['military']['tech_level']
            bonuses = civ.get('bonuses', {})

            base_strength = soldiers * 10 + spies * 5
            tech_bonus = tech_level * 50
            territory_bonus = civ['territory']['land_size'] / 100
            defense_bonus = bonuses.get('defense_strength', 0) / 100

            return (base_strength + tech_bonus + territory_bonus) * (1 + defense_bonus)
        except KeyError as e:
            logger.error(f"Error calculating military strength - missing key {e}", exc_info=True)
            return 0
        except Exception as e:
            logger.error(f"Error calculating military strength: {e}", exc_info=True)
            return 0
    # ---------- COMMANDS ----------

    @commands.command(name='train')
    @app_commands.describe(unit_type="Type of unit to train", amount="How many units to train")
    @app_commands.choices(unit_type=[
        app_commands.Choice(name="soldiers", value="soldiers"),
        app_commands.Choice(name="spies", value="spies"),
    ])
    async def train_soldiers(self, ctx, unit_type: Optional[Literal["soldiers", "spies"]] = None, amount: int = None):
        """Train military units (2min cooldown)"""
        try:
            user_id = str(ctx.author.id)
            
            if not self._check_cooldown(user_id, 'train', 120):
                remaining = self._get_cooldown_remaining(user_id, 'train')
                mins = remaining // 60
                secs = remaining % 60
                await ctx.send(f"⏳ Please wait {mins}m {secs}s before training again!")
                return
            
            if not unit_type:
                embed = create_embed(
                    "⚔️ Military Training",
                    "Train units to strengthen your army!",
                    guilded.Color.blue()
                )
                embed.add_field(name="Available Units", value="`soldiers` - Basic infantry (50 gold, 10 food each)\n`spies` - Intelligence operatives (100 gold, 5 food each)", inline=False)
                embed.add_field(name="Usage", value="`/train <unit_type> <amount>` (or `.train`)", inline=False)
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
                gold_cost = amount * 50
                food_cost = amount * 10
            else:
                gold_cost = amount * 100
                food_cost = amount * 5

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

            embed = create_embed(
                f"⚔️ Training Complete",
                f"Successfully trained {format_number(amount)} {unit_type}!",
                guilded.Color.green()
            )
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
        """Declare war on another civilization (No cooldown)"""
        try:
            if not target:
                await ctx.send("⚔️ **Declaration of War**\nUsage: `/declare <user>`\nNote: War must be declared before attacking!")
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

            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT id FROM wars 
                    WHERE ((attacker_id = ? AND defender_id = ?) OR (attacker_id = ? AND defender_id = ?))
                    AND result = 'ongoing'
                ''', (user_id, target_id, target_id, user_id))

                if cursor.fetchone():
                    await ctx.send("❌ You're already at war with this civilization!")
                    return

                cursor.execute('''
                    INSERT INTO wars (attacker_id, defender_id, war_type, declared_at, result)
                    VALUES (?, ?, ?, ?, ?)
                ''', (user_id, target_id, 'declared', datetime.utcnow(), 'ongoing'))
                conn.commit()

            self.db.log_event(user_id, "war_declaration", "War Declared",
                              f"{civ['name']} has declared war on {target_civ['name']}!")

            embed = create_embed(
                "⚔️ War Declared!",
                f"**{civ['name']}** has officially declared war on **{target_civ['name']}**!",
                guilded.Color.red()
            )
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
        """Launch a direct attack with intensity level 1-10 (3min cooldown)"""
        try:
            user_id = str(ctx.author.id)
            
            if not self._check_cooldown(user_id, 'attack', 180):
                remaining = self._get_cooldown_remaining(user_id, 'attack')
                mins = remaining // 60
                secs = remaining % 60
                await ctx.send(f"⏳ Please wait {mins}m {secs}s before attacking again!")
                return
            
            if not target:
                await ctx.send("⚔️ **Direct Attack**\nUsage: `.attack <user> <level>`\nLevel: 1-10 (higher = more damage, higher cost)\nNote: War must be declared first!")
                return
            
            # Validate level
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

            # Check war
            if not self._check_war(user_id, target_id):
                await ctx.send("❌ You must declare war first! Use `.declare @user`")
                return

            # ---- BORDER CHECK FOR DEFENSIVE MODIFIER ----
            shares_border = self._do_attackers_border_defender(user_id, target_id)
            
            # Calculate military strength using the new comprehensive method
            attacker_strength = self._calculate_military_strength(civ)
            defender_strength = self._calculate_military_strength(target_civ)

            # Apply attack level multiplier to attacker (level 1 = 0.5x, level 5 = 1x, level 10 = 2x)
            level_multiplier = 0.5 + (level - 1) * (1.5 / 9)  # 0.5 -> 2.0
            attacker_strength *= level_multiplier

            # Apply random factors
            attacker_roll = random.uniform(0.8, 1.2)
            defender_roll = random.uniform(0.8, 1.2)

            # ---- DEFENSIVE MODIFIER FOR NON-BORDERING ATTACKERS ----
            if not shares_border:
                defender_roll *= 1.5  # +50% defense bonus
                await ctx.send("🛡️ **DEFENSIVE ADVANTAGE!** The defender does not share a border with you, making invasion much harder! (+50% defense)")

            # Ideology modifiers (keep as before)
            if civ.get('ideology') == 'fascism':
                attacker_roll *= 1.1
            if target_civ.get('ideology') == 'fascism':
                defender_roll *= 1.1

            if civ.get('ideology') == 'destruction':
                attacker_roll *= 1.15
                defender_roll *= 0.9
            if target_civ.get('ideology') == 'pacifist':
                defender_roll *= 0.85

            # Underdog victory system
            strength_ratio = defender_strength / max(1, attacker_strength)
            if strength_ratio < 0.5:
                underdog_bonus = (0.5 - strength_ratio) * 0.8
                defender_roll *= (1 + underdog_bonus)
                if strength_ratio < 0.25 and random.random() < 0.15:
                    defender_roll *= 1.5
                    await ctx.send("🎯 **UNDERDOG SPIRIT!** The defenders fight with incredible determination against overwhelming odds!")

            final_attacker_strength = attacker_strength * attacker_roll
            final_defender_strength = defender_strength * defender_roll

            # Level-based cost scaling (soldiers and gold)
            soldier_cost_multiplier = 1 + (level - 1) * 0.1  # 1x at level 1, 1.9x at level 10
            gold_cost_multiplier = 1 + (level - 1) * 0.15   # 1x at level 1, 2.35x at level 10
            
            soldier_cost = int(10 * soldier_cost_multiplier)  # minimum 10 soldiers
            gold_cost = int(200 * gold_cost_multiplier)       # minimum 200 gold

            if civ['military']['soldiers'] < soldier_cost:
                await ctx.send(f"❌ You need at least {soldier_cost} soldiers for a level {level} attack!")
                return
            if civ['resources']['gold'] < gold_cost:
                await ctx.send(f"❌ You need at least {gold_cost} gold for a level {level} attack!")
                return

            # Spend costs
            self.civ_manager.update_military(user_id, {"soldiers": -soldier_cost})
            self.civ_manager.update_resources(user_id, {"gold": -gold_cost})

            if final_attacker_strength > final_defender_strength:
                victory_margin = final_attacker_strength / max(1, final_defender_strength)
                await self._process_attack_victory(ctx, user_id, target_id, civ, target_civ, victory_margin, level)
            else:
                defeat_margin = final_defender_strength / max(1, final_attacker_strength)
                await self._process_attack_defeat(ctx, user_id, target_id, civ, target_civ, defeat_margin, level)

        except Exception as e:
            logger.error(f"Error in attack command: {e}", exc_info=True)

    async def _process_attack_victory(self, ctx, attacker_id, defender_id, attacker_civ, defender_civ, margin, level):
        """Process successful attack with level scaling"""
        try:
            damage_multiplier = 0.5 + (level - 1) * (1.5 / 9)  # 0.5 -> 2.0
            
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

            embed = create_embed(
                "⚔️ Victory!",
                f"**{attacker_civ['name']}** has defeated **{defender_civ['name']}** in battle! (Level {level} attack)",
                guilded.Color.green()
            )
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
        """Process failed attack with level scaling"""
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

            embed = create_embed(
                "⚔️ Defeat!",
                f"**{attacker_civ['name']}** was defeated by **{defender_civ['name']}**! (Level {level} attack)",
                guilded.Color.red()
            )
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
        """Conduct a spy-based stealth attack (4min cooldown)"""
        try:
            user_id = str(ctx.author.id)
            
            if not self._check_cooldown(user_id, 'stealthbattle', 240):
                remaining = self._get_cooldown_remaining(user_id, 'stealthbattle')
                mins = remaining // 60
                secs = remaining % 60
                await ctx.send(f"⏳ Please wait {mins}m {secs}s before using stealth battle again!")
                return
            
            if not target:
                await ctx.send("🕵️ **Stealth Battle**\nUsage: `/stealthbattle <user>`\nUses spies instead of soldiers for covert operations.")
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

            # Spy power now uses tech from military_tech
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
                    damage = {
                        "stone": -random.randint(50, 200),
                        "wood": -random.randint(30, 150)
                    }
                    self.civ_manager.update_resources(target_id, damage)
                    result_text = "Your spies sabotaged enemy infrastructure!"
                    if civ.get('ideology') == 'destruction':
                        extra_damage = {
                            "gold": -random.randint(20, 100),
                            "food": -random.randint(30, 120)
                        }
                        self.civ_manager.update_resources(target_id, extra_damage)
                        result_text += f" Your destructive spies caused extra chaos!"
                elif operation_type == 'theft':
                    stolen = min(int(target_civ['resources']['gold'] * random.uniform(0.05, 0.15)), target_civ['resources']['gold'])
                    self.civ_manager.update_resources(target_id, {"gold": -stolen})
                    self.civ_manager.update_resources(user_id, {"gold": stolen})
                    result_text = f"Your spies stole {format_number(stolen)} gold!"
                else:
                    # Intel: maybe steal tech level? 
                    tech_gain = 1 if random.random() < 0.3 else 0
                    if tech_gain:
                        self._update_military_tech(user_id, {"ground_tech": tech_gain})
                    result_text = "Your spies gathered valuable intelligence!" + (f" (+{tech_gain} ground tech level)" if tech_gain else "")

                if spy_losses > 0:
                    self.civ_manager.update_military(user_id, {"spies": -spy_losses})

                embed = create_embed(
                    "🕵️ Stealth Operation Success!",
                    result_text,
                    guilded.Color.purple()
                )
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
                embed = create_embed(
                    "🕵️ Stealth Operation Failed!",
                    f"Your stealth mission was detected! Lost {spy_losses} spies.",
                    guilded.Color.red()
                )
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
        """Lay siege to an enemy civilization (10min cooldown)"""
        try:
            user_id = str(ctx.author.id)
            
            if not self._check_cooldown(user_id, 'siege', 600):
                remaining = self._get_cooldown_remaining(user_id, 'siege')
                mins = remaining // 60
                secs = remaining % 60
                await ctx.send(f"⏳ Please wait {mins}m {secs}s before sieging again!")
                return
            
            if not target:
                await ctx.send("🏰 **Siege Warfare**\nUsage: `/siege <user>`\nDrains enemy resources over time but requires large army.")
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

            # Siege power now uses the new comprehensive strength (including navy/air) but we'll use a simplified version for siege: ground troops + tech
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

            maintenance_cost = {
                "gold": civ['military']['soldiers'] * 2,
                "food": civ['military']['soldiers'] * 3
            }

            if not self.civ_manager.can_afford(user_id, maintenance_cost):
                await ctx.send("❌ You cannot afford to maintain the siege! Need more gold and food.")
                return

            self.civ_manager.spend_resources(user_id, maintenance_cost)
            negative_drain = {res: -amt for res, amt in resource_drain.items()}
            self.civ_manager.update_resources(target_id, negative_drain)
            self.civ_manager.update_population(target_id, {"happiness": -15})
            self.civ_manager.update_population(user_id, {"happiness": -5})

            embed = create_embed(
                "🏰 Siege in Progress",
                f"**{civ['name']}** has laid siege to **{target_civ['name']}**!",
                guilded.Color.orange()
            )
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
        """Search for wandering soldiers to recruit (1min cooldown)"""
        try:
            user_id = str(ctx.author.id)
            
            if not self._check_cooldown(user_id, 'find', 60):
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
                embed = create_embed(
                    "🔍 Soldiers Found!",
                    f"You've discovered {soldiers_found} wandering soldiers who have joined your army!" +
                    (f" (including {bonus} coerced by your destructive reputation)" if bonus else ""),
                    guilded.Color.green()
                )
                if civ.get('ideology') == 'pacifist':
                    embed.add_field(name="Pacifist Note", value="These soldiers joined reluctantly, drawn by your peaceful ideals.", inline=False)
            else:
                embed = create_embed(
                    "🔍 Search Unsuccessful",
                    "You couldn't find any willing soldiers to join your cause.",
                    guilded.Color.blue()
                )
                if civ.get('ideology') == 'destruction':
                    embed.add_field(name="Destruction Backfire", value="Your reputation scared away potential recruits.", inline=False)

            await ctx.send(embed=embed)

        except Exception as e:
            logger.error(f"Error in find command: {e}", exc_info=True)

    @commands.command(name='peace')
    @app_commands.describe(target="Civilization leader to offer peace to")
    async def make_peace(self, ctx, target: Optional[guilded.Member] = None):
        """Offer peace to an enemy civilization (No cooldown)"""
        try:
            if not target:
                await ctx.send("🕊️ **Peace Offering**\nUsage: `/peace <user>`\nSend a peace offer to end a war. They can accept with `/accept_peace <you>`.")
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

            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT id FROM wars 
                    WHERE ((attacker_id = ? AND defender_id = ?) OR (attacker_id = ? AND defender_id = ?))
                    AND result = 'ongoing'
                ''', (user_id, target_id, target_id, user_id))
                war = cursor.fetchone()
                if not war:
                    await ctx.send("❌ You're not at war with this civilization!")
                    return

                cursor.execute('''
                    SELECT COUNT(*) FROM peace_offers 
                    WHERE offerer_id = ? AND receiver_id = ? AND status = 'pending'
                ''', (user_id, target_id))
                if cursor.fetchone()[0] > 0:
                    await ctx.send("❌ You already have a pending peace offer to this civilization!")
                    return

                cursor.execute('''
                    INSERT INTO peace_offers (offerer_id, receiver_id)
                    VALUES (?, ?)
                ''', (user_id, target_id))
                conn.commit()

            embed = create_embed(
                "🕊️ Peace Offer Sent!",
                f"**{civ['name']}** has offered peace to **{target_civ['name']}**! They can accept with `.accept_peace @{ctx.author.display_name}`.",
                guilded.Color.green()
            )
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
        """Accept a peace offer from another civilization (No cooldown)"""
        try:
            if not target:
                await ctx.send("🕊️ **Accept Peace**\nUsage: `/accept_peace <user>`\nAccept a pending peace offer to end the war.")
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

            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT id FROM wars 
                    WHERE ((attacker_id = ? AND defender_id = ?) OR (attacker_id = ? AND defender_id = ?))
                    AND result = 'ongoing'
                ''', (user_id, offerer_id, offerer_id, user_id))
                war = cursor.fetchone()
                if not war:
                    await ctx.send("❌ You're not at war with this civilization!")
                    return

                cursor.execute('''
                    SELECT id FROM peace_offers 
                    WHERE offerer_id = ? AND receiver_id = ? AND status = 'pending'
                ''', (offerer_id, user_id))
                offer = cursor.fetchone()
                if not offer:
                    await ctx.send("❌ No pending peace offer from this civilization!")
                    return

                war_id = war[0]
                cursor.execute('''
                    UPDATE wars SET result = 'peace', ended_at = ?
                    WHERE id = ?
                ''', (datetime.utcnow(), war_id))
                cursor.execute('''
                    UPDATE peace_offers SET status = 'accepted', responded_at = ?
                    WHERE id = ?
                ''', (datetime.utcnow(), offer[0]))
                conn.commit()

            self.civ_manager.update_population(user_id, {"happiness": 15})
            self.civ_manager.update_population(offerer_id, {"happiness": 15})

            embed = create_embed(
                "🕊️ Peace Achieved!",
                f"**{civ['name']}** has accepted peace from **{offerer_civ['name']}**! The war is over.",
                guilded.Color.green()
            )
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

    @commands.command(name='cards')
    @app_commands.describe(
        action="Choose whether to view or use cards",
        card_name="Card name (required when action=use)",
        target="Optional target for cards that affect other players"
    )
    @app_commands.choices(
        action=[
            app_commands.Choice(name="view", value="view"),
            app_commands.Choice(name="use", value="use"),
        ],
        card_name=[
            app_commands.Choice(name="Gamble Card", value="Gamble Card"),
            app_commands.Choice(name="Resource Heist", value="Resource Heist"),
            app_commands.Choice(name="Military Coup", value="Military Coup"),
            app_commands.Choice(name="Territory Gambit", value="Territory Gambit"),
            app_commands.Choice(name="Population Swap", value="Population Swap"),
        ]
    )
    async def manage_cards(
        self,
        ctx,
        action: Optional[Literal["view", "use"]] = None,
        card_name: str = None,
        target: Optional[guilded.Member] = None
    ):
        """View or use your unlocked cards (No cooldown)"""
        try:
            user_id = str(ctx.author.id)
            
            if not await self.check_civil_war_and_proceed(ctx, user_id):
                return
                
            civ = self.civ_manager.get_civilization(user_id)

            if not civ:
                await ctx.send("❌ You need to start a civilization first! Use `.start`")
                return

            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                
                if action is None or action.lower() == 'view':
                    cursor.execute('''
                        SELECT card_name, used FROM unlocked_cards 
                        WHERE user_id = ? ORDER BY unlocked_at DESC
                    ''', (user_id,))
                    cards = cursor.fetchall()
                    
                    if not cards:
                        embed = create_embed(
                            "🎴 Your Cards",
                            "You haven't unlocked any cards yet! Cards are unlocked when you reach new tech levels.",
                            guilded.Color.blue()
                        )
                    else:
                        embed = create_embed(
                            "🎴 Your Cards",
                            f"You have {len(cards)} unlocked card(s):",
                            guilded.Color.blue()
                        )
                        for card_name, used in cards:
                            status = "✅ Used" if used else "🟢 Available"
                            embed.add_field(name=card_name, value=status, inline=True)
                        embed.add_field(
                            name="How to Use", 
                            value="Use `/cards use <card_name> [target]` to use a card. Example: `/cards use Resource Heist @username`", 
                            inline=False
                        )
                    await ctx.send(embed=embed)
                    return
                
                elif action.lower() == 'use':
                    if not card_name:
                        await ctx.send("❌ Please specify a card name! Usage: `/cards use <card_name> [target]`")
                        return
                    
                    cursor.execute('''
                        SELECT card_name, used FROM unlocked_cards 
                        WHERE user_id = ? AND card_name = ?
                    ''', (user_id, card_name))
                    card_data = cursor.fetchone()
                    
                    if not card_data:
                        await ctx.send(f"❌ You don't have the card '{card_name}' or it doesn't exist!")
                        return
                    
                    if card_data[1]:
                        await ctx.send("❌ You've already used this card!")
                        return
                    
                    result = await self._process_card_effect(ctx, user_id, card_name, target)
                    
                    if result:
                        cursor.execute('''
                            UPDATE unlocked_cards SET used = 1 
                            WHERE user_id = ? AND card_name = ?
                        ''', (user_id, card_name))
                        conn.commit()
                        self.db.log_event(user_id, "card_used", f"Card Used: {card_name}", result)
                        await ctx.send(result)
                    else:
                        await ctx.send("❌ Failed to process card effect!")
                    
                else:
                    await ctx.send("❌ Invalid action! Use `/cards view` or `/cards use <card_name> [target]`.")

        except Exception as e:
            logger.error(f"Error in cards command: {e}", exc_info=True)

    async def _process_card_effect(self, ctx, user_id, card_name, target_member: Optional[guilded.Member]):
        """Process card effects (unchanged)"""
        try:
            civ = self.civ_manager.get_civilization(user_id)
            
            if card_name == "Gamble Card":
                if not target_member:
                    return "❌ Gamble Card requires a target! Usage: `/cards use \"Gamble Card\" <target>`"
                
                target = target_member
                target_id = str(target.id)
                target_civ = self.civ_manager.get_civilization(target_id)
                
                if not target_civ:
                    return "❌ Target doesn't have a civilization!"
                
                population_loss = civ['population']['citizens'] // 2
                self.civ_manager.update_population(user_id, {"citizens": -population_loss})
                
                if random.random() < 0.001:
                    self.civ_manager.reset_civilization(target_id)
                    return f"🎰 **JACKPOT!** You sacrificed {format_number(population_loss)} people and **COMPLETELY DESTROYED {target_civ['name'].upper()}!** 🎰"
                else:
                    return f"💀 You sacrificed {format_number(population_loss)} people for nothing... The gamble failed."
                
            elif card_name == "Resource Heist":
                if not target_member:
                    return "❌ Resource Heist requires a target! Usage: `/cards use \"Resource Heist\" <target>`"
                
                target = target_member
                target_id = str(target.id)
                target_civ = self.civ_manager.get_civilization(target_id)
                
                if not target_civ:
                    return "❌ Target doesn't have a civilization!"
                
                if random.random() < 0.1:
                    stolen = {
                        "gold": max(1, civ['resources']['gold'] // 4),
                        "food": max(1, civ['resources']['food'] // 4),
                        "stone": max(1, civ['resources']['stone'] // 4),
                        "wood": max(1, civ['resources']['wood'] // 4)
                    }
                    self.civ_manager.update_resources(user_id, {k: -v for k, v in stolen.items()})
                    self.civ_manager.update_resources(target_id, stolen)
                    return f"😱 **HEIST BACKFIRED!** {target_civ['name']} stole your resources instead!"
                else:
                    stolen = {
                        "gold": max(1, target_civ['resources']['gold'] // 4),
                        "food": max(1, target_civ['resources']['food'] // 4),
                        "stone": max(1, target_civ['resources']['stone'] // 4),
                        "wood": max(1, target_civ['resources']['wood'] // 4)
                    }
                    self.civ_manager.update_resources(user_id, stolen)
                    self.civ_manager.update_resources(target_id, {k: -v for k, v in stolen.items()})
                    return f"💰 **Successful Heist!** Stole 25% of {target_civ['name']}'s resources!"
            
            elif card_name == "Military Coup":
                if random.random() < 0.5:
                    self.civ_manager.update_military(user_id, {
                        "soldiers": civ['military']['soldiers'],
                        "spies": civ['military']['spies']
                    })
                    return "🎖️ **Successful Coup!** Your military has doubled in size!"
                else:
                    self.civ_manager.update_military(user_id, {
                        "soldiers": -civ['military']['soldiers'],
                        "spies": -civ['military']['spies']
                    })
                    return "💥 **Coup Failed!** Your military has been disbanded!"
            
            elif card_name == "Territory Gambit":
                current_territory = civ['territory']['land_size']
                if random.random() < 0.3:
                    new_territory = current_territory * 3
                    self.civ_manager.update_territory(user_id, {"land_size": new_territory - current_territory})
                    return f"🎯 **Gambit Success!** Your territory tripled from {format_number(current_territory)} to {format_number(new_territory)} km²!"
                else:
                    lost_territory = max(1, current_territory // 2)
                    self.civ_manager.update_territory(user_id, {"land_size": -lost_territory})
                    return f"💸 **Gambit Failed!** You lost half your territory, now at {format_number(current_territory - lost_territory)} km²."
            
            elif card_name == "Population Swap":
                if not target_member:
                    return "❌ Population Swap requires a target! Usage: `/cards use \"Population Swap\" <target>`"
                
                target = target_member
                target_id = str(target.id)
                target_civ = self.civ_manager.get_civilization(target_id)
                
                if not target_civ:
                    return "❌ Target doesn't have a civilization!"
                
                your_pop = civ['population']['citizens']
                their_pop = target_civ['population']['citizens']
                
                self.civ_manager.update_population(user_id, {"citizens": their_pop - your_pop})
                self.civ_manager.update_population(target_id, {"citizens": your_pop - their_pop})
                
                return f"🔄 **Population Swap!** You now have {format_number(their_pop)} people, they have {format_number(your_pop)}!"
            
            else:
                return f"❌ Card '{card_name}' effect not implemented yet!"
                
        except Exception as e:
            logger.error(f"Error processing card effect: {e}", exc_info=True)
            return f"❌ Error processing card effect: {str(e)}"

    # ---------- BORDER COMMANDS (unchanged, but updated to use new helpers if needed) ----------
    @commands.command(name='addborder')
    async def add_border(self, ctx):
        """Add a defensive border to your territory (5min cooldown)"""
        try:
            user_id = str(ctx.author.id)
            
            if not self._check_cooldown(user_id, 'addborder', 300):
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

            border_cost = {
                "gold": 1000,
                "stone": 500,
                "wood": 300
            }

            if not self.civ_manager.can_afford(user_id, border_cost):
                await ctx.send(f"❌ Not enough resources! Need {format_number(border_cost['gold'])} gold, {format_number(border_cost['stone'])} stone, and {format_number(border_cost['wood'])} wood.")
                return

            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT has_border FROM borders WHERE user_id = ?', (user_id,))
                existing_border = cursor.fetchone()

                if existing_border and existing_border[0]:
                    await ctx.send("❌ You already have a border! Use `.removeborder` to remove it first.")
                    return

                self.civ_manager.spend_resources(user_id, border_cost)
                cursor.execute('''
                    INSERT OR REPLACE INTO borders (user_id, has_border, border_strength, border_soldiers)
                    VALUES (?, TRUE, 100, 0)
                ''', (user_id,))
                conn.commit()

            embed = create_embed(
                "🛡️ Border Established!",
                f"**{civ['name']}** has built a defensive border around their territory!",
                guilded.Color.green()
            )
            embed.add_field(name="Border Strength", value="100/100", inline=True)
            embed.add_field(name="Cost", value=f"🪙 1,000 Gold\n🪨 500 Stone\n🪵 300 Wood", inline=True)
            embed.add_field(name="Next Steps", value="Use `.rectract <percentage>` to assign soldiers to your border for extra defense!", inline=False)

            await ctx.send(embed=embed)

        except Exception as e:
            logger.error(f"Error in addborder command: {e}", exc_info=True)

    @commands.command(name='removeborder')
    async def remove_border(self, ctx):
        """Remove your defensive border and retrieve all soldiers (2min cooldown)"""
        try:
            user_id = str(ctx.author.id)
            
            if not self._check_cooldown(user_id, 'removeborder', 120):
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

            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT border_soldiers FROM borders WHERE user_id = ? AND has_border = TRUE', (user_id,))
                border_data = cursor.fetchone()

                if not border_data:
                    await ctx.send("❌ You don't have a border to remove!")
                    return

                soldiers_to_return = border_data[0]
                if soldiers_to_return > 0:
                    self.civ_manager.update_military(user_id, {"soldiers": soldiers_to_return})

                cursor.execute('DELETE FROM borders WHERE user_id = ?', (user_id,))
                conn.commit()

            embed = create_embed(
                "🛡️ Border Removed!",
                f"**{civ['name']}** has dismantled their defensive border.",
                guilded.Color.blue()
            )
            if soldiers_to_return > 0:
                embed.add_field(name="Soldiers Returned", value=f"⚔️ {format_number(soldiers_to_return)} soldiers have returned to your main army.", inline=False)

            await ctx.send(embed=embed)

        except Exception as e:
            logger.error(f"Error in removeborder command: {e}", exc_info=True)

    @commands.command(name='rectract', aliases=['retract'])
    async def rectract_soldiers(self, ctx, percentage: int = None):
        """Assign a percentage of your soldiers to the border (1min cooldown)"""
        try:
            user_id = str(ctx.author.id)
            
            if not self._check_cooldown(user_id, 'rectract', 60):
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

            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT border_strength, border_soldiers FROM borders WHERE user_id = ? AND has_border = TRUE', (user_id,))
                border_data = cursor.fetchone()

                if not border_data:
                    await ctx.send("❌ You need to build a border first! Use `.addborder`")
                    return

            current_border_strength, current_border_soldiers = border_data
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
            
            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    UPDATE borders 
                    SET border_soldiers = ?, border_strength = ?, updated_at = ?
                    WHERE user_id = ?
                ''', (new_border_soldiers, current_border_strength + border_strength_increase, datetime.utcnow(), user_id))
                conn.commit()

            self.civ_manager.update_military(user_id, {"soldiers": -soldiers_to_assign})

            embed = create_embed(
                "🛡️ Soldiers Assigned to Border!",
                f"**{civ['name']}** has assigned {format_number(soldiers_to_assign)} soldiers to reinforce the border.",
                guilded.Color.green()
            )
            embed.add_field(name="Border Soldiers", value=f"⚔️ {format_number(new_border_soldiers)} total", inline=True)
            embed.add_field(name="Border Strength", value=f"🛡️ {format_number(current_border_strength + border_strength_increase)}", inline=True)
            embed.add_field(name="Main Army", value=f"⚔️ {format_number(available_soldiers - soldiers_to_assign)} soldiers remaining", inline=True)

            await ctx.send(embed=embed)

        except Exception as e:
            logger.error(f"Error in rectract command: {e}", exc_info=True)

    @commands.command(name='retrieve')
    async def retrieve_soldiers(self, ctx, percentage: int = None):
        """Retrieve a percentage of soldiers from the border (1min cooldown)"""
        try:
            user_id = str(ctx.author.id)
            
            if not self._check_cooldown(user_id, 'retrieve', 60):
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

            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT border_strength, border_soldiers FROM borders WHERE user_id = ? AND has_border = TRUE', (user_id,))
                border_data = cursor.fetchone()

                if not border_data:
                    await ctx.send("❌ You need to build a border first! Use `.addborder`")
                    return

            current_border_strength, current_border_soldiers = border_data
            
            if current_border_soldiers == 0:
                await ctx.send("❌ You don't have any soldiers assigned to your border!")
                return

            soldiers_to_retrieve = min((current_border_soldiers * percentage) // 100, current_border_soldiers)
            strength_loss = (current_border_strength * soldiers_to_retrieve) // current_border_soldiers
            new_border_strength = max(1, current_border_strength - strength_loss)
            new_border_soldiers = current_border_soldiers - soldiers_to_retrieve

            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    UPDATE borders 
                    SET border_soldiers = ?, border_strength = ?, updated_at = ?
                    WHERE user_id = ?
                ''', (new_border_soldiers, new_border_strength, datetime.utcnow(), user_id))
                conn.commit()

            self.civ_manager.update_military(user_id, {"soldiers": soldiers_to_retrieve})

            embed = create_embed(
                "🛡️ Soldiers Retrieved from Border!",
                f"**{civ['name']}** has retrieved {format_number(soldiers_to_retrieve)} soldiers from the border.",
                guilded.Color.blue()
            )
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
        """Check your border status (1min cooldown)"""
        try:
            user_id = str(ctx.author.id)
            
            if not self._check_cooldown(user_id, 'borderinfo', 60):
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

            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT border_strength, border_soldiers, created_at FROM borders WHERE user_id = ? AND has_border = TRUE', (user_id,))
                border_data = cursor.fetchone()

            if not border_data:
                embed = create_embed(
                    "🛡️ Border Status",
                    "You don't have a defensive border yet!",
                    guilded.Color.blue()
                )
                embed.add_field(name="How to Build", value="Use `.addborder` to build a defensive border (costs resources).", inline=False)
            else:
                border_strength, border_soldiers, created_at = border_data
                embed = create_embed(
                    "🛡️ Border Status",
                    f"**{civ['name']}**'s defensive border",
                    guilded.Color.green()
                )
                embed.add_field(name="Border Strength", value=f"🛡️ {format_number(border_strength)}", inline=True)
                embed.add_field(name="Border Soldiers", value=f"⚔️ {format_number(border_soldiers)}", inline=True)
                embed.add_field(name="Main Army", value=f"⚔️ {format_number(civ['military']['soldiers'])}", inline=True)
                defense_bonus = min(50, border_strength // 10)
                embed.add_field(name="Defense Bonus", value=f"🛡️ +{defense_bonus}% in defensive battles", inline=False)
                embed.add_field(name="Management", value="Use `.rectract <percentage>` to assign soldiers\nUse `.retrieve <percentage>` to retrieve soldiers", inline=False)

            await ctx.send(embed=embed)

        except Exception as e:
            logger.error(f"Error in borderinfo command: {e}", exc_info=True)
                # ---------- NEW COMMANDS: NAVY, AIRFORCE, TECH, TRAINING ----------

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
        """Build navy ships (5min cooldown)"""
        try:
            user_id = str(ctx.author.id)
            
            if not self._check_cooldown(user_id, 'buildship', 300):
                remaining = self._get_cooldown_remaining(user_id, 'buildship')
                mins = remaining // 60
                secs = remaining % 60
                await ctx.send(f"⏳ Please wait {mins}m {secs}s before building ships again!")
                return
            
            if not ship_type:
                embed = create_embed(
                    "🚢 Build Ships",
                    "Build navy ships to strengthen your fleet!",
                    guilded.Color.blue()
                )
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

            # Spend resources
            self.civ_manager.spend_resources(user_id, total_cost)

            # Update navy
            self._update_navy(user_id, {ship_type: amount})

            embed = create_embed(
                "🚢 Ship Build Complete!",
                f"Built **{amount} {ship_data['name']}(s)** for **{civ['name']}**!",
                guilded.Color.green()
            )
            embed.add_field(name="Cost", value=f"🪙 {format_number(total_cost['gold'])} Gold\n🪵 {format_number(total_cost['wood'])} Wood\n🪨 {format_number(total_cost['stone'])} Stone", inline=True)
            embed.add_field(name="Total Navy", value=f"Check with `.navy`", inline=True)

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
        """Build airforce planes (10min cooldown)"""
        try:
            user_id = str(ctx.author.id)
            
            if not self._check_cooldown(user_id, 'buildplane', 600):
                remaining = self._get_cooldown_remaining(user_id, 'buildplane')
                mins = remaining // 60
                secs = remaining % 60
                await ctx.send(f"⏳ Please wait {mins}m {secs}s before building planes again!")
                return
            
            if not plane_type:
                embed = create_embed(
                    "✈️ Build Planes",
                    "Build airforce planes to dominate the skies!",
                    guilded.Color.blue()
                )
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

            # Check requirements for attacker and bomber
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

            # Spend resources
            self.civ_manager.spend_resources(user_id, total_cost)

            # Update airforce
            self._update_airforce(user_id, {plane_type: amount})

            embed = create_embed(
                "✈️ Plane Build Complete!",
                f"Built **{amount} {plane_data['name']}(s)** for **{civ['name']}**!",
                guilded.Color.green()
            )
            embed.add_field(name="Cost", value=f"🪙 {format_number(total_cost['gold'])} Gold\n🪵 {format_number(total_cost['wood'])} Wood\n🪨 {format_number(total_cost['stone'])} Stone", inline=True)
            embed.add_field(name="Total Airforce", value=f"Check with `.airforce`", inline=True)

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
        """Upgrade military tech levels (500 gold per level, max level 10)"""
        try:
            if not branch:
                embed = create_embed(
                    "🔬 Military Tech Upgrade",
                    "Upgrade your military technology for 500 gold per level.",
                    guilded.Color.blue()
                )
                embed.add_field(name="Branches", value="`ground` – improves soldiers and ground combat\n`naval` – improves ship strength\n`air` – improves plane strength", inline=False)
                embed.add_field(name="Usage", value="`.tech <branch> [amount]` (e.g., `.tech ground 2`)", inline=False)
                embed.add_field(name="Current Tech", value=f"Check with `.tech status`", inline=False)
                await ctx.send(embed=embed)
                return

            user_id = str(ctx.author.id)
            civ = self.civ_manager.get_civilization(user_id)
            if not civ:
                await ctx.send("❌ You need to start a civilization first! Use `.start`")
                return

            if branch == "status":
                tech = self._get_military_tech(user_id)
                embed = create_embed(
                    "🔬 Current Military Tech",
                    f"**{civ['name']}**'s technology levels:",
                    guilded.Color.blue()
                )
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

            cost = 500 * amount
            if not self.civ_manager.can_afford(user_id, {"gold": cost}):
                await ctx.send(f"❌ Not enough gold! Need {format_number(cost)} gold for {amount} level(s).")
                return

            self.civ_manager.spend_resources(user_id, {"gold": cost})
            self._update_military_tech(user_id, {f"{branch}_tech": amount})

            new_level = current_level + amount
            embed = create_embed(
                "🔬 Tech Upgrade Complete!",
                f"**{branch.capitalize()} Tech** increased from **{current_level}** to **{new_level}**!",
                guilded.Color.green()
            )
            embed.add_field(name="Cost", value=f"🪙 {format_number(cost)} Gold", inline=True)

            # Unlock cards on tech level milestones (if using card system)
            # (Optional)
            await ctx.send(embed=embed)

        except Exception as e:
            logger.error(f"Error in tech command: {e}", exc_info=True)

    @commands.command(name='trainboost')
    @app_commands.describe(amount="How many training levels to increase (default 1)")
    async def train_boost(self, ctx, amount: int = 1):
        """
        Increase soldier training level (max level 3).
        Cost: 500 gold per level.
        Training levels: 0=1x, 1=1.5x, 2=2x, 3=3x (up to 300 soldiers).
        """
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

            cost = 500 * actual_increase
            if not self.civ_manager.can_afford(user_id, {"gold": cost}):
                await ctx.send(f"❌ Not enough gold! Need {format_number(cost)} gold to increase training by {actual_increase} level(s).")
                return

            self.civ_manager.spend_resources(user_id, {"gold": cost})
            self._update_training(user_id, {"level": actual_increase})

            # Update boosted_soldiers to min(soldiers, 300) automatically (optional)
            # We'll just set boosted_soldiers = min(soldiers, 300) in the calculation, so we can ignore this.

            embed = create_embed(
                "⚔️ Training Level Up!",
                f"Training level increased from **{current_level}** to **{new_level}**!\n"
                f"Multiplier: {TRAINING_LEVELS[current_level]}x → {TRAINING_LEVELS[new_level]}x (up to 300 soldiers)",
                guilded.Color.gold()
            )
            embed.add_field(name="Cost", value=f"🪙 {format_number(cost)} Gold", inline=True)
            await ctx.send(embed=embed)

        except Exception as e:
            logger.error(f"Error in trainboost command: {e}", exc_info=True)

    @commands.command(name='navy')
    async def show_navy(self, ctx):
        """View your navy fleet"""
        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need to start a civilization first! Use `.start`")
            return

        navy = self._get_navy(user_id)
        tech = self._get_military_tech(user_id)
        naval_tech = tech.get("naval_tech", 1)

        embed = create_embed(
            "🚢 Navy Fleet",
            f"**{civ['name']}**'s naval forces (Naval Tech: {naval_tech})",
            guilded.Color.blue()
        )
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
        """View your airforce fleet"""
        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need to start a civilization first! Use `.start`")
            return

        air = self._get_airforce(user_id)
        tech = self._get_military_tech(user_id)
        air_tech = tech.get("air_tech", 1)

        embed = create_embed(
            "✈️ Airforce Fleet",
            f"**{civ['name']}**'s air forces (Air Tech: {air_tech})",
            guilded.Color.blue()
        )
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

    # ---- Override _calculate_military_strength to use computed boosted_soldiers ----
    # We'll replace the existing _calculate_military_strength with this version that uses min(soldiers, 300)
    # But we already have it in Part 1, so we can just leave as is – it uses boosted_soldiers from table.
    # To keep it simple, we'll update the training command to also set boosted_soldiers to min(soldiers, 300)
    # when training level changes. So we'll add a step in trainboost to update boosted_soldiers.
    # But that would require modifying the helper. Actually we can compute on the fly.
    # We'll keep the existing _calculate_military_strength as is (it uses boosted_soldiers from DB).
    # In trainboost, we can also set boosted_soldiers = min(civ['military']['soldiers'], 300).
    # We'll do that now in the trainboost command to keep it consistent.

    # However, we already have the helper function _update_training which only updates level, not boosted_soldiers.
    # We'll extend it to also set boosted_soldiers if needed? No, we'll just update boosted_soldiers separately.
    # In trainboost, after updating level, we'll also update boosted_soldiers:
    #     self._update_training(user_id, {"boosted_soldiers": min(civ['military']['soldiers'], 300)})
    # But we have _update_training that only does addition, not set. We'll add a method to set directly.
    # To avoid complicating, we'll just compute boosted_soldiers in _calculate_military_strength.
    # We'll override that method in this part.

    # We'll redefine _calculate_military_strength here (replace the one from Part 1) – but that's not possible in a separate part.
    # Instead, we'll modify the method in place – we can just include a new version in Part 3 that replaces it.
    # But since we are appending code, we can just override the method by redefining it in the class.
    # Python allows that – we can just define a new method with the same name later, and it will override the previous.
    # So we'll put a new _calculate_military_strength method in Part 3 that uses the computed boosted count.

    # We'll also need to ensure that training level is taken into account.
    # So we'll add the following method at the end of the cog (overriding the old one).

    def _calculate_military_strength(self, civ: dict, navy_counts: dict = None, air_counts: dict = None, training: dict = None) -> float:
        """
        Calculate total military strength based on:
        - Soldiers (with training multiplier, capped at 300 boosted soldiers)
        - Spies
        - Ground tech (from military_tech)
        - Navy ships (strength multiplied by naval tech)
        - Airforce planes (strength multiplied by air tech)
        """
        # Get tech levels
        tech = self._get_military_tech(civ['user_id']) if civ else {"ground_tech": 1, "naval_tech": 1, "air_tech": 1}
        ground_tech = tech.get("ground_tech", 1)
        naval_tech = tech.get("naval_tech", 1)
        air_tech = tech.get("air_tech", 1)

        # Training
        if training is None:
            training = self._get_training(civ['user_id'])
        training_level = training.get("level", 0)
        multiplier = TRAINING_LEVELS[training_level] if training_level < len(TRAINING_LEVELS) else 1.0

        # Ground forces: soldiers with training multiplier (capped at 300 boosted)
        soldiers = civ['military']['soldiers']
        boosted_count = min(soldiers, 300)  # at most 300 boosted soldiers
        normal_count = soldiers - boosted_count
        effective_soldiers = normal_count + (boosted_count * multiplier)
        ground_power = effective_soldiers * 10  # base 10 per soldier equivalent

        # Spies (no multiplier)
        spies = civ['military']['spies']
        spy_power = spies * 5

        # Navy
        if navy_counts is None:
            navy_counts = self._get_navy(civ['user_id'])
        navy_power = 0
        for ship_type, count in navy_counts.items():
            ship_stats = SHIP_TYPES.get(ship_type)
            if ship_stats:
                navy_power += count * ship_stats["strength"] * naval_tech

        # Airforce
        if air_counts is None:
            air_counts = self._get_airforce(civ['user_id'])
        air_power = 0
        for plane_type, count in air_counts.items():
            plane_stats = PLANE_TYPES.get(plane_type)
            if plane_stats:
                air_power += count * plane_stats["strength"] * air_tech

        # Territory bonus (slight)
        territory_bonus = civ['territory']['land_size'] / 10000

        total = ground_power + spy_power + navy_power + air_power + territory_bonus
        return total


async def setup(bot):
    await bot.add_cog(MilitaryCommands(bot))
