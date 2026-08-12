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
from typing import List

logger = logging.getLogger(__name__)

# ---- Cooldown decorator using config ----
def check_cooldown_decorator(command_name: str):
    def decorator(func):
        @wraps(func)
        async def wrapper(self, ctx, *args, **kwargs):
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

        # Random events
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

    @commands.command(name='harvest')
    async def harvest_food(self, ctx):
        user_id = str(ctx.author.id)
        if not await self.check_civil_war_and_proceed(ctx, user_id):
            return
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need to start a civilization first! Use `.start <name>`")
            return

        base_harvest = random.randint(config.ECONOMY["harvest_base_min"], config.ECONOMY["harvest_base_max"])
        population_bonus = civ['population']['citizens'] // config.ECONOMY["harvest_citizen_divisor"]
        happiness_bonus = civ['population']['happiness'] // config.ECONOMY["harvest_happiness_divisor"]
        employment_rate = self.civ_manager.get_employment_rate(user_id) / 100
        employment_factor = 1 + employment_rate * config.ECONOMY["harvest_employment_coeff"]
        territory_factor = get_territory_modifier(civ['territory']['land_size'])

        total_harvest = int((base_harvest + population_bonus + happiness_bonus) * employment_factor * territory_factor)
        if civ.get('ideology') == 'theocracy':
            total_harvest = int(total_harvest * 1.1)
        total_harvest = min(total_harvest, config.CAPS["harvest"])

        self.civ_manager.update_resources(user_id, {"food": total_harvest})
        self.civ_manager.update_population(user_id, {"happiness": 3})

        embed = create_embed("🌽 Great Harvest", f"A bountiful harvest brings {format_number(total_harvest)} food to your civilization!", guilded.Color.gold())
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
            await ctx.send("❌ You need Tech Level 2 or higher to use advanced drilling equipment!")
            return

        rare_minerals = random.randint(config.ECONOMY["drill_minerals_min"], config.ECONOMY["drill_minerals_max"])
        gold_value = rare_minerals * 2

        employment_rate = self.civ_manager.get_employment_rate(user_id) / 100
        employment_factor = 1 + employment_rate * config.ECONOMY["drill_employment_coeff"]
        territory_factor = get_territory_modifier(civ['territory']['land_size'])

        gold_value = int(gold_value * employment_factor * territory_factor)
        stone_value = int((rare_minerals // 2) * employment_factor * territory_factor)

        gold_value = min(gold_value, config.CAPS["drill_gold"])
        stone_value = min(stone_value, config.CAPS["drill_stone"])

        bonus_text = ""
        if random.random() < config.ECONOMY["drill_bonus_gold_chance"]:
            bonus_gold = random.randint(config.ECONOMY["drill_bonus_gold_min"], config.ECONOMY["drill_bonus_gold_max"])
            gold_value += bonus_gold
            bonus_text = f"💎 Struck a rich vein! (+{format_number(bonus_gold)} gold)"

        self.civ_manager.update_resources(user_id, {"gold": gold_value, "stone": stone_value})

        embed = create_embed("🏗️ Deep Drilling", f"Advanced drilling equipment extracted valuable minerals worth {format_number(gold_value)} gold!", guilded.Color.purple())
        if bonus_text:
            embed.add_field(name="Lucky Strike!", value=bonus_text, inline=False)
        await ctx.send(embed=embed)

    @commands.command(name='fish')
    @check_cooldown_decorator("fish")
    async def fish_resources(self, ctx):
        user_id = str(ctx.author.id)
        if not await self.check_civil_war_and_proceed(ctx, user_id):
            return
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need to start a civilization first! Use `.start <name>`")
            return

        employment_rate = self.civ_manager.get_employment_rate(user_id) / 100
        employment_factor = 1 + employment_rate * config.ECONOMY["fish_employment_coeff"]
        territory_factor = get_territory_modifier(civ['territory']['land_size'])

        if random.random() < 0.8:
            food_caught = int(random.randint(config.ECONOMY["fish_food_base_min"], config.ECONOMY["fish_food_base_max"]) * employment_factor * territory_factor)
            food_caught = min(food_caught, config.CAPS["fish"])
            self.civ_manager.update_resources(user_id, {"food": food_caught})
            embed = create_embed("🎣 Fishing", f"Your fishermen caught {format_number(food_caught)} food from the waters!", guilded.Color.teal())
        else:
            treasure_gold = int(random.randint(config.ECONOMY["fish_treasure_base_min"], config.ECONOMY["fish_treasure_base_max"]) * employment_factor * territory_factor)
            treasure_gold = min(treasure_gold, config.CAPS["fish"])
            self.civ_manager.update_resources(user_id, {"gold": treasure_gold})
            embed = create_embed("🎣 Fishing - Lucky Find!", f"Your nets pulled up a treasure chest worth {format_number(treasure_gold)} gold!", guilded.Color.gold())
        await ctx.send(embed=embed)

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
            employment_rate = self.civ_manager.get_employment_rate(user_id) / 100
            employment_factor = 1 + employment_rate * config.ECONOMY["raid_employment_coeff"]
            territory_factor = get_territory_modifier(civ['territory']['land_size'])

            loot = {
                "gold": int(random.randint(config.ECONOMY["raid_gold_min"], config.ECONOMY["raid_gold_max"]) * employment_factor * territory_factor),
                "food": int(random.randint(config.ECONOMY["raid_food_min"], config.ECONOMY["raid_food_max"]) * employment_factor * territory_factor),
                "wood": int(random.randint(config.ECONOMY["raid_wood_min"], config.ECONOMY["raid_wood_max"]) * employment_factor * territory_factor),
                "stone": int(random.randint(config.ECONOMY["raid_stone_min"], config.ECONOMY["raid_stone_max"]) * employment_factor * territory_factor)
            }
            if random.random() < config.ECONOMY["raid_bonus_gold_chance"]:
                bonus_gold = random.randint(config.ECONOMY["raid_bonus_gold_min"], config.ECONOMY["raid_bonus_gold_max"])
                loot["gold"] += bonus_gold

            for key in loot:
                loot[key] = min(loot[key], config.CAPS["raidcaravan"])

            self.civ_manager.update_resources(user_id, loot)

            embed = create_embed("🏴‍☠️ Caravan Raid - Success!", "Your raiders ambushed a wealthy merchant caravan!", guilded.Color.green())
            loot_text = "\n".join([f"{'🪙' if res == 'gold' else '🌾' if res == 'food' else '🪵' if res == 'wood' else '🪨'} {format_number(amt)} {res.capitalize()}" for res, amt in loot.items() if amt > 0])
            embed.add_field(name="Loot Acquired", value=loot_text, inline=False)
        else:
            soldier_loss = random.randint(1, 3)
            self.civ_manager.update_military(user_id, {"soldiers": -soldier_loss})
            embed = create_embed("🏴‍☠️ Caravan Raid - Failed!", f"The caravan's guards were too strong! You lost {soldier_loss} soldiers in the failed attack.", guilded.Color.red())
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

        military = civ['military']
        if military['soldiers'] < config.ECONOMY["labor_min_soldiers"]:
            await ctx.send(f"❌ You need at least {config.ECONOMY['labor_min_soldiers']} soldiers to enforce labor!")
            return

        employment_rate = self.civ_manager.get_employment_rate(user_id) / 100
        employment_factor = 1 + employment_rate * config.ECONOMY["labor_employment_coeff"]
        territory_factor = get_territory_modifier(civ['territory']['land_size'])

        loot = {
            "gold": int(random.randint(config.ECONOMY["labor_gold_min"], config.ECONOMY["labor_gold_max"]) * employment_factor * territory_factor),
            "food": int(random.randint(config.ECONOMY["labor_food_min"], config.ECONOMY["labor_food_max"]) * employment_factor * territory_factor),
            "wood": int(random.randint(config.ECONOMY["labor_wood_min"], config.ECONOMY["labor_wood_max"]) * employment_factor * territory_factor),
            "stone": int(random.randint(config.ECONOMY["labor_stone_min"], config.ECONOMY["labor_stone_max"]) * employment_factor * territory_factor)
        }
        if random.random() < 0.1:
            bonus_gold = random.randint(200, 500)
            loot["gold"] += bonus_gold

        for key in loot:
            loot[key] = min(loot[key], config.CAPS["labor"])

        self.civ_manager.update_resources(user_id, loot)
        self.civ_manager.update_population(user_id, {"happiness": config.ECONOMY["labor_happiness_cost"]})

        embed = create_embed("⛏️ Forced Labor", "Your citizens have been forced to work extra shifts!", guilded.Color.orange())
        loot_text = "\n".join([f"{'🪙' if res == 'gold' else '🌾' if res == 'food' else '🪵' if res == 'wood' else '🪨'} {format_number(amt)} {res.capitalize()}" for res, amt in loot.items() if amt > 0])
        embed.add_field(name="Resources Extracted", value=loot_text, inline=False)
        embed.add_field(name="😠 Morale Cost", value=f"Citizens are unhappy! ({config.ECONOMY['labor_happiness_cost']} happiness)", inline=False)
        await ctx.send(embed=embed)

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

        # Use config for price ranges
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

async def setup(bot):
    await bot.add_cog(EconomyCommands(bot))
