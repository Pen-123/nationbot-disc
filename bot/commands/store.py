import random
import discord
from discord.ext import commands
from discord import app_commands
import logging
from typing import Literal, Optional
from bot.utils import format_number, create_embed
from bot import config

logger = logging.getLogger(__name__)


class StoreCommands(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.db = bot.db
        self.civ_manager = bot.civ_manager

        self.store_items = {
            "farm_upgrade": {
                "name": "Farm Upgrade",
                "cost": {"gold": 500, "wood": 200},
                "description": "Increases food production efficiency by 25%",
                "effect": {"farm_bonus": 0.25}
            },
            "mine_upgrade": {
                "name": "Mining Equipment",
                "cost": {"gold": 800, "stone": 150},
                "description": "Improves stone and wood extraction by 30%",
                "effect": {"mine_bonus": 0.30}
            },
            "barracks": {
                "name": "Military Barracks",
                "cost": {"gold": 1000, "stone": 300, "wood": 200},
                "description": "Reduces soldier training cost by 20%",
                "effect": {"training_cost_reduction": 0.20}
            },
            "walls": {
                "name": "City Walls",
                "cost": {"gold": 1500, "stone": 500},
                "description": "Provides +25% defensive bonus in battles",
                "effect": {"defense_bonus": 0.25}
            },
            "marketplace": {
                "name": "Grand Marketplace",
                "cost": {"gold": 2000, "wood": 400},
                "description": "Increases trade efficiency and tax income by 15%",
                "effect": {"trade_bonus": 0.15, "tax_bonus": 0.15}
            },
            "library": {
                "name": "Great Library",
                "cost": {"gold": 3000, "stone": 200, "wood": 300},
                "description": "Accelerates technology research by 50%",
                "effect": {"tech_speed": 0.50}
            },
            "granary": {
                "name": "Food Granary",
                "cost": {"gold": 750, "wood": 350},
                "description": "Reduces food consumption by 20%",
                "effect": {"food_efficiency": 0.20}
            },
            "spy_network": {
                "name": "Intelligence Network",
                "cost": {"gold": 1200, "stone": 100},
                "description": "Improves spy mission success rate by 30%",
                "effect": {"spy_bonus": 0.30}
            }
        }

        # Black Market pool — NO pity, adjusted weights so rare items are more accessible.
        self.hyperitem_pool = {
            # Common
            "Lucky Charm":      {"rarity": "common",    "weight": 22, "description": "Guarantees critical success on your next action", "command": "luckystrike"},
            "Propaganda Kit":   {"rarity": "common",    "weight": 22, "description": "Steal soldiers from enemy civilizations",         "command": "propaganda"},
            "Mercenary Contract":{"rarity": "common",   "weight": 20, "description": "Instantly hire professional soldiers",             "command": "hiremercs"},
            # Uncommon
            "Spy Network":      {"rarity": "uncommon",  "weight": 18, "description": "Elite espionage mission with high success rate",   "command": "superspy"},
            "Ancient Scroll":   {"rarity": "uncommon",  "weight": 18, "description": "Instantly advance technology level",               "command": "boosttech"},
            "Gold Mint":        {"rarity": "uncommon",  "weight": 18, "description": "Generate large amounts of gold instantly",         "command": "mintgold"},
            "Harvest Engine":   {"rarity": "uncommon",  "weight": 18, "description": "Massive instant food production",                  "command": "superharvest"},
            # Rare
            "Nuclear Warhead":  {"rarity": "rare",      "weight": 12, "description": "Devastating nuclear attack on enemy cities",       "command": "nuke"},
            "Dagger":           {"rarity": "rare",      "weight": 12, "description": "Assassination: zeroes target happiness, steals gold", "command": "backstab"},
            "Missiles":         {"rarity": "rare",      "weight": 12, "description": "Mid-tier military strike capability",               "command": "bomb"},
            "Mirror":           {"rarity": "rare",      "weight": 10, "description": "Reflects the next attack back at the attacker",    "command": "mirror"},
            # Epic
            "Anti-Nuke Shield": {"rarity": "epic",      "weight": 10, "description": "Blocks one attack completely",                     "command": "shield"},
            # Legendary
            "HyperLaser":       {"rarity": "legendary", "weight": 4,  "description": "Complete civilization obliteration weapon",        "command": "obliterate"},
            "Tech Core":        {"rarity": "legendary", "weight": 4,  "description": "Advance multiple technology levels instantly",     "command": "megainvent"},
            "Sacrifice":        {"rarity": "legendary", "weight": 3,  "description": "Mutual destruction — destroy both civs",           "command": "sacrifice"},
        }

    # ---------- ENTRY FEE (does NOT compound) ----------
    def _calculate_entry_fee(self, civ: dict) -> int:
        """Fee is 20% of the civ's PEAK gold, not current gold.

        This means spending 200K doesn't make the next fee drop to 160K —
        it stays at 20% of your all-time high. Prevents the shrinking-fee
        problem where repeated purchases get progressively cheaper.
        """
        current_gold = civ['resources']['gold']
        peak = civ.get('black_market_peak_gold', 0)
        if current_gold > peak:
            peak = current_gold
            self.db.update_civilization(civ['user_id'], {"black_market_peak_gold": peak})
            civ['black_market_peak_gold'] = peak

        # Minimum 1,000; cap the fee at 20% of peak
        fee = max(1000, int(peak * 0.20))
        return min(fee, current_gold)

    # ---------- RNG ----------
    def _roll_hyperitem(self) -> str:
        weighted_items = []
        for item_name, item_data in self.hyperitem_pool.items():
            weighted_items.extend([item_name] * item_data['weight'])
        return random.choice(weighted_items)

    # =================================================================
    # STORE
    # =================================================================

    @commands.hybrid_command(name='store')
    @app_commands.describe(item="Upgrade to purchase (optional)")
    @app_commands.choices(item=[
        app_commands.Choice(name="farm_upgrade", value="farm_upgrade"),
        app_commands.Choice(name="mine_upgrade", value="mine_upgrade"),
        app_commands.Choice(name="barracks", value="barracks"),
        app_commands.Choice(name="walls", value="walls"),
        app_commands.Choice(name="marketplace", value="marketplace"),
        app_commands.Choice(name="library", value="library"),
        app_commands.Choice(name="granary", value="granary"),
        app_commands.Choice(name="spy_network", value="spy_network"),
    ])
    async def view_store(
        self,
        ctx,
        item: Optional[
            Literal[
                "farm_upgrade", "mine_upgrade", "barracks", "walls",
                "marketplace", "library", "granary", "spy_network"
            ]
        ] = None
    ):
        user_id = str(ctx.author.id if not isinstance(ctx, discord.Interaction) else ctx.user.id)
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need to start a civilization first! Use `.start <name>`")
            return

        if not item:
            embed = create_embed(
                "🏪 Civilization Store",
                "Purchase permanent upgrades for your civilization!",
                discord.Color.blue()
            )
            categories = {
                "🌾 Economic": ["farm_upgrade", "marketplace", "granary"],
                "⛏️ Industrial": ["mine_upgrade", "library"],
                "⚔️ Military": ["barracks", "walls", "spy_network"]
            }
            for category, items in categories.items():
                item_list = []
                for item_key in items:
                    item_data = self.store_items[item_key]
                    cost_str = ", ".join([f"{amt} {res}" for res, amt in item_data["cost"].items()])
                    item_list.append(f"**{item_data['name']}** - {cost_str}")
                embed.add_field(name=category, value="\n".join(item_list), inline=False)
            embed.add_field(
                name="Usage",
                value="`.store <item_name>` to view details and purchase\nAvailable items: " + ", ".join(self.store_items.keys()),
                inline=False
            )
            await ctx.send(embed=embed)
            return

        if item not in self.store_items:
            await ctx.send(f"❌ Item '{item}' not found in store! Use `.store` to see available items.")
            return

        item_data = self.store_items[item]
        bonuses = civ.get('bonuses', {})
        if any(effect_key in bonuses for effect_key in item_data['effect'].keys()):
            await ctx.send(f"❌ You already own {item_data['name']} or a similar upgrade!")
            return

        if not self.civ_manager.can_afford(user_id, item_data['cost']):
            cost_str = ", ".join([f"{format_number(amt)} {res}" for res, amt in item_data['cost'].items()])
            await ctx.send(f"❌ Cannot afford {item_data['name']}! Requires: {cost_str}")
            return

        self.civ_manager.spend_resources(user_id, item_data['cost'])
        new_bonuses = bonuses.copy()
        new_bonuses.update(item_data['effect'])
        self.civ_manager.db.update_civilization(user_id, {"bonuses": new_bonuses})

        embed = create_embed(
            "🏪 Purchase Successful!",
            f"You have purchased **{item_data['name']}**!",
            discord.Color.green()
        )
        embed.add_field(name="Description", value=item_data['description'], inline=False)
        cost_text = "\n".join([f"{'🪙' if res == 'gold' else '🌾' if res == 'food' else '🪨' if res == 'stone' else '🪵'} {format_number(amt)} {res.capitalize()}"
                              for res, amt in item_data['cost'].items()])
        embed.add_field(name="Cost", value=cost_text, inline=True)
        embed.add_field(name="Status", value="✅ Upgrade Active", inline=True)
        await ctx.send(embed=embed)
        self.db.log_event(user_id, "store_purchase", "Store Purchase", f"Purchased {item_data['name']}")

    # =================================================================
    # BLACK MARKET (no pity, non-compounding 20% fee)
    # =================================================================

    @commands.hybrid_command(name='blackmarket')
    async def black_market(self, ctx):
        user_id = str(ctx.author.id if not isinstance(ctx, discord.Interaction) else ctx.user.id)
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need to start a civilization first! Use `.start <name>`")
            return

        gold = civ['resources']['gold']
        if gold < 1000:
            await ctx.send("❌ You need at least **1,000 gold** to enter the Black Market.")
            return

        # ---- Fee based on PEAK gold (20%), non-compounding ----
        fee = self._calculate_entry_fee(civ)

        if not self.civ_manager.can_afford(user_id, {"gold": fee}):
            await ctx.send(f"❌ Black Market entry fee: {format_number(fee)} gold! You cannot afford it.")
            return

        self.civ_manager.spend_resources(user_id, {"gold": fee})

        # ---- Roll (pure weighted RNG, NO PITY) ----
        hyper_item = self._roll_hyperitem()
        item_data = self.hyperitem_pool[hyper_item]

        self.civ_manager.add_hyper_item(user_id, hyper_item)

        rarity_colors = {
            "common": discord.Color.green(),
            "uncommon": discord.Color.blue(),
            "rare": discord.Color.purple(),
            "epic": discord.Color.magenta(),
            "legendary": discord.Color.gold()
        }
        rarity_emojis = {
            "common": "🟢",
            "uncommon": "🔵",
            "rare": "🟣",
            "epic": "🟪",
            "legendary": "🟡"
        }

        embed = create_embed(
            "🕴️ Black Market Transaction",
            f"The shadowy dealer hands you a mysterious package... (Entry fee: {format_number(fee)} gold)",
            rarity_colors.get(item_data['rarity'], discord.Color.dark_gray())
        )

        embed.add_field(
            name=f"{rarity_emojis.get(item_data['rarity'], '🟢')} {hyper_item}",
            value=f"**Rarity**: {item_data['rarity'].capitalize()}\n**Description**: {item_data['description']}\n**Command**: `.{item_data['command']}`",
            inline=False
        )

        if item_data['rarity'] == 'legendary':
            embed.add_field(name="🌟 LEGENDARY ITEM!", value="You have obtained an extremely rare and powerful artifact!", inline=False)
        elif item_data['rarity'] == 'epic':
            embed.add_field(name="💠 Epic Find!", value="A powerful relic that will turn the tide of battle!", inline=False)
        elif item_data['rarity'] == 'rare':
            embed.add_field(name="💎 Rare Find!", value="This powerful item will serve you well in battle!", inline=False)

        embed.add_field(name="Entry Fee", value=f"🪙 {format_number(fee)} Gold", inline=True)
        embed.add_field(name="Item Obtained", value=f"{rarity_emojis.get(item_data['rarity'], '🟢')} {hyper_item}", inline=True)
        embed.set_footer(text="Fee = 20% of your peak gold. Does not shrink with each purchase.")

        await ctx.send(embed=embed)

        if item_data['rarity'] == 'legendary':
            global_embed = create_embed(
                "🌟 LEGENDARY DISCOVERY!",
                f"**{civ.get('name','Unknown')}** has obtained the legendary **{hyper_item}** from the Black Market!",
                discord.Color.gold()
            )
            try:
                await ctx.send(embed=global_embed)
            except Exception:
                pass

        self.db.log_event(user_id, "black_market", "Black Market Purchase",
                         f"Obtained {hyper_item} ({item_data['rarity']}) for {fee} gold")

    # =================================================================
    # INVENTORY
    # =================================================================

    @commands.hybrid_command(name='inventory')
    async def view_inventory(self, ctx):
        user_id = str(ctx.author.id if not isinstance(ctx, discord.Interaction) else ctx.user.id)
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need to start a civilization first! Use `.start <name>`")
            return

        hyper_items = civ.get('hyper_items', [])
        bonuses = civ.get('bonuses', {})

        embed = create_embed(
            f"🎒 {civ.get('name','Unknown')} Inventory",
            f"Leader: {ctx.author.name if not isinstance(ctx, discord.Interaction) else ctx.user.name}",
            discord.Color.blue()
        )

        if hyper_items:
            item_list = []
            for item in hyper_items:
                if item in self.hyperitem_pool:
                    item_data = self.hyperitem_pool[item]
                    rarity_emoji = {
                        "common": "🟢",
                        "uncommon": "🔵",
                        "rare": "🟣",
                        "epic": "🟪",
                        "legendary": "🟡"
                    }.get(item_data['rarity'], "🟢")
                    item_list.append(f"{rarity_emoji} **{item}** - `.{item_data['command']}`")
            embed.add_field(
                name="🎁 HyperItems",
                value="\n".join(item_list) if item_list else "No HyperItems",
                inline=False
            )
        else:
            embed.add_field(name="🎁 HyperItems", value="No HyperItems", inline=False)

        if bonuses:
            upgrades = []
            for bonus_key in bonuses.keys():
                for item_data in self.store_items.values():
                    if bonus_key in item_data['effect']:
                        upgrade_name = f"✅ {item_data['name']}"
                        if upgrade_name not in upgrades:
                            upgrades.append(upgrade_name)
                        break
            if upgrades:
                embed.add_field(name="🏪 Store Upgrades", value="\n".join(upgrades), inline=False)

        if not hyper_items and not bonuses:
            embed.add_field(
                name="Empty Inventory",
                value="Visit the `.store` for upgrades or try the `.blackmarket` for HyperItems!",
                inline=False
            )

        await ctx.send(embed=embed)

    # =================================================================
    # MARKET INFO
    # =================================================================

    @commands.hybrid_command(name='market')
    async def market_info(self, ctx):
        embed = create_embed(
            "🕴️ Black Market Information",
            "A shadowy organization dealing in rare and powerful artifacts...",
            discord.Color.dark_gray()
        )
        embed.add_field(
            name="💰 Entry Fee",
            value="**20% of your PEAK gold (minimum 1,000)**\n*Does not compound — the fee stays the same for every purchase, no shrinking.*",
            inline=False
        )
        embed.add_field(
            name="⏰ Cooldown",
            value="No cooldown! Purchase as often as you can afford!",
            inline=True
        )
        embed.add_field(
            name="🎲 Drop Rates",
            value=("🟢 Common: ~30%\n"
                   "🔵 Uncommon: ~34%\n"
                   "🟣 Rare: ~22%\n"
                   "🟪 Epic: ~5%\n"
                   "🟡 Legendary: ~5%"),
            inline=False
        )
        embed.add_field(
            name="🎁 HyperItem Types",
            value=("• **Weapons**: Nuclear Warhead, HyperLaser, Missiles, Dagger\n"
                   "• **Tools**: Lucky Charm, Ancient Scroll, Gold Mint, Harvest Engine\n"
                   "• **Support**: Anti-Nuke Shield, Mirror, Spy Network, Propaganda Kit, Mercenary Contract, Sacrifice"),
            inline=False
        )
        embed.add_field(
            name="⚠️ Warning",
            value="All sales are final! No choice in what you receive — it's all RNG!",
            inline=False
        )
        embed.add_field(
            name="Usage",
            value="Use `.blackmarket` to make a purchase\nUse `.inventory` to check your items",
            inline=False
        )
        await ctx.send(embed=embed)


async def setup(bot):
    await bot.add_cog(StoreCommands(bot))
