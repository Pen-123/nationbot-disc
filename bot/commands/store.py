import random
import discord
from discord.ext import commands
from discord import app_commands
import logging
from typing import Literal, Optional
from bot.utils import format_number, check_cooldown_decorator, create_embed
from bot import config

logger = logging.getLogger(__name__)

class StoreCommands(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.db = bot.db
        self.civ_manager = bot.civ_manager
        
        # Store items with costs and effects (can be moved to config later)
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
        
        # Black Market HyperItems with drop rates
        self.hyperitem_pool = {
            # Common (30-40%)
            "Lucky Charm": {
                "rarity": "common",
                "weight": 35,
                "description": "Guarantees critical success on next action",
                "command": "luckystrike"
            },
            "Propaganda Kit": {
                "rarity": "common", 
                "weight": 35,
                "description": "Steal soldiers from enemy civilizations",
                "command": "propaganda"
            },
            "Mercenary Contract": {
                "rarity": "common",
                "weight": 30,
                "description": "Instantly hire professional soldiers",
                "command": "hiremercs"
            },
            # Uncommon (20%)
            "Spy Network": {
                "rarity": "uncommon",
                "weight": 20,
                "description": "Elite espionage mission with high success rate",
                "command": "superspy"
            },
            "Ancient Scroll": {
                "rarity": "uncommon",
                "weight": 20,
                "description": "Instantly advance technology level",
                "command": "boosttech"
            },
            "Gold Mint": {
                "rarity": "uncommon",
                "weight": 20,
                "description": "Generate large amounts of gold instantly",
                "command": "mintgold"
            },
            "Harvest Engine": {
                "rarity": "uncommon",
                "weight": 20,
                "description": "Massive instant food production",
                "command": "superharvest"
            },
            # Rare (8%)
            "Nuclear Warhead": {
                "rarity": "rare",
                "weight": 8,
                "description": "Devastating nuclear attack on enemy cities",
                "command": "nuke"
            },
            "Dagger": {
                "rarity": "rare",
                "weight": 8,
                "description": "Assassination attempt on enemy leaders",
                "command": "backstab"
            },
            "Missiles": {
                "rarity": "rare",
                "weight": 8,
                "description": "Mid-tier military strike capability",
                "command": "bomb"
            },
            # Legendary (1-2%)
            "HyperLaser": {
                "rarity": "legendary",
                "weight": 1,
                "description": "Complete civilization obliteration weapon",
                "command": "obliterate"
            },
            "Tech Core": {
                "rarity": "legendary",
                "weight": 1,
                "description": "Advance multiple technology levels instantly",
                "command": "megainvent"
            },
            "Anti-Nuke Shield": {
                "rarity": "epic",           # changed from legendary
                "weight": 6,                # increased
                "description": "Blocks one nuclear attack completely",
                "command": "shield"
            }
        }

    def _ensure_history_keys(self, history: dict) -> dict:
        """Ensure all required keys exist in black market history."""
        defaults = {
            'total_purchases': 0,
            'since_uncommon': 0,
            'since_rare': 0,
            'since_legendary': 0
        }
        for key, value in defaults.items():
            if key not in history:
                history[key] = value
        return history

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
        """View the civilization store and purchase upgrades"""
        user_id = str(ctx.author.id if not isinstance(ctx, discord.Interaction) else ctx.user.id)
        civ = self.civ_manager.get_civilization(user_id)
        
        if not civ:
            await ctx.send("❌ You need to start a civilization first! Use `.start <name>`")
            return
            
        if not item:
            # Display store catalog
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
            
        # Purchase specific item
        if item not in self.store_items:
            await ctx.send(f"❌ Item '{item}' not found in store! Use `.store` to see available items.")
            return
            
        item_data = self.store_items[item]
        
        # Check if already purchased (simplified)
        bonuses = civ.get('bonuses', {})
        if any(effect_key in bonuses for effect_key in item_data['effect'].keys()):
            await ctx.send(f"❌ You already own {item_data['name']} or a similar upgrade!")
            return
            
        # Check if can afford
        if not self.civ_manager.can_afford(user_id, item_data['cost']):
            cost_str = ", ".join([f"{format_number(amt)} {res}" for res, amt in item_data['cost'].items()])
            await ctx.send(f"❌ Cannot afford {item_data['name']}! Requires: {cost_str}")
            return
            
        # Process purchase
        self.civ_manager.spend_resources(user_id, item_data['cost'])
        
        # Apply permanent bonuses
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

    @commands.hybrid_command(name='blackmarket')
    async def black_market(self, ctx):
        """Enter the black market to purchase random HyperItems (No cooldown)"""
        user_id = str(ctx.author.id if not isinstance(ctx, discord.Interaction) else ctx.user.id)
        civ = self.civ_manager.get_civilization(user_id)
        
        if not civ:
            await ctx.send("❌ You need to start a civilization first! Use `.start <name>`")
            return
            
        # Black market entry fee - from config
        entry_fee = {"gold": config.BLACK_MARKET["entry_cost"]}
        
        if not self.civ_manager.can_afford(user_id, entry_fee):
            await ctx.send(f"❌ Black Market entry fee: {config.BLACK_MARKET['entry_cost']} gold! You cannot afford it.")
            return
            
        # Pay entry fee
        self.civ_manager.spend_resources(user_id, entry_fee)
        
        # Get user's black market history
        raw_history = civ.get('black_market_history', {})
        black_market_history = self._ensure_history_keys(raw_history)
        
        # Update purchase counts
        black_market_history['total_purchases'] += 1
        black_market_history['since_uncommon'] += 1
        black_market_history['since_rare'] += 1
        black_market_history['since_legendary'] += 1
        
        # Check pity system (from config)
        forced_rarity = None
        pity_message = ""
        
        if black_market_history['since_legendary'] >= config.BLACK_MARKET["pity_legendary"]:
            forced_rarity = "legendary"
            pity_message = f"🎉 **PITY SYSTEM ACTIVATED!** After {config.BLACK_MARKET['pity_legendary']} purchases, you're guaranteed a LEGENDARY item!"
            black_market_history['since_legendary'] = 0
        elif black_market_history['since_rare'] >= config.BLACK_MARKET["pity_rare"]:
            forced_rarity = "rare"
            pity_message = f"💎 **PITY SYSTEM!** After {config.BLACK_MARKET['pity_rare']} purchases, you're guaranteed a RARE item!"
            black_market_history['since_rare'] = 0
        elif black_market_history['since_uncommon'] >= config.BLACK_MARKET["pity_uncommon"]:
            forced_rarity = "uncommon"
            pity_message = f"🔵 **PITY SYSTEM!** After {config.BLACK_MARKET['pity_uncommon']} purchases, you're guaranteed an UNCOMMON item!"
            black_market_history['since_uncommon'] = 0
        
        # Roll for HyperItem (with pity system if applicable)
        if forced_rarity:
            hyper_item = self._roll_hyperitem_with_pity(forced_rarity)
        else:
            hyper_item = self._roll_hyperitem()
        
        # Update pity counters based on actual rarity obtained
        item_data = self.hyperitem_pool[hyper_item]
        actual_rarity = item_data['rarity']
        
        if actual_rarity in ['uncommon', 'rare', 'legendary']:
            black_market_history['since_uncommon'] = 0
        if actual_rarity in ['rare', 'legendary']:
            black_market_history['since_rare'] = 0
        if actual_rarity == 'legendary':
            black_market_history['since_legendary'] = 0
        
        # Add to user's collection
        self.civ_manager.add_hyper_item(user_id, hyper_item)
        
        # Update black market history
        self.civ_manager.db.update_civilization(user_id, {'black_market_history': black_market_history})
        
        # Create dramatic reveal embed
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
            "epic": "🟣",
            "legendary": "🟡"
        }
        
        embed = create_embed(
            "🕴️ Black Market Transaction",
            "The shadowy dealer hands you a mysterious package...",
            rarity_colors.get(item_data['rarity'], discord.Color.dark_gray())
        )
        
        if pity_message:
            embed.add_field(name="Pity System", value=pity_message, inline=False)
        
        embed.add_field(
            name=f"{rarity_emojis.get(item_data['rarity'], '🟢')} {hyper_item}",
            value=f"**Rarity**: {item_data['rarity'].capitalize()}\n**Description**: {item_data['description']}\n**Command**: `.{item_data['command']}`",
            inline=False
        )
        
        embed.add_field(
            name="Purchase Stats", 
            value=f"Total Purchases: {black_market_history['total_purchases']}\n"
                  f"Since Uncommon: {black_market_history['since_uncommon']}/{config.BLACK_MARKET['pity_uncommon']}\n"
                  f"Since Rare: {black_market_history['since_rare']}/{config.BLACK_MARKET['pity_rare']}\n"
                  f"Since Legendary: {black_market_history['since_legendary']}/{config.BLACK_MARKET['pity_legendary']}",
            inline=True
        )
        
        if item_data['rarity'] == 'legendary':
            embed.add_field(name="🌟 LEGENDARY ITEM!", value="You have obtained an extremely rare and powerful artifact!", inline=False)
        elif item_data['rarity'] == 'rare':
            embed.add_field(name="💎 Rare Find!", value="This powerful item will serve you well in battle!", inline=False)
            
        embed.add_field(name="Entry Fee", value=f"🪙 {config.BLACK_MARKET['entry_cost']} Gold", inline=True)
        embed.add_field(name="Item Obtained", value=f"{rarity_emojis.get(item_data['rarity'], '🟢')} {hyper_item}", inline=True)
        
        await ctx.send(embed=embed)
        
        # Global announcement for legendary items
        if item_data['rarity'] == 'legendary':
            global_embed = create_embed(
                "🌟 LEGENDARY DISCOVERY!",
                f"**{civ.get('name','Unknown')}** has obtained the legendary **{hyper_item}** from the Black Market!",
                discord.Color.gold()
            )
            try:
                await ctx.send(embed=global_embed)
            except:
                pass
                
        self.db.log_event(user_id, "black_market", "Black Market Purchase", 
                         f"Obtained {hyper_item} ({item_data['rarity']}) - Total: {black_market_history['total_purchases']}")

    def _roll_hyperitem(self) -> str:
        """Roll for a random HyperItem based on drop rates"""
        weighted_items = []
        for item_name, item_data in self.hyperitem_pool.items():
            weighted_items.extend([item_name] * item_data['weight'])
        return random.choice(weighted_items)

    def _roll_hyperitem_with_pity(self, forced_rarity: str) -> str:
        """Roll for a HyperItem with forced rarity (pity system)"""
        items_of_rarity = [item for item, data in self.hyperitem_pool.items() if data['rarity'] == forced_rarity]
        if not items_of_rarity:
            return self._roll_hyperitem()
        return random.choice(items_of_rarity)

    @commands.hybrid_command(name='inventory')
    async def view_inventory(self, ctx):
        """View your HyperItems and store upgrades"""
        user_id = str(ctx.author.id if not isinstance(ctx, discord.Interaction) else ctx.user.id)
        civ = self.civ_manager.get_civilization(user_id)
        
        if not civ:
            await ctx.send("❌ You need to start a civilization first! Use `.start <name>`")
            return
            
        hyper_items = civ.get('hyper_items', [])
        bonuses = civ.get('bonuses', {})
        raw_history = civ.get('black_market_history', {})
        black_market_history = self._ensure_history_keys(raw_history)
        
        embed = create_embed(
            f"🎒 {civ.get('name','Unknown')} Inventory",
            f"Leader: {ctx.author.name if not isinstance(ctx, discord.Interaction) else ctx.user.name}",
            discord.Color.blue()
        )
        
        # HyperItems section
        if hyper_items:
            item_list = []
            for item in hyper_items:
                if item in self.hyperitem_pool:
                    item_data = self.hyperitem_pool[item]
                    rarity_emoji = {
                        "common": "🟢",
                        "uncommon": "🔵", 
                        "rare": "🟣",
                        "epic": "🟣",
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
            
        # Store upgrades section
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
        
        # Black Market stats
        if black_market_history:
            until_uncommon = max(0, config.BLACK_MARKET["pity_uncommon"] - black_market_history.get('since_uncommon', 0))
            until_rare = max(0, config.BLACK_MARKET["pity_rare"] - black_market_history.get('since_rare', 0))
            until_legendary = max(0, config.BLACK_MARKET["pity_legendary"] - black_market_history.get('since_legendary', 0))
            embed.add_field(
                name="🕴️ Black Market Stats",
                value=(
                    f"Total Purchases: {black_market_history.get('total_purchases', 0)}\n"
                    f"Until Uncommon: {until_uncommon}/{config.BLACK_MARKET['pity_uncommon']}\n"
                    f"Until Rare: {until_rare}/{config.BLACK_MARKET['pity_rare']}\n"
                    f"Until Legendary: {until_legendary}/{config.BLACK_MARKET['pity_legendary']}"
                ),
                inline=False
            )
        
        if not hyper_items and not bonuses:
            embed.add_field(
                name="Empty Inventory", 
                value="Visit the `.store` for upgrades or try the `.blackmarket` for HyperItems!",
                inline=False
            )
            
        await ctx.send(embed=embed)

    @commands.hybrid_command(name='market')
    async def market_info(self, ctx):
        """Display information about the Black Market"""
        embed = create_embed(
            "🕴️ Black Market Information",
            "A shadowy organization dealing in rare and powerful artifacts...",
            discord.Color.dark_gray()
        )
        
        embed.add_field(
            name="💰 Entry Fee",
            value=f"{config.BLACK_MARKET['entry_cost']} Gold per transaction",
            inline=True
        )
        
        embed.add_field(
            name="⏰ Cooldown",
            value="No cooldown! Purchase as often as you can afford!",
            inline=True
        )
        
        embed.add_field(
            name="🎲 Drop Rates",
            value="🟢 Common: 30-40%\n🔵 Uncommon: 20%\n🟣 Rare: 8%\n🟡 Legendary: 1-2%",
            inline=False
        )
        
        embed.add_field(
            name="🎁 Pity System",
            value=f"**Guaranteed drops after certain purchases:**\n"
                  f"• 🔵 Uncommon: Every {config.BLACK_MARKET['pity_uncommon']} purchases\n"
                  f"• 🟣 Rare: Every {config.BLACK_MARKET['pity_rare']} purchases\n"
                  f"• 🟡 Legendary: Every {config.BLACK_MARKET['pity_legendary']} purchases\n"
                  f"*Counters reset when you hit the pity or when you naturally roll that rarity.*",
            inline=False
        )
        
        embed.add_field(
            name="🎁 HyperItem Types",
            value="• **Weapons**: Nuclear Warhead, HyperLaser, Missiles, Dagger\n"
                  "• **Tools**: Lucky Charm, Ancient Scroll, Gold Mint, Harvest Engine\n"
                  "• **Support**: Anti-Nuke Shield, Spy Network, Propaganda Kit, Mercenary Contract",
            inline=False
        )
        
        embed.add_field(
            name="⚠️ Warning",
            value="All sales are final! No choice in what you receive - it's all RNG!",
            inline=False
        )
        
        embed.add_field(
            name="Usage",
            value="Use `.blackmarket` to make a purchase\nUse `.inventory` to check your pity progress",
            inline=False
        )
        
        await ctx.send(embed=embed)


async def setup(bot):
    await bot.add_cog(StoreCommands(bot))
