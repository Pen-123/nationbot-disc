import random
import discord as guilded
from discord.ext import commands
import logging
from bot.utils import format_number, check_cooldown_decorator, create_embed

logger = logging.getLogger(__name__)

class StoreCommands(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.db = bot.db
        self.civ_manager = bot.civ_manager
        
        # Store items with costs and effects
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
                "rarity": "legendary",
                "weight": 2,
                "description": "Blocks one nuclear attack completely",
                "command": "shield"
            }
        }

    @commands.command(name='store')
    async def view_store(self, ctx, item: str = None):
        """View the civilization store and purchase upgrades"""
        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        
        if not civ:
            await ctx.send("❌ You need to start a civilization first! Use `.start <name>`")
            return
            
        if not item:
            # Display store catalog
            embed = create_embed(
                "🏪 Civilization Store",
                "Purchase permanent upgrades for your civilization!",
                guilded.Color.blue()
            )
            
            # Group items by category
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
        
        # Check if already purchased (simplified - could track individual upgrades)
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
            guilded.Color.green()
        )
        
        embed.add_field(name="Description", value=item_data['description'], inline=False)
        
        cost_text = "\n".join([f"{'🪙' if res == 'gold' else '🌾' if res == 'food' else '🪨' if res == 'stone' else '🪵'} {format_number(amt)} {res.capitalize()}" 
                              for res, amt in item_data['cost'].items()])
        embed.add_field(name="Cost", value=cost_text, inline=True)
        embed.add_field(name="Status", value="✅ Upgrade Active", inline=True)
        
        await ctx.send(embed=embed)
        
        # Log the purchase
        self.db.log_event(user_id, "store_purchase", "Store Purchase", f"Purchased {item_data['name']}")

    @commands.command(name='blackmarket')
    async def black_market(self, ctx):
        """Enter the black market to purchase random HyperItems (No cooldown)"""
        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        
        if not civ:
            await ctx.send("❌ You need to start a civilization first! Use `.start <name>`")
            return
            
        # Black market entry fee
        entry_fee = {"gold": 1000}
        
        if not self.civ_manager.can_afford(user_id, entry_fee):
            await ctx.send("❌ Black Market entry fee: 1,000 gold! You cannot afford it.")
            return
            
        # Pay entry fee
        self.civ_manager.spend_resources(user_id, entry_fee)
        
        # Get user's black market history
        black_market_history = civ.get('black_market_history', {
            'total_purchases': 0,
            'since_uncommon': 0,
            'since_rare': 0,
            'since_legendary': 0
        })
        
        # Update purchase counts
        black_market_history['total_purchases'] += 1
        black_market_history['since_uncommon'] += 1
        black_market_history['since_rare'] += 1
        black_market_history['since_legendary'] += 1
        
        # Check pity system
        forced_rarity = None
        pity_message = ""
        
        if black_market_history['since_legendary'] >= 10:
            forced_rarity = "legendary"
            pity_message = "🎉 **PITY SYSTEM ACTIVATED!** After 10 purchases, you're guaranteed a LEGENDARY item!"
            black_market_history['since_legendary'] = 0
        elif black_market_history['since_rare'] >= 6:
            forced_rarity = "rare"
            pity_message = "💎 **PITY SYSTEM!** After 6 purchases, you're guaranteed a RARE item!"
            black_market_history['since_rare'] = 0
        elif black_market_history['since_uncommon'] >= 3:
            forced_rarity = "uncommon"
            pity_message = "🔵 **PITY SYSTEM!** After 3 purchases, you're guaranteed an UNCOMMON item!"
            black_market_history['since_uncommon'] = 0
        
        # Roll for HyperItem (with pity system if applicable)
        if forced_rarity:
            hyper_item = self._roll_hyperitem_with_pity(forced_rarity)
        else:
            hyper_item = self._roll_hyperitem()
        
        # Update pity counters based on actual rarity obtained
        item_data = self.hyperitem_pool[hyper_item]
        actual_rarity = item_data['rarity']
        
        # Reset appropriate counters based on actual rarity
        if actual_rarity in ['uncommon', 'rare', 'legendary']:
            black_market_history['since_uncommon'] = 0
        if actual_rarity in ['rare', 'legendary']:
            black_market_history['since_rare'] = 0
        if actual_rarity == 'legendary':
            black_market_history['since_legendary'] = 0
        
        # Add to user's collection
        self.civ_manager.add_hyper_item(user_id, hyper_item)
        
        # Update black market history in database
        self.civ_manager.db.update_civilization(user_id, {'black_market_history': black_market_history})
        
        # Create dramatic reveal embed
        rarity_colors = {
            "common": guilded.Color.green(),
            "uncommon": guilded.Color.blue(), 
            "rare": guilded.Color.purple(),
            "legendary": guilded.Color.gold()
        }
        
        rarity_emojis = {
            "common": "🟢",
            "uncommon": "🔵", 
            "rare": "🟣",
            "legendary": "🟡"
        }
        
        embed = create_embed(
            "🕴️ Black Market Transaction",
            "The shadowy dealer hands you a mysterious package...",
            rarity_colors[item_data['rarity']]
        )
        
        # Add pity message if applicable
        if pity_message:
            embed.add_field(name="Pity System", value=pity_message, inline=False)
        
        embed.add_field(
            name=f"{rarity_emojis[item_data['rarity']]} {hyper_item}",
            value=f"**Rarity**: {item_data['rarity'].capitalize()}\n**Description**: {item_data['description']}\n**Command**: `.{item_data['command']}`",
            inline=False
        )
        
        # Add purchase stats
        embed.add_field(
            name="Purchase Stats", 
            value=f"Total Purchases: {black_market_history['total_purchases']}\nSince Uncommon: {black_market_history['since_uncommon']}/3\nSince Rare: {black_market_history['since_rare']}/6\nSince Legendary: {black_market_history['since_legendary']}/10",
            inline=True
        )
        
        if item_data['rarity'] == 'legendary':
            embed.add_field(name="🌟 LEGENDARY ITEM!", value="You have obtained an extremely rare and powerful artifact!", inline=False)
        elif item_data['rarity'] == 'rare':
            embed.add_field(name="💎 Rare Find!", value="This powerful item will serve you well in battle!", inline=False)
            
        embed.add_field(name="Entry Fee", value="🪙 1,000 Gold", inline=True)
        embed.add_field(name="Item Obtained", value=f"{rarity_emojis[item_data['rarity']]} {hyper_item}", inline=True)
        
        await ctx.send(embed=embed)
        
        # Global announcement for legendary items
        if item_data['rarity'] == 'legendary':
            global_embed = create_embed(
                "🌟 LEGENDARY DISCOVERY!",
                f"**{civ['name']}** has obtained the legendary **{hyper_item}** from the Black Market!",
                guilded.Color.gold()
            )
            
            # Send to all channels (simplified - in real implementation would track channels)
            try:
                await ctx.send(global_embed)
            except:
                pass
                
        # Log the transaction
        self.db.log_event(user_id, "black_market", "Black Market Purchase", 
                         f"Obtained {hyper_item} ({item_data['rarity']}) - Total: {black_market_history['total_purchases']}")

    def _roll_hyperitem(self) -> str:
        """Roll for a random HyperItem based on drop rates"""
        # Create weighted list
        weighted_items = []
        for item_name, item_data in self.hyperitem_pool.items():
            weighted_items.extend([item_name] * item_data['weight'])
            
        return random.choice(weighted_items)

    def _roll_hyperitem_with_pity(self, forced_rarity: str) -> str:
        """Roll for a HyperItem with forced rarity (pity system)"""
        # Get all items of the forced rarity
        items_of_rarity = [item for item, data in self.hyperitem_pool.items() if data['rarity'] == forced_rarity]
        
        if not items_of_rarity:
            # Fallback to normal roll if no items of that rarity exist
            return self._roll_hyperitem()
            
        return random.choice(items_of_rarity)

    @commands.command(name='inventory')
    async def view_inventory(self, ctx):
        """View your HyperItems and store upgrades"""
        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        
        if not civ:
            await ctx.send("❌ You need to start a civilization first! Use `.start <name>`")
            return
            
        hyper_items = civ.get('hyper_items', [])
        bonuses = civ.get('bonuses', {})
        black_market_history = civ.get('black_market_history', {})
        
        embed = create_embed(
            f"🎒 {civ['name']} Inventory",
            f"Leader: {ctx.author.name}",
            guilded.Color.blue()
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
                        "legendary": "🟡"
                    }[item_data['rarity']]
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
            for bonus_key, bonus_value in bonuses.items():
                if bonus_key.endswith('_bonus') and not bonus_key.endswith('_bonus'):
                    # Find corresponding store item
                    for item_key, item_data in self.store_items.items():
                        if bonus_key in item_data['effect']:
                            upgrades.append(f"✅ {item_data['name']}")
                            break
                            
            if upgrades:
                embed.add_field(name="🏪 Store Upgrades", value="\n".join(upgrades), inline=False)
        
        # Black Market stats
        if black_market_history:
            embed.add_field(
                name="🕴️ Black Market Stats",
                value=f"Total Purchases: {black_market_history.get('total_purchases', 0)}\nUntil Uncommon: {3 - black_market_history.get('since_uncommon', 0)}/3\nUntil Rare: {6 - black_market_history.get('since_rare', 0)}/6\nUntil Legendary: {10 - black_market_history.get('since_legendary', 0)}/10",
                inline=False
            )
        
        if not hyper_items and not bonuses:
            embed.add_field(
                name="Empty Inventory", 
                value="Visit the `.store` for upgrades or try the `.blackmarket` for HyperItems!",
                inline=False
            )
            
        await ctx.send(embed=embed)

    @commands.command(name='market')
    async def market_info(self, ctx):
        """Display information about the Black Market"""
        embed = create_embed(
            "🕴️ Black Market Information",
            "A shadowy organization dealing in rare and powerful artifacts...",
            guilded.Color.dark_gray()
        )
        
        embed.add_field(
            name="💰 Entry Fee",
            value="1,000 Gold per transaction",
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
            value="**Guaranteed drops after certain purchases:**\n• 🔵 Uncommon: Every 3 purchases\n• 🟣 Rare: Every 6 purchases\n• 🟡 Legendary: Every 10 purchases\n*Counters reset when you get that rarity*",
            inline=False
        )
        
        embed.add_field(
            name="🎁 HyperItem Types",
            value="• **Weapons**: Nuclear Warhead, HyperLaser, Missiles, Dagger\n• **Tools**: Lucky Charm, Ancient Scroll, Gold Mint, Harvest Engine\n• **Support**: Anti-Nuke Shield, Spy Network, Propaganda Kit\n• **Military**: Mercenary Contract",
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
