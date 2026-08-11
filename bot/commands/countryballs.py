import discord
from discord.ext import commands
from discord import app_commands
import json
import logging
import random
import os
from datetime import datetime
from typing import Optional, Dict, List, Set
from bot.utils import create_embed, format_number

logger = logging.getLogger(__name__)

# -------------------------------------------------------------------
# DATA
# -------------------------------------------------------------------

# Map subregion -> primary countryball (used for auto-unlock)
REGION_TO_COUNTRYBALL = {
    "Eastern Europe": "soviet_union",
    "Western Europe": "france",        # or "german_empire"? we'll use france as default
    "Southern Europe": "italy",
    "Northern Europe": "british_empire",
    "Central Asia": "soviet_union",    # placeholder
    "East Asia": "japanese_empire",
    "South Asia": "british_empire",
    "Southeast Asia": "japanese_empire",
    "Middle East": "ottoman_empire",
    "North Africa": "ottoman_empire",
    "West Africa": "france",
    "Central Africa": "france",
    "East Africa": "british_empire",
    "Southern Africa": "british_empire",
    "Western North America": "america",
    "Central North America": "america",
    "Eastern North America": "america",
    "Mexico": "america",
    "Central America": "america",
    "Northern South America": "america",
    "Western South America": "america",
    "Eastern South America": "america",
    "Brazil": "america",
    "Southern Cone": "america",
    "Australia": "british_empire",
    "New Zealand": "british_empire",
    "Pacific Islands": "british_empire",
    "Antarctic Peninsula": None,
    "East Antarctica": None,
    "West Antarctica": None,
}

# Full countryball definitions
COUNTRYBALLS = {
    "china": {
        "name": "China",
        "continent": "Asia",
        "flag_colors": ["#ff0000", "#ffff00"],
        "power_rank": 3,
        "image_file": "china.png",
        "evolution": {"base": None, "condition": None},  # no evolution
        "synergy_group": "axis",  # Axis powers
        "modifiers": {"military": 1.10, "production": 1.05}
    },
    "reich": {
        "name": "German Reich",
        "continent": "Europe",
        "flag_colors": ["#000000", "#ffffff", "#ff0000"],
        "power_rank": 2,
        "image_file": "reich.jpg",
        "evolution": {"base": None, "condition": None},
        "synergy_group": "axis",
        "modifiers": {"soldier_training": 1.25, "tech": 1.10}
    },
    "america": {
        "name": "United States",
        "continent": "North America",
        "flag_colors": ["#ff0000", "#ffffff", "#0000ff"],
        "power_rank": 1,
        "image_file": "america.jpg",
        "evolution": {"base": None, "condition": None},
        "synergy_group": "allies",
        "modifiers": {"industry": 1.20, "trade": 1.15}
    },
    "austria-hungary": {
        "name": "Austria-Hungary",
        "continent": "Europe",
        "flag_colors": ["#ff0000", "#ffffff", "#006600"],
        "power_rank": 4,
        "image_file": "austria-hungary.png",
        "evolution": {"base": None, "condition": None},
        "synergy_group": "central_powers",
        "modifiers": {"population_growth": 1.15, "happiness": 1.10}
    },
    "british_empire": {
        "name": "British Empire",
        "continent": "Europe",
        "flag_colors": ["#ff0000", "#ffffff", "#0000ff"],
        "power_rank": 1,
        "image_file": "british empire.jpg",
        "evolution": {"base": "united_kingdom", "condition": "Own all provinces in Western Europe, Southern Europe, and Eastern North America"},
        "synergy_group": "allies",
        "modifiers": {"trade": 1.30, "diplomacy": 1.20, "naval": 1.25}
    },
    "france": {
        "name": "France",
        "continent": "Europe",
        "flag_colors": ["#0000ff", "#ffffff", "#ff0000"],
        "power_rank": 2,
        "image_file": "france.png",
        "evolution": {"base": None, "condition": None},
        "synergy_group": "allies",
        "modifiers": {"diplomacy": 1.15, "culture": 1.10}
    },
    "german_empire": {
        "name": "German Empire",
        "continent": "Europe",
        "flag_colors": ["#000000", "#ffffff", "#ff0000"],
        "power_rank": 2,
        "image_file": "german empire.jpg",
        "evolution": {"base": "reich", "condition": "Own all provinces in Western Europe and Eastern Europe"},
        "synergy_group": "central_powers",
        "modifiers": {"soldier_training": 1.30, "tech": 1.15, "industry": 1.10}
    },
    "italy": {
        "name": "Kingdom of Italy",
        "continent": "Europe",
        "flag_colors": ["#009246", "#ffffff", "#ce2b37"],
        "power_rank": 3,
        "image_file": "italy.jpg",
        "evolution": {"base": None, "condition": None},
        "synergy_group": "axis",
        "modifiers": {"naval": 1.20, "trade": 1.10}
    },
    "japanese_empire": {
        "name": "Japanese Empire",
        "continent": "Asia",
        "flag_colors": ["#ff0000", "#ffffff"],
        "power_rank": 2,
        "image_file": "japanese empire.jpg",
        "evolution": {"base": "japan", "condition": "Own all provinces in East Asia and Southeast Asia"},
        "synergy_group": "axis",
        "modifiers": {"military": 1.20, "naval": 1.30}
    },
    "north_korea": {
        "name": "North Korea",
        "continent": "Asia",
        "flag_colors": ["#ff0000", "#ffffff", "#0000ff"],
        "power_rank": 5,
        "image_file": "north korea.jpg",
        "evolution": {"base": None, "condition": None},
        "synergy_group": "axis",  # loosely
        "modifiers": {"military": 1.05, "unrest": -0.10}
    },
    "ottoman_empire": {
        "name": "Ottoman Empire",
        "continent": "Asia",
        "flag_colors": ["#ff0000", "#ffffff", "#006600"],
        "power_rank": 3,
        "image_file": "ottoman empire.jpg",
        "evolution": {"base": "turkey", "condition": "Own all provinces in Middle East, North Africa, and Eastern Europe"},
        "synergy_group": "central_powers",
        "modifiers": {"trade": 1.20, "culture": 1.15, "happiness": 1.10}
    },
    "soviet_union": {
        "name": "Soviet Union",
        "continent": "Europe/Asia",
        "flag_colors": ["#ff0000", "#ffff00"],
        "power_rank": 1,
        "image_file": "soviet union.jpg",
        "evolution": {"base": "russia", "condition": "Own all provinces in Eastern Europe and Central Asia"},
        "synergy_group": "allies",
        "modifiers": {"production": 1.30, "military": 1.25, "tech": 1.15}
    },
    "taiwan": {
        "name": "Taiwan",
        "continent": "Asia",
        "flag_colors": ["#ff0000", "#ffffff", "#0000ff"],
        "power_rank": 4,
        "image_file": "taiwan.jpg",
        "evolution": {"base": None, "condition": None},
        "synergy_group": "allies",
        "modifiers": {"tech": 1.15, "trade": 1.10}
    }
}

# Synergy definitions
SYNERGIES = {
    "axis": {
        "name": "Axis Powers",
        "members": ["reich", "italy", "japanese_empire", "north_korea"],
        "bonuses": {"military": 0.20, "soldier_training": 0.15}
    },
    "allies": {
        "name": "Allied Powers",
        "members": ["america", "british_empire", "france", "soviet_union", "taiwan"],
        "bonuses": {"trade": 0.25, "diplomacy": 0.20, "industry": 0.15}
    },
    "central_powers": {
        "name": "Central Powers",
        "members": ["austria-hungary", "german_empire", "ottoman_empire"],
        "bonuses": {"soldier_training": 0.25, "tech": 0.20, "industry": 0.15}
    }
}

# -------------------------------------------------------------------
# COUNTRYBALL MANAGER
# -------------------------------------------------------------------

class CountryballManager:
    def __init__(self, db, bot):
        self.db = db
        self.bot = bot
        self.images_path = "images"  # folder containing countryball images
        self._init_tables()

    def _init_tables(self):
        conn = self.db.get_connection()
        cursor = conn.cursor()
        # Table for player collection
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS player_countryballs (
                user_id TEXT,
                countryball_id TEXT,
                unlocked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT 0,
                evolution_stage TEXT DEFAULT 'base',
                PRIMARY KEY (user_id, countryball_id)
            )
        ''')
        # Table for active managers (max 3)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS active_managers (
                user_id TEXT,
                countryball_id TEXT,
                PRIMARY KEY (user_id, countryball_id)
            )
        ''')
        conn.commit()

    # ---- Collection management ----
    def unlock_countryball(self, user_id: str, ball_id: str) -> bool:
        """Add a countryball to a player's collection if not already owned."""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT 1 FROM player_countryballs WHERE user_id = ? AND countryball_id = ?', (user_id, ball_id))
        if cursor.fetchone():
            return False  # already unlocked
        cursor.execute('INSERT INTO player_countryballs (user_id, countryball_id) VALUES (?, ?)', (user_id, ball_id))
        conn.commit()
        return True

    def get_collection(self, user_id: str) -> List[Dict]:
        """Return list of unlocked countryballs with their data."""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT countryball_id, unlocked_at, is_active, evolution_stage FROM player_countryballs WHERE user_id = ?', (user_id,))
        rows = cursor.fetchall()
        collection = []
        for row in rows:
            ball_data = COUNTRYBALLS.get(row['countryball_id'])
            if ball_data:
                item = {
                    'id': row['countryball_id'],
                    'name': ball_data['name'],
                    'unlocked_at': row['unlocked_at'],
                    'is_active': bool(row['is_active']),
                    'evolution_stage': row['evolution_stage'],
                    'image_file': ball_data['image_file'],
                    'continent': ball_data['continent'],
                    'flag_colors': ball_data['flag_colors'],
                    'power_rank': ball_data['power_rank'],
                    'modifiers': ball_data['modifiers'],
                    'synergy_group': ball_data['synergy_group']
                }
                collection.append(item)
        return collection

    def get_active_managers(self, user_id: str) -> List[str]:
        """Return list of active countryball IDs (max 3)."""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT countryball_id FROM active_managers WHERE user_id = ?', (user_id,))
        return [row['countryball_id'] for row in cursor.fetchall()]

    def activate(self, user_id: str, ball_id: str) -> bool:
        """Activate a countryball if in collection and under limit."""
        collection = self.get_collection(user_id)
        if ball_id not in [c['id'] for c in collection]:
            return False
        active = self.get_active_managers(user_id)
        if len(active) >= 3:
            return False
        if ball_id in active:
            return False  # already active
        conn = self.db.get_connection()
        cursor = conn.cursor()
        cursor.execute('UPDATE player_countryballs SET is_active = 1 WHERE user_id = ? AND countryball_id = ?', (user_id, ball_id))
        cursor.execute('INSERT INTO active_managers (user_id, countryball_id) VALUES (?, ?)', (user_id, ball_id))
        conn.commit()
        return True

    def deactivate(self, user_id: str, ball_id: str) -> bool:
        """Deactivate a countryball."""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        cursor.execute('UPDATE player_countryballs SET is_active = 0 WHERE user_id = ? AND countryball_id = ?', (user_id, ball_id))
        cursor.execute('DELETE FROM active_managers WHERE user_id = ? AND countryball_id = ?', (user_id, ball_id))
        conn.commit()
        return True

    def check_evolution(self, user_id: str, ball_id: str, territory_cog) -> bool:
        """Check if evolution condition is met and upgrade if possible."""
        ball_def = COUNTRYBALLS.get(ball_id)
        if not ball_def or not ball_def['evolution']['condition']:
            return False
        condition = ball_def['evolution']['condition']
        # For now, we only support territory-based conditions
        # Example: "Own all provinces in Western Europe and Eastern Europe"
        # We'll parse the condition string. Simpler: implement a generic evaluator.
        # We'll use a simplified check: if the condition mentions subregions, we check if the player owns all provinces in those subregions.
        # This is a placeholder; we'll just check if the player owns all provinces in their region.
        # For advanced, we'll parse and check.
        # We'll implement a simple version: if condition contains "Own all provinces in X", we check that.
        import re
        subregions = re.findall(r"in ([A-Za-z ]+)", condition)
        if not subregions:
            return False
        owned = territory_cog._get_owned_provinces(user_id)
        for sub in subregions:
            sub = sub.strip()
            # find matching subregion key (case-insensitive)
            match = None
            for key in PROVINCES.keys():
                if key.lower() == sub.lower():
                    match = key
                    break
            if not match:
                return False
            provinces_in_sub = PROVINCES[match]
            if not all(p in owned for p in provinces_in_sub):
                return False
        # If all conditions passed, evolve
        conn = self.db.get_connection()
        cursor = conn.cursor()
        cursor.execute('UPDATE player_countryballs SET evolution_stage = "evolved" WHERE user_id = ? AND countryball_id = ?', (user_id, ball_id))
        conn.commit()
        return True

    def get_synergy_bonuses(self, user_id: str) -> Dict[str, float]:
        """Calculate active synergy bonuses based on active managers."""
        active = self.get_active_managers(user_id)
        active_groups = set()
        for ball_id in active:
            ball_def = COUNTRYBALLS.get(ball_id)
            if ball_def:
                active_groups.add(ball_def['synergy_group'])
        bonuses = {}
        for group, synergy in SYNERGIES.items():
            if group in active_groups:
                members = synergy['members']
                # Check how many members are active
                active_members = [b for b in active if b in members]
                if len(active_members) >= 2:  # at least 2 for synergy
                    for key, val in synergy['bonuses'].items():
                        bonuses[key] = bonuses.get(key, 0) + val
        return bonuses

# -------------------------------------------------------------------
# DISCORD COG
# -------------------------------------------------------------------

class CountryballCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.db = bot.db
        self.civ_manager = bot.civ_manager
        self.ball_manager = CountryballManager(self.db, bot)
        self.territory_cog = None  # set later

    async def cog_load(self):
        """After all cogs loaded, get reference to TerritoryCog."""
        self.territory_cog = self.bot.get_cog("TerritoryCog")
        # If territory cog is missing, log warning
        if not self.territory_cog:
            logger.warning("TerritoryCog not found; countryball auto-unlock on region completion will not work.")

    # ---- Helper: reveal embed sequence ----
    async def _reveal_countryball(self, ctx, ball_id: str, user_id: str):
        """Post progressive reveal of a countryball (4 embeds)."""
        ball_def = COUNTRYBALLS.get(ball_id)
        if not ball_def:
            await ctx.send("❌ Unknown countryball.")
            return

        # Ensure the image exists
        image_path = os.path.join(self.ball_manager.images_path, ball_def['image_file'])
        if not os.path.exists(image_path):
            await ctx.send(f"❌ Image file `{ball_def['image_file']}` not found.")
            return

        # Stage 1: Continent
        embed1 = discord.Embed(
            title="🌍 **A New Power Rises!**",
            description=f"From the continent of **{ball_def['continent']}**...",
            color=discord.Color.blue()
        )
        embed1.set_image(url="https://cdn.discordapp.com/attachments/...")  # placeholder or use a continent icon
        # We can't set image to a local file in embed, so we'll send as file attachments later.
        # Instead, we'll send a plain embed with emoji and then attach the file at the final stage.
        # We'll keep it simple: send text embeds and then a final image.

        # Stage 2: Flag colors
        colors_str = " ".join([f"`{c}`" for c in ball_def['flag_colors']])
        embed2 = discord.Embed(
            title=f"🎨 **Colors of {ball_def['name']}**",
            description=f"Its flag bears the colors: {colors_str}",
            color=discord.Color.gold()
        )

        # Stage 3: Power Rank
        rank = ball_def['power_rank']
        rank_emoji = "👑" if rank == 1 else ("🥈" if rank == 2 else ("🥉" if rank == 3 else "🏅"))
        embed3 = discord.Embed(
            title=f"{rank_emoji} **Rank #{rank} – {ball_def['name']}**",
            description=f"This power is one of the most influential of its time.",
            color=discord.Color.purple()
        )

        # Stage 4: Full reveal with image
        embed4 = discord.Embed(
            title=f"**{ball_def['name']}** Unlocked!",
            description=f"Added to your collection! {self._format_modifiers(ball_def['modifiers'])}",
            color=discord.Color.green()
        )
        file = discord.File(image_path, filename=ball_def['image_file'])
        embed4.set_image(url=f"attachment://{ball_def['image_file']}")

        # Send stages in order with small delays (simulate progressive reveal)
        await ctx.send(embed=embed1)
        await asyncio.sleep(1)
        await ctx.send(embed=embed2)
        await asyncio.sleep(1)
        await ctx.send(embed=embed3)
        await asyncio.sleep(1.5)
        await ctx.send(embed=embed4, file=file)

    def _format_modifiers(self, modifiers):
        lines = []
        for key, val in modifiers.items():
            sign = "+" if val > 0 else ""
            lines.append(f"**{key.replace('_',' ').title()}:** {sign}{int(val*100)}%")
        return "\n".join(lines)

    # ---- Commands ----
    @commands.command(name='packs')
    async def packs_list(self, ctx):
        """View your unlocked countryballs and collection progress."""
        user_id = str(ctx.author.id)
        collection = self.ball_manager.get_collection(user_id)
        if not collection:
            await ctx.send("📦 You haven't unlocked any countryballs yet. Conquer regions to find them!")
            return

        embed = discord.Embed(title="📦 Your Countryball Collection", color=discord.Color.blue())
        active = self.ball_manager.get_active_managers(user_id)
        for ball in collection:
            status = "✅ Active" if ball['id'] in active else "🔒 Inactive"
            evo = f" ({ball['evolution_stage']})" if ball['evolution_stage'] != 'base' else ""
            embed.add_field(
                name=f"{ball['name']}{evo}",
                value=f"Rank #{ball['power_rank']} | {ball['continent']}\n{status}",
                inline=True
            )
        embed.set_footer(text=f"Total: {len(collection)} | Active managers: {len(active)}/3")
        await ctx.send(embed=embed)

    @commands.command(name='activate')
    @app_commands.describe(ball_name="Name of the countryball to activate")
    async def activate_manager(self, ctx, *, ball_name: str):
        """Activate a countryball as a manager (max 3)."""
        user_id = str(ctx.author.id)
        # Find ball by name (case-insensitive partial match)
        matches = []
        for ball_id, data in COUNTRYBALLS.items():
            if ball_name.lower() in data['name'].lower():
                matches.append((ball_id, data['name']))
        if len(matches) > 1:
            await ctx.send(f"⚠️ Multiple matches: {', '.join([m[1] for m in matches])}. Please be more specific.")
            return
        if not matches:
            await ctx.send("❌ No countryball found with that name.")
            return
        ball_id = matches[0][0]
        if self.ball_manager.activate(user_id, ball_id):
            await ctx.send(f"✅ **{matches[0][1]}** is now an active manager!")
            # Recalculate synergies
            syn_bonuses = self.ball_manager.get_synergy_bonuses(user_id)
            if syn_bonuses:
                bonus_str = ", ".join([f"{k}: +{int(v*100)}%" for k,v in syn_bonuses.items()])
                await ctx.send(f"⚡ **Synergy activated!** {bonus_str}")
        else:
            await ctx.send("❌ Could not activate. Either not owned, already active, or limit of 3 reached.")

    @commands.command(name='deactivate')
    @app_commands.describe(ball_name="Name of the countryball to deactivate")
    async def deactivate_manager(self, ctx, *, ball_name: str):
        """Deactivate a countryball manager."""
        user_id = str(ctx.author.id)
        active = self.ball_manager.get_active_managers(user_id)
        if not active:
            await ctx.send("❌ You have no active managers.")
            return
        # Find by name
        match = None
        for ball_id in active:
            data = COUNTRYBALLS.get(ball_id)
            if data and ball_name.lower() in data['name'].lower():
                match = ball_id
                break
        if not match:
            await ctx.send("❌ No active countryball matches that name.")
            return
        if self.ball_manager.deactivate(user_id, match):
            await ctx.send(f"✅ Deactivated **{COUNTRYBALLS[match]['name']}**.")
        else:
            await ctx.send("❌ Deactivation failed.")

    @commands.command(name='synergies')
    async def show_synergies(self, ctx):
        """Show active synergy bonuses."""
        user_id = str(ctx.author.id)
        bonuses = self.ball_manager.get_synergy_bonuses(user_id)
        if not bonuses:
            await ctx.send("❌ No active synergies. Activate at least 2 countryballs from the same faction.")
            return
        embed = discord.Embed(title="⚡ Active Synergies", color=discord.Color.gold())
        for key, val in bonuses.items():
            embed.add_field(name=key.replace('_',' ').title(), value=f"+{int(val*100)}%", inline=True)
        await ctx.send(embed=embed)

    # ---- Auto-unlock trigger ----
    async def check_region_unlock(self, user_id: str, subregion: str):
        """Called when a player completes a subregion."""
        ball_id = REGION_TO_COUNTRYBALL.get(subregion)
        if not ball_id:
            return
        if self.ball_manager.unlock_countryball(user_id, ball_id):
            # Get context to send reveal? We don't have ctx here. We'll need to find a channel.
            # For now, just log and send a DM.
            try:
                user = await self.bot.fetch_user(int(user_id))
                if user:
                    # We'll create a fake context-like DM channel
                    # But we can't send embeds with files easily in DM without ctx.
                    # We'll send a simple notification and the reveal will be shown when they use .packs
                    await user.send(f"🎉 You unlocked **{COUNTRYBALLS[ball_id]['name']}**! Use `.packs` to see your collection.")
            except:
                pass

# -------------------------------------------------------------------
# SETUP
# -------------------------------------------------------------------

async def setup(bot):
    await bot.add_cog(CountryballCog(bot))
