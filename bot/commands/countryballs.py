import discord
from discord.ext import commands
from discord import app_commands
import json
import logging
import random
import os
import asyncio
from datetime import datetime
from typing import Optional, Dict, List, Set
from bot.utils import create_embed, format_number

logger = logging.getLogger(__name__)

# ---- DATA ----
REGION_TO_COUNTRYBALL = {
    "Eastern Europe": "soviet_union",
    "Western Europe": "france",
    "Southern Europe": "italy",
    "Northern Europe": "british_empire",
    "Central Asia": "soviet_union",
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

COUNTRYBALLS = {
    "china": {
        "name": "China",
        "continent": "Asia",
        "flag_colors": ["#ff0000", "#ffff00"],
        "power_rank": 3,
        "image_file": "china.png",
        "evolution": {"base": None, "condition": None},
        "synergy_group": "axis",
        "modifiers": {
            "military": 1.10,
            "production": 1.05,
            "gold": 1.10,
            "stone": 1.15,
            "wood": 1.15,
            "food": 1.05
        }
    },
    "reich": {
        "name": "German Reich",
        "continent": "Europe",
        "flag_colors": ["#000000", "#ffffff", "#ff0000"],
        "power_rank": 2,
        "image_file": "reich.jpg",
        "evolution": {"base": None, "condition": None},
        "synergy_group": "axis",
        "modifiers": {
            "soldier_training": 1.25,
            "tech": 1.10,
            "gold": 1.15,
            "stone": 1.10,
            "wood": 1.10,
            "food": 1.05
        }
    },
    "america": {
        "name": "United States",
        "continent": "North America",
        "flag_colors": ["#ff0000", "#ffffff", "#0000ff"],
        "power_rank": 1,
        "image_file": "america.jpg",
        "evolution": {"base": None, "condition": None},
        "synergy_group": "allies",
        "modifiers": {
            "industry": 1.20,
            "trade": 1.15,
            "gold": 1.20,
            "stone": 1.15,
            "wood": 1.15,
            "food": 1.20
        }
    },
    "austria-hungary": {
        "name": "Austria-Hungary",
        "continent": "Europe",
        "flag_colors": ["#ff0000", "#ffffff", "#006600"],
        "power_rank": 4,
        "image_file": "austria-hungary.png",
        "evolution": {"base": None, "condition": None},
        "synergy_group": "central_powers",
        "modifiers": {
            "population_growth": 1.15,
            "happiness": 1.10,
            "gold": 1.10,
            "stone": 1.05,
            "wood": 1.05,
            "food": 1.10
        }
    },
    "british_empire": {
        "name": "British Empire",
        "continent": "Europe",
        "flag_colors": ["#ff0000", "#ffffff", "#0000ff"],
        "power_rank": 1,
        "image_file": "british empire.jpg",
        "evolution": {"base": "united_kingdom", "condition": "Own all provinces in Western Europe, Southern Europe, and Eastern North America"},
        "synergy_group": "allies",
        "modifiers": {
            "trade": 1.30,
            "diplomacy": 1.20,
            "naval": 1.25,
            "gold": 1.25,
            "stone": 1.15,
            "wood": 1.15,
            "food": 1.15
        }
    },
    "france": {
        "name": "France",
        "continent": "Europe",
        "flag_colors": ["#0000ff", "#ffffff", "#ff0000"],
        "power_rank": 2,
        "image_file": "france.png",
        "evolution": {"base": None, "condition": None},
        "synergy_group": "allies",
        "modifiers": {
            "diplomacy": 1.15,
            "culture": 1.10,
            "gold": 1.15,
            "stone": 1.10,
            "wood": 1.10,
            "food": 1.15
        }
    },
    "german_empire": {
        "name": "German Empire",
        "continent": "Europe",
        "flag_colors": ["#000000", "#ffffff", "#ff0000"],
        "power_rank": 2,
        "image_file": "german empire.jpg",
        "evolution": {"base": "reich", "condition": "Own all provinces in Western Europe and Eastern Europe"},
        "synergy_group": "central_powers",
        "modifiers": {
            "soldier_training": 1.30,
            "tech": 1.15,
            "industry": 1.10,
            "gold": 1.20,
            "stone": 1.20,
            "wood": 1.20,
            "food": 1.10
        }
    },
    "italy": {
        "name": "Kingdom of Italy",
        "continent": "Europe",
        "flag_colors": ["#009246", "#ffffff", "#ce2b37"],
        "power_rank": 3,
        "image_file": "italy.jpg",
        "evolution": {"base": None, "condition": None},
        "synergy_group": "axis",
        "modifiers": {
            "naval": 1.20,
            "trade": 1.10,
            "gold": 1.10,
            "stone": 1.05,
            "wood": 1.05,
            "food": 1.10
        }
    },
    "japanese_empire": {
        "name": "Japanese Empire",
        "continent": "Asia",
        "flag_colors": ["#ff0000", "#ffffff"],
        "power_rank": 2,
        "image_file": "japanese empire.jpg",
        "evolution": {"base": "japan", "condition": "Own all provinces in East Asia and Southeast Asia"},
        "synergy_group": "axis",
        "modifiers": {
            "military": 1.20,
            "naval": 1.30,
            "gold": 1.15,
            "stone": 1.15,
            "wood": 1.15,
            "food": 1.10
        }
    },
    "north_korea": {
        "name": "North Korea",
        "continent": "Asia",
        "flag_colors": ["#ff0000", "#ffffff", "#0000ff"],
        "power_rank": 5,
        "image_file": "north korea.jpg",
        "evolution": {"base": None, "condition": None},
        "synergy_group": "axis",
        "modifiers": {
            "military": 1.05,
            "unrest": -0.10,
            "gold": 1.05,
            "stone": 1.05,
            "wood": 1.05,
            "food": 1.05
        }
    },
    "ottoman_empire": {
        "name": "Ottoman Empire",
        "continent": "Asia",
        "flag_colors": ["#ff0000", "#ffffff", "#006600"],
        "power_rank": 3,
        "image_file": "ottoman empire.jpg",
        "evolution": {"base": "turkey", "condition": "Own all provinces in Middle East, North Africa, and Eastern Europe"},
        "synergy_group": "central_powers",
        "modifiers": {
            "trade": 1.20,
            "culture": 1.15,
            "happiness": 1.10,
            "gold": 1.20,
            "stone": 1.15,
            "wood": 1.15,
            "food": 1.15
        }
    },
    "soviet_union": {
        "name": "Soviet Union",
        "continent": "Europe/Asia",
        "flag_colors": ["#ff0000", "#ffff00"],
        "power_rank": 1,
        "image_file": "soviet union.jpg",
        "evolution": {"base": "russia", "condition": "Own all provinces in Eastern Europe and Central Asia"},
        "synergy_group": "allies",
        "modifiers": {
            "production": 1.30,
            "military": 1.25,
            "tech": 1.15,
            "gold": 1.20,
            "stone": 1.25,
            "wood": 1.25,
            "food": 1.20
        }
    },
    "taiwan": {
        "name": "Taiwan",
        "continent": "Asia",
        "flag_colors": ["#ff0000", "#ffffff", "#0000ff"],
        "power_rank": 4,
        "image_file": "taiwan.jpg",
        "evolution": {"base": None, "condition": None},
        "synergy_group": "allies",
        "modifiers": {
            "tech": 1.15,
            "trade": 1.10,
            "gold": 1.10,
            "stone": 1.05,
            "wood": 1.05,
            "food": 1.10
        }
    }
}

SYNERGIES = {
    "axis": {
        "name": "Axis Powers",
        "members": ["reich", "italy", "japanese_empire", "north_korea"],
        "bonuses": {
            "military": 0.25,
            "soldier_training": 0.20,
            "gold": 0.15,
            "stone": 0.15,
            "wood": 0.15,
            "food": 0.15
        }
    },
    "allies": {
        "name": "Allied Powers",
        "members": ["america", "british_empire", "france", "soviet_union", "taiwan"],
        "bonuses": {
            "trade": 0.25,
            "diplomacy": 0.20,
            "industry": 0.15,
            "gold": 0.25,
            "stone": 0.20,
            "wood": 0.20,
            "food": 0.20,
            "military": 0.15
        }
    },
    "central_powers": {
        "name": "Central Powers",
        "members": ["austria-hungary", "german_empire", "ottoman_empire"],
        "bonuses": {
            "soldier_training": 0.30,
            "tech": 0.20,
            "industry": 0.15,
            "gold": 0.20,
            "stone": 0.20,
            "wood": 0.20,
            "food": 0.15,
            "military": 0.20
        }
    }
}

# -------------------------------------------------------------------
# COUNTRYBALL MANAGER
# -------------------------------------------------------------------
class CountryballManager:
    def __init__(self, db, bot):
        self.db = db
        self.bot = bot
        self.images_path = os.path.join(os.path.dirname(__file__), '..', '..', 'images')
        self._init_tables()

    def _init_tables(self):
        conn = self.db.get_connection()
        cursor = conn.cursor()
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
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS active_managers (
                user_id TEXT,
                countryball_id TEXT,
                PRIMARY KEY (user_id, countryball_id)
            )
        ''')
        conn.commit()

    def unlock_countryball(self, user_id: str, ball_id: str) -> bool:
        conn = self.db.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT 1 FROM player_countryballs WHERE user_id = ? AND countryball_id = ?', (user_id, ball_id))
        if cursor.fetchone():
            return False
        cursor.execute('INSERT INTO player_countryballs (user_id, countryball_id) VALUES (?, ?)', (user_id, ball_id))
        conn.commit()
        return True

    def get_collection(self, user_id: str) -> List[Dict]:
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
        conn = self.db.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT countryball_id FROM active_managers WHERE user_id = ?', (user_id,))
        return [row['countryball_id'] for row in cursor.fetchall()]

    def activate(self, user_id: str, ball_id: str) -> bool:
        collection = self.get_collection(user_id)
        if ball_id not in [c['id'] for c in collection]:
            return False
        active = self.get_active_managers(user_id)
        if len(active) >= 3:
            return False
        if ball_id in active:
            return False
        conn = self.db.get_connection()
        cursor = conn.cursor()
        cursor.execute('UPDATE player_countryballs SET is_active = 1 WHERE user_id = ? AND countryball_id = ?', (user_id, ball_id))
        cursor.execute('INSERT INTO active_managers (user_id, countryball_id) VALUES (?, ?)', (user_id, ball_id))
        conn.commit()
        self._apply_modifiers(user_id)
        return True

    def deactivate(self, user_id: str, ball_id: str) -> bool:
        conn = self.db.get_connection()
        cursor = conn.cursor()
        cursor.execute('UPDATE player_countryballs SET is_active = 0 WHERE user_id = ? AND countryball_id = ?', (user_id, ball_id))
        cursor.execute('DELETE FROM active_managers WHERE user_id = ? AND countryball_id = ?', (user_id, ball_id))
        conn.commit()
        self._apply_modifiers(user_id)
        return True

    def _apply_modifiers(self, user_id: str):
        civ = self.db.get_civilization(user_id)
        if not civ:
            return
        active = self.get_active_managers(user_id)
        bonuses = civ.get('bonuses', {})
        to_remove = [k for k in bonuses if k.startswith('countryball_')]
        for k in to_remove:
            del bonuses[k]

        for ball_id in active:
            ball_data = COUNTRYBALLS.get(ball_id)
            if ball_data:
                for key, val in ball_data['modifiers'].items():
                    bonus_key = f"countryball_{ball_id}_{key}"
                    bonuses[bonus_key] = (val - 1) * 100

        synergy_bonuses = self._get_synergy_bonuses(user_id)
        for key, val in synergy_bonuses.items():
            bonus_key = f"countryball_synergy_{key}"
            bonuses[bonus_key] = val * 100

        self.db.update_civilization(user_id, {'bonuses': bonuses})

    def _get_synergy_bonuses(self, user_id: str) -> Dict[str, float]:
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
                active_members = [b for b in active if b in members]
                if len(active_members) >= 2:
                    for key, val in synergy['bonuses'].items():
                        bonuses[key] = bonuses.get(key, 0) + val
        return bonuses

    def check_evolution(self, user_id: str, ball_id: str, territory_cog) -> bool:
        ball_def = COUNTRYBALLS.get(ball_id)
        if not ball_def or not ball_def['evolution']['condition']:
            return False
        condition = ball_def['evolution']['condition']
        import re
        subregions = re.findall(r"in ([A-Za-z ]+)", condition)
        if not subregions:
            return False
        owned = territory_cog._get_owned_provinces(user_id)
        from bot.commands.territory import PROVINCES
        for sub in subregions:
            sub = sub.strip()
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
        conn = self.db.get_connection()
        cursor = conn.cursor()
        cursor.execute('UPDATE player_countryballs SET evolution_stage = "evolved" WHERE user_id = ? AND countryball_id = ?', (user_id, ball_id))
        conn.commit()
        return True

    def get_synergy_bonuses(self, user_id: str) -> Dict[str, float]:
        return self._get_synergy_bonuses(user_id)


# -------------------------------------------------------------------
# DISCORD COG
# -------------------------------------------------------------------
class CountryballCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.db = bot.db
        self.civ_manager = bot.civ_manager
        self.ball_manager = CountryballManager(self.db, bot)

    async def cog_load(self):
        self.territory_cog = self.bot.get_cog("TerritoryCog")
        if not self.territory_cog:
            logger.warning("TerritoryCog not found; countryball auto-unlock will not work.")

    # ---- PROGRESSIVE REVEAL ----
    async def reveal_countryball(self, ctx, ball_id: str, user_id: str):
        ball_def = COUNTRYBALLS.get(ball_id)
        if not ball_def:
            await ctx.send("❌ Unknown countryball.")
            return

        image_path = os.path.join(self.ball_manager.images_path, ball_def['image_file'])
        if not os.path.exists(image_path):
            await ctx.send(f"❌ Image file `{ball_def['image_file']}` not found. Path: {image_path}")
            return

        embed1 = discord.Embed(
            title="🌍 **A New Power Rises!**",
            description=f"From the continent of **{ball_def['continent']}**...",
            color=discord.Color.blue()
        )
        colors_str = " ".join([f"`{c}`" for c in ball_def['flag_colors']])
        embed2 = discord.Embed(
            title=f"🎨 **Colors of {ball_def['name']}**",
            description=f"Its flag bears the colors: {colors_str}",
            color=discord.Color.gold()
        )
        rank = ball_def['power_rank']
        rank_emoji = "👑" if rank == 1 else ("🥈" if rank == 2 else ("🥉" if rank == 3 else "🏅"))
        embed3 = discord.Embed(
            title=f"{rank_emoji} **Rank #{rank} – {ball_def['name']}**",
            description=f"This power is one of the most influential of its time.",
            color=discord.Color.purple()
        )
        embed4 = discord.Embed(
            title=f"**{ball_def['name']}** Unlocked!",
            description=f"Added to your collection! {self._format_modifiers(ball_def['modifiers'])}",
            color=discord.Color.green()
        )
        file = discord.File(image_path, filename=ball_def['image_file'])
        embed4.set_image(url=f"attachment://{ball_def['image_file']}")

        await ctx.send(embed=embed1)
        await asyncio.sleep(1.5)
        await ctx.send(embed=embed2)
        await asyncio.sleep(1.5)
        await ctx.send(embed=embed3)
        await asyncio.sleep(2)
        await ctx.send(embed=embed4, file=file)

    def _format_modifiers(self, modifiers):
        lines = []
        for key, val in modifiers.items():
            sign = "+" if val > 0 else ""
            lines.append(f"**{key.replace('_',' ').title()}:** {sign}{int((val-1)*100)}%")
        return "\n".join(lines)

    # ---- AUTO-UNLOCK TRIGGER ----
    async def check_region_unlock(self, ctx, region: str, user_id: str):
        ball_id = REGION_TO_COUNTRYBALL.get(region)
        if not ball_id:
            return

        if not self.ball_manager.unlock_countryball(user_id, ball_id):
            return

        if self.territory_cog:
            self.ball_manager.check_evolution(user_id, ball_id, self.territory_cog)

        active = self.ball_manager.get_active_managers(user_id)
        if len(active) < 3:
            self.ball_manager.activate(user_id, ball_id)

        await self.reveal_countryball(ctx, ball_id, user_id)

    # ---- NEW COMMAND: .openpacks ----
    @commands.command(name='openpacks')
    async def open_packs(self, ctx):
        """
        Check all subregions you have fully conquered and unlock the corresponding countryballs.
        """
        user_id = str(ctx.author.id)
        if not self.territory_cog:
            await ctx.send("❌ Territory system not available.")
            return

        owned = self.territory_cog._get_owned_provinces(user_id)
        if not owned:
            await ctx.send("🌍 You haven't conquered any provinces yet. Start with `.expand`.")
            return

        # Find all subregions fully owned
        from bot.commands.territory import PROVINCES
        completed_regions = []
        for subregion, provinces in PROVINCES.items():
            if provinces and all(p in owned for p in provinces):
                completed_regions.append(subregion)

        if not completed_regions:
            await ctx.send("📦 You haven't fully conquered any subregion yet. Keep expanding!")
            return

        # For each completed region, unlock the corresponding countryball
        unlocked_any = False
        for region in completed_regions:
            ball_id = REGION_TO_COUNTRYBALL.get(region)
            if not ball_id:
                continue
            if self.ball_manager.unlock_countryball(user_id, ball_id):
                unlocked_any = True
                if self.territory_cog:
                    self.ball_manager.check_evolution(user_id, ball_id, self.territory_cog)
                active = self.ball_manager.get_active_managers(user_id)
                if len(active) < 3:
                    self.ball_manager.activate(user_id, ball_id)
                await self.reveal_countryball(ctx, ball_id, user_id)
                await asyncio.sleep(2)  # small delay between reveals

        if not unlocked_any:
            await ctx.send("📦 You've already unlocked all countryballs for your conquered regions!")

    # ---- COMMANDS (unchanged) ----
    @commands.command(name='packs')
    async def packs_list(self, ctx):
        user_id = str(ctx.author.id)
        collection = self.ball_manager.get_collection(user_id)
        if not collection:
            await ctx.send("📦 You haven't unlocked any countryballs yet. Conquer regions and use `.openpacks` to unlock them!")
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
        user_id = str(ctx.author.id)
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
            syn_bonuses = self.ball_manager.get_synergy_bonuses(user_id)
            if syn_bonuses:
                bonus_str = ", ".join([f"{k}: +{int(v*100)}%" for k,v in syn_bonuses.items()])
                await ctx.send(f"⚡ **Synergy activated!** {bonus_str}")
        else:
            await ctx.send("❌ Could not activate. Either not owned, already active, or limit of 3 reached.")

    @commands.command(name='deactivate')
    @app_commands.describe(ball_name="Name of the countryball to deactivate")
    async def deactivate_manager(self, ctx, *, ball_name: str):
        user_id = str(ctx.author.id)
        active = self.ball_manager.get_active_managers(user_id)
        if not active:
            await ctx.send("❌ You have no active managers.")
            return
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
        user_id = str(ctx.author.id)
        bonuses = self.ball_manager.get_synergy_bonuses(user_id)
        if not bonuses:
            await ctx.send("❌ No active synergies. Activate at least 2 countryballs from the same faction.")
            return
        embed = discord.Embed(title="⚡ Active Synergies", color=discord.Color.gold())
        for key, val in bonuses.items():
            embed.add_field(name=key.replace('_',' ').title(), value=f"+{int(val*100)}%", inline=True)
        await ctx.send(embed=embed)


async def setup(bot):
    await bot.add_cog(CountryballCog(bot))
