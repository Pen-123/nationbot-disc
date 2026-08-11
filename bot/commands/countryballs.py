import discord
from discord.ext import commands
from discord import app_commands
import json
import logging
import random
import os
import asyncio
from datetime import datetime, timezone
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
        "flag_colors_human": "Red and Yellow",
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
        "flag_colors_human": "Black, White, and Red",
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
        "flag_colors_human": "Red, White, and Blue",
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
        "flag_colors_human": "Red, White, and Green",
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
        "flag_colors_human": "Red, White, and Blue",
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
        "flag_colors_human": "Blue, White, and Red",
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
        "flag_colors_human": "Black, White, and Red",
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
        "flag_colors_human": "Green, White, and Red",
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
        "flag_colors_human": "Red and White",
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
        "flag_colors_human": "Red, White, and Blue",
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
        "flag_colors_human": "Red, White, and Green",
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
        "flag_colors_human": "Red and Yellow",
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
        "flag_colors_human": "Red, White, and Blue",
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
# COUNTRYBALL MANAGER (Firestore-based)
# -------------------------------------------------------------------
class CountryballManager:
    def __init__(self, db, bot):
        self.db = db          # Database instance with Firestore client
        self.bot = bot
        self.images_path = os.path.join(os.path.dirname(__file__), '..', '..', 'images')
        # No SQLite table creation needed; Firestore is schemaless.

    def _get_user_balls_ref(self, user_id: str):
        """Return reference to the player_countryballs document for this user."""
        return self.db.client.collection("player_countryballs").document(user_id)

    def _get_active_managers_ref(self, user_id: str):
        """Return reference to the active_managers document for this user."""
        return self.db.client.collection("active_managers").document(user_id)

    def unlock_countryball(self, user_id: str, ball_id: str) -> bool:
        """Unlock a countryball for a player. Returns True if newly unlocked."""
        try:
            doc_ref = self._get_user_balls_ref(user_id)
            doc = doc_ref.get()
            if doc.exists:
                data = doc.to_dict()
                balls = data.get("balls", {})
                if ball_id in balls:
                    return False  # already unlocked
            else:
                balls = {}

            # Add the new ball
            balls[ball_id] = {
                "unlocked_at": datetime.now(timezone.utc).isoformat(),
                "is_active": False,
                "evolution_stage": "base"
            }
            doc_ref.set({"balls": balls}, merge=True)
            logger.info(f"Unlocked countryball {ball_id} for {user_id}")
            return True
        except Exception as e:
            logger.error(f"Error unlocking countryball for {user_id}: {e}")
            return False

    def get_collection(self, user_id: str) -> List[Dict]:
        """Get a list of all countryballs owned by the user, with details."""
        try:
            doc = self._get_user_balls_ref(user_id).get()
            if not doc.exists:
                return []
            data = doc.to_dict()
            balls = data.get("balls", {})
            collection = []
            for ball_id, info in balls.items():
                ball_data = COUNTRYBALLS.get(ball_id)
                if ball_data:
                    collection.append({
                        'id': ball_id,
                        'name': ball_data['name'],
                        'unlocked_at': info.get('unlocked_at'),
                        'is_active': info.get('is_active', False),
                        'evolution_stage': info.get('evolution_stage', 'base'),
                        'image_file': ball_data['image_file'],
                        'continent': ball_data['continent'],
                        'flag_colors_human': ball_data['flag_colors_human'],
                        'power_rank': ball_data['power_rank'],
                        'modifiers': ball_data['modifiers'],
                        'synergy_group': ball_data['synergy_group']
                    })
            return collection
        except Exception as e:
            logger.error(f"Error getting collection for {user_id}: {e}")
            return []

    def get_active_managers(self, user_id: str) -> List[str]:
        """Return list of active manager ball IDs."""
        try:
            doc = self._get_active_managers_ref(user_id).get()
            if not doc.exists:
                return []
            data = doc.to_dict()
            return data.get("active", [])
        except Exception as e:
            logger.error(f"Error getting active managers for {user_id}: {e}")
            return []

    def activate(self, user_id: str, ball_id: str) -> bool:
        """Activate a countryball as a manager (max 3)."""
        try:
            # Check ownership and not already active
            user_doc = self._get_user_balls_ref(user_id).get()
            if not user_doc.exists:
                return False
            balls = user_doc.to_dict().get("balls", {})
            if ball_id not in balls:
                return False
            if balls[ball_id].get("is_active", False):
                return False

            # Check current active count
            active_ref = self._get_active_managers_ref(user_id)
            active_doc = active_ref.get()
            if active_doc.exists:
                active = active_doc.to_dict().get("active", [])
                if len(active) >= 3:
                    return False
            else:
                active = []

            # Add to active list
            active.append(ball_id)
            active_ref.set({"active": active}, merge=True)

            # Mark is_active in player_countryballs
            balls[ball_id]["is_active"] = True
            self._get_user_balls_ref(user_id).set({"balls": balls}, merge=True)

            # Apply modifiers
            self._apply_modifiers(user_id)
            return True
        except Exception as e:
            logger.error(f"Error activating countryball {ball_id} for {user_id}: {e}")
            return False

    def deactivate(self, user_id: str, ball_id: str) -> bool:
        """Deactivate a countryball manager."""
        try:
            # Check ownership and active
            user_doc = self._get_user_balls_ref(user_id).get()
            if not user_doc.exists:
                return False
            balls = user_doc.to_dict().get("balls", {})
            if ball_id not in balls:
                return False
            if not balls[ball_id].get("is_active", False):
                return False

            # Remove from active list
            active_ref = self._get_active_managers_ref(user_id)
            active_doc = active_ref.get()
            if not active_doc.exists:
                return False
            active = active_doc.to_dict().get("active", [])
            if ball_id not in active:
                return False
            active.remove(ball_id)
            active_ref.set({"active": active}, merge=True)

            # Mark is_active in player_countryballs
            balls[ball_id]["is_active"] = False
            self._get_user_balls_ref(user_id).set({"balls": balls}, merge=True)

            # Apply modifiers
            self._apply_modifiers(user_id)
            return True
        except Exception as e:
            logger.error(f"Error deactivating countryball {ball_id} for {user_id}: {e}")
            return False

    def _apply_modifiers(self, user_id: str):
        """Recalculate bonuses from active countryballs and synergies, update civilization bonuses."""
        civ = self.db.get_civilization(user_id)
        if not civ:
            return

        active = self.get_active_managers(user_id)
        bonuses = civ.get('bonuses', {})
        # Remove all countryball-related bonuses
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
        """Calculate synergy bonuses based on active managers."""
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
        """Check if a countryball can evolve, and update if possible."""
        ball_def = COUNTRYBALLS.get(ball_id)
        if not ball_def or not ball_def['evolution']['condition']:
            return False

        condition = ball_def['evolution']['condition']
        import re
        # Extract subregion names from condition (e.g., "Own all provinces in Western Europe, ...")
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

        # Evolution condition met: update evolution_stage
        try:
            balls_ref = self._get_user_balls_ref(user_id)
            doc = balls_ref.get()
            if not doc.exists:
                return False
            balls = doc.to_dict().get("balls", {})
            if ball_id not in balls:
                return False
            balls[ball_id]["evolution_stage"] = "evolved"
            balls_ref.set({"balls": balls}, merge=True)
            logger.info(f"Countryball {ball_id} evolved for {user_id}")
            return True
        except Exception as e:
            logger.error(f"Error evolving countryball {ball_id} for {user_id}: {e}")
            return False

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

        # Stage 1: Continent (no name)
        embed1 = discord.Embed(
            title="🌍 **A New Power Rises!**",
            description=f"From the continent of **{ball_def['continent']}**...",
            color=discord.Color.blue()
        )

        # Stage 2: Colors and rank (no name)
        colors_str = ball_def['flag_colors_human']
        rank = ball_def['power_rank']
        rank_emoji = "👑" if rank == 1 else ("🥈" if rank == 2 else ("🥉" if rank == 3 else "🏅"))
        embed2 = discord.Embed(
            title=f"{rank_emoji} **Rank #{rank}**",
            description=f"Bears the colors of **{colors_str}**.",
            color=discord.Color.gold()
        )

        # Stage 3: Mystery hint (still no name)
        embed3 = discord.Embed(
            title="🔮 **A Legendary Power Emerges**",
            description="Ancient texts speak of a mighty empire...\nIts true name will be revealed shortly.",
            color=discord.Color.purple()
        )

        # Stage 4: Full reveal with name and image
        embed4 = discord.Embed(
            title=f"**{ball_def['name']}** Unlocked!",
            description=f"Added to your collection! {self._format_modifiers(ball_def['modifiers'])}",
            color=discord.Color.green()
        )
        file = discord.File(image_path, filename=ball_def['image_file'])
        embed4.set_image(url=f"attachment://{ball_def['image_file']}")

        # Send stages
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
        user_id = str(ctx.author.id)
        if not self.territory_cog:
            await ctx.send("❌ Territory system not available.")
            return

        owned = self.territory_cog._get_owned_provinces(user_id)
        if not owned:
            await ctx.send("🌍 You haven't conquered any provinces yet. Start with `.expand`.")
            return

        from bot.commands.territory import PROVINCES
        completed_regions = []
        for subregion, provinces in PROVINCES.items():
            if provinces and all(p in owned for p in provinces):
                completed_regions.append(subregion)

        if not completed_regions:
            await ctx.send("📦 You haven't fully conquered any subregion yet. Keep expanding!")
            return

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
                await asyncio.sleep(2)

        if not unlocked_any:
            await ctx.send("📦 You've already unlocked all countryballs for your conquered regions!")

    # ---- COMMANDS ----
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
