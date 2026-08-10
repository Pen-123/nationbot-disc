import random
import json
import logging
from typing import List, Optional
import discord
from discord.ext import commands
from discord import app_commands

from bot.utils import create_embed, format_number

logger = logging.getLogger(__name__)

# ---------------------------
# WORLD DATA – Continents & Sub‑regions
# ---------------------------
WORLD = {
    "Europe": {
        "Western Europe": {"cost": {"gold": 500, "food": 200, "wood": 100, "stone": 50}, "neighbours": ["Southern Europe", "Northern Europe", "Eastern Europe"]},
        "Eastern Europe": {"cost": {"gold": 400, "food": 300, "wood": 150, "stone": 100}, "neighbours": ["Western Europe", "Northern Europe", "Central Asia"]},
        "Southern Europe": {"cost": {"gold": 600, "food": 150, "wood": 80, "stone": 120}, "neighbours": ["Western Europe", "Middle East", "North Africa"]},
        "Northern Europe": {"cost": {"gold": 450, "food": 250, "wood": 200, "stone": 80}, "neighbours": ["Western Europe", "Eastern Europe"]},
    },
    "Asia": {
        "Central Asia": {"cost": {"gold": 300, "food": 400, "wood": 100, "stone": 200}, "neighbours": ["Eastern Europe", "South Asia", "East Asia", "Middle East"]},
        "East Asia": {"cost": {"gold": 700, "food": 600, "wood": 300, "stone": 150}, "neighbours": ["Central Asia", "South Asia", "Southeast Asia", "Oceania"]},
        "South Asia": {"cost": {"gold": 500, "food": 500, "wood": 200, "stone": 100}, "neighbours": ["Central Asia", "East Asia", "Southeast Asia", "Middle East"]},
        "Southeast Asia": {"cost": {"gold": 400, "food": 700, "wood": 250, "stone": 80}, "neighbours": ["East Asia", "South Asia", "Oceania"]},
        "Middle East": {"cost": {"gold": 800, "food": 200, "wood": 50, "stone": 300}, "neighbours": ["Southern Europe", "Central Asia", "South Asia", "North Africa"]},
    },
    "Africa": {
        "North Africa": {"cost": {"gold": 600, "food": 300, "wood": 80, "stone": 200}, "neighbours": ["Southern Europe", "Middle East", "West Africa", "Central Africa"]},
        "West Africa": {"cost": {"gold": 400, "food": 500, "wood": 150, "stone": 100}, "neighbours": ["North Africa", "Central Africa", "Southern Africa"]},
        "Central Africa": {"cost": {"gold": 350, "food": 600, "wood": 200, "stone": 150}, "neighbours": ["North Africa", "West Africa", "East Africa", "Southern Africa"]},
        "East Africa": {"cost": {"gold": 450, "food": 550, "wood": 100, "stone": 120}, "neighbours": ["North Africa", "Central Africa", "Southern Africa"]},
        "Southern Africa": {"cost": {"gold": 500, "food": 400, "wood": 150, "stone": 250}, "neighbours": ["West Africa", "Central Africa", "East Africa"]},
    },
    "North America": {
        "Western North America": {"cost": {"gold": 700, "food": 300, "wood": 200, "stone": 150}, "neighbours": ["Central North America", "Mexico"]},
        "Central North America": {"cost": {"gold": 600, "food": 400, "wood": 150, "stone": 100}, "neighbours": ["Western North America", "Eastern North America", "Mexico"]},
        "Eastern North America": {"cost": {"gold": 650, "food": 350, "wood": 180, "stone": 80}, "neighbours": ["Central North America"]},
        "Mexico": {"cost": {"gold": 500, "food": 300, "wood": 100, "stone": 80}, "neighbours": ["Western North America", "Central North America", "Central America"]},
    },
    "South America": {
        "Central America": {"cost": {"gold": 400, "food": 300, "wood": 150, "stone": 50}, "neighbours": ["Mexico", "Northern South America"]},
        "Northern South America": {"cost": {"gold": 500, "food": 400, "wood": 200, "stone": 100}, "neighbours": ["Central America", "Western South America", "Eastern South America", "Brazil"]},
        "Western South America": {"cost": {"gold": 550, "food": 350, "wood": 150, "stone": 200}, "neighbours": ["Northern South America", "Brazil", "Southern Cone"]},
        "Eastern South America": {"cost": {"gold": 450, "food": 500, "wood": 180, "stone": 120}, "neighbours": ["Northern South America", "Brazil", "Southern Cone"]},
        "Brazil": {"cost": {"gold": 600, "food": 600, "wood": 250, "stone": 150}, "neighbours": ["Northern South America", "Western South America", "Eastern South America", "Southern Cone"]},
        "Southern Cone": {"cost": {"gold": 500, "food": 400, "wood": 120, "stone": 200}, "neighbours": ["Western South America", "Eastern South America", "Brazil"]},
    },
    "Oceania": {
        "Australia": {"cost": {"gold": 800, "food": 400, "wood": 150, "stone": 300}, "neighbours": ["Southeast Asia", "New Zealand", "Pacific Islands"]},
        "New Zealand": {"cost": {"gold": 600, "food": 300, "wood": 200, "stone": 100}, "neighbours": ["Australia", "Pacific Islands"]},
        "Pacific Islands": {"cost": {"gold": 400, "food": 200, "wood": 100, "stone": 50}, "neighbours": ["Australia", "New Zealand", "Southeast Asia"]},
    },
    "Antarctica": {
        "Antarctic Peninsula": {"cost": {"gold": 1000, "food": 50, "wood": 50, "stone": 500}, "neighbours": ["Southern Cone"]},
        "East Antarctica": {"cost": {"gold": 1200, "food": 50, "wood": 50, "stone": 600}, "neighbours": ["Antarctic Peninsula"]},
        "West Antarctica": {"cost": {"gold": 1100, "food": 50, "wood": 50, "stone": 550}, "neighbours": ["Antarctic Peninsula"]},
    },
}

# Flatten for easy lookup
SUBREGION_TO_CONTINENT = {sub: cont for cont, subs in WORLD.items() for sub in subs}
SUBREGION_DATA = {sub: data for cont, subs in WORLD.items() for sub, data in subs.items()}
ALL_SUBREGIONS = list(SUBREGION_DATA.keys())

# Map region choices (from basic.py) to sub‑regions (used for giving starting territory)
REGION_TO_SUBREGION = {
    "asia": "East Asia",
    "europe": "Western Europe",
    "africa": "West Africa",
    "north_america": "Central North America",
    "south_america": "Brazil",
    "middle_east": "Middle East",
    "oceania": "Australia",
    "antarctica": "Antarctic Peninsula",
}


class TerritoryCog(commands.Cog):
    """Territorial expansion and management"""

    def __init__(self, bot):
        self.bot = bot
        self.db = bot.db
        self.civ_manager = bot.civ_manager
        self._init_tables()

    def _init_tables(self):
        conn = self.db.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS territories (
                user_id TEXT PRIMARY KEY,
                owned_territories TEXT NOT NULL DEFAULT '[]'
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS territory_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                territory_name TEXT NOT NULL,
                claimed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()

    # ---------- Internal database helpers ----------
    def _get_owned_territories(self, user_id: str) -> List[str]:
        conn = self.db.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT owned_territories FROM territories WHERE user_id = ?', (user_id,))
        row = cursor.fetchone()
        if row:
            return json.loads(row[0])
        return []

    def _set_owned_territories(self, user_id: str, territories: List[str]):
        conn = self.db.get_connection()
        cursor = conn.cursor()
        cursor.execute('INSERT OR REPLACE INTO territories (user_id, owned_territories) VALUES (?, ?)',
                       (user_id, json.dumps(territories)))
        conn.commit()

    def _add_territory(self, user_id: str, territory: str) -> bool:
        """Add a territory to a user's owned list. Returns True if added, False if already owned."""
        owned = self._get_owned_territories(user_id)
        if territory in owned:
            return False
        owned.append(territory)
        self._set_owned_territories(user_id, owned)
        # Log history
        conn = self.db.get_connection()
        cursor = conn.cursor()
        cursor.execute('INSERT INTO territory_history (user_id, territory_name) VALUES (?, ?)', (user_id, territory))
        conn.commit()
        return True

    def _is_adjacent(self, user_id: str, territory: str) -> bool:
        owned = self._get_owned_territories(user_id)
        if not owned:
            return False
        neighbours = SUBREGION_DATA[territory]["neighbours"]
        for t in owned:
            if t in neighbours:
                return True
        return False

    def _get_expansion_options(self, user_id: str) -> List[str]:
        """Return a list of sub‑regions that are adjacent and not yet owned."""
        owned = self._get_owned_territories(user_id)
        possible = set()
        for t in owned:
            neighbours = SUBREGION_DATA.get(t, {}).get("neighbours", [])
            possible.update(neighbours)
        possible = possible - set(owned)
        return sorted(possible)

    # ---------- Commands ----------
    @commands.command(name='territories')
    async def list_territories(self, ctx):
        """List all sub‑regions you own, grouped by continent."""
        user_id = str(ctx.author.id)
        owned = self._get_owned_territories(user_id)
        if not owned:
            await ctx.send("🌍 You don't own any territories yet! Use `.expand` to claim your first, or select a region with `.regions` to start.")
            return
        embed = discord.Embed(title="🗺️ Your Territories", color=discord.Color.green())
        by_continent = {}
        for terr in owned:
            cont = SUBREGION_TO_CONTINENT.get(terr, "Unknown")
            by_continent.setdefault(cont, []).append(terr)
        for cont, territories in by_continent.items():
            embed.add_field(name=cont, value=", ".join(territories), inline=False)
        embed.set_footer(text=f"Total: {len(owned)} sub‑regions")
        await ctx.send(embed=embed)

    @commands.command(name='expand')
    @app_commands.describe(territory="Name of the sub‑region to expand into")
    async def expand(self, ctx, *, territory: str = None):
        """Claim a new sub‑region adjacent to your territory (costs resources)."""
        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need a civilization first! Use `.start`.")
            return

        owned = self._get_owned_territories(user_id)

        # If no territory specified, show available expansions
        if territory is None:
            if not owned:
                await ctx.send("❌ You have no territory. Select a region with `.regions` to get your starting territory, or use `.expand <territory>` to claim any territory as your first (costs resources).")
                return
            possible = self._get_expansion_options(user_id)
            if not possible:
                await ctx.send("❌ No adjacent territories available to expand into.")
                return
            embed = discord.Embed(title="🌍 Possible Expansions", description="Use `.expand <territory>` to claim one.", color=discord.Color.blue())
            by_cont = {}
            for p in possible:
                cont = SUBREGION_TO_CONTINENT.get(p, "Unknown")
                by_cont.setdefault(cont, []).append(p)
            for cont, names in by_cont.items():
                embed.add_field(name=cont, value=", ".join(names), inline=False)
            await ctx.send(embed=embed)
            return

        # Find exact match (case‑insensitive)
        match = None
        for name in ALL_SUBREGIONS:
            if name.lower() == territory.lower():
                match = name
                break
        if not match:
            # try partial match
            for name in ALL_SUBREGIONS:
                if territory.lower() in name.lower():
                    match = name
                    break
        if not match:
            await ctx.send(f"❌ Unknown sub‑region: `{territory}`. Use `.expand` to see available territories.")
            return
        territory = match

        # If this is the first territory, allow claiming any (no adjacency check, but still costs)
        if not owned:
            cost = SUBREGION_DATA[territory]["cost"]
            if not self.civ_manager.can_afford(user_id, cost):
                cost_str = ", ".join([f"{amount} {res}" for res, amount in cost.items()])
                await ctx.send(f"❌ Cannot afford to claim **{territory}**. Requires: {cost_str}.")
                return
            # Spend resources
            self.civ_manager.spend_resources(user_id, cost)
            if self._add_territory(user_id, territory):
                land_gain = random.randint(100, 300)
                self.civ_manager.update_territory(user_id, {"land_size": land_gain})
                embed = discord.Embed(title="🏹 First Territory Claimed!", description=f"**{civ['name']}** has claimed **{territory}**!", color=discord.Color.green())
                embed.add_field(name="Cost", value=", ".join([f"{amount} {res}" for res, amount in cost.items()]), inline=True)
                embed.add_field(name="Land Gained", value=f"+{land_gain} km²", inline=True)
                await ctx.send(embed=embed)
                self.db.log_event(user_id, "expansion", "First Territory", f"Claimed {territory} as first territory")
            else:
                await ctx.send("❌ Failed to claim territory.")
            return

        # Normal expansion: check adjacency
        if not self._is_adjacent(user_id, territory):
            await ctx.send(f"❌ **{territory}** is not adjacent to any of your territories. You can only expand into neighbouring regions.")
            return

        # Check cost
        cost = SUBREGION_DATA[territory]["cost"]
        if not self.civ_manager.can_afford(user_id, cost):
            cost_str = ", ".join([f"{amount} {res}" for res, amount in cost.items()])
            await ctx.send(f"❌ Cannot afford to claim **{territory}**. Requires: {cost_str}.")
            return

        # Spend and claim
        self.civ_manager.spend_resources(user_id, cost)
        if self._add_territory(user_id, territory):
            land_gain = random.randint(50, 200)
            self.civ_manager.update_territory(user_id, {"land_size": land_gain})
            embed = discord.Embed(title="🏹 Expansion Successful!", description=f"**{civ['name']}** has expanded into **{territory}**!", color=discord.Color.green())
            embed.add_field(name="Cost", value=", ".join([f"{amount} {res}" for res, amount in cost.items()]), inline=True)
            embed.add_field(name="Land Gained", value=f"+{land_gain} km²", inline=True)
            await ctx.send(embed=embed)
            self.db.log_event(user_id, "expansion", "Territory Claimed", f"Claimed {territory}")
        else:
            await ctx.send("❌ Failed to claim territory. Please try again.")


async def setup(bot):
    await bot.add_cog(TerritoryCog(bot))
