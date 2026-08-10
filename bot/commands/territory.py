import random
import json
import logging
import math
from typing import List, Optional, Set
import discord
from discord.ext import commands
from discord import app_commands

from bot.utils import create_embed, format_number

logger = logging.getLogger(__name__)

# ---------------------------
# PROVINCE DATA – each sub‑region is split into provinces
# ---------------------------
PROVINCES = {
    "Eastern Europe": ["Poland", "Ukraine", "Belarus", "Moldova", "Romania", "Bulgaria"],
    "Western Europe": ["France", "Germany", "UK", "Ireland", "Benelux", "Switzerland", "Austria"],
    "Southern Europe": ["Portugal", "Spain", "Italy", "Greece", "Croatia", "Serbia"],
    "Northern Europe": ["Norway", "Sweden", "Finland", "Denmark", "Iceland", "Baltic States"],
    "Central Asia": ["Kazakhstan", "Uzbekistan", "Turkmenistan", "Kyrgyzstan", "Tajikistan"],
    "East Asia": ["China", "Japan", "South Korea", "North Korea", "Mongolia", "Taiwan"],
    "South Asia": ["India", "Pakistan", "Bangladesh", "Sri Lanka", "Nepal", "Bhutan"],
    "Southeast Asia": ["Thailand", "Vietnam", "Indonesia", "Philippines", "Malaysia", "Singapore"],
    "Middle East": ["Turkey", "Iran", "Iraq", "Syria", "Israel", "Saudi Arabia", "UAE", "Qatar"],
    "North Africa": ["Morocco", "Algeria", "Tunisia", "Libya", "Egypt"],
    "West Africa": ["Nigeria", "Ghana", "Ivory Coast", "Senegal", "Mali", "Burkina Faso"],
    "Central Africa": ["DR Congo", "Cameroon", "Angola", "Zambia", "Zimbabwe"],
    "East Africa": ["Kenya", "Tanzania", "Ethiopia", "Somalia", "Uganda", "Mozambique"],
    "Southern Africa": ["South Africa", "Namibia", "Botswana", "Lesotho", "Eswatini"],
    "Western North America": ["Canada", "Alaska", "California", "Pacific Northwest"],
    "Central North America": ["Midwest", "Texas", "Great Plains", "Rocky Mountains"],
    "Eastern North America": ["New York", "Florida", "New England", "Appalachia"],
    "Mexico": ["Mexico", "Yucatan"],
    "Central America": ["Guatemala", "Honduras", "Nicaragua", "Costa Rica", "Panama"],
    "Northern South America": ["Venezuela", "Colombia", "Guyana", "Suriname"],
    "Western South America": ["Peru", "Ecuador", "Bolivia", "Chile"],
    "Eastern South America": ["Brazil East"],
    "Brazil": ["Brazil", "Amazonas", "São Paulo"],
    "Southern Cone": ["Argentina", "Uruguay", "Paraguay"],
    "Australia": ["Australia East", "Australia West", "Australia South"],
    "New Zealand": ["New Zealand North", "New Zealand South"],
    "Pacific Islands": ["Papua New Guinea", "Fiji", "Samoa", "Tonga"],
    "Antarctic Peninsula": ["Antarctic Coast"],
    "East Antarctica": ["East Antarctic Plateau"],
    "West Antarctica": ["West Antarctic Ice Sheet"],
}

# Build reverse mapping: province -> subregion
PROVINCE_TO_SUBREGION = {}
for subregion, province_list in PROVINCES.items():
    for province in province_list:
        PROVINCE_TO_SUBREGION[province] = subregion

# All provinces flat list
ALL_PROVINCES = list(PROVINCE_TO_SUBREGION.keys())

# All subregions flat list (FOR EXPORT)
ALL_SUBREGIONS = list(PROVINCES.keys())

# Sub‑region neighbour data (same as before)
SUBREGION_DATA = {
    "Eastern Europe": {"neighbours": ["Western Europe", "Northern Europe", "Central Asia"]},
    "Western Europe": {"neighbours": ["Southern Europe", "Northern Europe", "Eastern Europe"]},
    "Southern Europe": {"neighbours": ["Western Europe", "Middle East", "North Africa"]},
    "Northern Europe": {"neighbours": ["Western Europe", "Eastern Europe"]},
    "Central Asia": {"neighbours": ["Eastern Europe", "South Asia", "East Asia", "Middle East"]},
    "East Asia": {"neighbours": ["Central Asia", "South Asia", "Southeast Asia", "Oceania"]},
    "South Asia": {"neighbours": ["Central Asia", "East Asia", "Southeast Asia", "Middle East"]},
    "Southeast Asia": {"neighbours": ["East Asia", "South Asia", "Oceania"]},
    "Middle East": {"neighbours": ["Southern Europe", "Central Asia", "South Asia", "North Africa"]},
    "North Africa": {"neighbours": ["Southern Europe", "Middle East", "West Africa", "Central Africa"]},
    "West Africa": {"neighbours": ["North Africa", "Central Africa", "Southern Africa"]},
    "Central Africa": {"neighbours": ["North Africa", "West Africa", "East Africa", "Southern Africa"]},
    "East Africa": {"neighbours": ["North Africa", "Central Africa", "Southern Africa"]},
    "Southern Africa": {"neighbours": ["West Africa", "Central Africa", "East Africa"]},
    "Western North America": {"neighbours": ["Central North America", "Mexico"]},
    "Central North America": {"neighbours": ["Western North America", "Eastern North America", "Mexico"]},
    "Eastern North America": {"neighbours": ["Central North America"]},
    "Mexico": {"neighbours": ["Western North America", "Central North America", "Central America"]},
    "Central America": {"neighbours": ["Mexico", "Northern South America"]},
    "Northern South America": {"neighbours": ["Central America", "Western South America", "Eastern South America", "Brazil"]},
    "Western South America": {"neighbours": ["Northern South America", "Brazil", "Southern Cone"]},
    "Eastern South America": {"neighbours": ["Northern South America", "Brazil", "Southern Cone"]},
    "Brazil": {"neighbours": ["Northern South America", "Western South America", "Eastern South America", "Southern Cone"]},
    "Southern Cone": {"neighbours": ["Western South America", "Eastern South America", "Brazil"]},
    "Australia": {"neighbours": ["Southeast Asia", "New Zealand", "Pacific Islands"]},
    "New Zealand": {"neighbours": ["Australia", "Pacific Islands"]},
    "Pacific Islands": {"neighbours": ["Australia", "New Zealand", "Southeast Asia"]},
    "Antarctic Peninsula": {"neighbours": ["Southern Cone"]},
    "East Antarctica": {"neighbours": ["Antarctic Peninsula"]},
    "West Antarctica": {"neighbours": ["Antarctic Peninsula"]},
}

SUBREGION_TO_CONTINENT = {
    "Eastern Europe": "Europe",
    "Western Europe": "Europe",
    "Southern Europe": "Europe",
    "Northern Europe": "Europe",
    "Central Asia": "Asia",
    "East Asia": "Asia",
    "South Asia": "Asia",
    "Southeast Asia": "Asia",
    "Middle East": "Asia",
    "North Africa": "Africa",
    "West Africa": "Africa",
    "Central Africa": "Africa",
    "East Africa": "Africa",
    "Southern Africa": "Africa",
    "Western North America": "North America",
    "Central North America": "North America",
    "Eastern North America": "North America",
    "Mexico": "North America",
    "Central America": "South America",
    "Northern South America": "South America",
    "Western South America": "South America",
    "Eastern South America": "South America",
    "Brazil": "South America",
    "Southern Cone": "South America",
    "Australia": "Oceania",
    "New Zealand": "Oceania",
    "Pacific Islands": "Oceania",
    "Antarctic Peninsula": "Antarctica",
    "East Antarctica": "Antarctica",
    "West Antarctica": "Antarctica",
}

# Region to subregion mapping (for starting territory in basic.py) – kept for compatibility
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
    """Territorial expansion and management (province-based)"""

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
                owned_provinces TEXT NOT NULL DEFAULT '[]'
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
    def _get_owned_provinces(self, user_id: str) -> List[str]:
        """Get the list of owned province names for a user."""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT owned_provinces FROM territories WHERE user_id = ?', (user_id,))
        row = cursor.fetchone()
        if row:
            return json.loads(row[0])
        return []

    def _set_owned_provinces(self, user_id: str, provinces: List[str]):
        """Overwrite the owned provinces list for a user."""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        cursor.execute('INSERT OR REPLACE INTO territories (user_id, owned_provinces) VALUES (?, ?)',
                       (user_id, json.dumps(provinces)))
        conn.commit()

    def _add_province(self, user_id: str, province: str) -> bool:
        """Add a province to a user's owned list. Returns True if added, False if already owned."""
        owned = self._get_owned_provinces(user_id)
        if province in owned:
            return False
        owned.append(province)
        self._set_owned_provinces(user_id, owned)
        conn = self.db.get_connection()
        cursor = conn.cursor()
        cursor.execute('INSERT INTO territory_history (user_id, territory_name) VALUES (?, ?)', (user_id, province))
        conn.commit()
        return True

    def _get_owned_subregions(self, user_id: str) -> Set[str]:
        """Return sub‑regions where the user owns all provinces."""
        owned = self._get_owned_provinces(user_id)
        fully_owned = set()
        for subregion, province_list in PROVINCES.items():
            if all(p in owned for p in province_list):
                fully_owned.add(subregion)
        return fully_owned

    def _get_expansion_options(self, user_id: str) -> List[str]:
        """
        Return a list of provinces that are available to expand into.
        This includes:
          - Any province in a subregion that borders a fully owned subregion.
          - Any province in the same subregion as an already owned province (within the same region).
        """
        owned = set(self._get_owned_provinces(user_id))
        fully_owned_subregions = self._get_owned_subregions(user_id)
        
        possible = set()
        # 1. Provinces from neighbouring subregions
        for subregion in fully_owned_subregions:
            neighbours = SUBREGION_DATA.get(subregion, {}).get("neighbours", [])
            for neighbour_subregion in neighbours:
                for province in PROVINCES.get(neighbour_subregion, []):
                    if province not in owned:
                        possible.add(province)
        
        # 2. Provinces in the same subregion as any owned province (allows intra-region expansion)
        for province in owned:
            subregion = PROVINCE_TO_SUBREGION.get(province)
            if subregion:
                for p in PROVINCES.get(subregion, []):
                    if p not in owned:
                        possible.add(p)
        
        return sorted(list(possible))

    def _calculate_soldier_cost(self, civ) -> int:
        """Calculate 10% of current soldiers (rounded up, minimum 1)."""
        soldiers = civ['military']['soldiers']
        cost = math.ceil(soldiers * 0.1)
        return max(1, cost)

    # ---------- Commands ----------
    @commands.command(name='territories')
    async def list_territories(self, ctx):
        """List all provinces you own, grouped by sub‑region."""
        user_id = str(ctx.author.id)
        owned = self._get_owned_provinces(user_id)
        if not owned:
            await ctx.send("🌍 You don't own any provinces yet! Use `.expand` to claim your first.")
            return
        
        embed = discord.Embed(title="🗺️ Your Provinces", color=discord.Color.green())
        by_subregion = {}
        for province in owned:
            subregion = PROVINCE_TO_SUBREGION.get(province, "Unknown")
            by_subregion.setdefault(subregion, []).append(province)
        
        total_provinces = len(owned)
        fully_owned = self._get_owned_subregions(user_id)
        
        for subregion, provinces in by_subregion.items():
            total_in_subregion = len(PROVINCES.get(subregion, []))
            status = "✅ Complete" if subregion in fully_owned else f"{len(provinces)}/{total_in_subregion}"
            embed.add_field(name=f"{subregion} ({status})", value=", ".join(provinces), inline=False)
        
        embed.set_footer(text=f"Total: {total_provinces} provinces")
        await ctx.send(embed=embed)

    @commands.command(name='expand')
    @app_commands.describe(province="Name of the province to claim")
    async def expand(self, ctx, *, province: str = None):
        """Claim a new province (costs resources + 10% of your soldiers)."""
        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need a civilization first! Use `.start`.")
            return

        owned = self._get_owned_provinces(user_id)

        # If no province specified, show available options
        if province is None:
            if not owned:
                await ctx.send("❌ You have no province. Select a region with `.regions` to get your starting province, or use `.expand <province>` to claim any province as your first.")
                return
            possible = self._get_expansion_options(user_id)
            if not possible:
                await ctx.send("❌ No available provinces to expand into.")
                return
            embed = discord.Embed(title="🌍 Available Provinces", description="Use `.expand <province>` to claim one.", color=discord.Color.blue())
            by_subregion = {}
            for p in possible:
                subregion = PROVINCE_TO_SUBREGION.get(p, "Unknown")
                by_subregion.setdefault(subregion, []).append(p)
            for subregion, names in by_subregion.items():
                embed.add_field(name=subregion, value=", ".join(names), inline=False)
            embed.set_footer(text="Each expansion costs resources + 10% of your soldiers.")
            await ctx.send(embed=embed)
            return

        # Find exact match (case-insensitive)
        match = None
        for p in ALL_PROVINCES:
            if p.lower() == province.lower():
                match = p
                break
        if not match:
            # Partial match
            for p in ALL_PROVINCES:
                if province.lower() in p.lower():
                    match = p
                    break
        if not match:
            await ctx.send(f"❌ Unknown province: `{province}`. Use `.expand` to see available provinces.")
            return
        province = match

        # Determine cost based on subregion (scale by number of provinces)
        subregion = PROVINCE_TO_SUBREGION[province]
        province_count = len(PROVINCES[subregion])
        base_cost = {
            "gold": 300 + (100 // max(1, province_count)),
            "food": 100 + (50 // max(1, province_count)),
            "wood": 50 + (25 // max(1, province_count)),
            "stone": 50 + (25 // max(1, province_count)),
        }
        cost = base_cost

        # First province: allow claiming any (no adjacency check)
        if not owned:
            if not self.civ_manager.can_afford(user_id, cost):
                cost_str = ", ".join([f"{amount} {res}" for res, amount in cost.items()])
                await ctx.send(f"❌ Cannot afford to claim **{province}**. Requires: {cost_str}.")
                return

            soldier_cost = self._calculate_soldier_cost(civ)
            if civ['military']['soldiers'] < soldier_cost:
                await ctx.send(f"❌ You need at least {soldier_cost} soldiers (10% of your army) to claim this province! You have {civ['military']['soldiers']}.")
                return

            self.civ_manager.spend_resources(user_id, cost)
            self.civ_manager.update_military(user_id, {"soldiers": -soldier_cost})

            if self._add_province(user_id, province):
                land_gain = random.randint(50, 150)
                self.civ_manager.update_territory(user_id, {"land_size": land_gain})
                embed = discord.Embed(title="🏹 First Province Claimed!", description=f"**{civ['name']}** has claimed **{province}**!", color=discord.Color.green())
                embed.add_field(name="Cost", value=", ".join([f"{amount} {res}" for res, amount in cost.items()]) + f"\n⚔️ {soldier_cost} soldiers", inline=True)
                embed.add_field(name="Land Gained", value=f"+{land_gain} km²", inline=True)
                await ctx.send(embed=embed)
                self.db.log_event(user_id, "expansion", "First Province", f"Claimed {province}")
            else:
                await ctx.send("❌ Failed to claim province.")
            return

        # Normal expansion: check if province is already owned
        if province in owned:
            await ctx.send(f"❌ You already own **{province}**.")
            return

        # Check adjacency (is it in the list of possible expansions?)
        possible = self._get_expansion_options(user_id)
        if province not in possible:
            await ctx.send(f"❌ **{province}** is not adjacent to your territories. You can only expand into neighbouring provinces.")
            return

        # Check resource cost
        if not self.civ_manager.can_afford(user_id, cost):
            cost_str = ", ".join([f"{amount} {res}" for res, amount in cost.items()])
            await ctx.send(f"❌ Cannot afford to claim **{province}**. Requires: {cost_str}.")
            return

        # Soldier cost
        soldier_cost = self._calculate_soldier_cost(civ)
        if civ['military']['soldiers'] < soldier_cost:
            await ctx.send(f"❌ You need at least {soldier_cost} soldiers (10% of your army) to expand! You have {civ['military']['soldiers']}.")
            return

        # Spend resources and soldiers, then claim
        self.civ_manager.spend_resources(user_id, cost)
        self.civ_manager.update_military(user_id, {"soldiers": -soldier_cost})

        if self._add_province(user_id, province):
            land_gain = random.randint(30, 100)
            self.civ_manager.update_territory(user_id, {"land_size": land_gain})
            embed = discord.Embed(title="🏹 Expansion Successful!", description=f"**{civ['name']}** has claimed **{province}**!", color=discord.Color.green())
            embed.add_field(name="Cost", value=", ".join([f"{amount} {res}" for res, amount in cost.items()]) + f"\n⚔️ {soldier_cost} soldiers", inline=True)
            embed.add_field(name="Land Gained", value=f"+{land_gain} km²", inline=True)
            await ctx.send(embed=embed)
            self.db.log_event(user_id, "expansion", "Province Claimed", f"Claimed {province}")
        else:
            await ctx.send("❌ Failed to claim province.")


async def setup(bot):
    await bot.add_cog(TerritoryCog(bot))
