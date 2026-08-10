import random
import json
import logging
import math
import os
from typing import List, Optional, Set
import discord
from discord.ext import commands
from discord import app_commands

from bot.utils import create_embed, format_number

logger = logging.getLogger(__name__)

# --- Load province areas from JSON ---
PROVINCE_AREAS = {}
try:
    with open('province_areas.json', 'r') as f:
        PROVINCE_AREAS = json.load(f)
    logger.info("Loaded province areas from province_areas.json")
except FileNotFoundError:
    logger.warning("province_areas.json not found; using default area of 1000 km².")
    PROVINCE_AREAS = {}

# --- Province definitions (all UN members except tiny islands, plus Palestine/Kosovo) ---
PROVINCES = {
    "Eastern Europe": [
        "Poland", "Czechia", "Slovakia", "Hungary", "Romania", "Bulgaria",
        "Ukraine", "Belarus", "Moldova", "Russia", "Kosovo", "Serbia",
        "Bosnia and Herzegovina", "Montenegro", "Albania", "North Macedonia",
        "Slovenia", "Croatia"
    ],
    "Western Europe": [
        "France", "Germany", "United Kingdom", "Ireland", "Netherlands",
        "Belgium", "Luxembourg", "Switzerland", "Austria", "Monaco",
        "Andorra", "Liechtenstein", "San Marino"
    ],
    "Southern Europe": [
        "Portugal", "Spain", "Italy", "Greece", "Malta", "Cyprus"
    ],
    "Northern Europe": [
        "Norway", "Sweden", "Finland", "Denmark", "Iceland",
        "Estonia", "Latvia", "Lithuania", "Greenland"
    ],
    "Central Asia": [
        "Kazakhstan", "Uzbekistan", "Turkmenistan", "Kyrgyzstan",
        "Tajikistan", "Afghanistan"
    ],
    "East Asia": [
        "China", "Japan", "South Korea", "North Korea", "Mongolia",
        "Taiwan"
    ],
    "South Asia": [
        "India", "Pakistan", "Bangladesh", "Sri Lanka", "Nepal", "Bhutan"
    ],
    "Southeast Asia": [
        "Thailand", "Vietnam", "Indonesia", "Philippines", "Malaysia",
        "Singapore", "Cambodia", "Laos", "Timor-Leste", "Brunei",
        "Myanmar"
    ],
    "Middle East": [
        "Turkey", "Iran", "Iraq", "Syria", "Lebanon", "Israel",
        "Palestine", "Jordan", "Saudi Arabia", "Yemen", "Oman",
        "United Arab Emirates", "Qatar", "Kuwait",
        "Georgia", "Armenia", "Azerbaijan"
    ],
    "North Africa": [
        "Morocco", "Algeria", "Tunisia", "Libya", "Egypt", "Western Sahara"
    ],
    "West Africa": [
        "Mauritania", "Senegal", "Gambia", "Mali", "Burkina Faso",
        "Benin", "Togo", "Ghana", "Ivory Coast", "Liberia",
        "Sierra Leone", "Guinea", "Guinea-Bissau", "Cape Verde",
        "Nigeria", "Niger"
    ],
    "Central Africa": [
        "Chad", "Cameroon", "Central African Republic", "DR Congo",
        "Republic of the Congo", "Gabon", "Equatorial Guinea"
    ],
    "East Africa": [
        "Sudan", "South Sudan", "Eritrea", "Ethiopia", "Djibouti",
        "Somalia", "Kenya", "Uganda", "Rwanda", "Burundi",
        "Tanzania", "Mozambique", "Madagascar", "Comoros", "Seychelles",
        "Mauritius"
    ],
    "Southern Africa": [
        "Angola", "Zambia", "Malawi", "Zimbabwe", "Botswana",
        "Namibia", "South Africa", "Eswatini", "Lesotho"
    ],
    "Western North America": [
        "Canada", "United States"
    ],
    "Central North America": [
        "Mexico"
    ],
    "Eastern North America": [
        "United States"  # duplicate? We'll keep as is.
    ],
    "Mexico": ["Mexico"],
    "Central America": [
        "Guatemala", "Belize", "Honduras", "El Salvador", "Nicaragua",
        "Costa Rica", "Panama"
    ],
    "Northern South America": [
        "Venezuela", "Colombia", "Guyana", "Suriname"
    ],
    "Western South America": [
        "Ecuador", "Peru", "Bolivia", "Chile"
    ],
    "Eastern South America": [
        "Brazil"
    ],
    "Brazil": ["Brazil"],
    "Southern Cone": [
        "Argentina", "Uruguay", "Paraguay"
    ],
    "Australia": ["Australia"],
    "New Zealand": ["New Zealand"],
    "Pacific Islands": [
        "Papua New Guinea"
    ],
    "Antarctic Peninsula": [],
    "East Antarctica": [],
    "West Antarctica": [],
}

# Build reverse mapping
PROVINCE_TO_SUBREGION = {}
for subregion, province_list in PROVINCES.items():
    for province in province_list:
        PROVINCE_TO_SUBREGION[province] = subregion

ALL_PROVINCES = list(PROVINCE_TO_SUBREGION.keys())
ALL_SUBREGIONS = list(PROVINCES.keys())

# Neighbour data (used when no subregion fully owned)
SUBREGION_DATA = {
    "Eastern Europe": {"neighbours": ["Western Europe", "Northern Europe", "Central Asia"]},
    "Western Europe": {"neighbours": ["Southern Europe", "Northern Europe", "Eastern Europe"]},
    "Southern Europe": {"neighbours": ["Western Europe", "Middle East", "North Africa"]},
    "Northern Europe": {"neighbours": ["Western Europe", "Eastern Europe"]},
    "Central Asia": {"neighbours": ["Eastern Europe", "South Asia", "East Asia", "Middle East"]},
    "East Asia": {"neighbours": ["Central Asia", "South Asia", "Southeast Asia"]},
    "South Asia": {"neighbours": ["Central Asia", "East Asia", "Southeast Asia", "Middle East"]},
    "Southeast Asia": {"neighbours": ["East Asia", "South Asia", "Australia"]},
    "Middle East": {"neighbours": ["Southern Europe", "Central Asia", "South Asia", "North Africa"]},
    "North Africa": {"neighbours": ["Southern Europe", "Middle East", "West Africa", "Central Africa"]},
    "West Africa": {"neighbours": ["North Africa", "Central Africa", "Southern Africa"]},
    "Central Africa": {"neighbours": ["North Africa", "West Africa", "East Africa", "Southern Africa"]},
    "East Africa": {"neighbours": ["North Africa", "Central Africa", "Southern Africa"]},
    "Southern Africa": {"neighbours": ["West Africa", "Central Africa", "East Africa"]},
    "Western North America": {"neighbours": ["Central North America"]},
    "Central North America": {"neighbours": ["Western North America", "Eastern North America", "Mexico"]},
    "Eastern North America": {"neighbours": ["Central North America"]},
    "Mexico": {"neighbours": ["Western North America", "Central North America", "Central America"]},
    "Central America": {"neighbours": ["Mexico", "Northern South America"]},
    "Northern South America": {"neighbours": ["Central America", "Western South America", "Brazil"]},
    "Western South America": {"neighbours": ["Northern South America", "Brazil", "Southern Cone"]},
    "Eastern South America": {"neighbours": ["Northern South America", "Brazil", "Southern Cone"]},
    "Brazil": {"neighbours": ["Northern South America", "Western South America", "Eastern South America", "Southern Cone"]},
    "Southern Cone": {"neighbours": ["Western South America", "Eastern South America", "Brazil"]},
    "Australia": {"neighbours": ["Southeast Asia", "New Zealand"]},
    "New Zealand": {"neighbours": ["Australia"]},
    "Pacific Islands": {"neighbours": ["Australia", "New Zealand"]},
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

# ----------------------------------------------------------------------
class TerritoryCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.db = bot.db
        self.civ_manager = bot.civ_manager
        self.province_areas = PROVINCE_AREAS
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

    def _get_owned_provinces(self, user_id: str) -> List[str]:
        conn = self.db.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT owned_provinces FROM territories WHERE user_id = ?', (user_id,))
        row = cursor.fetchone()
        if row:
            return json.loads(row[0])
        return []

    def _set_owned_provinces(self, user_id: str, provinces: List[str]):
        conn = self.db.get_connection()
        cursor = conn.cursor()
        cursor.execute('INSERT OR REPLACE INTO territories (user_id, owned_provinces) VALUES (?, ?)',
                       (user_id, json.dumps(provinces)))
        conn.commit()

    def _add_province(self, user_id: str, province: str) -> bool:
        """Add a province and update territory land size appropriately."""
        owned = self._get_owned_provinces(user_id)
        if province in owned:
            return False

        is_first = (len(owned) == 0)
        owned.append(province)
        self._set_owned_provinces(user_id, owned)

        area = self.province_areas.get(province, 1000)  # default 1000 km²
        civ = self.civ_manager.get_civilization(user_id)
        if civ:
            if is_first:
                # First province: set land size to the area directly
                new_land = area
            else:
                current_land = civ['territory']['land_size']
                new_land = current_land + area
            self.civ_manager.update_territory(user_id, {"land_size": new_land})

        # Log history
        conn = self.db.get_connection()
        cursor = conn.cursor()
        cursor.execute('INSERT INTO territory_history (user_id, territory_name) VALUES (?, ?)', (user_id, province))
        conn.commit()
        return True

    def _get_owned_subregions(self, user_id: str) -> Set[str]:
        owned = self._get_owned_provinces(user_id)
        fully_owned = set()
        for subregion, province_list in PROVINCES.items():
            if all(p in owned for p in province_list):
                fully_owned.add(subregion)
        return fully_owned

    def _get_expansion_options(self, user_id: str) -> List[str]:
        owned = set(self._get_owned_provinces(user_id))
        fully_owned_subregions = self._get_owned_subregions(user_id)

        possible = set()
        if fully_owned_subregions:
            for province in ALL_PROVINCES:
                if province not in owned:
                    possible.add(province)
        else:
            for province in owned:
                subregion = PROVINCE_TO_SUBREGION.get(province)
                if subregion:
                    for p in PROVINCES.get(subregion, []):
                        if p not in owned:
                            possible.add(p)
            for subregion in fully_owned_subregions:
                neighbours = SUBREGION_DATA.get(subregion, {}).get("neighbours", [])
                for neighbour_subregion in neighbours:
                    for province in PROVINCES.get(neighbour_subregion, []):
                        if province not in owned:
                            possible.add(province)
        return sorted(list(possible))

    def _calculate_soldier_cost(self, civ) -> int:
        soldiers = civ['military']['soldiers']
        cost = math.ceil(soldiers * 0.1)
        return max(10, cost)

    @commands.command(name='territories')
    async def list_territories(self, ctx):
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
        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need a civilization first! Use `.start`.")
            return

        owned = self._get_owned_provinces(user_id)

        if province is None:
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
            embed.set_footer(text="Each expansion costs resources + minimum 10 soldiers.")
            await ctx.send(embed=embed)
            return

        # Find match
        match = None
        for p in ALL_PROVINCES:
            if p.lower() == province.lower():
                match = p
                break
        if not match:
            for p in ALL_PROVINCES:
                if province.lower() in p.lower():
                    match = p
                    break
        if not match:
            await ctx.send(f"❌ Unknown province: `{province}`. Use `.expand` to see available provinces.")
            return
        province = match

        if province in owned:
            await ctx.send(f"❌ You already own **{province}**.")
            return

        possible = self._get_expansion_options(user_id)
        if province not in possible:
            await ctx.send(f"❌ **{province}** is not currently available for expansion.")
            return

        # Resource cost: 5x original
        subregion = PROVINCE_TO_SUBREGION[province]
        province_count = len(PROVINCES[subregion])
        base_cost = {
            "gold": 300 + (100 // max(1, province_count)),
            "food": 100 + (50 // max(1, province_count)),
            "wood": 50 + (25 // max(1, province_count)),
            "stone": 50 + (25 // max(1, province_count)),
        }
        cost = {k: v * 5 for k, v in base_cost.items()}

        if not self.civ_manager.can_afford(user_id, cost):
            cost_str = ", ".join([f"{amount} {res}" for res, amount in cost.items()])
            await ctx.send(f"❌ Cannot afford to claim **{province}**. Requires: {cost_str}.")
            return

        soldier_cost = self._calculate_soldier_cost(civ)
        if civ['military']['soldiers'] < soldier_cost:
            await ctx.send(f"❌ You need at least {soldier_cost} soldiers (10% of your army, minimum 10) to expand! You have {civ['military']['soldiers']}.")
            return

        self.civ_manager.spend_resources(user_id, cost)
        self.civ_manager.update_military(user_id, {"soldiers": -soldier_cost})

        if self._add_province(user_id, province):
            area = self.province_areas.get(province, 1000)
            embed = discord.Embed(title="🏹 Expansion Successful!", description=f"**{civ['name']}** has claimed **{province}**!", color=discord.Color.green())
            embed.add_field(name="Cost", value=", ".join([f"{amount} {res}" for res, amount in cost.items()]) + f"\n⚔️ {soldier_cost} soldiers", inline=True)
            embed.add_field(name="Area Added", value=f"+{area:,} km²", inline=True)
            await ctx.send(embed=embed)
            self.db.log_event(user_id, "expansion", "Province Claimed", f"Claimed {province}")
        else:
            await ctx.send("❌ Failed to claim province.")


async def setup(bot):
    await bot.add_cog(TerritoryCog(bot))
