import random
import json
import logging
import math
import os
import asyncio
from typing import List, Optional, Set
import discord
from discord.ext import commands
from discord import app_commands

from bot.utils import create_embed, format_number
from bot.utils import get_territory_modifier

logger = logging.getLogger(__name__)

# ---- HARDCODED AREA OVERRIDES FOR ALL COUNTRIES (km²) ----
# Source: approximate real land areas (excluding water bodies)
AREA_OVERRIDES = {
    "Afghanistan": 652230,
    "Albania": 28748,
    "Algeria": 2381741,
    "Andorra": 468,
    "Angola": 1246700,
    "Antigua and Barbuda": 442,
    "Argentina": 2780400,
    "Armenia": 29743,
    "Australia": 7741220,
    "Austria": 83871,
    "Azerbaijan": 86600,
    "Bahamas": 13880,
    "Bahrain": 765,
    "Bangladesh": 147570,
    "Barbados": 430,
    "Belarus": 207600,
    "Belgium": 30528,
    "Belize": 22966,
    "Benin": 112622,
    "Bhutan": 38394,
    "Bolivia": 1098581,
    "Bosnia and Herzegovina": 51197,
    "Botswana": 581730,
    "Brazil": 8515767,
    "Brunei": 5765,
    "Bulgaria": 110879,
    "Burkina Faso": 274200,
    "Burundi": 27834,
    "Cabo Verde": 4033,
    "Cambodia": 181035,
    "Cameroon": 475442,
    "Canada": 9984670,
    "Central African Republic": 622984,
    "Chad": 1284000,
    "Chile": 756102,
    "China": 9596961,
    "Colombia": 1141748,
    "Comoros": 2235,
    "DR Congo": 2344858,
    "Republic of the Congo": 342000,
    "Costa Rica": 51100,
    "Croatia": 56594,
    "Cuba": 109884,
    "Cyprus": 9251,
    "Czechia": 78867,
    "Denmark": 43094,
    "Djibouti": 23200,
    "Dominica": 751,
    "Dominican Republic": 48671,
    "Ecuador": 283561,
    "Egypt": 1002450,
    "El Salvador": 21041,
    "Equatorial Guinea": 28051,
    "Eritrea": 117600,
    "Estonia": 45228,
    "Eswatini": 17364,
    "Ethiopia": 1104300,
    "Fiji": 18274,
    "Finland": 338424,
    "France": 551695,
    "Gabon": 267668,
    "Gambia": 11295,
    "Georgia": 69700,
    "Germany": 357022,
    "Ghana": 238533,
    "Greece": 131957,
    "Grenada": 344,
    "Guatemala": 108889,
    "Guinea": 245857,
    "Guinea-Bissau": 36125,
    "Guyana": 214969,
    "Haiti": 27750,
    "Honduras": 112492,
    "Hungary": 93028,
    "Iceland": 103000,
    "India": 3287263,
    "Indonesia": 1904569,
    "Iran": 1648195,
    "Iraq": 438317,
    "Ireland": 70273,
    "Israel": 20770,
    "Italy": 301340,
    "Jamaica": 10991,
    "Japan": 377930,
    "Jordan": 89342,
    "Kazakhstan": 2724900,
    "Kenya": 580367,
    "Kiribati": 811,
    "North Korea": 120538,
    "South Korea": 100210,
    "Kosovo": 10908,
    "Kuwait": 17818,
    "Kyrgyzstan": 199951,
    "Laos": 236800,
    "Latvia": 64589,
    "Lebanon": 10452,
    "Lesotho": 30355,
    "Liberia": 111369,
    "Libya": 1759540,
    "Liechtenstein": 160,
    "Lithuania": 65300,
    "Luxembourg": 2586,
    "Madagascar": 587041,
    "Malawi": 118484,
    "Malaysia": 329847,
    "Maldives": 298,
    "Mali": 1240192,
    "Malta": 316,
    "Marshall Islands": 181,
    "Mauritania": 1030700,
    "Mauritius": 2040,
    "Mexico": 1964375,
    "Micronesia": 702,
    "Moldova": 33851,
    "Monaco": 2,
    "Mongolia": 1564116,
    "Montenegro": 13812,
    "Morocco": 446550,
    "Mozambique": 801590,
    "Myanmar": 676578,
    "Namibia": 824292,
    "Nauru": 21,
    "Nepal": 147181,
    "Netherlands": 41850,
    "New Zealand": 268838,
    "Nicaragua": 130373,
    "Niger": 1267000,
    "Nigeria": 923768,
    "North Macedonia": 25713,
    "Norway": 323802,
    "Oman": 309500,
    "Pakistan": 881913,
    "Palau": 459,
    "Palestine": 6020,   # West Bank + Gaza
    "Panama": 75417,
    "Papua New Guinea": 462840,
    "Paraguay": 406752,
    "Peru": 1285216,
    "Philippines": 300000,
    "Poland": 312696,
    "Portugal": 92090,
    "Qatar": 11586,
    "Romania": 238397,
    "Russia": 17098242,
    "Rwanda": 26338,
    "Saint Kitts and Nevis": 261,
    "Saint Lucia": 616,
    "Saint Vincent and the Grenadines": 389,
    "Samoa": 2842,
    "San Marino": 61,
    "Sao Tome and Principe": 964,
    "Saudi Arabia": 2149690,
    "Senegal": 196722,
    "Serbia": 77474,
    "Seychelles": 455,
    "Sierra Leone": 71740,
    "Singapore": 728,
    "Slovakia": 49035,
    "Slovenia": 20273,
    "Solomon Islands": 28896,
    "Somalia": 637657,
    "South Africa": 1221037,
    "South Sudan": 644329,
    "Spain": 505990,
    "Sri Lanka": 65610,
    "Sudan": 1861484,
    "Suriname": 163820,
    "Sweden": 450295,
    "Switzerland": 41284,
    "Syria": 185180,
    "Taiwan": 36193,
    "Tajikistan": 143100,
    "Tanzania": 947300,
    "Thailand": 513120,
    "Timor-Leste": 14874,
    "Togo": 56785,
    "Tonga": 747,
    "Trinidad and Tobago": 5130,
    "Tunisia": 163610,
    "Turkey": 783562,
    "Turkmenistan": 488100,
    "Tuvalu": 26,
    "Uganda": 241038,
    "Ukraine": 603500,
    "United Arab Emirates": 83600,
    "United Kingdom": 242495,
    "United States": 9833517,
    "United States of America": 9833517,
    "USA": 9833517,
    "Uruguay": 176215,
    "Uzbekistan": 447400,
    "Vanuatu": 12189,
    "Vatican City": 0.44,  # too small, ignore
    "Venezuela": 912050,
    "Vietnam": 331212,
    "Yemen": 527968,
    "Zambia": 752612,
    "Zimbabwe": 390757,
    # Additional aliases or countries in PROVINCES not listed above:
    "Czech Republic": 78867,  # alias
    "Czechia": 78867,
    "Bosnia and Herz.": 51197,
    "Dem. Rep. Korea": 120538,
    "Congo (Kinshasa)": 2344858,
    "Congo (Brazzaville)": 342000,
    "Côte d'Ivoire": 322463,
    "Ivory Coast": 322463,
    "North Macedonia": 25713,
    "Eswatini": 17364,
    "Swaziland": 17364,
    "Greenland": 2166086,
    "Western Sahara": 266000,  # disputed, but we have it
    "W. Sahara": 266000,
    "S. Sudan": 644329,
    "S. Sudan": 644329,
    "Eq. Guinea": 28051,
    "C.A.R.": 622984,
    "Central African Rep.": 622984,
}

# Load province areas from JSON, then apply overrides
PROVINCE_AREAS = {}
try:
    with open('province_areas.json', 'r') as f:
        PROVINCE_AREAS = json.load(f)
    logger.info("Loaded province areas from province_areas.json")
except FileNotFoundError:
    logger.warning("province_areas.json not found; using default area of 1000 km².")
    PROVINCE_AREAS = {}

# Override with hardcoded values (these take precedence)
for name, area in AREA_OVERRIDES.items():
    PROVINCE_AREAS[name] = area
logger.info(f"Applied {len(AREA_OVERRIDES)} area overrides")

# ---- PROVINCES (same as before) ----
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
        "United States"
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

# ---- FORBIDDEN START PROVINCES (cannot be chosen as starting country) ----
FORBIDDEN_START_PROVINCES = {
    "Western Sahara",   # disputed territory, not a UN member
    # Add others if you want: "Palestine", "Kosovo", etc. but I'll leave them.
}

# Build reverse mapping
PROVINCE_TO_SUBREGION = {}
for subregion, province_list in PROVINCES.items():
    for province in province_list:
        PROVINCE_TO_SUBREGION[province] = subregion

ALL_PROVINCES = list(PROVINCE_TO_SUBREGION.keys())
ALL_SUBREGIONS = list(PROVINCES.keys())

# Neighbour data (same as before)
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
        owned = self._get_owned_provinces(user_id)
        if province in owned:
            return False

        is_first = (len(owned) == 0)
        owned.append(province)
        self._set_owned_provinces(user_id, owned)

        # Get area (now with overrides)
        area = self.province_areas.get(province, 1000)
        civ = self.civ_manager.get_civilization(user_id)
        if civ:
            if is_first:
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

        # Auto-unlock countryball
        asyncio.create_task(self._check_subregion_completion(user_id))
        return True

    async def _check_subregion_completion(self, user_id: str):
        owned = self._get_owned_provinces(user_id)
        completed_regions = []
        for subregion, provinces in PROVINCES.items():
            if provinces and all(p in owned for p in provinces):
                completed_regions.append(subregion)

        if not completed_regions:
            return

        cog = self.bot.get_cog("CountryballCog")
        if not cog:
            logger.warning("CountryballCog not loaded; cannot unlock countryballs.")
            return

        for region in completed_regions:
            await cog.check_region_unlock(user_id, region)

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

        # ---- Area and cost ----
        area = self.province_areas.get(province, 1000)
        # Cost multiplier based on area: sqrt(area/1000), capped at 20
        cost_multiplier = min(math.sqrt(area / 1000), 20.0)

        subregion = PROVINCE_TO_SUBREGION[province]
        province_count = len(PROVINCES[subregion])
        base_cost = {
            "gold": 300 + (100 // max(1, province_count)),
            "food": 100 + (50 // max(1, province_count)),
            "wood": 50 + (25 // max(1, province_count)),
            "stone": 50 + (25 // max(1, province_count)),
        }
        cost = {k: int(v * cost_multiplier) for k, v in base_cost.items()}

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
            embed = discord.Embed(title="🏹 Expansion Successful!", description=f"**{civ['name']}** has claimed **{province}**!", color=discord.Color.green())
            embed.add_field(name="Cost", value=", ".join([f"{amount} {res}" for res, amount in cost.items()]) + f"\n⚔️ {soldier_cost} soldiers", inline=True)
            embed.add_field(name="Area Added", value=f"+{area:,} km²", inline=True)
            await ctx.send(embed=embed)
            self.db.log_event(user_id, "expansion", "Province Claimed", f"Claimed {province}")
        else:
            await ctx.send("❌ Failed to claim province.")

async def setup(bot):
    await bot.add_cog(TerritoryCog(bot))
