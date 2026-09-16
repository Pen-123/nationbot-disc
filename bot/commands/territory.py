import random
import json
import logging
import math
import os
import asyncio
from typing import List, Optional, Set, Dict
import discord
from discord.ext import commands
from discord import app_commands

from bot.utils import create_embed, format_number, get_territory_modifier
from bot import config

logger = logging.getLogger(__name__)

# =====================================================================
# HARDCODED COUNTRY AREAS (km²)
# Includes all Natural Earth variants + modern country names.
# These are used as fallbacks if province_areas.json is missing.
# =====================================================================
AREA_OVERRIDES: Dict[str, float] = {
    # ----- Africa -----
    "Algeria": 2381741,
    "Angola": 1246700,
    "Benin": 112622,
    "Botswana": 581730,
    "Burkina Faso": 274200,
    "Burundi": 27834,
    "Cabo Verde": 4033,
    "Cape Verde": 4033,
    "Cameroon": 475442,
    "Central African Republic": 622984,
    "Central African Rep.": 622984,
    "C.A.R.": 622984,
    "Chad": 1284000,
    "Comoros": 2235,
    "Congo": 342000,
    "Republic of the Congo": 342000,
    "Republic of Congo": 342000,
    "Congo (Brazzaville)": 342000,
    "DR Congo": 2344858,
    "Democratic Republic of the Congo": 2344858,
    "Dem. Rep. Congo": 2344858,
    "Congo (Kinshasa)": 2344858,
    "Djibouti": 23200,
    "Egypt": 1002450,
    "Equatorial Guinea": 28051,
    "Eq. Guinea": 28051,
    "Eritrea": 117600,
    "Eswatini": 17364,
    "eSwatini": 17364,
    "Swaziland": 17364,
    "Ethiopia": 1104300,
    "Gabon": 267668,
    "Gambia": 11295,
    "Ghana": 238533,
    "Guinea": 245857,
    "Guinea-Bissau": 36125,
    "Ivory Coast": 322463,
    "Côte d'Ivoire": 322463,
    "Kenya": 580367,
    "Lesotho": 30355,
    "Liberia": 111369,
    "Libya": 1759540,
    "Madagascar": 587041,
    "Malawi": 118484,
    "Mali": 1240192,
    "Mauritania": 1030700,
    "Mauritius": 2040,
    "Morocco": 446550,
    "Mozambique": 801590,
    "Namibia": 824292,
    "Niger": 1267000,
    "Nigeria": 923768,
    "Rwanda": 26338,
    "Sao Tome and Principe": 964,
    "Senegal": 196722,
    "Seychelles": 455,
    "Sierra Leone": 71740,
    "Somalia": 637657,
    "Somaliland": 137600,
    "South Africa": 1221037,
    "South Sudan": 644329,
    "S. Sudan": 644329,
    "Sudan": 1861484,
    "Tanzania": 947300,
    "United Republic of Tanzania": 947300,
    "Togo": 56785,
    "Tunisia": 163610,
    "Uganda": 241038,
    "Western Sahara": 266000,
    "W. Sahara": 266000,
    "Zambia": 752612,
    "Zimbabwe": 390757,

    # ----- Asia -----
    "Afghanistan": 652230,
    "Armenia": 29743,
    "Azerbaijan": 86600,
    "Bahrain": 765,
    "Bangladesh": 147570,
    "Bhutan": 38394,
    "Brunei": 5765,
    "Cambodia": 181035,
    "China": 9596961,
    "Cyprus": 9251,
    "N. Cyprus": 3355,
    "Georgia": 69700,
    "India": 3287263,
    "Indonesia": 1904569,
    "Iran": 1648195,
    "Iraq": 438317,
    "Israel": 20770,
    "Japan": 377930,
    "Jordan": 89342,
    "Kazakhstan": 2724900,
    "Kuwait": 17818,
    "Kyrgyzstan": 199951,
    "Laos": 236800,
    "Lebanon": 10452,
    "Malaysia": 329847,
    "Maldives": 298,
    "Mongolia": 1564116,
    "Myanmar": 676578,
    "Nepal": 147181,
    "North Korea": 120538,
    "Dem. Rep. Korea": 120538,
    "Korea": 100210,
    "South Korea": 100210,
    "Oman": 309500,
    "Pakistan": 881913,
    "Palestine": 6020,
    "Philippines": 300000,
    "Qatar": 11586,
    "Saudi Arabia": 2149690,
    "Singapore": 728,
    "Sri Lanka": 65610,
    "Syria": 185180,
    "Taiwan": 36193,
    "Tajikistan": 143100,
    "Thailand": 513120,
    "Timor-Leste": 14874,
    "Turkey": 783562,
    "Turkmenistan": 488100,
    "United Arab Emirates": 83600,
    "UAE": 83600,
    "Uzbekistan": 447400,
    "Vietnam": 331212,
    "Yemen": 527968,

    # ----- Europe -----
    "Albania": 28748,
    "Andorra": 468,
    "Austria": 83871,
    "Belarus": 207600,
    "Belgium": 30528,
    "Bosnia and Herzegovina": 51197,
    "Bosnia and Herz.": 51197,
    "Bulgaria": 110879,
    "Croatia": 56594,
    "Czechia": 78867,
    "Czech Republic": 78867,
    "Denmark": 43094,
    "Estonia": 45228,
    "Faroe Is.": 1393,
    "Finland": 338424,
    "France": 551695,
    "Germany": 357022,
    "Greece": 131957,
    "Greenland": 2166086,
    "Hungary": 93028,
    "Iceland": 103000,
    "Ireland": 70273,
    "Italy": 301340,
    "Kosovo": 10908,
    "Latvia": 64589,
    "Liechtenstein": 160,
    "Lithuania": 65300,
    "Luxembourg": 2586,
    "Malta": 316,
    "Moldova": 33851,
    "Monaco": 2,
    "Montenegro": 13812,
    "Netherlands": 41850,
    "North Macedonia": 25713,
    "Macedonia": 25713,
    "Norway": 323802,
    "Poland": 312696,
    "Portugal": 92090,
    "Romania": 238397,
    "Russia": 17098242,
    "San Marino": 61,
    "Serbia": 77474,
    "Slovakia": 49035,
    "Slovenia": 20273,
    "Spain": 505990,
    "Sweden": 450295,
    "Switzerland": 41284,
    "Ukraine": 603500,
    "United Kingdom": 242495,
    "Vatican": 0.44,
    "Vatican City": 0.44,

    # ----- North America -----
    "Bahamas": 13880,
    "Barbados": 430,
    "Belize": 22966,
    "Canada": 9984670,
    "Costa Rica": 51100,
    "Cuba": 109884,
    "Dominica": 751,
    "Dominican Republic": 48671,
    "Dominican Rep.": 48671,
    "El Salvador": 21041,
    "Grenada": 344,
    "Guatemala": 108889,
    "Haiti": 27750,
    "Honduras": 112492,
    "Jamaica": 10991,
    "Mexico": 1964375,
    "Nicaragua": 130373,
    "Panama": 75417,
    "Saint Kitts and Nevis": 261,
    "Saint Lucia": 616,
    "St. Lucia": 616,
    "Saint Vincent and the Grenadines": 389,
    "St. Vincent": 389,
    "Trinidad and Tobago": 5130,
    "United States": 9833517,
    "United States of America": 9833517,
    "USA": 9833517,
    "US": 9833517,

    # ----- South America -----
    "Argentina": 2780400,
    "Bolivia": 1098581,
    "Brazil": 8515767,
    "Chile": 756102,
    "Colombia": 1141748,
    "Ecuador": 283561,
    "Falkland Is.": 12173,
    "French Guiana": 83534,
    "Guyana": 214969,
    "Paraguay": 406752,
    "Peru": 1285216,
    "Suriname": 163820,
    "Uruguay": 176215,
    "Venezuela": 912050,

    # ----- Oceania -----
    "Australia": 7741220,
    "Fiji": 18274,
    "Kiribati": 811,
    "Marshall Islands": 181,
    "Marshall Is.": 181,
    "Micronesia": 702,
    "Nauru": 21,
    "New Zealand": 268838,
    "Palau": 459,
    "Papua New Guinea": 462840,
    "Samoa": 2842,
    "Solomon Islands": 28896,
    "Solomon Is.": 28896,
    "Tonga": 747,
    "Tuvalu": 26,
    "Vanuatu": 12189,

    # ----- Caribbean + misc small states -----
    "Antigua and Barbuda": 442,
    "St. Kitts and Nevis": 261,

    # ----- Antarctica (no fixed owner) -----
    "Antarctica": 14000000,
}

# Load province_areas.json for any overrides it provides (usually the
# geojson generator writes more accurate values). Otherwise fall back to
# the hardcoded dict above.
PROVINCE_AREAS: Dict[str, float] = dict(AREA_OVERRIDES)
try:
    with open('province_areas.json', 'r') as f:
        loaded = json.load(f)
        PROVINCE_AREAS.update(loaded)
        logger.info(f"Loaded {len(loaded)} areas from province_areas.json (merged with {len(AREA_OVERRIDES)} hardcoded)")
except FileNotFoundError:
    logger.info(f"province_areas.json not found; using {len(AREA_OVERRIDES)} hardcoded area overrides.")
except Exception as e:
    logger.error(f"Failed to load province_areas.json: {e}; using hardcoded overrides.")

FORBIDDEN_START_PROVINCES = {"Western Sahara"}

# =====================================================================
# PROVINCES (states) grouped by subregions
# =====================================================================
PROVINCES: Dict[str, List[str]] = {
    "Eastern Europe": [
        "Poland", "Czechia", "Slovakia", "Hungary", "Romania", "Bulgaria",
        "Ukraine", "Belarus", "Moldova", "Russia"
    ],
    "Western Europe": [
        "France", "United Kingdom", "Ireland", "Netherlands",
        "Belgium", "Luxembourg", "Monaco", "Andorra", "San Marino"
    ],
    "Central Europe": [
        "Germany", "Austria", "Switzerland", "Liechtenstein"
    ],
    "Balkans": [
        "Kosovo", "Serbia", "Bosnia and Herzegovina", "Montenegro",
        "Albania", "North Macedonia", "Slovenia", "Croatia"
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
    "Northeast Asia": [
        "China", "Japan", "South Korea", "North Korea", "Mongolia", "Taiwan"
    ],
    "South Asia": [
        "India", "Pakistan", "Bangladesh", "Sri Lanka", "Nepal", "Bhutan"
    ],
    "Southeast Asia": [
        "Thailand", "Vietnam", "Indonesia", "Philippines", "Malaysia",
        "Singapore", "Cambodia", "Laos", "Timor-Leste", "Brunei", "Myanmar"
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

# ---- Reverse mapping: province -> subregion ----
PROVINCE_TO_SUBREGION: Dict[str, str] = {}
for subregion, province_list in PROVINCES.items():
    for province in province_list:
        PROVINCE_TO_SUBREGION[province] = subregion

ALL_PROVINCES: List[str] = list(PROVINCE_TO_SUBREGION.keys())
ALL_SUBREGIONS: List[str] = list(PROVINCES.keys())

# =====================================================================
# SUBREGION NEIGHBOURS
# =====================================================================
SUBREGION_DATA: Dict[str, Dict[str, List[str]]] = {
    "Eastern Europe": {"neighbours": ["Central Europe", "Balkans", "Northern Europe", "Central Asia"]},
    "Western Europe": {"neighbours": ["Southern Europe", "Central Europe", "Northern Europe"]},
    "Central Europe": {"neighbours": ["Western Europe", "Eastern Europe", "Balkans", "Southern Europe"]},
    "Balkans": {"neighbours": ["Central Europe", "Eastern Europe", "Southern Europe", "Middle East"]},
    "Southern Europe": {"neighbours": ["Western Europe", "Central Europe", "Balkans", "Middle East", "North Africa"]},
    "Northern Europe": {"neighbours": ["Western Europe", "Central Europe", "Eastern Europe"]},
    "Central Asia": {"neighbours": ["Eastern Europe", "South Asia", "Northeast Asia", "Middle East"]},
    "Northeast Asia": {"neighbours": ["Central Asia", "South Asia", "Southeast Asia"]},
    "South Asia": {"neighbours": ["Central Asia", "Northeast Asia", "Southeast Asia", "Middle East"]},
    "Southeast Asia": {"neighbours": ["Northeast Asia", "South Asia", "Australia"]},
    "Middle East": {"neighbours": ["Southern Europe", "Balkans", "Central Asia", "South Asia", "North Africa"]},
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

# =====================================================================
# SUBREGION -> CONTINENT
# =====================================================================
SUBREGION_TO_CONTINENT: Dict[str, str] = {
    "Eastern Europe": "Europe",
    "Western Europe": "Europe",
    "Central Europe": "Europe",
    "Balkans": "Europe",
    "Southern Europe": "Europe",
    "Northern Europe": "Europe",
    "Central Asia": "Asia",
    "Northeast Asia": "Asia",
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

# =====================================================================
# SUBREGION -> COUNTRYBALL UNLOCK
# =====================================================================
REGION_TO_COUNTRYBALL: Dict[str, Optional[str]] = {
    "Eastern Europe": "soviet_union",
    "Western Europe": "reich",
    "Central Europe": "german_empire",
    "Balkans": "austria-hungary",
    "Southern Europe": "italy",
    "Northern Europe": "british_empire",
    "Central Asia": "soviet_union",
    "Northeast Asia": "china",
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


class TerritoryCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.db = bot.db
        self.civ_manager = bot.civ_manager
        self.province_areas = PROVINCE_AREAS
        self._recent_expansions = {}

    # ---- Firestore-based territory helpers ----
    def _get_owned_provinces(self, user_id: str) -> List[str]:
        return self.db.get_player_territories(user_id)

    def _add_province(self, user_id: str, province: str, ctx=None) -> bool:
        owned = self._get_owned_provinces(user_id)
        if province in owned:
            return True
        owner = self.db.get_territory_owner(province)
        if owner and owner != user_id:
            return False
        success = self.db.conquer_territory(user_id, None, province)
        if success:
            area = self.province_areas.get(province, 1000)
            self.civ_manager.update_territory(user_id, {"land_size": area})
            return True
        return False

    def _calculate_base_soldier_cost(self, area: int) -> int:
        if area >= 1_000_000:
            base = max(config.EXPANSION["min_soldier_cost"], area // config.EXPANSION["soldier_per_area_large"])
        else:
            base = max(config.EXPANSION["min_soldier_cost"], area // config.EXPANSION["soldier_per_area_small"])
        return min(base, config.EXPANSION["max_soldier_cost"])

    def _get_navy_and_airforce(self, user_id: str):
        navy = self.db.get_navy(user_id)
        air = self.db.get_airforce(user_id)
        return sum(navy.values()) > 0, sum(air.values()) > 0

    def _get_owned_continents(self, user_id: str) -> Set[str]:
        owned = self._get_owned_provinces(user_id)
        continents = set()
        for p in owned:
            sub = PROVINCE_TO_SUBREGION.get(p)
            if sub:
                cont = SUBREGION_TO_CONTINENT.get(sub)
                if cont:
                    continents.add(cont)
        return continents

    def _is_overseas(self, user_id: str, target_subregion: str) -> bool:
        owned_continents = self._get_owned_continents(user_id)
        if not owned_continents:
            return False
        target_continent = SUBREGION_TO_CONTINENT.get(target_subregion)
        return target_continent not in owned_continents

    def _apply_expansion_reductions(self, user_id: str, target_subregion: str, resource_cost: dict, soldier_cost: int):
        has_navy, has_airforce = self._get_navy_and_airforce(user_id)
        is_overseas = self._is_overseas(user_id, target_subregion)
        if is_overseas:
            if has_navy:
                reduction = 0.75
                reason = "🛳️ Navy (25% off – overseas)"
            else:
                return resource_cost, soldier_cost, "⚠️ No navy – overseas expansion blocked"
        else:
            if has_airforce:
                reduction = 0.5
                reason = "✈️ Airforce (50% off – land)"
            elif has_navy:
                reduction = 0.75
                reason = "🛳️ Navy (25% off – land)"
            else:
                reduction = 1.0
                reason = "No reduction (land)"
        new_resource_cost = {res: max(1, int(round(cost * reduction))) for res, cost in resource_cost.items()}
        new_soldier_cost = max(1, int(round(soldier_cost * reduction)))
        return new_resource_cost, new_soldier_cost, reason

    def _get_expansion_options(self, user_id: str) -> List[str]:
        owned = set(self._get_owned_provinces(user_id))
        if not owned:
            return []
        owned_subregions = set()
        for p in owned:
            sub = PROVINCE_TO_SUBREGION.get(p)
            if sub:
                owned_subregions.add(sub)
        possible = set()
        for sub in owned_subregions:
            for p in PROVINCES.get(sub, []):
                if p not in owned:
                    possible.add(p)
        neighbours = set()
        for sub in owned_subregions:
            for nsub in SUBREGION_DATA.get(sub, {}).get('neighbours', []):
                neighbours.add(nsub)
        for nsub in neighbours:
            for p in PROVINCES.get(nsub, []):
                if p not in owned:
                    possible.add(p)
        for sub in ALL_SUBREGIONS:
            if sub not in owned_subregions and sub not in neighbours:
                for p in PROVINCES.get(sub, []):
                    if p not in owned:
                        possible.add(p)
        return sorted(list(possible))

    def _get_countries_available(self, user_id: str) -> Set[str]:
        owned = self._get_owned_provinces(user_id)
        if not owned:
            return set(ALL_PROVINCES)
        return set(self._get_expansion_options(user_id))

    # =================================================================
    # LIST TERRITORIES
    # =================================================================

    @commands.command(name='territories')
    async def list_territories(self, ctx):
        user_id = str(ctx.author.id)
        owned = self._get_owned_provinces(user_id)
        if not owned:
            await ctx.send("🌍 You don't own any provinces yet! Use `.expand` to claim your first.")
            return

        embed = discord.Embed(title="🗺️ Your Provinces (States)", color=discord.Color.green())
        embed.add_field(name="Total States", value=f"{len(owned)}", inline=True)

        # Total land size
        total_area = sum(self.province_areas.get(p, 1000) for p in owned)
        embed.add_field(name="Total Land", value=f"{total_area:,.0f} km²", inline=True)

        # Civil war state
        civ = self.civ_manager.get_civilization(user_id)
        cw = (civ or {}).get('civil_war') or {}
        if cw.get('active'):
            embed.add_field(
                name="⚠️ Civil War",
                value=(f"Loyalist: {len(cw.get('loyalist_territories', []))} | "
                       f"Rebel: {len(cw.get('rebel_territories', []))}"),
                inline=True
            )

        # Show up to 20 territories with their areas
        shown = owned[:20]
        lines = []
        for p in shown:
            area = self.province_areas.get(p, 1000)
            lines.append(f"• **{p}** ({area:,.0f} km²)")
        value = "\n".join(lines)
        if len(owned) > 20:
            value += f"\n*...and {len(owned) - 20} more*"
        embed.add_field(name="Owned States", value=value[:1024], inline=False)
        await ctx.send(embed=embed)

    @commands.command(name='states')
    async def list_all_states(self, ctx, country: str = None):
        territories = self.db.get_all_territories()
        if country:
            if country not in ALL_PROVINCES:
                matches = [p for p in ALL_PROVINCES if country.lower() in p.lower()]
                if not matches:
                    await ctx.send(f"❌ No country/state named `{country}` found.")
                    return
                country = matches[0]
            owner_id = territories.get(country, {}).get("owner_id")
            area = self.province_areas.get(country, 1000)
            if owner_id:
                civ = self.civ_manager.get_civilization(owner_id)
                owner_name = civ['name'] if civ else "Unknown"
                await ctx.send(f"**{country}** ({area:,.0f} km²) is owned by **{owner_name}**.")
            else:
                await ctx.send(f"**{country}** ({area:,.0f} km²) is unowned.")
            return

        embed = discord.Embed(title="🌍 Global State Map", color=discord.Color.blue())
        by_country: Dict[str, List[str]] = {}
        for province, data in territories.items():
            owner_id = data.get("owner_id")
            if owner_id:
                civ = self.civ_manager.get_civilization(owner_id)
                owner_name = civ['name'] if civ else "Unknown"
            else:
                owner_name = "Unowned"
            by_country.setdefault(owner_name, []).append(province)
        for owner, states in by_country.items():
            if owner == "Unowned":
                embed.add_field(
                    name="🌍 Unowned",
                    value=", ".join(states[:10]) + ("..." if len(states) > 10 else ""),
                    inline=False
                )
            else:
                embed.add_field(
                    name=f"👑 {owner} ({len(states)} states)",
                    value=", ".join(states[:10]) + ("..." if len(states) > 10 else ""),
                    inline=False
                )
        await ctx.send(embed=embed)

    # =================================================================
    # EXPAND
    # =================================================================

    @commands.command(name='expand')
    @app_commands.describe(country="Name of the country to expand into (e.g., 'India', 'Germany')")
    async def expand(self, ctx, *, country: str = None):
        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need a civilization first! Use `.start`.")
            return

        if not country:
            available = self._get_countries_available(user_id)
            if not available:
                await ctx.send("❌ No available countries to expand into.")
                return
            subregion_map: Dict[str, List[str]] = {}
            for subregion, provinces in PROVINCES.items():
                avail = sorted([p for p in provinces if p in available])
                if avail:
                    subregion_map[subregion] = avail
            items = list(subregion_map.items())
            page_size = 20
            pages = [items[i:i + page_size] for i in range(0, len(items), page_size)] or [[]]
            for idx, page in enumerate(pages, 1):
                embed = discord.Embed(
                    title=f"🌍 Available Countries (Page {idx}/{len(pages)})",
                    description=("Use `.expand <country_name>` to claim a country.\n"
                                 "Every nation in every subregion is listed below."),
                    color=discord.Color.blue()
                )
                for subregion, countries in page:
                    value = ", ".join(countries)
                    if len(value) > 1024:
                        value = value[:1021] + "..."
                    embed.add_field(name=f"📍 {subregion} ({len(countries)})", value=value, inline=False)
                embed.set_footer(text="Tip: .rapidexpansion <country> uses soldiers only (2× cost)")
                await ctx.send(embed=embed)
            return

        country_match = None
        for p in ALL_PROVINCES:
            if p.lower() == country.lower():
                country_match = p
                break
        if not country_match:
            for p in ALL_PROVINCES:
                if country.lower() in p.lower():
                    country_match = p
                    break
        if not country_match:
            await ctx.send(f"❌ Unknown country: `{country}`. Use `.expand` to see available countries.")
            return

        country = country_match
        available = self._get_countries_available(user_id)
        if country not in available:
            await ctx.send(f"❌ **{country}** is not currently available for expansion.")
            return
        owned = self._get_owned_provinces(user_id)
        if country in owned:
            await ctx.send(f"❌ You already own **{country}**.")
            return

        target_state = country
        target_subregion = PROVINCE_TO_SUBREGION.get(target_state)
        if not target_subregion:
            await ctx.send("❌ State has no subregion mapping. Contact admin.")
            return

        is_overseas = self._is_overseas(user_id, target_subregion)
        has_navy, has_air = self._get_navy_and_airforce(user_id)
        if is_overseas and not has_navy:
            await ctx.send(f"❌ **{target_state}** is overseas! You need a navy to expand there.")
            return

        area = self.province_areas.get(target_state, 1000)
        cost_multiplier = min(math.sqrt(area / 1000), 20.0)
        province_count = len(PROVINCES.get(target_subregion, []))
        base_cost = {
            "gold": config.EXPANSION["base_gold_per_province"] + (100 // max(1, province_count)),
            "food": config.EXPANSION["base_food_per_province"] + (50 // max(1, province_count)),
            "wood": config.EXPANSION["base_wood_per_province"] + (10 // max(1, province_count)),
            "stone": config.EXPANSION["base_stone_per_province"] + (10 // max(1, province_count)),
        }
        resource_cost = {k: int(v * cost_multiplier * config.EXPANSION["resource_cost_multiplier"]) for k, v in base_cost.items()}
        soldier_cost = self._calculate_base_soldier_cost(area)
        resource_cost, soldier_cost, reduction_reason = self._apply_expansion_reductions(
            user_id, target_subregion, resource_cost, soldier_cost
        )
        owned_count = len(owned)
        if owned_count > 0:
            scale_factor = 1 + owned_count * 0.05
            resource_cost = {k: max(1, int(v * scale_factor)) for k, v in resource_cost.items()}
            soldier_cost = max(1, int(soldier_cost * scale_factor))

        repel_chance = 0.25
        if random.random() < repel_chance:
            lost_resources = {k: int(v * 0.3) for k, v in resource_cost.items()}
            lost_soldiers = max(1, int(soldier_cost * 0.3))
            self.civ_manager.spend_resources(user_id, lost_resources)
            self.civ_manager.update_military(user_id, {"soldiers": -lost_soldiers})
            embed = discord.Embed(
                title="🛡️ Expansion Repelled!",
                description=f"The defenders of **{target_state}** have repelled your invasion!",
                color=discord.Color.red()
            )
            embed.add_field(
                name="Lost Resources",
                value="\n".join([
                    f"{'🪙' if res=='gold' else '🌾' if res=='food' else '🪵' if res=='wood' else '🪨'} {format_number(amt)} {res.capitalize()}"
                    for res, amt in lost_resources.items()
                ]),
                inline=True
            )
            embed.add_field(name="Lost Soldiers", value=f"⚔️ {format_number(lost_soldiers)}", inline=True)
            await ctx.send(embed=embed)
            self.db.log_event(user_id, "expansion_repelled", "Expansion Repelled", f"Repelled from {target_state}")
            return

        if not self.civ_manager.can_afford(user_id, resource_cost):
            cost_str = ", ".join([f"{amt} {res}" for res, amt in resource_cost.items()])
            await ctx.send(f"❌ Cannot afford to claim **{target_state}**. Requires: {cost_str}.")
            return
        if civ['military']['soldiers'] < soldier_cost:
            await ctx.send(f"❌ You need at least {soldier_cost} soldiers to claim **{target_state}**! You have {civ['military']['soldiers']}.")
            return

        self.civ_manager.spend_resources(user_id, resource_cost)
        self.civ_manager.update_military(user_id, {"soldiers": -soldier_cost})

        if self._add_province(user_id, target_state, ctx):
            embed = discord.Embed(
                title="🏹 Expansion Successful!",
                description=f"**{civ['name']}** has conquered **{target_state}**!",
                color=discord.Color.green()
            )
            cost_display = ", ".join([f"{amt} {res}" for res, amt in resource_cost.items()])
            embed.add_field(name="Cost", value=cost_display + f"\n⚔️ {soldier_cost} soldiers", inline=True)
            embed.add_field(name="Area Added", value=f"+{area:,.0f} km²", inline=True)
            embed.add_field(name="Reduction Applied", value=reduction_reason, inline=False)
            embed.add_field(name="Overseas", value="✅" if is_overseas else "❌", inline=True)
            await ctx.send(embed=embed)
            self.db.log_event(user_id, "expansion", "State Claimed", f"Claimed {target_state} (overseas: {is_overseas})")
        else:
            await ctx.send("❌ Failed to claim state. Please try again.")

    @commands.command(name='rapidexpansion')
    @app_commands.describe(country="Name of the country to expand into")
    async def rapid_expansion(self, ctx, *, country: str = None):
        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need a civilization first! Use `.start`.")
            return

        if not country:
            available = self._get_countries_available(user_id)
            if not available:
                await ctx.send("❌ No available countries to expand into.")
                return
            embed = discord.Embed(
                title="⚡ Rapid Expansion",
                description="Use `.rapidexpansion <country_name>` to expand using only soldiers (2x cost).",
                color=discord.Color.orange()
            )
            embed.add_field(
                name="Available Countries",
                value=", ".join(sorted(available)[:25]) + ("..." if len(available) > 25 else ""),
                inline=False
            )
            await ctx.send(embed=embed)
            return

        country_match = None
        for p in ALL_PROVINCES:
            if p.lower() == country.lower():
                country_match = p
                break
        if not country_match:
            for p in ALL_PROVINCES:
                if country.lower() in p.lower():
                    country_match = p
                    break
        if not country_match:
            await ctx.send(f"❌ Unknown country: `{country}`.")
            return

        country = country_match
        available = self._get_countries_available(user_id)
        if country not in available:
            await ctx.send(f"❌ **{country}** is not currently available.")
            return
        owned = self._get_owned_provinces(user_id)
        if country in owned:
            await ctx.send(f"❌ You already own **{country}**.")
            return

        target_state = country
        target_subregion = PROVINCE_TO_SUBREGION.get(target_state)
        if not target_subregion:
            await ctx.send("❌ State has no subregion mapping.")
            return
        is_overseas = self._is_overseas(user_id, target_subregion)
        has_navy, _ = self._get_navy_and_airforce(user_id)
        if is_overseas and not has_navy:
            await ctx.send(f"❌ **{target_state}** is overseas! You need a navy.")
            return

        area = self.province_areas.get(target_state, 1000)
        soldier_cost = self._calculate_base_soldier_cost(area) * 2
        soldier_cost = min(soldier_cost, config.EXPANSION["rapid_max_soldier_cost"])
        owned_count = len(owned)
        if owned_count > 0:
            scale_factor = 1 + owned_count * 0.05
            soldier_cost = max(1, int(soldier_cost * scale_factor))

        if random.random() < 0.25:
            lost_soldiers = max(1, int(soldier_cost * 0.3))
            self.civ_manager.update_military(user_id, {"soldiers": -lost_soldiers})
            embed = discord.Embed(
                title="🛡️ Expansion Repelled!",
                description=f"The defenders of **{target_state}** repelled your rapid invasion!",
                color=discord.Color.red()
            )
            embed.add_field(name="Lost Soldiers", value=f"⚔️ {format_number(lost_soldiers)}", inline=True)
            await ctx.send(embed=embed)
            return

        if civ['military']['soldiers'] < soldier_cost:
            await ctx.send(f"❌ You need {soldier_cost} soldiers! You have {civ['military']['soldiers']}.")
            return

        self.civ_manager.update_military(user_id, {"soldiers": -soldier_cost})

        if self._add_province(user_id, target_state, ctx):
            embed = discord.Embed(
                title="⚡ Rapid Expansion Successful!",
                description=f"**{civ['name']}** has rapidly conquered **{target_state}** using {soldier_cost} soldiers!",
                color=discord.Color.gold()
            )
            embed.add_field(name="Soldiers Spent", value=f"⚔️ {format_number(soldier_cost)}", inline=True)
            embed.add_field(name="Area Added", value=f"+{area:,.0f} km²", inline=True)
            embed.add_field(name="Overseas", value="✅" if is_overseas else "❌", inline=True)
            await ctx.send(embed=embed)
            self.db.log_event(user_id, "rapid_expansion", "Rapid Expansion", f"Rapidly claimed {target_state}")
        else:
            await ctx.send("❌ Failed to claim state. Please try again.")

    # =================================================================
    # CIVIL WAR — RECLAIM & STATUS
    # =================================================================

    @commands.command(name='reclaim')
    @app_commands.describe(territory="Specific rebel territory to attack (optional)")
    async def reclaim_territory(self, ctx, *, territory: str = None):
        """Fight a rebel-held territory during a civil war. +30% offensive boost applies."""
        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need a civilization first!")
            return

        state = self.civ_manager.get_civil_war_state(user_id)
        if not state:
            await ctx.send("❌ You are not currently in a civil war.")
            return

        rebel_territories = state.get('rebel_territories', [])
        if not rebel_territories:
            await ctx.send("✅ No rebel territories remain! The war is essentially over.")
            return

        target = None
        if territory:
            for r in rebel_territories:
                if r.lower() == territory.lower():
                    target = r
                    break
            if not target:
                for r in rebel_territories:
                    if territory.lower() in r.lower():
                        target = r
                        break
            if not target:
                await ctx.send(
                    f"❌ **{territory}** is not a rebel-held territory.\n"
                    f"Rebel territories: {', '.join(rebel_territories[:10])}"
                )
                return

        result = self.civ_manager.fight_civil_war_battle(user_id, target)

        if result.get("error") == "not_enough_soldiers":
            await ctx.send(f"❌ You need at least **{result['min']}** soldiers to fight the rebels!")
            return
        if result.get("error"):
            await ctx.send(f"❌ Error: {result['error']}")
            return

        if result.get("victory"):
            embed = discord.Embed(
                title="⚔️ Loyalist Victory!",
                description=f"Your forces reclaimed **{result['territory']}** from the rebels!",
                color=discord.Color.green()
            )
            embed.add_field(name="Soldier Losses", value=f"⚔️ {format_number(result['soldier_losses'])}", inline=True)
            embed.add_field(name="Rebel Strength Now", value=f"🏴 {result['rebel_strength']}", inline=True)
            embed.add_field(name="Remaining Rebel Territories", value=f"{len(result['remaining_rebel_territories'])}", inline=True)
            embed.set_footer(text="+30% offensive bonus applied")

            if result.get("war_over") or result.get("victory_final"):
                embed.add_field(
                    name="🏆 CIVIL WAR WON",
                    value="All rebel territories have been reclaimed! The nation is whole again.",
                    inline=False
                )
            await ctx.send(embed=embed)
        else:
            embed = discord.Embed(
                title="💥 Assault Repelled!",
                description=f"The rebels held **{result['territory']}** against your assault.",
                color=discord.Color.red()
            )
            embed.add_field(name="Soldier Losses", value=f"⚔️ {format_number(result['soldier_losses'])}", inline=True)
            embed.add_field(name="Rebel Strength Now", value=f"🏴 {result['rebel_strength']}", inline=True)
            if result.get("captured"):
                embed.add_field(name="😱 Territory Lost!", value=f"Rebels captured **{result['captured']}**!", inline=False)
            embed.set_footer(text="Rebel strength increased. Try again with more soldiers.")
            await ctx.send(embed=embed)

    @commands.command(name='civilwar')
    async def civil_war_status(self, ctx):
        """Show your current civil war status."""
        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need a civilization first!")
            return

        state = self.civ_manager.get_civil_war_state(user_id)
        if not state:
            await ctx.send("✅ Your nation is not currently in a civil war.")
            return

        embed = discord.Embed(
            title="💥 CIVIL WAR STATUS",
            description=f"**{civ['name']}** is at war with itself.",
            color=discord.Color.dark_red()
        )
        embed.add_field(
            name="🏛️ Loyalist Territories",
            value=f"{len(state.get('loyalist_territories', []))}\n" +
                  ", ".join(state.get('loyalist_territories', [])[:10]),
            inline=False
        )
        embed.add_field(
            name="🏴 Rebel Territories",
            value=f"{len(state.get('rebel_territories', []))}\n" +
                  ", ".join(state.get('rebel_territories', [])[:10]),
            inline=False
        )
        embed.add_field(name="Rebel Strength", value=f"⚔️ {state.get('rebel_strength', 0)}", inline=True)
        embed.add_field(name="Player Wins", value=f"🏆 {state.get('player_wins', 0)}", inline=True)
        embed.add_field(name="Rebel Wins", value=f"💀 {state.get('rebel_wins', 0)}", inline=True)
        embed.set_footer(text="Use .reclaim <territory> to fight. You have a +30% offensive bonus.")
        await ctx.send(embed=embed)

    # =================================================================
    # HELPERS
    # =================================================================

    def _get_owned_subregions(self, user_id: str) -> Set[str]:
        owned = self._get_owned_provinces(user_id)
        fully_owned = set()
        for subregion, province_list in PROVINCES.items():
            if province_list and all(p in owned for p in province_list):
                fully_owned.add(subregion)
        return fully_owned


async def start_civil_war_ai_news(bot, channel, civ_name, state):
    """Helper exposed for the civil war AI news generation from economy.py."""
    try:
        openrouter_key = os.getenv("OPENROUTER")
        article = await asyncio.to_thread(
            bot.civ_manager.generate_civil_war_article,
            civ_name, state, openrouter_key
        )
        if article:
            for i in range(0, len(article), 1800):
                chunk = article[i:i + 1800]
                await channel.send(f"📰 **BREAKING NEWS**\n\n{chunk}")
                await asyncio.sleep(1)
        image_url = bot.civ_manager.generate_civil_war_image_url(civ_name, state)
        await channel.send(image_url)
    except Exception as e:
        logger.error(f"start_civil_war_ai_news error: {e}")


async def setup(bot):
    await bot.add_cog(TerritoryCog(bot))
