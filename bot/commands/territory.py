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

# ---- HARDCODED AREA OVERRIDES ----
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
    "Palestine": 6020,
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
    "Vatican City": 0.44,
    "Venezuela": 912050,
    "Vietnam": 331212,
    "Yemen": 527968,
    "Zambia": 752612,
    "Zimbabwe": 390757,
    "Czech Republic": 78867,
    "Bosnia and Herz.": 51197,
    "Dem. Rep. Korea": 120538,
    "Congo (Kinshasa)": 2344858,
    "Congo (Brazzaville)": 342000,
    "Côte d'Ivoire": 322463,
    "Ivory Coast": 322463,
    "Eswatini": 17364,
    "Swaziland": 17364,
    "Greenland": 2166086,
    "Western Sahara": 266000,
    "W. Sahara": 266000,
    "S. Sudan": 644329,
    "Eq. Guinea": 28051,
    "C.A.R.": 622984,
    "Central African Rep.": 622984,
}

PROVINCE_AREAS = {}
try:
    with open('province_areas.json', 'r') as f:
        PROVINCE_AREAS = json.load(f)
    logger.info("Loaded province areas from province_areas.json")
except FileNotFoundError:
    logger.warning("province_areas.json not found; using default area of 1000 km².")
    PROVINCE_AREAS = {}

for name, area in AREA_OVERRIDES.items():
    PROVINCE_AREAS[name] = area
logger.info(f"Applied {len(AREA_OVERRIDES)} area overrides")

FORBIDDEN_START_PROVINCES = {
    "Western Sahara",
}

PROVINCES = {
    "Eastern Europe": [
        "Poland", "Czechia", "Slovakia", "Hungary", "Romania", "Bulgaria",
        "Ukraine", "Belarus", "Moldova", "Russia"
    ],
    "Western Europe": [
        "France", "United Kingdom", "Ireland", "Netherlands",
        "Belgium", "Luxembourg", "Monaco", "Andorra",
        "San Marino"
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

PROVINCE_TO_SUBREGION = {}
for subregion, province_list in PROVINCES.items():
    for province in province_list:
        PROVINCE_TO_SUBREGION[province] = subregion

ALL_PROVINCES = list(PROVINCE_TO_SUBREGION.keys())
ALL_SUBREGIONS = list(PROVINCES.keys())

SUBREGION_DATA = {
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

SUBREGION_TO_CONTINENT = {
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

# ---- COUNTRYBALL MAPPING ----
REGION_TO_COUNTRYBALL = {
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

    # ---- Firestore-based territory helpers ----
    def _get_owned_provinces(self, user_id: str) -> List[str]:
        return self.db.get_player_territories(user_id)

    def _add_province(self, user_id: str, province: str, ctx=None) -> bool:
        owned = self._get_owned_provinces(user_id)
        if province in owned:
            logger.debug(f"Province {province} already owned by {user_id}")
            return True

        owner = self.db.get_territory_owner(province)
        if owner and owner != user_id:
            logger.warning(f"Province {province} owned by {owner}, cannot add to {user_id}")
            return False

        success = self.db.conquer_territory(user_id, None, province)
        if success:
            area = self.province_areas.get(province, 1000)
            self.civ_manager.update_territory(user_id, {"land_size": area})
            logger.info(f"Added province {province} to {user_id}, land_size +{area}")
            return True
        else:
            logger.error(f"Failed to conquer province {province} for {user_id}")
            return False

    def _calculate_soldier_cost(self, area: int) -> int:
        """Base cost: 1 soldier per 595 km², min 10, max 5000."""
        cost = max(10, int(area / 595))
        return min(cost, 5000)

    def _calculate_rapid_soldier_cost(self, area: int) -> int:
        """Rapid expansion cost: 2× normal – 1 per 297.5 km², min 20, max 10000."""
        cost = max(20, int(area / 297.5))
        return min(cost, 10000)

    def _get_navy_and_airforce(self, user_id: str):
        """Return (has_navy, has_airforce) booleans."""
        navy = self.db.get_navy(user_id)
        air = self.db.get_airforce(user_id)
        has_navy = sum(navy.values()) > 0
        has_airforce = sum(air.values()) > 0
        return has_navy, has_airforce

    def _get_owned_continents(self, user_id: str) -> Set[str]:
        """Return set of continents that the player owns any province in."""
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
        """True if target subregion is on a continent not owned by the player."""
        owned_continents = self._get_owned_continents(user_id)
        if not owned_continents:
            # Shouldn't happen if they have at least one province
            return False
        target_continent = SUBREGION_TO_CONTINENT.get(target_subregion)
        return target_continent not in owned_continents

    def _apply_expansion_reductions(self, user_id: str, target_subregion: str, resource_cost: dict, soldier_cost: int):
        """
        Apply navy/airforce reductions based on continent (overseas vs land).
        Returns (new_resource_cost, new_soldier_cost, reduction_type)
        """
        has_navy, has_airforce = self._get_navy_and_airforce(user_id)
        is_overseas = self._is_overseas(user_id, target_subregion)

        # If overseas, navy is required (checked elsewhere), but reduction:
        if is_overseas:
            if has_navy:
                reduction = 0.75  # 25% off
                reason = "🛳️ Navy (25% off – overseas)"
            else:
                # Should not happen, but fallback
                return resource_cost, soldier_cost, "⚠️ No navy – overseas expansion blocked"
        else:
            # Land (same continent)
            if has_airforce:
                reduction = 0.5   # 50% off
                reason = "✈️ Airforce (50% off – land)"
            elif has_navy:
                reduction = 0.75  # 25% off
                reason = "🛳️ Navy (25% off – land)"
            else:
                reduction = 1.0
                reason = "No reduction (land)"

        new_resource_cost = {res: max(1, int(round(cost * reduction))) for res, cost in resource_cost.items()}
        new_soldier_cost = max(1, int(round(soldier_cost * reduction)))
        return new_resource_cost, new_soldier_cost, reason

    # ---- Other methods ----
    async def _check_subregion_completion(self, user_id: str, ctx=None):
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
            if ctx:
                await cog.check_region_unlock(ctx, region, user_id)
            else:
                logger.info(f"Region {region} completed by {user_id} but no ctx to send reveal.")

    def _get_owned_subregions(self, user_id: str) -> Set[str]:
        owned = self._get_owned_provinces(user_id)
        fully_owned = set()
        for subregion, province_list in PROVINCES.items():
            if all(p in owned for p in province_list):
                fully_owned.add(subregion)
        return fully_owned

    def _get_expansion_options(self, user_id: str) -> List[str]:
        owned = set(self._get_owned_provinces(user_id))
        if not owned:
            return []

        fully_owned_subregions = self._get_owned_subregions(user_id)

        possible = set()
        if fully_owned_subregions:
            for province in ALL_PROVINCES:
                if province not in owned:
                    possible.add(province)
            return sorted(possible)

        owned_subregions = set()
        for p in owned:
            sub = PROVINCE_TO_SUBREGION.get(p)
            if sub:
                owned_subregions.add(sub)

        for sub in owned_subregions:
            for p in PROVINCES.get(sub, []):
                if p not in owned:
                    possible.add(p)
            neighbours = SUBREGION_DATA.get(sub, {}).get('neighbours', [])
            for nsub in neighbours:
                for p in PROVINCES.get(nsub, []):
                    if p not in owned:
                        possible.add(p)

        return sorted(list(possible))

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

            embed = discord.Embed(
                title="🌍 Available Provinces",
                description="Use `.expand <province>` to claim one. Each expansion costs resources + soldiers.\n\n**Legend:** 🛳️ = Navy available (25% off), ✈️ = Airforce available (50% off, land only), ⚠️ = Navy required (overseas)",
                color=discord.Color.blue()
            )

            subregion_dict = {}
            for p in possible:
                sub = PROVINCE_TO_SUBREGION.get(p, "Unknown")
                subregion_dict.setdefault(sub, []).append(p)

            field_lines = []
            for sub, provinces in subregion_dict.items():
                has_navy, has_air = self._get_navy_and_airforce(user_id)
                is_overseas = self._is_overseas(user_id, sub)
                if is_overseas:
                    if not has_navy:
                        req = "⚠️ Navy required"
                    else:
                        req = "🛳️ Navy (25% off)"
                else:
                    if has_air:
                        req = "✈️ Airforce (50% off)"
                    elif has_navy:
                        req = "🛳️ Navy (25% off)"
                    else:
                        req = "🟢 Land"
                province_list = ', '.join(provinces[:5])
                if len(provinces) > 5:
                    province_list += '…'
                field_lines.append(f"**{sub}** ({req}): {province_list}")

            if len(field_lines) > 25:
                chunks = [field_lines[i:i+5] for i in range(0, len(field_lines), 5)]
                for i, chunk in enumerate(chunks, 1):
                    embed.add_field(
                        name=f"Available Provinces (Part {i})",
                        value="\n\n".join(chunk),
                        inline=False
                    )
            else:
                for line in field_lines:
                    if ": " in line:
                        name, value = line.split(": ", 1)
                        embed.add_field(name=name, value=value, inline=False)
                    else:
                        embed.add_field(name="Available", value=line, inline=False)

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

        target_subregion = PROVINCE_TO_SUBREGION.get(province)
        if not target_subregion:
            await ctx.send("❌ Province has no subregion mapping. Contact admin.")
            return

        is_overseas = self._is_overseas(user_id, target_subregion)

        # Navy requirement for overseas
        has_navy, has_air = self._get_navy_and_airforce(user_id)
        if is_overseas and not has_navy:
            await ctx.send(f"❌ **{province}** is overseas (different continent)! You need a navy to expand there. Build ships with `.buildship`.")
            return

        # Calculate base costs
        area = self.province_areas.get(province, 1000)
        cost_multiplier = min(math.sqrt(area / 1000), 20.0)

        subregion = target_subregion
        province_count = len(PROVINCES[subregion])

        base_cost = {
            "gold": 300 + (100 // max(1, province_count)),
            "food": 100 + (50 // max(1, province_count)),
            "wood": 20 + (10 // max(1, province_count)),
            "stone": 20 + (10 // max(1, province_count)),
        }

        resource_cost = {k: int(v * cost_multiplier * 0.75) for k, v in base_cost.items()}
        soldier_cost = self._calculate_soldier_cost(area)

        # Apply reductions
        resource_cost, soldier_cost, reduction_reason = self._apply_expansion_reductions(
            user_id, target_subregion, resource_cost, soldier_cost
        )

        # Check affordability
        if not self.civ_manager.can_afford(user_id, resource_cost):
            cost_str = ", ".join([f"{amount} {res}" for res, amount in resource_cost.items()])
            await ctx.send(f"❌ Cannot afford to claim **{province}**. Requires: {cost_str}.")
            return

        if civ['military']['soldiers'] < soldier_cost:
            await ctx.send(f"❌ You need at least {soldier_cost} soldiers to claim **{province}**! You have {civ['military']['soldiers']}.")
            return

        # Deduct costs
        self.civ_manager.spend_resources(user_id, resource_cost)
        self.civ_manager.update_military(user_id, {"soldiers": -soldier_cost})

        if province in self._get_owned_provinces(user_id):
            await ctx.send(f"❌ You already own **{province}** (appeared between checks).")
            return

        if self._add_province(user_id, province, ctx):
            embed = discord.Embed(
                title="🏹 Expansion Successful!",
                description=f"**{civ['name']}** has claimed **{province}**!",
                color=discord.Color.green()
            )
            cost_display = ", ".join([f"{amount} {res}" for res, amount in resource_cost.items()])
            embed.add_field(name="Cost", value=cost_display + f"\n⚔️ {soldier_cost} soldiers", inline=True)
            embed.add_field(name="Area Added", value=f"+{area:,} km²", inline=True)
            embed.add_field(name="Reduction Applied", value=reduction_reason, inline=False)
            embed.add_field(name="Overseas", value="✅" if is_overseas else "❌", inline=True)
            await ctx.send(embed=embed)
            self.db.log_event(user_id, "expansion", "Province Claimed", f"Claimed {province} (overseas: {is_overseas})")
        else:
            await ctx.send("❌ Failed to claim province. Please try again.")

    @commands.command(name='rapidexpansion')
    @app_commands.describe(province="Name of the province to claim")
    async def rapid_expansion(self, ctx, *, province: str = None):
        """
        Rapidly expand using soldiers instead of resources.
        Cost: 2× normal soldier cost (1 per 297.5 km²), capped at 10,000.
        """
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

            embed = discord.Embed(
                title="⚡ Rapid Expansion",
                description="Claim a province using soldiers instead of resources.\n"
                            "Cost: **2× normal** – 1 soldier per 297.5 km² (min 20, max 10,000).\n\n"
                            "**Legend:** 🛳️ = Navy available (25% off), ✈️ = Airforce available (50% off, land only), ⚠️ = Navy required (overseas)",
                color=discord.Color.orange()
            )

            subregion_dict = {}
            for p in possible:
                sub = PROVINCE_TO_SUBREGION.get(p, "Unknown")
                subregion_dict.setdefault(sub, []).append(p)

            field_lines = []
            for sub, provinces in subregion_dict.items():
                has_navy, has_air = self._get_navy_and_airforce(user_id)
                is_overseas = self._is_overseas(user_id, sub)
                if is_overseas:
                    if not has_navy:
                        req = "⚠️ Navy required"
                    else:
                        req = "🛳️ Navy (25% off)"
                else:
                    if has_air:
                        req = "✈️ Airforce (50% off)"
                    elif has_navy:
                        req = "🛳️ Navy (25% off)"
                    else:
                        req = "🟢 Land"
                province_list = ', '.join(provinces[:5])
                if len(provinces) > 5:
                    province_list += '…'
                field_lines.append(f"**{sub}** ({req}): {province_list}")

            if len(field_lines) > 25:
                chunks = [field_lines[i:i+5] for i in range(0, len(field_lines), 5)]
                for i, chunk in enumerate(chunks, 1):
                    embed.add_field(
                        name=f"Available Provinces (Part {i})",
                        value="\n\n".join(chunk),
                        inline=False
                    )
            else:
                for line in field_lines:
                    if ": " in line:
                        name, value = line.split(": ", 1)
                        embed.add_field(name=name, value=value, inline=False)
                    else:
                        embed.add_field(name="Available", value=line, inline=False)

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
            await ctx.send(f"❌ Unknown province: `{province}`. Use `.rapidexpansion` to see available provinces.")
            return
        province = match

        if province in owned:
            await ctx.send(f"❌ You already own **{province}**.")
            return

        possible = self._get_expansion_options(user_id)
        if province not in possible:
            await ctx.send(f"❌ **{province}** is not currently available for expansion.")
            return

        target_subregion = PROVINCE_TO_SUBREGION.get(province)
        if not target_subregion:
            await ctx.send("❌ Province has no subregion mapping. Contact admin.")
            return

        is_overseas = self._is_overseas(user_id, target_subregion)

        has_navy, has_air = self._get_navy_and_airforce(user_id)
        if is_overseas and not has_navy:
            await ctx.send(f"❌ **{province}** is overseas (different continent)! You need a navy to expand there. Build ships with `.buildship`.")
            return

        # Base soldier cost
        area = self.province_areas.get(province, 1000)
        soldier_cost = self._calculate_rapid_soldier_cost(area)

        # Apply reductions (only soldier cost)
        dummy_resources = {"gold": 0, "food": 0, "wood": 0, "stone": 0}
        _, soldier_cost, reduction_reason = self._apply_expansion_reductions(
            user_id, target_subregion, dummy_resources, soldier_cost
        )

        if civ['military']['soldiers'] < soldier_cost:
            await ctx.send(f"❌ You need at least {soldier_cost} soldiers to rapidly expand into **{province}**! You have {civ['military']['soldiers']}.")
            return

        # Deduct soldiers
        self.civ_manager.update_military(user_id, {"soldiers": -soldier_cost})

        if province in self._get_owned_provinces(user_id):
            await ctx.send(f"❌ You already own **{province}** (appeared between checks).")
            return

        if self._add_province(user_id, province, ctx):
            embed = discord.Embed(
                title="⚡ Rapid Expansion Successful!",
                description=f"**{civ['name']}** has rapidly expanded into **{province}** using {soldier_cost} soldiers!",
                color=discord.Color.gold()
            )
            embed.add_field(name="Soldiers Spent", value=f"⚔️ {soldier_cost}", inline=True)
            embed.add_field(name="Area Added", value=f"+{area:,} km²", inline=True)
            embed.add_field(name="Reduction Applied", value=reduction_reason, inline=False)
            embed.add_field(name="Overseas", value="✅" if is_overseas else "❌", inline=True)
            await ctx.send(embed=embed)
            self.db.log_event(user_id, "rapid_expansion", "Rapid Expansion", f"Expanded into {province} using {soldier_cost} soldiers (overseas: {is_overseas})")
        else:
            await ctx.send("❌ Failed to claim province. Please try again.")

async def setup(bot):
    await bot.add_cog(TerritoryCog(bot))
