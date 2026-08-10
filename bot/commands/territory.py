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
# PROVINCE DATA – Updated to include all UN nations + Palestine & Kosovo
# ---------------------------
PROVINCES = {
    "Western Europe": ["France", "Germany", "United Kingdom", "Ireland", "Netherlands", "Belgium", "Luxembourg", "Switzerland", "Austria", "Monaco", "Andorra", "Liechtenstein"],
    "Eastern Europe": ["Poland", "Czech Republic", "Slovakia", "Hungary", "Romania", "Bulgaria", "Ukraine", "Belarus", "Moldova", "Russia"],
    "Southern Europe": ["Portugal", "Spain", "Italy", "Greece", "Croatia", "Slovenia", "Bosnia and Herzegovina", "Serbia", "Montenegro", "Albania", "North Macedonia", "Kosovo", "Malta", "Cyprus", "San Marino", "Vatican"],
    "Northern Europe": ["Norway", "Sweden", "Finland", "Denmark", "Iceland", "Estonia", "Latvia", "Lithuania"],
    "Central Asia": ["Kazakhstan", "Uzbekistan", "Turkmenistan", "Kyrgyzstan", "Tajikistan", "Afghanistan"],
    "East Asia": ["China", "Japan", "South Korea", "North Korea", "Mongolia", "Taiwan"],
    "South Asia": ["India", "Pakistan", "Bangladesh", "Sri Lanka", "Nepal", "Bhutan"],
    "Southeast Asia": ["Thailand", "Vietnam", "Indonesia", "Philippines", "Malaysia", "Singapore", "Cambodia", "Laos", "Timor-Leste", "Brunei", "Myanmar"],
    "Middle East": ["Turkey", "Iran", "Iraq", "Syria", "Lebanon", "Israel", "Palestine", "Jordan", "Saudi Arabia", "Yemen", "Oman", "United Arab Emirates", "Qatar", "Kuwait", "Bahrain", "Georgia", "Armenia", "Azerbaijan"],
    "North Africa": ["Morocco", "Algeria", "Tunisia", "Libya", "Egypt", "Western Sahara"],
    "West Africa": ["Mauritania", "Senegal", "Gambia", "Mali", "Burkina Faso", "Benin", "Togo", "Ghana", "Ivory Coast", "Liberia", "Sierra Leone", "Guinea", "Guinea-Bissau", "Cape Verde", "Nigeria", "Niger"],
    "Central Africa": ["Chad", "Cameroon", "Central African Republic", "DR Congo", "Republic of Congo", "Gabon", "Equatorial Guinea", "Sao Tome and Principe"],
    "East Africa": ["Sudan", "South Sudan", "Eritrea", "Ethiopia", "Djibouti", "Somalia", "Kenya", "Uganda", "Rwanda", "Burundi", "Tanzania", "Mozambique", "Madagascar", "Comoros", "Seychelles"],
    "Southern Africa": ["Angola", "Zambia", "Malawi", "Zimbabwe", "Botswana", "Namibia", "South Africa", "Eswatini", "Lesotho"],
    "Western North America": ["Canada", "United States"],
    "Central North America": ["Mexico"],
    "Central America": ["Guatemala", "Belize", "Honduras", "El Salvador", "Nicaragua", "Costa Rica", "Panama"],
    "Caribbean": ["Cuba", "Haiti", "Dominican Republic", "Jamaica", "Bahamas", "Trinidad and Tobago"],
    "Northern South America": ["Venezuela", "Colombia", "Guyana", "Suriname", "French Guiana"],
    "Western South America": ["Ecuador", "Peru", "Bolivia", "Chile"],
    "Eastern South America": ["Brazil"],
    "Southern Cone": ["Argentina", "Uruguay", "Paraguay"],
    "Australia": ["Australia"],
    "New Zealand": ["New Zealand"],
    "Pacific Islands": ["Fiji", "Solomon Islands", "Vanuatu", "Papua New Guinea", "Samoa", "Tonga", "Micronesia", "Marshall Islands", "Palau", "Nauru", "Kiribati", "Tuvalu"]
}

# ---------- NEW MAPPING: Subregion -> Continent ----------
SUBREGION_TO_CONTINENT = {
    "Western Europe": "Europe",
    "Eastern Europe": "Europe",
    "Southern Europe": "Europe",
    "Northern Europe": "Europe",
    "Central Asia": "Asia",
    "East Asia": "Asia",
    "South Asia": "Asia",
    "Southeast Asia": "Asia",
    "Middle East": "Asia",          # Often considered part of Asia
    "North Africa": "Africa",
    "West Africa": "Africa",
    "Central Africa": "Africa",
    "East Africa": "Africa",
    "Southern Africa": "Africa",
    "Western North America": "North America",
    "Central North America": "North America",
    "Central America": "North America",
    "Caribbean": "North America",
    "Northern South America": "South America",
    "Western South America": "South America",
    "Eastern South America": "South America",
    "Southern Cone": "South America",
    "Australia": "Oceania",
    "New Zealand": "Oceania",
    "Pacific Islands": "Oceania",
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

# Sub‑region neighbour data 
SUBREGION_DATA = {
    "Eastern Europe": {"neighbours": ["Western Europe", "Northern Europe", "Central Asia", "Southern Europe"]},
    "Western Europe": {"neighbours": ["Southern Europe", "Northern Europe", "Eastern Europe"]},
    "Southern Europe": {"neighbours": ["Western Europe", "Middle East", "North Africa", "Eastern Europe"]},
    "Northern Europe": {"neighbours": ["Western Europe", "Eastern Europe"]},
    "Central Asia": {"neighbours": ["Eastern Europe", "South Asia", "East Asia", "Middle East"]},
    "East Asia": {"neighbours": ["Central Asia", "South Asia", "Southeast Asia"]},
    "South Asia": {"neighbours": ["Central Asia", "East Asia", "Southeast Asia", "Middle East"]},
    "Southeast Asia": {"neighbours": ["East Asia", "South Asia", "Australia", "Pacific Islands"]},
    "Middle East": {"neighbours": ["Southern Europe", "Central Asia", "South Asia", "North Africa", "East Africa"]},
    "North Africa": {"neighbours": ["Southern Europe", "Middle East", "West Africa", "Central Africa", "East Africa"]},
    "West Africa": {"neighbours": ["North Africa", "Central Africa"]},
    "Central Africa": {"neighbours": ["North Africa", "West Africa", "East Africa", "Southern Africa"]},
    "East Africa": {"neighbours": ["North Africa", "Central Africa", "Southern Africa", "Middle East"]},
    "Southern Africa": {"neighbours": ["West Africa", "Central Africa", "East Africa"]},
    "Western North America": {"neighbours": ["Central North America", "Eastern North America"]},
    "Central North America": {"neighbours": ["Western North America", "Central America", "Caribbean"]},
    "Central America": {"neighbours": ["Central North America", "Northern South America", "Caribbean"]},
    "Caribbean": {"neighbours": ["Central North America", "Central America", "Northern South America"]},
    "Northern South America": {"neighbours": ["Central America", "Western South America", "Eastern South America", "Caribbean"]},
    "Western South America": {"neighbours": ["Northern South America", "Eastern South America", "Southern Cone"]},
    "Eastern South America": {"neighbours": ["Northern South America", "Western South America", "Southern Cone"]},
    "Southern Cone": {"neighbours": ["Western South America", "Eastern South America"]},
    "Australia": {"neighbours": ["Southeast Asia", "New Zealand", "Pacific Islands"]},
    "New Zealand": {"neighbours": ["Australia", "Pacific Islands"]},
    "Pacific Islands": {"neighbours": ["Australia", "New Zealand", "Southeast Asia"]}
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
        Includes global expansion if a subregion is fully conquered.
        """
        owned = set(self._get_owned_provinces(user_id))
        fully_owned_subregions = self._get_owned_subregions(user_id)
        
        possible = set()

        # If they fully own ANY subregion, they unlock global expansion without borders
        if fully_owned_subregions:
            for p in ALL_PROVINCES:
                if p not in owned:
                    possible.add(p)
            return sorted(list(possible))

        # 1. Provinces from neighbouring subregions
        for subregion in fully_owned_subregions:
            neighbours = SUBREGION_DATA.get(subregion, {}).get("neighbours", [])
            for neighbour_subregion in neighbours:
                for province in PROVINCES.get(neighbour_subregion, []):
                    if province not in owned:
                        possible.add(province)
        
        # 2. Provinces in the same subregion as any owned province
        for province in owned:
            subregion = PROVINCE_TO_SUBREGION.get(province)
            if subregion:
                for p in PROVINCES.get(subregion, []):
                    if p not in owned:
                        possible.add(p)
        
        return sorted(list(possible))

    def _calculate_soldier_cost(self, civ) -> int:
        """Calculate 10% of current soldiers (minimum 50)."""
        soldiers = civ['military'].get('soldiers', 0)
        cost = math.ceil(soldiers * 0.1)
        return max(50, cost)

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
        """Claim a new province (costs 10x resources + min 50/10% of your soldiers)."""
        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need a civilization first! Use `.start`.")
            return

        owned = self._get_owned_provinces(user_id)
        fully_owned_subregions = self._get_owned_subregions(user_id)

        # If no province specified, show available options
        if province is None:
            if not owned:
                await ctx.send("❌ You have no province. Use `.expand <province>` to claim any country as your first.")
                return
            
            possible = self._get_expansion_options(user_id)
            if not possible:
                await ctx.send("❌ No available provinces to expand into.")
                return
                
            embed = discord.Embed(title="🌍 Available Provinces", description="Use `.expand <country>` to claim one.", color=discord.Color.blue())
            
            if fully_owned_subregions:
                embed.description = "🎉 **Global Expansion Unlocked!** You have fully conquered a subregion. You can now `.expand` into ANY unowned country in the world!"
                # To prevent character limits, group by subregions instead of listing 150+ countries
                available_subregions = set([PROVINCE_TO_SUBREGION.get(p) for p in possible if PROVINCE_TO_SUBREGION.get(p)])
                embed.add_field(name="Available Subcontinents", value=", ".join(list(available_subregions)[:15]) + "... (Type any country name to claim it)", inline=False)
            else:
                by_subregion = {}
                for p in possible:
                    subregion = PROVINCE_TO_SUBREGION.get(p, "Unknown")
                    by_subregion.setdefault(subregion, []).append(p)
                for subregion, names in by_subregion.items():
                    embed.add_field(name=subregion, value=", ".join(names), inline=False)
            
            embed.set_footer(text="Cost: 10x Resources + 10% Army (Min 50). Must have 100+ total soldiers.")
            await ctx.send(embed=embed)
            return

        # Find exact match (case-insensitive)
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
            await ctx.send(f"❌ Unknown country: `{province}`. Check spelling or use `.expand` to see options.")
            return
            
        province = match

        # 10x Cost Multiplier Applied
        subregion = PROVINCE_TO_SUBREGION[province]
        province_count = len(PROVINCES[subregion])
        cost = {
            "gold": (300 + (100 // max(1, province_count))) * 10,
            "food": (100 + (50 // max(1, province_count))) * 10,
            "wood": (50 + (25 // max(1, province_count))) * 10,
            "stone": (50 + (25 // max(1, province_count))) * 10,
        }

        # Validate minimum total soldiers constraint
        total_soldiers = civ['military'].get('soldiers', 0)
        if total_soldiers < 100:
            await ctx.send(f"❌ You need at least **100 total soldiers** in your army to expand! You currently have {total_soldiers}.")
            return

        # First province: allow claiming any
        if not owned:
            if not self.civ_manager.can_afford(user_id, cost):
                cost_str = ", ".join([f"{amount} {res}" for res, amount in cost.items()])
                await ctx.send(f"❌ Cannot afford to claim **{province}**. Requires: {cost_str}.")
                return

            soldier_cost = self._calculate_soldier_cost(civ)
            if total_soldiers < soldier_cost:
                await ctx.send(f"❌ You need at least {soldier_cost} soldiers to claim this province! You have {total_soldiers}.")
                return

            self.civ_manager.spend_resources(user_id, cost)
            self.civ_manager.update_military(user_id, {"soldiers": -soldier_cost})

            if self._add_province(user_id, province):
                land_gain = random.randint(500, 1500)
                self.civ_manager.update_territory(user_id, {"land_size": land_gain})
                embed = discord.Embed(title="🏹 First Province Claimed!", description=f"**{civ['name']}** has established their empire in **{province}**!", color=discord.Color.green())
                embed.add_field(name="Cost Paid", value=", ".join([f"{amount} {res}" for res, amount in cost.items()]) + f"\n⚔️ {soldier_cost} soldiers deployed", inline=True)
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

        # Check adjacency (unless a subregion is fully conquered, unlocking global expansion)
        possible = self._get_expansion_options(user_id)
        if province not in possible:
            await ctx.send(f"❌ **{province}** is not adjacent to your territories. Conquer a full subregion first to unlock global expansion!")
            return

        # Check resource cost
        if not self.civ_manager.can_afford(user_id, cost):
            cost_str = ", ".join([f"{amount} {res}" for res, amount in cost.items()])
            await ctx.send(f"❌ Cannot afford 10x expansion cost for **{province}**.\nRequires: {cost_str}.")
            return

        # Check soldier cost
        soldier_cost = self._calculate_soldier_cost(civ)
        if total_soldiers < soldier_cost:
            await ctx.send(f"❌ You need at least {soldier_cost} soldiers (10% / min 50) to expand! You have {total_soldiers}.")
            return

        # Spend resources and soldiers, then claim
        self.civ_manager.spend_resources(user_id, cost)
        self.civ_manager.update_military(user_id, {"soldiers": -soldier_cost})

        if self._add_province(user_id, province):
            land_gain = random.randint(300, 1000)
            self.civ_manager.update_territory(user_id, {"land_size": land_gain})
            embed = discord.Embed(title="🏹 Expansion Successful!", description=f"**{civ['name']}** has annexed **{province}**!", color=discord.Color.green())
            embed.add_field(name="Cost Paid", value=", ".join([f"{amount} {res}" for res, amount in cost.items()]) + f"\n⚔️ {soldier_cost} soldiers deployed", inline=True)
            embed.add_field(name="Land Gained", value=f"+{land_gain} km²", inline=True)
            
            # Check if this completion unlocked global expansion
            if not fully_owned_subregions and subregion in self._get_owned_subregions(user_id):
                embed.add_field(name="🌍 Global Expansion Unlocked!", value=f"You have fully conquered **{subregion}**! You can now expand into ANY country in the world without needing borders.", inline=False)
                
            await ctx.send(embed=embed)
            self.db.log_event(user_id, "expansion", "Province Claimed", f"Claimed {province}")
        else:
            await ctx.send("❌ Failed to claim province.")

async def setup(bot):
    await bot.add_cog(TerritoryCog(bot))
