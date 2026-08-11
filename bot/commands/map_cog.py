import discord
from discord.ext import commands
import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from io import BytesIO
import json
import os
import hashlib
import logging

logger = logging.getLogger(__name__)

class MapCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.db = bot.db
        self.civ_manager = bot.civ_manager
        self.geojson_path = "regions.geojson"
        if not os.path.exists(self.geojson_path):
            logger.error("regions.geojson not found. Map will not work.")
            self.gdf = None
        else:
            try:
                self.gdf = gpd.read_file(self.geojson_path)
                if self.gdf.crs is None:
                    self.gdf = self.gdf.set_crs('EPSG:4326', allow_override=True)
                elif self.gdf.crs != 'EPSG:4326':
                    self.gdf = self.gdf.to_crs('EPSG:4326')
                self.gdf['geometry'] = self.gdf['geometry'].buffer(0)
            except Exception as e:
                logger.error(f"Failed to load regions.geojson: {e}")
                self.gdf = None
        self.cache = {}

    def get_ownership_data(self):
        """
        Fetch all users' owned provinces from Firestore and map them to countries.
        Returns dict: user_id -> {"provinces": list, "name": civ_name}
        """
        if self.gdf is None:
            return {}
        # Use Firestore method to get all territories
        territories = self.db.get_all_territories()  # dict {province_name: {owner_id: ...}}
        ownership = {}
        for province_name, data in territories.items():
            owner_id = data.get("owner_id")
            if owner_id:
                if owner_id not in ownership:
                    civ = self.civ_manager.get_civilization(owner_id)
                    name = civ['name'] if civ else owner_id[:6]
                    ownership[owner_id] = {"provinces": [], "name": name}
                ownership[owner_id]["provinces"].append(province_name)
        return ownership

    def generate_map(self, ownership_data):
        if self.gdf is None:
            fig, ax = plt.subplots(figsize=(10, 6))
            ax.text(0.5, 0.5, "Map data not available\nRun generate_geojson.py", 
                    ha='center', va='center', fontsize=14)
            ax.set_axis_off()
            buf = BytesIO()
            plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
            plt.close(fig)
            buf.seek(0)
            return buf

        colors = plt.cm.tab20.colors
        user_colors = {}
        for i, (user_id, info) in enumerate(ownership_data.items()):
            user_colors[user_id] = colors[i % len(colors)]

        fig, ax = plt.subplots(figsize=(15, 10))

        # Plot each country/province individually
        for idx, row in self.gdf.iterrows():
            country_name = row.get('NAME', 'Unknown')
            
            # Find which user owns this specific country
            owner = None
            for user_id, info in ownership_data.items():
                for province in info["provinces"]:
                    # Check for exact or partial matches (case insensitive)
                    if country_name.lower() == province.lower() or \
                       province.lower() in country_name.lower() or \
                       country_name.lower() in province.lower():
                        owner = user_id
                        break
                if owner:
                    break

            color = user_colors.get(owner, (0.8, 0.8, 0.8, 1))  # grey if unowned

            # Draw country with white border
            gpd.GeoDataFrame([row], crs=self.gdf.crs).plot(
                ax=ax, facecolor=color, edgecolor='white', linewidth=0.5
            )

        # Legend
        patches = []
        for user_id, color in user_colors.items():
            name = ownership_data[user_id]["name"]
            patches.append(mpatches.Patch(color=color, label=name))
        if patches:
            ax.legend(handles=patches, loc='lower left', fontsize=8)

        ax.set_title("World Map of Civilizations", fontsize=14)
        ax.set_axis_off()

        buf = BytesIO()
        plt.savefig(buf, format='png', dpi=150, bbox_inches='tight')
        plt.close(fig)
        buf.seek(0)
        return buf

    @commands.command(name='map')
    async def show_map(self, ctx):
        if self.gdf is None:
            await ctx.send("❌ Map data is not available. Please run `generate_geojson.py` to create the map file.")
            return

        ownership = self.get_ownership_data()
        key = hashlib.md5(json.dumps(ownership).encode()).hexdigest()
        if key in self.cache:
            buf = self.cache[key]
            buf.seek(0)
        else:
            buf = self.generate_map(ownership)
            self.cache[key] = buf
            if len(self.cache) > 10:
                self.cache.pop(next(iter(self.cache)))

        file = discord.File(buf, filename="world_map.png")
        await ctx.send("🗺️ Here's the current world map:", file=file)

async def setup(bot):
    await bot.add_cog(MapCog(bot))
