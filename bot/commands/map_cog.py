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
        # Load the GeoJSON once
        self.geojson_path = "regions.geojson"
        if not os.path.exists(self.geojson_path):
            logger.error("regions.geojson not found. Map will not work.")
            self.gdf = None
        else:
            try:
                self.gdf = gpd.read_file(self.geojson_path)
            except Exception as e:
                logger.error(f"Failed to load regions.geojson: {e}")
                self.gdf = None
        self.cache = {}  # key: hash of ownership data -> BytesIO image

    def get_ownership_data(self):
        """Fetch all users' owned provinces from the database."""
        if self.gdf is None:
            return {}
        conn = self.db.get_connection()
        cursor = conn.cursor()
        # FIXED: use owned_provinces instead of owned_territories
        cursor.execute("SELECT user_id, owned_provinces FROM territories")
        rows = cursor.fetchall()
        data = {}
        for row in rows:
            user_id = row[0]
            provinces = json.loads(row[1]) if row[1] else []
            # Get the civilization name for the legend
            civ = self.civ_manager.get_civilization(user_id)
            name = civ['name'] if civ else user_id[:6]
            # Map provinces to subregions (for map display)
            # We need to convert province names to subregion names for the map
            from bot.commands.territory import PROVINCE_TO_SUBREGION
            subregions = set()
            for province in provinces:
                sub = PROVINCE_TO_SUBREGION.get(province)
                if sub:
                    subregions.add(sub)
            data[user_id] = {"territories": list(subregions), "name": name}
        return data

    def generate_map(self, ownership_data):
        """Generate a Matplotlib figure and return as BytesIO."""
        if self.gdf is None:
            # Return a simple error image
            fig, ax = plt.subplots(figsize=(10, 6))
            ax.text(0.5, 0.5, "Map data not available\nRun generate_geojson.py", 
                    ha='center', va='center', fontsize=14)
            ax.set_axis_off()
            buf = BytesIO()
            plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
            plt.close(fig)
            buf.seek(0)
            return buf

        # Assign a colour per user
        colors = plt.cm.tab20.colors  # 20 distinct colours
        user_colors = {}
        for i, (user_id, info) in enumerate(ownership_data.items()):
            user_colors[user_id] = colors[i % len(colors)]

        fig, ax = plt.subplots(figsize=(15, 10))

        # Plot each sub‑region
        for idx, row in self.gdf.iterrows():
            region_name = row['subregion']
            # Find which user owns this region
            owner = None
            for user_id, info in ownership_data.items():
                if region_name in info["territories"]:
                    owner = user_id
                    break
            color = user_colors.get(owner, (0.8, 0.8, 0.8, 1))  # grey if unowned
            ax.add_geometries(row.geometry, crs='EPSG:4326', facecolor=color, edgecolor='white', linewidth=0.5)

        # Add labels for each region (centroid)
        for idx, row in self.gdf.iterrows():
            region_name = row['subregion']
            if row.geometry and not row.geometry.is_empty:
                centroid = row.geometry.centroid
                ax.annotate(region_name, xy=(centroid.x, centroid.y), fontsize=6, ha='center', va='center',
                            bbox=dict(boxstyle="round,pad=0.2", facecolor="white", alpha=0.6))

        # Legend
        patches = []
        for user_id, color in user_colors.items():
            name = ownership_data[user_id]["name"]
            patches.append(mpatches.Patch(color=color, label=name))
        if patches:
            ax.legend(handles=patches, loc='lower left', fontsize=8)

        ax.set_title("World Map of Civilizations", fontsize=14)
        ax.set_axis_off()

        # Save to BytesIO
        buf = BytesIO()
        plt.savefig(buf, format='png', dpi=150, bbox_inches='tight')
        plt.close(fig)
        buf.seek(0)
        return buf

    @commands.command(name='map')
    async def show_map(self, ctx):
        """Show the world map with your territories coloured."""
        if self.gdf is None:
            await ctx.send("❌ Map data is not available. Please run `generate_geojson.py` to create the map file.")
            return

        ownership = self.get_ownership_data()
        # Create a cache key
        key = hashlib.md5(json.dumps(ownership).encode()).hexdigest()
        if key in self.cache:
            buf = self.cache[key]
            buf.seek(0)
        else:
            buf = self.generate_map(ownership)
            self.cache[key] = buf
            # Limit cache size (optional)
            if len(self.cache) > 10:
                self.cache.pop(next(iter(self.cache)))

        file = discord.File(buf, filename="world_map.png")
        await ctx.send("🗺️ Here's the current world map:", file=file)

async def setup(bot):
    await bot.add_cog(MapCog(bot))
