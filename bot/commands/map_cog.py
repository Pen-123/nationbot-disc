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
            raise FileNotFoundError("regions.geojson not found. Run generate_geojson.py first.")
        self.gdf = gpd.read_file(self.geojson_path)
        self.cache = {}  # key: hash of ownership data -> BytesIO image

    def get_ownership_data(self):
        """Fetch all users' owned territories from the database."""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT user_id, owned_territories FROM territories")
        rows = cursor.fetchall()
        data = {}
        for row in rows:
            user_id = row[0]
            territories = json.loads(row[1]) if row[1] else []
            # Get the civilization name for the legend
            civ = self.civ_manager.get_civilization(user_id)
            name = civ['name'] if civ else user_id[:6]
            data[user_id] = {"territories": territories, "name": name}
        return data

    def generate_map(self, ownership_data):
        """Generate a Matplotlib figure and return as BytesIO."""
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
        ownership = self.get_ownership_data()
        # Create a cache key
        key = hashlib.md5(json.dumps(ownership).encode()).hexdigest()
        if key in self.cache:
            buf = self.cache[key]
            # reset pointer in case it was consumed
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
