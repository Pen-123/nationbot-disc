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
        """Fetch ownership and collapse union members into one map country."""
        if self.gdf is None:
            return {}
        territories = self.db.get_all_territories()
        ownership = {}
        union_groups = {}

        for province_name, data in territories.items():
            owner_id = data.get("owner_id")
            if not owner_id:
                continue

            civ = self.civ_manager.get_civilization(owner_id)
            union = (civ or {}).get("union")
            if union and union.get("members"):
                members = sorted(str(m) for m in union.get("members", []))
                map_id = "union:" + ":".join(members)
                name = union.get("name") or (civ['name'] if civ else owner_id[:6])
                union_groups[map_id] = {"members": members, "name": name}
            else:
                map_id = str(owner_id)
                name = civ['name'] if civ else owner_id[:6]

            if map_id not in ownership:
                ownership[map_id] = {"provinces": [], "name": name}
            ownership[map_id]["provinces"].append(province_name)

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
        for i, (map_id, info) in enumerate(ownership_data.items()):
            user_colors[map_id] = colors[i % len(colors)]

        fig, ax = plt.subplots(figsize=(15, 10))

        for idx, row in self.gdf.iterrows():
            country_name = row.get('NAME', 'Unknown')
            owner = None
            for map_id, info in ownership_data.items():
                for province in info["provinces"]:
                    if country_name.lower() == province.lower() or \
                       province.lower() in country_name.lower() or \
                       country_name.lower() in province.lower():
                        owner = map_id
                        break
                if owner:
                    break

            color = user_colors.get(owner, (0.8, 0.8, 0.8, 1))
            gpd.GeoDataFrame([row], crs=self.gdf.crs).plot(
                ax=ax, facecolor=color, edgecolor='white', linewidth=0.5
            )

        patches = []
        for map_id, color in user_colors.items():
            name = ownership_data[map_id]["name"]
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
        key = hashlib.md5(json.dumps(ownership, sort_keys=True).encode()).hexdigest()
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