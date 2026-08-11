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
from pathlib import Path

logger = logging.getLogger(__name__)

class MapCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.db = bot.db
        self.civ_manager = bot.civ_manager

        # Try multiple locations for regions.geojson so cog works regardless of cwd
        candidates = [
            Path(__file__).resolve().parent.parent / "regions.geojson",
            Path(__file__).resolve().parent / "regions.geojson",
            Path.cwd() / "regions.geojson",
        ]
        self.geojson_path = None
        for p in candidates:
            if p.exists():
                self.geojson_path = str(p)
                break

        if not self.geojson_path:
            logger.error("regions.geojson not found. Map will not work.")
            self.gdf = None
        else:
            try:
                gdf = gpd.read_file(self.geojson_path)
                # Robust CRS handling
                try:
                    current_crs = gdf.crs.to_string() if gdf.crs is not None else None
                except Exception:
                    current_crs = str(gdf.crs) if gdf.crs is not None else None

                if current_crs is None:
                    gdf = gdf.set_crs('EPSG:4326', allow_override=True)
                elif '4326' not in current_crs:
                    try:
                        gdf = gdf.to_crs('EPSG:4326')
                    except Exception:
                        gdf = gdf.set_crs('EPSG:4326', allow_override=True)

                # Try to fix invalid geometries; ignore if it fails
                if 'geometry' in gdf.columns:
                    try:
                        gdf['geometry'] = gdf['geometry'].buffer(0)
                    except Exception:
                        pass

                self.gdf = gdf
            except Exception as e:
                logger.error(f"Failed to load regions.geojson: {e}")
                self.gdf = None
        self.cache = {}

    def get_ownership_data(self):
        """Fetch all users' owned provinces and map them to countries."""
        # If no gdf we still want ownership for caching keys in bot contexts
        conn = self.db.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT user_id, owned_provinces FROM territories")
        rows = cursor.fetchall()
        data = {}
        for row in rows:
            user_id = row[0]
            provinces = json.loads(row[1]) if row[1] else []
            civ = self.civ_manager.get_civilization(user_id)
            name = civ['name'] if civ else user_id[:6]
            data[user_id] = {"provinces": provinces, "name": name}
        return data

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
            # skip invalid geometry rows
            try:
                geom = getattr(row, 'geometry', None)
                if geom is None:
                    continue
            except Exception:
                continue

            # prefer multiple name fields
            country_name = None
            for k in ('NAME', 'ADMIN', 'NAME_LONG', 'name'):
                try:
                    if k in row and row[k] is not None:
                        country_name = row[k]
                        break
                except Exception:
                    # row behaves like Series; ignore lookup errors
                    pass
            country_name = country_name or 'Unknown'

            # Find which user owns this specific country
            owner = None
            for user_id, info in ownership_data.items():
                for province in info.get('provinces', []) or []:
                    if not province:
                        continue
                    pn = province.lower()
                    cn = str(country_name).lower()
                    if cn == pn or pn in cn or cn in pn:
                        owner = user_id
                        break
                if owner:
                    break

            color = user_colors.get(owner, (0.8, 0.8, 0.8, 1))  # grey if unowned

            # Draw country with white border
            try:
                gpd.GeoDataFrame([row], crs=self.gdf.crs).plot(
                    ax=ax, facecolor=color, edgecolor='white', linewidth=0.5
                )
            except Exception:
                # skip problematic geometries
                continue

        # Legend
        patches = []
        for user_id, color in user_colors.items():
            try:
                name = ownership_data[user_id]["name"]
                patches.append(mpatches.Patch(color=color, label=name))
            except Exception:
                continue
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
            try:
                buf.seek(0)
            except Exception:
                pass
        else:
            buf = self.generate_map(ownership)
            self.cache[key] = buf
            if len(self.cache) > 10:
                self.cache.pop(next(iter(self.cache)))

        file = discord.File(buf, filename="world_map.png")
        await ctx.send("🗺️ Here's the current world map:", file=file)


async def setup(bot):
    await bot.add_cog(MapCog(bot))
