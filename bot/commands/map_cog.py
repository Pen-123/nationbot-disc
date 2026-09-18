import asyncio
import hashlib
import json
import logging
import os
import time
from io import BytesIO

import discord
import geopandas as gpd
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
from discord.ext import commands

logger = logging.getLogger(__name__)

class MapCog(commands.Cog):
    """Fast world-map renderer with bulk reads and background rendering."""
    OWNERSHIP_CACHE_TTL = 3.0

    def __init__(self, bot):
        self.bot = bot
        self.db = bot.db
        self.civ_manager = bot.civ_manager
        self.geojson_path = "regions.geojson"
        self.gdf = None
        self._ownership_cache = None
        self._ownership_cache_at = 0.0
        self._map_cache = {}
        self._map_cache_limit = 3
        self._map_lock = asyncio.Lock()

        if not os.path.exists(self.geojson_path):
            logger.error("regions.geojson not found. Map will not work.")
            return
        try:
            gdf = gpd.read_file(self.geojson_path)
            if gdf.crs is None:
                gdf = gdf.set_crs("EPSG:4326", allow_override=True)
            elif gdf.crs != "EPSG:4326":
                gdf = gdf.to_crs("EPSG:4326")
            gdf["geometry"] = gdf["geometry"].buffer(0)
            self.gdf = gdf
            self._name_column = "NAME" if "NAME" in gdf.columns else gdf.columns[0]
            self._normalized_names = (
                gdf[self._name_column].fillna("Unknown").astype(str).str.strip().str.casefold()
            )
        except Exception as e:
            logger.error(f"Failed to load regions.geojson: {e}", exc_info=True)

    @staticmethod
    def _normalize_name(value):
        return str(value or "").strip().casefold()

    def get_ownership_data(self, force=False):
        """Bulk-read territory ownership and civilization names."""
        if self.gdf is None:
            return {}
        now = time.monotonic()
        if (not force and self._ownership_cache is not None
                and now - self._ownership_cache_at < self.OWNERSHIP_CACHE_TTL):
            return self._ownership_cache

        territories = self.db.get_all_territories()
        try:
            civs = self.db.get_all_civilizations()
        except Exception:
            civs = []
        civ_by_id = {str(c.get("user_id")): c for c in civs if c.get("user_id")}

        ownership = {}
        for province_name, data in territories.items():
            owner_id = data.get("owner_id")
            if not owner_id:
                continue
            owner_id = str(owner_id)
            civ = civ_by_id.get(owner_id)
            union = (civ or {}).get("union")

            if union and union.get("members"):
                members = tuple(sorted(str(m) for m in union.get("members", [])))
                map_id = "union:" + ":".join(members)
                name = union.get("name") or (civ or {}).get("name") or owner_id[:6]
            else:
                map_id = owner_id
                name = (civ or {}).get("name") or owner_id[:6]

            ownership.setdefault(map_id, {"provinces": [], "name": name})["provinces"].append(province_name)

        self._ownership_cache = ownership
        self._ownership_cache_at = now
        return ownership

    @staticmethod
    def _signature(ownership_data):
        return hashlib.md5(
            json.dumps(ownership_data, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()

    def generate_map_bytes(self, ownership_data):
        """Render the map synchronously. Caller runs this in a worker thread."""
        if self.gdf is None:
            fig, ax = plt.subplots(figsize=(10, 6))
            ax.text(0.5, 0.5, "Map data not available\nRun generate_geojson.py",
                    ha="center", va="center", fontsize=14)
            ax.set_axis_off()
        else:
            colors = plt.cm.tab20.colors
            user_colors = {
                map_id: colors[i % len(colors)]
                for i, map_id in enumerate(ownership_data)
            }
            province_to_owner = {}
            for map_id, info in ownership_data.items():
                for province in info["provinces"]:
                    province_to_owner[self._normalize_name(province)] = map_id

            owners = []
            for name in self._normalized_names:
                owner = province_to_owner.get(name)
                if owner is None:
                    owner = next(
                        (map_id for province, map_id in province_to_owner.items()
                         if province in name or name in province),
                        None,
                    )
                owners.append(owner)

            plot_gdf = self.gdf.copy()
            plot_gdf["_map_owner"] = owners
            plot_gdf["_map_color"] = [
                user_colors.get(owner, (0.8, 0.8, 0.8, 1))
                for owner in owners
            ]

            fig, ax = plt.subplots(figsize=(15, 10))
            ax.set_facecolor('#76a9d1')
            plot_gdf.plot(
                ax=ax,
                color=plot_gdf["_map_color"],
                edgecolor="white",
                linewidth=0.5,
            )

            patches = [
                mpatches.Patch(color=color, label=ownership_data[map_id]["name"])
                for map_id, color in user_colors.items()
            ]
            if patches:
                ax.legend(handles=patches, loc="lower left", fontsize=8)
            ax.set_title("🌍 NationBot World Map", fontsize=16, fontweight='bold')
            ax.grid(True, alpha=0.18, linewidth=0.6)
            ax.set_axis_off()

        buf = BytesIO()
        plt.savefig(buf, format="png", dpi=110, bbox_inches="tight")
        plt.close(fig)
        return buf.getvalue()

    @commands.command(name="map")
    async def show_map(self, ctx):
        if self.gdf is None:
            await ctx.send(
                "❌ Map data is not available. Please run generate_geojson.py to create the map file."
            )
            return

        ownership = await asyncio.to_thread(self.get_ownership_data, True)
        key = self._signature(ownership)
        png_bytes = self._map_cache.get(key)

        if png_bytes is None:
            # Deduplicate simultaneous .map requests. Without this, 10 users
            # spamming .map could trigger 10 expensive Matplotlib renders.
            async with self._map_lock:
                ownership = await asyncio.to_thread(self.get_ownership_data, True)
                key = self._signature(ownership)
                png_bytes = self._map_cache.get(key)
                if png_bytes is None:
                    png_bytes = await asyncio.to_thread(self.generate_map_bytes, ownership)
                    self._map_cache[key] = png_bytes
                    if len(self._map_cache) > self._map_cache_limit:
                        self._map_cache.pop(next(iter(self._map_cache)), None)

        await ctx.send(
            "🗺️ Here's the current world map:",
            file=discord.File(BytesIO(png_bytes), filename="world_map.png"),
        )

async def setup(bot):
    await bot.add_cog(MapCog(bot))
