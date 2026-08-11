import discord
import io
import os
import logging
from discord.ext import commands
from discord import app_commands
from datetime import datetime

logger = logging.getLogger(__name__)

class AdminCommands(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @commands.hybrid_command(name="sync", with_app_command=True)
    @commands.is_owner()
    @app_commands.describe(
        scope="Where to sync commands: global, current_guild, or copy_global_to_guild",
        guild_id="Optional guild id (used for current_guild/copy_global_to_guild)"
    )
    @app_commands.choices(scope=[
        app_commands.Choice(name="global", value="global"),
        app_commands.Choice(name="current_guild", value="current_guild"),
        app_commands.Choice(name="copy_global_to_guild", value="copy_global_to_guild"),
    ])
    async def sync_commands(self, ctx, scope: str = "global", guild_id: int = None):
        """
        Owner-only command to sync slash commands.
        """
        scope = (scope or "global").lower().strip()

        async def _send(message: str, **kwargs):
            if isinstance(ctx, discord.Interaction):
                try:
                    if not ctx.response.is_done():
                        await ctx.response.send_message(message, **kwargs)
                    else:
                        await ctx.followup.send(message, **kwargs)
                except Exception:
                    try:
                        await ctx.followup.send(message, **kwargs)
                    except Exception:
                        pass
            else:
                await ctx.send(message, **kwargs)

        if scope not in {"global", "current_guild", "copy_global_to_guild"}:
            await _send(
                "❌ Invalid scope. Use one of: `global`, `current_guild`, `copy_global_to_guild`."
            )
            return

        target_guild = None
        if scope != "global":
            if guild_id is not None:
                target_guild = discord.Object(id=guild_id)
            elif getattr(ctx, "guild", None) is not None:
                target_guild = ctx.guild
            else:
                await _send("❌ No guild context found. Provide a `guild_id`.")
                return

        try:
            if scope == "global":
                synced = await self.bot.tree.sync()
                await _send(f"✅ Synced {len(synced)} global slash commands.")
                return

            if scope == "copy_global_to_guild":
                self.bot.tree.copy_global_to(guild=target_guild)

            synced = await self.bot.tree.sync(guild=target_guild)
            await _send(
                f"✅ Synced {len(synced)} slash commands to guild `{target_guild.id}` "
                f"(scope: `{scope}`)."
            )
        except Exception as e:
            await _send(f"❌ Sync failed: {e}")

    # .forcesync has been removed – no Dropbox sync.

    @commands.hybrid_command(name='exportdb')
    async def export_database(self, ctx):
        """
        Export the current database file (.db) with instructions.
        """
        try:
            db_path = self.bot.db.db_path
            if not os.path.exists(db_path):
                await ctx.send("❌ Database file not found.")
                return

            with open(db_path, 'rb') as f:
                file_data = f.read()

            file_size_mb = len(file_data) / (1024 * 1024)
            if file_size_mb > 8:
                await ctx.send(f"⚠️ Database file is **{file_size_mb:.1f} MB** – larger than Discord's 8MB limit. Please use a different method to retrieve it (e.g., direct download from Dropbox).")
                return

            file = discord.File(io.BytesIO(file_data), filename="warbot.db")
            embed = discord.Embed(
                title="📦 Database Export",
                description=(
                    "Here is the current database file.\n\n"
                    "**To restore:**\n"
                    "1. Stop the bot.\n"
                    "2. Replace the existing `warbot.db` with this file.\n"
                    "3. Restart the bot.\n\n"
                    "**To inspect:**\n"
                    "Open with any SQLite browser (e.g., DB Browser for SQLite)."
                ),
                color=discord.Color.green()
            )
            embed.add_field(name="File Size", value=f"{file_size_mb:.2f} MB", inline=True)
            embed.add_field(name="Created", value=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"), inline=True)
            await ctx.send(embed=embed, file=file)

        except Exception as e:
            logger.error(f"Error exporting database: {e}")
            await ctx.send(f"❌ Error exporting database: {e}")

async def setup(bot):
    await bot.add_cog(AdminCommands(bot))
