import discord
from discord.ext import commands
from discord import app_commands


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

        Usage:
        - .sync
        - .sync global
        - .sync current_guild
        - .sync copy_global_to_guild
        - .sync current_guild <guild_id>
        - .sync copy_global_to_guild <guild_id>
        """
        scope = (scope or "global").lower().strip()

        # Helper to send messages for both Context and Interaction
        async def _send(message: str, **kwargs):
            if isinstance(ctx, discord.Interaction):
                try:
                    if not ctx.response.is_done():
                        await ctx.response.send_message(message, **kwargs)
                    else:
                        await ctx.followup.send(message, **kwargs)
                except Exception:
                    # Best-effort fallback
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

    @commands.hybrid_command(name='forcesync')
    @commands.is_owner()
    async def force_sync_db(self, ctx):
        """Force‑sync the database from Dropbox (overwrites local)."""
        if not self.bot.db.dropbox_client:
            await ctx.send("❌ Dropbox client not available.")
            return
        if self.bot.db.force_sync():
            await ctx.send("✅ Database force‑synced from Dropbox.")
        else:
            await ctx.send("❌ Failed to force‑sync database. Check logs.")


async def setup(bot):
    await bot.add_cog(AdminCommands(bot))
