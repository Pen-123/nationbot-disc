import discord
from discord.ext import commands
from discord import app_commands


class AdminCommands(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @commands.hybrid_command(name="sync", with_app_command=False)
    @commands.is_owner()
    @app_commands.describe(
        scope="Where to sync commands: global, current_guild, or copy_global_to_guild",
        guild_id="Optional guild id (used for current_guild/copy_global_to_guild)"
    )
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

        if scope not in {"global", "current_guild", "copy_global_to_guild"}:
            await ctx.send(
                "❌ Invalid scope. Use one of: `global`, `current_guild`, `copy_global_to_guild`."
            )
            return

        target_guild = None
        if scope != "global":
            if guild_id is not None:
                target_guild = discord.Object(id=guild_id)
            elif ctx.guild is not None:
                target_guild = ctx.guild
            else:
                await ctx.send("❌ No guild context found. Provide a `guild_id`.")
                return

        if scope == "global":
            synced = await self.bot.tree.sync()
            await ctx.send(f"✅ Synced {len(synced)} global slash commands.")
            return

        if scope == "copy_global_to_guild":
            self.bot.tree.copy_global_to(guild=target_guild)

        synced = await self.bot.tree.sync(guild=target_guild)
        await ctx.send(
            f"✅ Synced {len(synced)} slash commands to guild `{target_guild.id}` "
            f"(scope: `{scope}`)."
        )


async def setup(bot):
    await bot.add_cog(AdminCommands(bot))
