import random
import discord
from discord.ext import commands
from discord import app_commands
import logging
from datetime import datetime, timedelta
from typing import Optional, List

from bot.utils import create_embed, format_number

logger = logging.getLogger(__name__)


class CeasefireCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.db = bot.db
        self.civ_manager = bot.civ_manager

    def _respond(self, ctx, content: str = None, embed: discord.Embed = None, ephemeral: bool = False):
        is_slash = getattr(ctx, "interaction", None) is not None
        use_eph = ephemeral and is_slash
        if is_slash:
            kwargs = {"ephemeral": use_eph}
            if content is not None:
                kwargs["content"] = content
            if embed is not None:
                kwargs["embed"] = embed
            try:
                if not ctx.interaction.response.is_done():
                    return ctx.interaction.response.send_message(**kwargs)
                return ctx.interaction.followup.send(**kwargs)
            except Exception:
                logger.exception("interaction response failed")
        return ctx.send(content=content, embed=embed)

    def _check_war(self, a_id: str, b_id: str) -> bool:
        for war in self.db.get_wars(status="ongoing"):
            a = war.get("attacker_id")
            d = war.get("defender_id")
            if (a == a_id and d == b_id) or (a == b_id and d == a_id):
                return True
        return False

    # =================================================================
    # CEASEFIRE
    # =================================================================

    @commands.hybrid_command(name='ceasefire')
    @app_commands.describe(target="Civilization leader to propose ceasefire with",
                           hours="Duration in hours (1-720)")
    async def ceasefire(self, ctx, target: Optional[discord.Member] = None, hours: int = None):
        if not target or hours is None:
            await self._respond(ctx, content="⏳ **Ceasefire**\nUsage: `.ceasefire <user> <hours>`\nMax 720 hours (30 days).")
            return
        if hours < 1 or hours > 720:
            await self._respond(ctx, content="❌ Hours must be between 1 and 720.")
            return

        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await self._respond(ctx, content="❌ You need a civilization first!")
            return

        target_id = str(target.id)
        if target_id == user_id:
            await self._respond(ctx, content="❌ You cannot ceasefire with yourself!")
            return

        target_civ = self.civ_manager.get_civilization(target_id)
        if not target_civ:
            await self._respond(ctx, content="❌ Target has no civilization!")
            return

        if not self._check_war(user_id, target_id):
            await self._respond(ctx, content="❌ You're not at war with them! Ceasefires only apply during active wars.")
            return

        # Check existing ceasefire
        try:
            active = self.db.get_active_ceasefire(user_id, target_id)
            if active:
                await self._respond(ctx, content="❌ There's already an active ceasefire between you!")
                return
        except AttributeError:
            pass  # method missing — skip check

        # Save as a peace offer with type=ceasefire
        try:
            offer_id = self.db.create_peace_offer(
                user_id, target_id,
                offer_type="ceasefire",
                terms={"ceasefire_hours": hours}
            )
        except AttributeError:
            await self._respond(ctx, content="❌ Ceasefire system not available (missing DB methods). Contact admin.")
            return

        if not offer_id:
            await self._respond(ctx, content="❌ Failed to create ceasefire proposal.")
            return

        embed = discord.Embed(
            title="⏳ Ceasefire Proposed",
            description=f"**{civ['name']}** → **{target_civ['name']}** for **{hours} hours**.",
            color=discord.Color.blue()
        )
        embed.add_field(
            name="Terms",
            value=f"War continues, but **no military actions** between you for {hours}h.",
            inline=False
        )
        embed.add_field(
            name="Respond",
            value=f"`{target.mention}` — `.acceptpeace {offer_id}` or `.rejectpeace {offer_id}`",
            inline=False
        )
        await ctx.send(embed=embed)

    @commands.hybrid_command(name='ceasefires')
    async def list_ceasefires(self, ctx):
        """List your active ceasefires."""
        user_id = str(ctx.author.id)
        try:
            ceasefires = self.db.get_all_ceasefires_for_user(user_id)
        except AttributeError:
            await self._respond(ctx, content="❌ Ceasefire system not available.")
            return
        if not ceasefires:
            await self._respond(ctx, content="📭 No active ceasefires.")
            return
        embed = discord.Embed(title="⏳ Active Ceasefires", color=discord.Color.blue())
        for cf in ceasefires:
            other_id = cf.get("civ_b") if cf.get("civ_a") == user_id else cf.get("civ_a")
            other_civ = self.civ_manager.get_civilization(other_id)
            other_name = other_civ['name'] if other_civ else other_id[:6]
            exp_iso = cf.get("expires_at", "")
            exp_ts = 0
            try:
                exp_dt = datetime.fromisoformat(exp_iso)
                exp_ts = int(exp_dt.timestamp())
            except Exception:
                pass
            embed.add_field(
                name=f"vs **{other_name}**",
                value=f"Expires <t:{exp_ts}:R>",
                inline=False
            )
        await self._respond(ctx, embed=embed, ephemeral=True)

    @commands.hybrid_command(name='breakceasefire')
    @app_commands.describe(target="Civilization leader to break ceasefire with")
    async def break_ceasefire(self, ctx, target: Optional[discord.Member] = None):
        """Break an active ceasefire — costs happiness."""
        if not target:
            await self._respond(ctx, content="Usage: `.breakceasefire <user>`")
            return
        user_id = str(ctx.author.id)
        target_id = str(target.id)
        try:
            cf = self.db.get_active_ceasefire(user_id, target_id)
        except AttributeError:
            await self._respond(ctx, content="❌ Ceasefire system not available.")
            return
        if not cf:
            await self._respond(ctx, content="❌ No active ceasefire between you.")
            return

        # Delete the ceasefire doc
        try:
            self.db.client.collection("ceasefires").document(cf["id"]).delete()
        except Exception as e:
            logger.error(f"Failed to delete ceasefire: {e}")

        self.civ_manager.update_population(user_id, {"happiness": -15})

        embed = discord.Embed(
            title="💔 Ceasefire Broken",
            description=f"**{ctx.author.display_name}** broke the ceasefire with **{target.display_name}**.",
            color=discord.Color.red()
        )
        embed.add_field(name="Consequence", value="-15 happiness", inline=False)
        await ctx.send(embed=embed)
        try:
            await ctx.send(f"{target.mention} 💔 **Ceasefire broken!** You can now attack freely.")
        except Exception:
            pass


async def setup(bot):
    await bot.add_cog(CeasefireCog(bot))
