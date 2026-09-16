import random
import asyncio
import discord
from discord.ext import commands
from discord import app_commands
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any

from bot import config

logger = logging.getLogger(__name__)

MAX_ANNEX_TERRITORIES = 5
ANNEX_FRACTION_CAP = 0.25


class DiplomacyCommands(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.db = bot.db
        self.civ_manager = bot.civ_manager

    # ============================================================
    # INTERNAL HELPERS
    # ============================================================
    def _are_allied(self, user_a: str, user_b: str) -> bool:
        try:
            docs = self.db.client.collection("alliances").where("members", "array_contains", user_a).stream()
            for doc in docs:
                if user_b in doc.to_dict().get("members", []):
                    return True
            return False
        except Exception as e:
            logger.error(f"_are_allied error: {e}")
            return False

    async def _respond(self, ctx, content: str = None, embed: discord.Embed = None, ephemeral: bool = False):
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
                    await ctx.interaction.response.send_message(**kwargs)
                else:
                    await ctx.interaction.followup.send(**kwargs)
                return
            except Exception:
                logger.exception("Interaction response failed")
        await ctx.send(content=content, embed=embed)

    def _check_cooldown(self, ctx, command_name: str):
        minutes = config.COOLDOWNS.get(command_name, 0)
        if minutes <= 0:
            return True, None
        user_id = str(ctx.author.id)
        last_used = self.db.get_command_cooldown(user_id, command_name)
        if last_used:
            cooldown_end = last_used + timedelta(minutes=minutes)
            if datetime.utcnow() < cooldown_end:
                remaining = cooldown_end - datetime.utcnow()
                mins = int(remaining.total_seconds() // 60)
                secs = int(remaining.total_seconds() % 60)
                return False, f"⏳ Please wait {mins}m {secs}s before using this command again!"
        self.db.set_command_cooldown(user_id, command_name, datetime.utcnow())
        return True, None

    def _check_war(self, a_id: str, b_id: str) -> bool:
        for war in self.db.get_wars(status="ongoing"):
            a = war.get("attacker_id")
            d = war.get("defender_id")
            if (a == a_id and d == b_id) or (a == b_id and d == a_id):
                return True
        return False

    # ============================================================
    # AUTOCOMPLETE
    # ============================================================
    async def _alliance_id_autocomplete(self, interaction: discord.Interaction, current: str):
        uid = str(interaction.user.id)
        candidates = []
        for proposal in self.db.get_alliance_proposals_for_user(uid):
            label = f"{proposal['id']} - {proposal.get('alliance_name','?')}"
            if current.lower() in label.lower():
                candidates.append(app_commands.Choice(name=label[:100], value=proposal["id"]))
        return candidates[:25]

    async def _trade_id_autocomplete(self, interaction: discord.Interaction, current: str):
        uid = str(interaction.user.id)
        candidates = []
        for trade in self.db.get_trade_proposals_for_user(uid):
            label = (f"{trade['id']} - {trade.get('offer_amount','?')} {trade.get('offer_resource','?')} "
                     f"for {trade.get('request_amount','?')} {trade.get('request_resource','?')}")
            if current.lower() in label.lower():
                candidates.append(app_commands.Choice(name=label[:100], value=trade["id"]))
        return candidates[:25]

    async def _peace_id_autocomplete(self, interaction: discord.Interaction, current: str):
        uid = str(interaction.user.id)
        candidates = []
        try:
            offers = self.db.get_peace_offers(uid)
        except Exception:
            offers = []
        for offer in offers:
            if offer.get("receiver_id") != uid:
                continue
            label = f"{offer['id']} - {offer.get('offerer_name','?')} ({offer.get('type','peace')})"
            if current.lower() in label.lower():
                candidates.append(app_commands.Choice(name=label[:100], value=offer["id"]))
        return candidates[:25]

    # ============================================================
    # ALLIANCES
    # ============================================================
    @commands.hybrid_command(name='ally')
    @app_commands.describe(target="Civilization leader to ally with", alliance_name="Name of the alliance")
    async def propose_alliance(self, ctx, target: Optional[discord.Member] = None, alliance_name: Optional[str] = None):
        if not target or not alliance_name:
            await self._respond(ctx, content="🤝 **Alliance Proposal**\nUsage: `.ally <user> <alliance_name>`")
            return
        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await self._respond(ctx, content="❌ You need a civilization first!")
            return
        target_id = str(target.id)
        if target_id == user_id:
            await self._respond(ctx, content="❌ You cannot ally with yourself!")
            return
        target_civ = self.civ_manager.get_civilization(target_id)
        if not target_civ:
            await self._respond(ctx, content="❌ Target user doesn't have a civilization!")
            return
        if self._check_war(user_id, target_id):
            await self._respond(ctx, content="❌ You cannot ally with someone you're at war with!")
            return
        if self._are_allied(user_id, target_id):
            await self._respond(ctx, content="❌ You're already in an alliance together!")
            return
        alliance_id = str(random.randint(100000, 999999))
        self.db.save_alliance_proposal(alliance_id, {
            "proposer_id": user_id,
            "target_id": target_id,
            "alliance_name": alliance_name,
            "expires": (datetime.utcnow() + timedelta(minutes=30)).isoformat(),
            "created_at": datetime.utcnow().isoformat(),
        })
        embed = discord.Embed(title="🤝 Alliance Proposal Received!",
                              description=f"From **{civ['name']}** (led by {ctx.author.name})",
                              color=discord.Color.blue())
        embed.add_field(name="Proposed Alliance",
                        value=f"Alliance Name: **{alliance_name}**", inline=False)
        embed.add_field(name="How to Respond",
                        value=f"`.acceptally {alliance_id}` or `.rejectally {alliance_id}`\nExpires in 30 minutes.",
                        inline=False)
        await ctx.send(f"<@{target_id}>", embed=embed)
        await self._respond(ctx, content=f"🤝 **Alliance Proposed!** Sent to **{target_civ['name']}**.")

    @commands.hybrid_command(name='acceptally')
    @app_commands.describe(alliance_id="Pending alliance proposal ID")
    @app_commands.autocomplete(alliance_id=_alliance_id_autocomplete)
    async def accept_alliance(self, ctx, alliance_id: str = None):
        if not alliance_id:
            await self._respond(ctx, content="Usage: `.acceptally <id>`")
            return
        user_id = str(ctx.author.id)
        proposal = self.db.get_alliance_proposal(alliance_id)
        if not proposal:
            await self._respond(ctx, content="❌ Invalid or expired alliance ID!")
            return
        if user_id != proposal["target_id"]:
            await self._respond(ctx, content="❌ This proposal isn't for you!")
            return
        success = self.db.create_alliance(proposal["alliance_name"], proposal["proposer_id"], description="")
        if not success:
            await self._respond(ctx, content="❌ Failed to create alliance.")
            return
        alliance = self.db.get_alliance_by_name(proposal["alliance_name"])
        if alliance:
            self.db.add_alliance_member(alliance["id"], user_id)
        embed = discord.Embed(title="🤝 Alliance Formed!",
                              description=f"**{proposal['alliance_name']}** has been established!",
                              color=discord.Color.green())
        await self._respond(ctx, embed=embed)
        await ctx.send(f"<@{proposal['proposer_id']}> 🤝 **Alliance Accepted!**")
        self.db.delete_alliance_proposal(alliance_id)

    @commands.hybrid_command(name='rejectally')
    @app_commands.describe(alliance_id="Pending alliance proposal ID")
    @app_commands.autocomplete(alliance_id=_alliance_id_autocomplete)
    async def reject_alliance(self, ctx, alliance_id: str = None):
        if not alliance_id:
            await self._respond(ctx, content="Usage: `.rejectally <id>`")
            return
        user_id = str(ctx.author.id)
        proposal = self.db.get_alliance_proposal(alliance_id)
        if not proposal:
            await self._respond(ctx, content="❌ Invalid or expired!")
            return
        if user_id != proposal["target_id"]:
            await self._respond(ctx, content="❌ Not for you!")
            return
        await ctx.send(f"<@{proposal['proposer_id']}> 🤝 **Alliance Rejected!**")
        await self._respond(ctx, content="🤝 **Rejected.**")
        self.db.delete_alliance_proposal(alliance_id)

    @commands.hybrid_command(name='break')
    async def break_alliance(self, ctx):
        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await self._respond(ctx, content="❌ You need a civilization first!")
            return
        alliance_doc = None
        for doc in self.db.client.collection("alliances").where("members", "array_contains", user_id).stream():
            alliance_doc = doc
            break
        if not alliance_doc:
            await self._respond(ctx, content="❌ You are not in an alliance!")
            return
        alliance_data = alliance_doc.to_dict()
        members = alliance_data.get("members", [])
        if len(members) <= 2:
            alliance_doc.reference.delete()
        else:
            members.remove(user_id)
            alliance_doc.reference.update({"members": members})
        self.civ_manager.update_population(user_id, {"happiness": -10})
        embed = discord.Embed(title="💔 Alliance Broken",
                              description=f"Left the **{alliance_data['name']}** alliance.",
                              color=discord.Color.red())
        await self._respond(ctx, embed=embed)
        for member_id in members:
            if member_id != user_id:
                await ctx.send(f"<@{member_id}> 💔 {civ['name']} left the **{alliance_data['name']}** alliance.")

    # ============================================================
    # SEND / TRADE
    # ============================================================
    @commands.hybrid_command(name='send')
    @app_commands.describe(target="Recipient", resource_type="Resource", amount="Amount")
    @app_commands.choices(resource_type=[
        app_commands.Choice(name="gold", value="gold"),
        app_commands.Choice(name="food", value="food"),
        app_commands.Choice(name="wood", value="wood"),
        app_commands.Choice(name="stone", value="stone"),
    ])
    async def send_resources(self, ctx, target: Optional[discord.Member] = None,
                             resource_type: Optional[str] = None, amount: Optional[int] = None):
        if not target or not resource_type or amount is None:
            await self._respond(ctx, content="📦 **Resource Transfer**\nUsage: `.send <user> <resource> <amount>`")
            return
        if resource_type not in ['gold', 'food', 'wood', 'stone']:
            await self._respond(ctx, content="❌ Invalid resource type!")
            return
        if amount < 1:
            await self._respond(ctx, content="❌ Amount must be positive!")
            return
        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await self._respond(ctx, content="❌ You need a civilization first!")
            return
        target_id = str(target.id)
        target_civ = self.civ_manager.get_civilization(target_id)
        if not target_civ:
            await self._respond(ctx, content="❌ Target has no civilization!")
            return
        if not self.civ_manager.can_afford(user_id, {resource_type: amount}):
            await self._respond(ctx, content=f"❌ You don't have {amount} {resource_type}!")
            return
        is_allied = self._are_allied(user_id, target_id)
        efficiency = 0.95 if is_allied else 0.9
        received = int(amount * efficiency)
        self.civ_manager.spend_resources(user_id, {resource_type: amount})
        self.civ_manager.update_resources(target_id, {resource_type: received})
        icons = {"gold": "🪙", "food": "🌾", "wood": "🪵", "stone": "🪨"}
        embed = discord.Embed(title="📦 Resources Sent",
                              description=f"Sent to **{target_civ['name']}**!",
                              color=discord.Color.blue())
        embed.add_field(name="Transfer",
                        value=f"{icons[resource_type]} Sent: {amount}\n{icons[resource_type]} Received: {received}\n📊 Efficiency: {int(efficiency * 100)}%",
                        inline=False)
        if is_allied:
            embed.add_field(name="Alliance Bonus", value="Higher efficiency!", inline=False)
        if ctx.interaction is not None:
            await self._respond(ctx, embed=embed, ephemeral=True)
            try:
                u = await self.bot.fetch_user(int(target_id))
                await u.send(f"📦 **Received** {received} {resource_type} from {civ['name']}!")
            except Exception:
                pass
        else:
            await ctx.send(embed=embed)
            await ctx.send(f"<@{target_id}> 📦 **Received** {received} {resource_type} from {civ['name']}!")

    @commands.hybrid_command(name='trade')
    @app_commands.describe(target="Trade partner", offer_resource="You offer", offer_amount="Amount",
                           request_resource="You want", request_amount="Amount")
    @app_commands.choices(
        offer_resource=[app_commands.Choice(name=r, value=r) for r in ["gold", "food", "wood", "stone"]],
        request_resource=[app_commands.Choice(name=r, value=r) for r in ["gold", "food", "wood", "stone"]]
    )
    async def propose_trade(self, ctx, target: Optional[discord.Member] = None,
                            offer_resource: Optional[str] = None, offer_amount: Optional[int] = None,
                            request_resource: Optional[str] = None, request_amount: Optional[int] = None):
        if not all([target, offer_resource, offer_amount, request_resource, request_amount]):
            await self._respond(ctx, content="💰 **Trade**\nUsage: `.trade <user> <offer_resource> <offer_amount> <request_resource> <request_amount>`")
            return
        valid = ['gold', 'food', 'wood', 'stone']
        if offer_resource not in valid or request_resource not in valid:
            await self._respond(ctx, content=f"❌ Invalid resource!")
            return
        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await self._respond(ctx, content="❌ You need a civilization first!")
            return
        target_id = str(target.id)
        target_civ = self.civ_manager.get_civilization(target_id)
        if not target_civ:
            await self._respond(ctx, content="❌ Target has no civilization!")
            return
        if not self.civ_manager.can_afford(user_id, {offer_resource: offer_amount}):
            await self._respond(ctx, content=f"❌ You don't have {offer_amount} {offer_resource}!")
            return
        trade_id = str(random.randint(100000, 999999))
        self.db.save_trade_proposal(trade_id, {
            "proposer_id": user_id, "target_id": target_id,
            "offer_resource": offer_resource, "offer_amount": offer_amount,
            "request_resource": request_resource, "request_amount": request_amount,
            "expires": (datetime.utcnow() + timedelta(minutes=30)).isoformat(),
            "created_at": datetime.utcnow().isoformat(),
        })
        icons = {"gold": "🪙", "food": "🌾", "wood": "🪵", "stone": "🪨"}
        embed = discord.Embed(title="💰 Trade Proposal",
                              description=f"From **{civ['name']}**",
                              color=discord.Color.blue())
        embed.add_field(name="Terms",
                        value=(f"They offer: {icons[offer_resource]} {offer_amount} {offer_resource.capitalize()}\n"
                               f"They request: {icons[request_resource]} {request_amount} {request_resource.capitalize()}"),
                        inline=False)
        embed.add_field(name="Respond",
                        value=f"`.accepttrade {trade_id}` or `.rejecttrade {trade_id}`\nExpires in 30 min.",
                        inline=False)
        await ctx.send(f"<@{target_id}>", embed=embed)
        await self._respond(ctx, content="💰 **Trade Proposed!**")

    @commands.hybrid_command(name='accepttrade')
    @app_commands.describe(trade_id="Pending trade ID")
    @app_commands.autocomplete(trade_id=_trade_id_autocomplete)
    async def accept_trade(self, ctx, trade_id: str = None):
        if not trade_id:
            await self._respond(ctx, content="Usage: `.accepttrade <id>`")
            return
        user_id = str(ctx.author.id)
        trade = self.db.get_trade_proposal(trade_id)
        if not trade:
            await self._respond(ctx, content="❌ Invalid or expired trade ID!")
            return
        if user_id != trade["target_id"]:
            await self._respond(ctx, content="❌ Not for you!")
            return
        if not self.civ_manager.can_afford(trade["proposer_id"], {trade["offer_resource"]: trade["offer_amount"]}):
            await self._respond(ctx, content="❌ Proposer no longer has the offered resources!")
            self.db.delete_trade_proposal(trade_id)
            return
        if not self.civ_manager.can_afford(user_id, {trade["request_resource"]: trade["request_amount"]}):
            await self._respond(ctx, content="❌ You no longer have the requested resources!")
            self.db.delete_trade_proposal(trade_id)
            return
        self.civ_manager.spend_resources(trade["proposer_id"], {trade["offer_resource"]: trade["offer_amount"]})
        self.civ_manager.update_resources(trade["proposer_id"], {trade["request_resource"]: trade["request_amount"]})
        self.civ_manager.spend_resources(user_id, {trade["request_resource"]: trade["request_amount"]})
        self.civ_manager.update_resources(user_id, {trade["offer_resource"]: trade["offer_amount"]})
        await ctx.send(f"<@{trade['proposer_id']}> 💰 **Trade Accepted!**")
        await self._respond(ctx, content="💰 **Trade Accepted!**")
        self.db.delete_trade_proposal(trade_id)

    @commands.hybrid_command(name='rejecttrade')
    @app_commands.describe(trade_id="Pending trade ID")
    @app_commands.autocomplete(trade_id=_trade_id_autocomplete)
    async def reject_trade(self, ctx, trade_id: str = None):
        if not trade_id:
            await self._respond(ctx, content="Usage: `.rejecttrade <id>`")
            return
        user_id = str(ctx.author.id)
        trade = self.db.get_trade_proposal(trade_id)
        if not trade:
            await self._respond(ctx, content="❌ Invalid trade!")
            return
        if user_id != trade["target_id"]:
            await self._respond(ctx, content="❌ Not for you!")
            return
        await ctx.send(f"<@{trade['proposer_id']}> 💰 **Trade Rejected!**")
        await self._respond(ctx, content="💰 **Rejected.**")
        self.db.delete_trade_proposal(trade_id)

    # ============================================================
    # MAIL
    # ============================================================
    @commands.hybrid_command(name='mail')
    @app_commands.describe(target="Recipient", message="Diplomatic message")
    async def send_diplomatic_message(self, ctx, target: Optional[discord.Member] = None, *, message: Optional[str] = None):
        if not target or not message:
            await self._respond(ctx, content="📜 **Mail**\nUsage: `.mail <user> <message>`")
            return
        if len(message) > 500:
            await self._respond(ctx, content="❌ Message too long! Max 500 chars.")
            return
        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await self._respond(ctx, content="❌ You need a civilization first!")
            return
        target_id = str(target.id)
        target_civ = self.civ_manager.get_civilization(target_id)
        if not target_civ:
            await self._respond(ctx, content="❌ Target has no civilization!")
            return
        self.db.send_message(user_id, target_id, message)
        if ctx.interaction is not None:
            await self._respond(ctx, content=f"📜 **Message sent to {target_civ['name']}.** Only you see this.", ephemeral=True)
            try:
                u = await self.bot.fetch_user(int(target_id))
                await u.send(f"📜 **Mail from {civ['name']}!** Check `.inbox`.")
            except Exception:
                pass
        else:
            await ctx.send(f"<@{target_id}> 📜 Mail from {civ['name']}! Check `.inbox`.")
            await ctx.send("📜 **Sent.**")

    @commands.hybrid_command(name='inbox')
    async def check_inbox(self, ctx):
        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await self._respond(ctx, content="❌ You need a civilization first!")
            return
        embed = discord.Embed(title="📬 Inbox",
                              description=f"Pending proposals for **{civ['name']}**",
                              color=discord.Color.blue())

        alliance_proposals = []
        for proposal in self.db.get_alliance_proposals_for_user(user_id):
            proposer_civ = self.civ_manager.get_civilization(proposal["proposer_id"])
            if proposer_civ:
                exp_raw = proposal.get("expires")
                exp_dt = datetime.fromisoformat(exp_raw) if isinstance(exp_raw, str) else exp_raw
                exp_ts = int(exp_dt.timestamp()) if exp_dt else 0
                alliance_proposals.append(
                    f"**ID**: {proposal['id']} — from **{proposer_civ['name']}**\n"
                    f"Alliance: **{proposal['alliance_name']}**\n"
                    f"`.acceptally {proposal['id']}` / `.rejectally {proposal['id']}` | Expires <t:{exp_ts}:R>"
                )

        trade_proposals = []
        icons = {"gold": "🪙", "food": "🌾", "wood": "🪵", "stone": "🪨"}
        for trade in self.db.get_trade_proposals_for_user(user_id):
            proposer_civ = self.civ_manager.get_civilization(trade["proposer_id"])
            if proposer_civ:
                exp_raw = trade.get("expires")
                exp_dt = datetime.fromisoformat(exp_raw) if isinstance(exp_raw, str) else exp_raw
                exp_ts = int(exp_dt.timestamp()) if exp_dt else 0
                trade_proposals.append(
                    f"**ID**: {trade['id']} — from **{proposer_civ['name']}**\n"
                    f"Offers: {icons[trade['offer_resource']]} {trade['offer_amount']}\n"
                    f"Requests: {icons[trade['request_resource']]} {trade['request_amount']}\n"
                    f"`.accepttrade {trade['id']}` / `.rejecttrade {trade['id']}` | Expires <t:{exp_ts}:R>"
                )

        peace_offers_list = []
        for offer in self.db.get_peace_offers(user_id):
            if offer.get("receiver_id") != user_id:
                continue
            terms = offer.get("terms", {}) or {}
            offer_type = offer.get("type", "peace")
            exp_iso = offer.get("expires_at")
            exp_ts = 0
            if exp_iso:
                try:
                    exp_dt = datetime.fromisoformat(exp_iso)
                    exp_ts = int(exp_dt.timestamp())
                except Exception:
                    pass
            terms_summary = self._format_terms(terms, offer_type)
            peace_offers_list.append(
                f"**ID**: {offer['id']} — from **{offer.get('offerer_name','?')}** ({offer_type})\n"
                f"{terms_summary}\n"
                f"`.acceptpeace {offer['id']}` / `.rejectpeace {offer['id']}` | Expires <t:{exp_ts}:R>"
            )

        messages_list = []
        try:
            for m in self.db.get_messages(user_id):
                sender_civ = self.civ_manager.get_civilization(m['sender_id'])
                if sender_civ:
                    timestamp = m['created_at']
                    if isinstance(timestamp, str):
                        timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    messages_list.append(
                        f"**From**: {sender_civ['name']}\n{m['message']}\n<t:{int(timestamp.timestamp())}:R>"
                    )
        except Exception as e:
            logger.error(f"Error fetching messages: {e}")

        embed.add_field(name="🤝 Alliance Proposals",
                        value="\n\n".join(alliance_proposals) if alliance_proposals else "None.",
                        inline=False)
        embed.add_field(name="💰 Trade Proposals",
                        value="\n\n".join(trade_proposals) if trade_proposals else "None.",
                        inline=False)
        embed.add_field(name="🕊️ Peace Offers",
                        value="\n\n".join(peace_offers_list) if peace_offers_list else "None.",
                        inline=False)
        embed.add_field(name="📜 Messages",
                        value="\n\n".join(messages_list) if messages_list else "None.",
                        inline=False)

        await self._respond(ctx, embed=embed, ephemeral=True)

    # ============================================================
    # SIMPLE PEACE (renamed from 'peace' to avoid military.py collision)
    # ============================================================
    @commands.hybrid_command(name='simplepeace', aliases=['sp'])
    @app_commands.describe(target="Civilization leader to offer peace to")
    async def simple_peace(self, ctx, target: Optional[discord.Member] = None):
        """Simple permanent peace — no terms. (Renamed from .peace to avoid clash with military.)"""
        if not target:
            await self._respond(ctx, content="🕊️ **Simple Peace**\nUsage: `.simplepeace <user>` (alias `.sp`)\nFor a customizable deal, use `.peacedraft`.")
            return
        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await self._respond(ctx, content="❌ You need a civilization first!")
            return
        target_id = str(target.id)
        if target_id == user_id:
            await self._respond(ctx, content="❌ You're at peace with yourself!")
            return
        target_civ = self.civ_manager.get_civilization(target_id)
        if not target_civ:
            await self._respond(ctx, content="❌ Target has no civilization!")
            return
        if not self._check_war(user_id, target_id):
            await self._respond(ctx, content="❌ You're not at war with them!")
            return
        existing = self.db.get_peace_offers(user_id)
        for offer in existing:
            if offer.get("offerer_id") == user_id and offer.get("receiver_id") == target_id:
                await self._respond(ctx, content="❌ You already have a pending offer!")
                return
        offer_id = self.db.create_peace_offer(
            user_id, target_id, offer_type="peace",
            terms={"gold": 0, "food": 0, "wood": 0, "stone": 0,
                   "annex_territories": [], "ceasefire_hours": 0}
        )
        if not offer_id:
            await self._respond(ctx, content="❌ Failed to create peace offer.")
            return
        embed = discord.Embed(title="🕊️ Peace Offer Sent",
                              description=f"**{civ['name']}** offered **permanent peace** to **{target_civ['name']}**.",
                              color=discord.Color.green())
        embed.add_field(name="No terms — just peace.",
                        value=f"Offer ID: `{offer_id}`\n`{target.mention}` respond with `.acceptpeace {offer_id}` or `.rejectpeace {offer_id}`",
                        inline=False)
        await ctx.send(embed=embed)
        try:
            await ctx.send(f"{target.mention} 🕊️ Peace offer received! Use `.acceptpeace {offer_id}`.")
        except Exception:
            pass

    # ============================================================
    # DRAFTED PEACE
    # ============================================================
    @commands.hybrid_command(name='peacedraft')
    @app_commands.describe(target="Civilization leader to negotiate with")
    async def peace_draft(self, ctx, target: Optional[discord.Member] = None):
        """Interactive peace deal drafting — annex territories, demand resources, set ceasefire time."""
        if not target:
            await self._respond(ctx, content="🕊️ **Draft Peace**\nUsage: `.peacedraft <user>`")
            return
        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await self._respond(ctx, content="❌ You need a civilization first!")
            return
        target_id = str(target.id)
        if target_id == user_id:
            await self._respond(ctx, content="❌ You cannot negotiate with yourself!")
            return
        target_civ = self.civ_manager.get_civilization(target_id)
        if not target_civ:
            await self._respond(ctx, content="❌ Target has no civilization!")
            return
        if not self._check_war(user_id, target_id):
            await self._respond(ctx, content="❌ You're not at war with them!")
            return

        embed = discord.Embed(
            title="🕊️ Draft a Peace Deal",
            description=(
                f"Negotiating with **{target_civ['name']}**.\n"
                "You'll be asked for each term one at a time.\n"
                "**Type `skip` for any field** to leave it at 0.\n"
                "Whole draft expires in **5 minutes**."
            ),
            color=discord.Color.blue()
        )
        await ctx.send(embed=embed)

        def check(m):
            return m.author.id == ctx.author.id and m.channel.id == ctx.channel.id

        async def ask(question: str, max_val: Optional[int] = None) -> Optional[int]:
            await ctx.send(question)
            try:
                msg = await self.bot.wait_for('message', timeout=90, check=check)
                content = msg.content.strip().lower()
                if content == "skip" or content == "0":
                    return 0
                val = int(content)
                if val < 0:
                    return 0
                if max_val is not None and val > max_val:
                    val = max_val
                return val
            except asyncio.TimeoutError:
                return None
            except (ValueError, AttributeError):
                return None

        gold_demand = await ask(f"💰 **How much gold?** Target has {target_civ['resources']['gold']:,}.", target_civ['resources']['gold'])
        if gold_demand is None:
            await ctx.send("❌ Draft timed out.")
            return
        food_demand = await ask(f"🌾 **How much food?** Target has {target_civ['resources']['food']:,}.", target_civ['resources']['food'])
        if food_demand is None:
            await ctx.send("❌ Draft timed out.")
            return
        wood_demand = await ask(f"🪵 **How much wood?** Target has {target_civ['resources']['wood']:,}.", target_civ['resources']['wood'])
        if wood_demand is None:
            await ctx.send("❌ Draft timed out.")
            return
        stone_demand = await ask(f"🪨 **How much stone?** Target has {target_civ['resources']['stone']:,}.", target_civ['resources']['stone'])
        if stone_demand is None:
            await ctx.send("❌ Draft timed out.")
            return
        hours = await ask(
            "⏳ **Ceasefire duration in hours?**\n"
            "`0` = permanent peace (ends the war)\n"
            "Any positive = temporary ceasefire (war continues after)\n"
            "Max 720 hours (30 days).",
            max_val=720
        )
        if hours is None:
            await ctx.send("❌ Draft timed out.")
            return

        target_territories = self.db.get_player_territories(target_id)
        annex_list: List[str] = []
        if target_territories:
            max_annex = min(MAX_ANNEX_TERRITORIES, max(1, int(len(target_territories) * ANNEX_FRACTION_CAP)))
            preview = ", ".join(target_territories[:20])
            if len(target_territories) > 20:
                preview += f"... (+{len(target_territories) - 20} more)"
            await ctx.send(
                f"🏴 **Which territories to annex?**\n"
                f"You can annex up to **{max_annex}**.\n"
                f"Target owns: {preview}\n\n"
                f"Type comma-separated names, or `skip` for none."
            )
            try:
                msg = await self.bot.wait_for('message', timeout=90, check=check)
                content = msg.content.strip()
                if content.lower() != "skip":
                    requested = [t.strip() for t in content.split(",") if t.strip()]
                    matched = []
                    for req in requested:
                        for t in target_territories:
                            if t.lower() == req.lower():
                                matched.append(t)
                                break
                    annex_list = matched[:max_annex]
                    if len(matched) > max_annex:
                        await ctx.send(f"⚠️ Only first **{max_annex}** will be included.")
            except asyncio.TimeoutError:
                await ctx.send("❌ Draft timed out.")
                return

        terms = {
            "gold": gold_demand,
            "food": food_demand,
            "wood": wood_demand,
            "stone": stone_demand,
            "annex_territories": annex_list,
            "ceasefire_hours": hours,
        }
        summary = self._format_terms(terms, "peace")

        embed = discord.Embed(title="📜 Peace Deal Summary",
                              description=f"From **{civ['name']}** → **{target_civ['name']}**",
                              color=discord.Color.gold())
        embed.add_field(name="Terms", value=summary, inline=False)
        embed.set_footer(text="Type 'confirm' within 60s to send, or 'cancel' to abort.")
        await ctx.send(embed=embed)

        try:
            msg = await self.bot.wait_for('message', timeout=60, check=check)
            if msg.content.strip().lower() != "confirm":
                await ctx.send("🛑 Draft cancelled.")
                return
        except asyncio.TimeoutError:
            await ctx.send("❌ Draft timed out.")
            return

        offer_id = self.db.create_peace_offer(user_id, target_id, offer_type="peace", terms=terms)
        if not offer_id:
            await ctx.send("❌ Failed to create peace offer.")
            return
        embed = discord.Embed(title="🕊️ Peace Offer Sent!",
                              description=f"**{civ['name']}** sent terms to **{target_civ['name']}**.",
                              color=discord.Color.green())
        embed.add_field(name="Terms", value=summary, inline=False)
        embed.add_field(name="Respond",
                        value=f"`{target.mention}` — `.acceptpeace {offer_id}` or `.rejectpeace {offer_id}`",
                        inline=False)
        await ctx.send(embed=embed)

    # ============================================================
    # ACCEPT / REJECT PEACE
    # ============================================================
    @commands.hybrid_command(name='acceptpeace')
    @app_commands.describe(offer_id="Peace offer ID")
    @app_commands.autocomplete(offer_id=_peace_id_autocomplete)
    async def accept_peace(self, ctx, offer_id: str = None):
        if not offer_id:
            await self._respond(ctx, content="Usage: `.acceptpeace <id>`")
            return
        user_id = str(ctx.author.id)
        offer = self.db.get_peace_offer_by_id(offer_id)
        if not offer:
            await self._respond(ctx, content="❌ Invalid or expired peace offer.")
            return
        if offer.get("receiver_id") != user_id:
            await self._respond(ctx, content="❌ Not for you!")
            return

        offerer_id = offer.get("offerer_id")
        offerer_civ = self.civ_manager.get_civilization(offerer_id)
        receiver_civ = self.civ_manager.get_civilization(user_id)
        if not offerer_civ or not receiver_civ:
            await self._respond(ctx, content="❌ One of the civilizations no longer exists.")
            self.db.delete_peace_offer(offer_id)
            return

        terms = offer.get("terms", {}) or {}
        offer_type = offer.get("type", "peace")

        costs = {
            "gold": terms.get("gold", 0),
            "food": terms.get("food", 0),
            "wood": terms.get("wood", 0),
            "stone": terms.get("stone", 0),
        }
        if not self.civ_manager.can_afford(user_id, costs):
            await self._respond(ctx, content="❌ You can no longer afford those terms!")
            self.db.delete_peace_offer(offer_id)
            return

        receiver_territories = set(self.db.get_player_territories(user_id))
        annex_list = terms.get("annex_territories", [])
        missing = [t for t in annex_list if t not in receiver_territories]
        if missing:
            await self._respond(ctx, content=f"❌ You no longer own: {', '.join(missing)}.")
            self.db.delete_peace_offer(offer_id)
            return

        paid = {}
        for res, amt in costs.items():
            if amt > 0:
                self.civ_manager.spend_resources(user_id, {res: amt})
                self.civ_manager.update_resources(offerer_id, {res: amt})
                paid[res] = amt

        transferred = []
        territory_cog = self.bot.get_cog("TerritoryCog")
        for t in annex_list:
            success = self.db.conquer_territory(offerer_id, user_id, t)
            if success:
                area = territory_cog.province_areas.get(t, 1000) if territory_cog else 1000
                self.civ_manager.update_territory(offerer_id, {"land_size": area})
                self.civ_manager.update_territory(user_id, {"land_size": -area})
                transferred.append(t)

        ceasefire_hours = terms.get("ceasefire_hours", 0)
        if ceasefire_hours and ceasefire_hours > 0:
            self.db.create_ceasefire(offerer_id, user_id, ceasefire_hours)
            war_ended = False
        else:
            self.db.end_war(offerer_id, user_id, "peace")
            war_ended = True

        self.civ_manager.update_population(user_id, {"happiness": 15})
        self.civ_manager.update_population(offerer_id, {"happiness": 15})
        self.db.delete_peace_offer(offer_id)

        icons = {"gold": "🪙", "food": "🌾", "wood": "🪵", "stone": "🪨"}
        embed = discord.Embed(
            title="🕊️ Peace Deal Accepted!" if war_ended else "⏳ Ceasefire Accepted!",
            description=f"**{receiver_civ['name']}** accepted terms from **{offerer_civ['name']}**.",
            color=discord.Color.green()
        )
        if paid:
            embed.add_field(name="Resources Transferred",
                            value="\n".join([f"{icons[r]} {v:,} {r.capitalize()}" for r, v in paid.items()]),
                            inline=False)
        if transferred:
            embed.add_field(name="Territories Annexed",
                            value=", ".join(transferred),
                            inline=False)
        if ceasefire_hours and ceasefire_hours > 0:
            embed.add_field(name="Ceasefire Duration",
                            value=f"{ceasefire_hours} hours",
                            inline=False)
        else:
            embed.add_field(name="War Status", value="⚔️ **War Ended** — permanent peace.", inline=False)
        embed.add_field(name="Morale Boost", value="Both nations gain +15 happiness.", inline=False)
        await ctx.send(embed=embed)

    @commands.hybrid_command(name='rejectpeace')
    @app_commands.describe(offer_id="Peace offer ID")
    @app_commands.autocomplete(offer_id=_peace_id_autocomplete)
    async def reject_peace(self, ctx, offer_id: str = None):
        if not offer_id:
            await self._respond(ctx, content="Usage: `.rejectpeace <id>`")
            return
        user_id = str(ctx.author.id)
        offer = self.db.get_peace_offer_by_id(offer_id)
        if not offer:
            await self._respond(ctx, content="❌ Invalid or expired.")
            return
        if offer.get("receiver_id") != user_id:
            await self._respond(ctx, content="❌ Not for you!")
            return
        offerer_id = offer.get("offerer_id")
        self.db.delete_peace_offer(offer_id)
        await ctx.send(f"<@{offerer_id}> 🕊️ **Peace offer rejected!**")
        await self._respond(ctx, content="🕊️ **Rejected.**")

    @commands.hybrid_command(name='peaceinfo')
    @app_commands.describe(offer_id="Peace offer ID")
    async def peace_info(self, ctx, offer_id: str = None):
        if not offer_id:
            await self._respond(ctx, content="Usage: `.peaceinfo <id>`")
            return
        offer = self.db.get_peace_offer_by_id(offer_id)
        if not offer:
            await self._respond(ctx, content="❌ Invalid or expired.")
            return
        terms = offer.get("terms", {}) or {}
        offer_type = offer.get("type", "peace")
        summary = self._format_terms(terms, offer_type)
        embed = discord.Embed(title=f"📜 Peace Offer {offer_id}",
                              description=f"From <@{offer.get('offerer_id')}> → <@{offer.get('receiver_id')}>",
                              color=discord.Color.blue())
        embed.add_field(name="Type", value=offer_type.capitalize(), inline=True)
        embed.add_field(name="Terms", value=summary, inline=False)
        await self._respond(ctx, embed=embed, ephemeral=True)

    @commands.hybrid_command(name='myoffers')
    async def my_offers(self, ctx):
        user_id = str(ctx.author.id)
        offers = self.db.get_peace_offers(user_id)
        if not offers:
            await self._respond(ctx, content="📭 You have no pending peace offers.")
            return
        embed = discord.Embed(title="📭 Your Peace Offers", color=discord.Color.blue())
        for offer in offers:
            direction = "📤 SENT" if offer.get("offerer_id") == user_id else "📥 RECEIVED"
            terms = offer.get("terms", {}) or {}
            summary = self._format_terms(terms, offer.get("type", "peace"))
            embed.add_field(name=f"{direction} — ID `{offer['id']}`", value=summary, inline=False)
        await self._respond(ctx, embed=embed, ephemeral=True)

    # ============================================================
    # CEASEFIRE (was in ceasefire.py — now baked here)
    # ============================================================
    @commands.hybrid_command(name='ceasefire')
    @app_commands.describe(target="Civilization leader", hours="Duration in hours (1-720)")
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
            await self._respond(ctx, content="❌ You're not at war with them!")
            return
        active = self.db.get_active_ceasefire(user_id, target_id)
        if active:
            await self._respond(ctx, content="❌ There's already an active ceasefire!")
            return
        offer_id = self.db.create_peace_offer(
            user_id, target_id, offer_type="ceasefire",
            terms={"ceasefire_hours": hours}
        )
        if not offer_id:
            await self._respond(ctx, content="❌ Failed to create ceasefire proposal.")
            return
        embed = discord.Embed(
            title="⏳ Ceasefire Proposed",
            description=f"**{civ['name']}** → **{target_civ['name']}** for **{hours} hours**.",
            color=discord.Color.blue()
        )
        embed.add_field(name="Terms",
                        value=f"War continues, but no military actions for {hours}h.",
                        inline=False)
        embed.add_field(name="Respond",
                        value=f"`{target.mention}` — `.acceptpeace {offer_id}` or `.rejectpeace {offer_id}`",
                        inline=False)
        await ctx.send(embed=embed)

    @commands.hybrid_command(name='ceasefires')
    async def list_ceasefires(self, ctx):
        user_id = str(ctx.author.id)
        ceasefires = self.db.get_all_ceasefires_for_user(user_id)
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
            embed.add_field(name=f"vs **{other_name}**", value=f"Expires <t:{exp_ts}:R>", inline=False)
        await self._respond(ctx, embed=embed, ephemeral=True)

    @commands.hybrid_command(name='breakceasefire')
    @app_commands.describe(target="Civilization leader")
    async def break_ceasefire(self, ctx, target: Optional[discord.Member] = None):
        if not target:
            await self._respond(ctx, content="Usage: `.breakceasefire <user>`")
            return
        user_id = str(ctx.author.id)
        target_id = str(target.id)
        cf = self.db.get_active_ceasefire(user_id, target_id)
        if not cf:
            await self._respond(ctx, content="❌ No active ceasefire between you.")
            return
        try:
            self.db.client.collection("ceasefires").document(cf["id"]).delete()
        except Exception as e:
            logger.error(f"Failed to delete ceasefire: {e}")
        self.civ_manager.update_population(user_id, {"happiness": -15})
        embed = discord.Embed(title="💔 Ceasefire Broken",
                              description=f"**{ctx.author.display_name}** broke the ceasefire with **{target.display_name}**.",
                              color=discord.Color.red())
        embed.add_field(name="Consequence", value="-15 happiness", inline=False)
        await ctx.send(embed=embed)

    # ============================================================
    # UTIL
    # ============================================================
    def _format_terms(self, terms: dict, offer_type: str) -> str:
        lines = []
        if offer_type == "ceasefire":
            hours = terms.get("ceasefire_hours", 0)
            lines.append(f"⏳ Ceasefire for **{hours}** hours")
            return "\n".join(lines)

        gold = terms.get("gold", 0)
        food = terms.get("food", 0)
        wood = terms.get("wood", 0)
        stone = terms.get("stone", 0)
        annex = terms.get("annex_territories", [])
        hours = terms.get("ceasefire_hours", 0)

        if gold:
            lines.append(f"🪙 **{gold:,}** gold")
        if food:
            lines.append(f"🌾 **{food:,}** food")
        if wood:
            lines.append(f"🪵 **{wood:,}** wood")
        if stone:
            lines.append(f"🪨 **{stone:,}** stone")
        if annex:
            lines.append(f"🏴 Annex: **{', '.join(annex)}**")
        if hours and hours > 0:
            lines.append(f"⏳ Ceasefire: **{hours}** hours (war continues)")
        else:
            lines.append("🕊️ **Permanent peace** (war ends)")

        if not lines:
            lines.append("*No terms — pure peace.*")
        return "\n".join(lines)

    # ============================================================
    # COALITION
    # ============================================================
    @commands.hybrid_command(name='coalition')
    @app_commands.describe(target_alliance="Target alliance name")
    async def form_coalition(self, ctx, target_alliance: str = None):
        if not target_alliance:
            await self._respond(ctx, content="⚔️ **Coalition**\nUsage: `.coalition <target_alliance_name>`")
            return
        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await self._respond(ctx, content="❌ You need a civilization first!")
            return
        user_alliance_doc = None
        for doc in self.db.client.collection("alliances").where("members", "array_contains", user_id).stream():
            user_alliance_doc = doc
            break
        if not user_alliance_doc:
            await self._respond(ctx, content="❌ You must be in an alliance!")
            return
        user_alliance_data = user_alliance_doc.to_dict()
        target_alliance_data = self.db.get_alliance_by_name(target_alliance)
        if not target_alliance_data:
            await self._respond(ctx, content=f"❌ Alliance '{target_alliance}' not found!")
            return
        if user_alliance_data['name'] == target_alliance:
            await self._respond(ctx, content="❌ Cannot target your own alliance!")
            return
        user_members = user_alliance_data.get("members", [])
        target_members = target_alliance_data.get("members", [])
        success_chance = min(0.8, len(user_members) / max(1, len(target_members)))
        if random.random() < success_chance:
            embed = discord.Embed(title="⚔️ Coalition Formed!",
                                  description=f"**{user_alliance_data['name']}** formed a coalition against **{target_alliance}**!",
                                  color=discord.Color.red())
            for m in user_members + target_members:
                if m != user_id:
                    if m in user_members:
                        await ctx.send(f"<@{m}> ⚔️ Coalition formed against {target_alliance}!")
                    else:
                        await ctx.send(f"<@{m}> ⚔️ Coalition formed against your alliance!")
            await self._respond(ctx, embed=embed)
        else:
            embed = discord.Embed(title="⚔️ Coalition Failed",
                                  description=f"Failed coalition against **{target_alliance}**.",
                                  color=discord.Color.red())
            embed.add_field(name="Consequence", value="-10 happiness", inline=False)
            self.civ_manager.update_population(user_id, {"happiness": -10})
            await self._respond(ctx, embed=embed)


async def setup(bot):
    await bot.add_cog(DiplomacyCommands(bot))
