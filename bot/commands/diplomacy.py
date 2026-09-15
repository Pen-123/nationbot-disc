import random
import discord
from discord.ext import commands
from discord import app_commands
import logging
from datetime import datetime, timedelta
from typing import Optional, List

from bot import config

logger = logging.getLogger(__name__)


class DiplomacyCommands(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.db = bot.db
        self.civ_manager = bot.civ_manager
        # Pending proposals now live in Firestore so they survive restarts.

    # ---------- Response helper ----------
    async def _respond(self, ctx, content: str = None, embed: discord.Embed = None, ephemeral: bool = False):
        """Send a response. Slash commands honour ephemeral; prefix commands stay public."""
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
                logger.exception("Interaction response failed; falling back to ctx.send")
        # Prefix / fallback
        await ctx.send(content=content, embed=embed)

    # ---------- Autocomplete ----------
    async def _alliance_id_autocomplete(self, interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
        uid = str(interaction.user.id)
        candidates = []
        for proposal in self.db.get_alliance_proposals_for_user(uid):
            label = f"{proposal['id']} - {proposal.get('alliance_name','?')}"
            if current.lower() in label.lower():
                candidates.append(app_commands.Choice(name=label[:100], value=proposal["id"]))
        return candidates[:25]

    async def _trade_id_autocomplete(self, interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
        uid = str(interaction.user.id)
        candidates = []
        for trade in self.db.get_trade_proposals_for_user(uid):
            label = (f"{trade['id']} - {trade.get('offer_amount','?')} {trade.get('offer_resource','?')} "
                     f"for {trade.get('request_amount','?')} {trade.get('request_resource','?')}")
            if current.lower() in label.lower():
                candidates.append(app_commands.Choice(name=label[:100], value=trade["id"]))
        return candidates[:25]

    # ---------- Cooldown ----------
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

    # =================================================================
    #                          ALLIANCES
    # =================================================================

    @commands.hybrid_command(name='ally')
    @app_commands.describe(target="Civilization leader to ally with", alliance_name="Name of the alliance")
    async def propose_alliance(self, ctx, target: Optional[discord.Member] = None, alliance_name: Optional[str] = None):
        """Propose an alliance with another civilization"""
        if not target or not alliance_name:
            await self._respond(
                ctx,
                content=("🤝 **Alliance Proposal**\n"
                         "Usage: `.ally <user> <alliance_name>` or `/ally`\n"
                         "Propose a mutual defense pact with another civilization.")
            )
            return

        ok, msg = self._check_cooldown(ctx, "ally")
        if not ok:
            await self._respond(ctx, content=msg, ephemeral=True)
            return

        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await self._respond(ctx, content="❌ You need to start a civilization first! Use `.start <name>`")
            return

        target_id = str(target.id)
        if target_id == user_id:
            await self._respond(ctx, content="❌ You cannot ally with yourself!")
            return

        target_civ = self.civ_manager.get_civilization(target_id)
        if not target_civ:
            await self._respond(ctx, content="❌ Target user doesn't have a civilization!")
            return

        # Already at war?
        for war in self.db.get_wars(status="ongoing"):
            a = war.get("attacker_id"); d = war.get("defender_id")
            if (a == user_id and d == target_id) or (a == target_id and d == user_id):
                await self._respond(ctx, content="❌ You cannot ally with a civilization you are at war with!")
                return

        # Already allied?
        existing = self.db.find_alliances_containing_both(user_id, target_id)
        if existing:
            await self._respond(ctx, content="❌ One of you is already in an alliance together!")
            return

        alliance_id = str(random.randint(100000, 999999))
        self.db.save_alliance_proposal(alliance_id, {
            "proposer_id": user_id,
            "target_id": target_id,
            "alliance_name": alliance_name,
            "expires": (datetime.utcnow() + timedelta(minutes=30)).isoformat(),
            "created_at": datetime.utcnow().isoformat(),
        })

        embed = discord.Embed(
            title="🤝 Alliance Proposal Received!",
            description=f"From **{civ['name']}** (led by {ctx.author.name})",
            color=discord.Color.blue()
        )
        embed.add_field(
            name="Proposed Alliance",
            value=f"Alliance Name: **{alliance_name}**\nBenefits: Mutual defense, resource sharing, coordinated actions",
            inline=False
        )
        embed.add_field(
            name="How to Respond",
            value=(f"Use `.acceptally {alliance_id}` or `/acceptally`\n"
                   f"Or `.rejectally {alliance_id}` or `/rejectally`\n"
                   "Proposal expires in 30 minutes."),
            inline=False
        )

        await ctx.send(f"<@{target_id}>", embed=embed)
        await self._respond(ctx, content=f"🤝 **Alliance Proposed!** Your proposal for **{alliance_name}** has been sent to **{target_civ['name']}**.")
        self.db.log_event(user_id, "alliance_proposal", "Alliance Proposed", f"Proposed alliance '{alliance_name}' to {target_civ['name']}")

    @commands.hybrid_command(name='acceptally')
    @app_commands.describe(alliance_id="Pending alliance proposal ID")
    @app_commands.autocomplete(alliance_id=_alliance_id_autocomplete)
    async def accept_alliance(self, ctx, alliance_id: str = None):
        if not alliance_id:
            await self._respond(ctx, content="Usage: `.acceptally <id>` or `/acceptally <id>`")
            return
        user_id = str(ctx.author.id)
        proposal = self.db.get_alliance_proposal(alliance_id)
        if not proposal:
            await self._respond(ctx, content="❌ Invalid or expired alliance ID!")
            return

        if user_id != proposal["target_id"]:
            await self._respond(ctx, content="❌ This alliance proposal isn't for you!")
            return

        ok, msg = self._check_cooldown(ctx, "acceptally")
        if not ok:
            await self._respond(ctx, content=msg, ephemeral=True)
            return

        try:
            success = self.db.create_alliance(proposal["alliance_name"], proposal["proposer_id"], description="")
            if not success:
                await self._respond(ctx, content="❌ Failed to create alliance. Please try again.")
                return
            alliance = self.db.get_alliance_by_name(proposal["alliance_name"])
            if not alliance:
                await self._respond(ctx, content="❌ Alliance creation succeeded but not found. Contact admin.")
                return
            self.db.add_alliance_member(alliance["id"], user_id)

            embed = discord.Embed(
                title="🤝 Alliance Formed!",
                description=f"**{proposal['alliance_name']}** has been established!",
                color=discord.Color.green()
            )
            embed.add_field(
                name="Alliance Benefits",
                value="• Mutual defense pact\n• Resource sharing available\n• Coordinated military actions\n• Trade bonuses",
                inline=False
            )
            await self._respond(ctx, embed=embed)
            await ctx.send(f"<@{proposal['proposer_id']}> 🤝 **Alliance Accepted!** Your proposal for **{proposal['alliance_name']}** has been accepted!")

            self.db.log_event(proposal["proposer_id"], "alliance", "Alliance Formed", f"Created alliance '{proposal['alliance_name']}'")
            self.db.log_event(user_id, "alliance", "Alliance Formed", f"Joined alliance '{proposal['alliance_name']}'")
            self.db.delete_alliance_proposal(alliance_id)
        except Exception as e:
            logger.error(f"Error creating alliance: {e}")
            await self._respond(ctx, content="❌ Failed to form alliance. Please try again.")

    @commands.hybrid_command(name='rejectally')
    @app_commands.describe(alliance_id="Pending alliance proposal ID")
    @app_commands.autocomplete(alliance_id=_alliance_id_autocomplete)
    async def reject_alliance(self, ctx, alliance_id: str = None):
        if not alliance_id:
            await self._respond(ctx, content="Usage: `.rejectally <id>` or `/rejectally <id>`")
            return
        user_id = str(ctx.author.id)
        proposal = self.db.get_alliance_proposal(alliance_id)
        if not proposal:
            await self._respond(ctx, content="❌ Invalid or expired alliance ID!")
            return
        if user_id != proposal["target_id"]:
            await self._respond(ctx, content="❌ This alliance proposal isn't for you!")
            return

        ok, msg = self._check_cooldown(ctx, "rejectally")
        if not ok:
            await self._respond(ctx, content=msg, ephemeral=True)
            return

        await ctx.send(f"<@{proposal['proposer_id']}> 🤝 **Alliance Rejected!** Your proposal for **{proposal['alliance_name']}** has been rejected.")
        await self._respond(ctx, content="🤝 **Alliance Rejected!** You've declined the proposal.")

        self.db.log_event(user_id, "alliance_reject", "Alliance Rejected", f"Rejected alliance {alliance_id}")
        self.db.log_event(proposal["proposer_id"], "alliance_reject", "Alliance Rejected", f"Alliance {alliance_id} rejected by target")
        self.db.delete_alliance_proposal(alliance_id)

    @commands.hybrid_command(name='break')
    async def break_alliance(self, ctx):
        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await self._respond(ctx, content="❌ You need to start a civilization first! Use `.start <name>`")
            return

        ok, msg = self._check_cooldown(ctx, "break")
        if not ok:
            await self._respond(ctx, content=msg, ephemeral=True)
            return

        alliance_doc = None
        for doc in self.db.client.collection("alliances").where("members", "array_contains", user_id).stream():
            alliance_doc = doc
            break

        if not alliance_doc:
            await self._respond(ctx, content="❌ You are not currently in an alliance!")
            return

        alliance_data = alliance_doc.to_dict()
        members = alliance_data.get("members", [])

        if len(members) <= 2:
            alliance_doc.reference.delete()
        else:
            members.remove(user_id)
            alliance_doc.reference.update({"members": members})

        self.civ_manager.update_population(user_id, {"happiness": -10})

        embed = discord.Embed(
            title="💔 Alliance Broken",
            description=f"Your civilization has left the **{alliance_data['name']}** alliance.",
            color=discord.Color.red()
        )
        embed.add_field(name="Consequence", value="Breaking diplomatic ties has upset your people. (-10 happiness)", inline=False)
        await self._respond(ctx, embed=embed)

        for member_id in members:
            if member_id != user_id:
                await ctx.send(f"<@{member_id}> 💔 **Alliance Update**: {civ['name']} has left the **{alliance_data['name']}** alliance.")

        self.db.log_event(user_id, "alliance_break", "Alliance Broken", f"Left the {alliance_data['name']} alliance")

    # =================================================================
    #                          RESOURCE TRANSFER
    # =================================================================

    @commands.hybrid_command(name='send')
    @app_commands.describe(target="Recipient", resource_type="Resource to transfer", amount="Amount to transfer")
    @app_commands.choices(resource_type=[
        app_commands.Choice(name="gold", value="gold"),
        app_commands.Choice(name="food", value="food"),
        app_commands.Choice(name="wood", value="wood"),
        app_commands.Choice(name="stone", value="stone"),
    ])
    async def send_resources(self, ctx, target: Optional[discord.Member] = None,
                             resource_type: Optional[str] = None, amount: Optional[int] = None):
        if not target or not resource_type or amount is None:
            await self._respond(ctx, content="📦 **Resource Transfer**\nUsage: `.send <user> <resource> <amount>` or `/send`\nResources: gold, food, wood, stone")
            return

        if resource_type not in ['gold', 'food', 'wood', 'stone']:
            await self._respond(ctx, content="❌ Invalid resource type! Choose from: gold, food, wood, stone")
            return
        if amount < 1:
            await self._respond(ctx, content="❌ Amount must be positive!")
            return

        ok, msg = self._check_cooldown(ctx, "send")
        if not ok:
            await self._respond(ctx, content=msg, ephemeral=True)
            return

        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await self._respond(ctx, content="❌ You need to start a civilization first! Use `.start <name>`")
            return

        target_id = str(target.id)
        target_civ = self.civ_manager.get_civilization(target_id)
        if not target_civ:
            await self._respond(ctx, content="❌ Target user doesn't have a civilization!")
            return

        if not self.civ_manager.can_afford(user_id, {resource_type: amount}):
            await self._respond(ctx, content=f"❌ You don't have {amount} {resource_type}!")
            return

        is_allied = bool(self.db.find_alliances_containing_both(user_id, target_id))
        transfer_efficiency = 0.95 if is_allied else 0.9

        received_amount = int(amount * transfer_efficiency)
        self.civ_manager.spend_resources(user_id, {resource_type: amount})
        self.civ_manager.update_resources(target_id, {resource_type: received_amount})

        resource_icons = {"gold": "🪙", "food": "🌾", "wood": "🪵", "stone": "🪨"}
        embed = discord.Embed(
            title="📦 Resources Sent",
            description=f"Successfully sent resources to **{target_civ['name']}**!",
            color=discord.Color.blue()
        )
        embed.add_field(
            name="Transfer Details",
            value=(f"{resource_icons[resource_type]} Sent: {amount} {resource_type.capitalize()}\n"
                   f"{resource_icons[resource_type]} Received: {received_amount} {resource_type.capitalize()}\n"
                   f"📊 Efficiency: {int(transfer_efficiency * 100)}%"),
            inline=False
        )
        if is_allied:
            embed.add_field(name="Alliance Bonus", value="Higher transfer efficiency due to alliance!", inline=False)

        # Slash: silent confirmation + DM target
        if ctx.interaction is not None:
            await self._respond(ctx, embed=embed, ephemeral=True)
            try:
                target_user = await self.bot.fetch_user(int(target_id))
                await target_user.send(f"📦 **Resources Received!** {civ['name']} has sent you {received_amount} {resource_type}!")
            except Exception:
                pass
        else:
            await ctx.send(embed=embed)
            await ctx.send(f"<@{target_id}> 📦 **Resources Received!** {civ['name']} has sent you {received_amount} {resource_type}!")

        self.db.log_event(user_id, "resource_transfer", "Resources Sent", f"Sent {amount} {resource_type} to {target_civ['name']}")
        self.db.log_event(target_id, "resource_transfer", "Resources Received", f"Received {received_amount} {resource_type} from {civ['name']}")

    # =================================================================
    #                          TRADES
    # =================================================================

    @commands.hybrid_command(name='trade')
    @app_commands.describe(
        target="Trade partner",
        offer_resource="Resource you're offering",
        offer_amount="Amount you're offering",
        request_resource="Resource you want",
        request_amount="Amount you want"
    )
    @app_commands.choices(
        offer_resource=[
            app_commands.Choice(name="gold", value="gold"),
            app_commands.Choice(name="food", value="food"),
            app_commands.Choice(name="wood", value="wood"),
            app_commands.Choice(name="stone", value="stone"),
        ],
        request_resource=[
            app_commands.Choice(name="gold", value="gold"),
            app_commands.Choice(name="food", value="food"),
            app_commands.Choice(name="wood", value="wood"),
            app_commands.Choice(name="stone", value="stone"),
        ]
    )
    async def propose_trade(self, ctx, target: Optional[discord.Member] = None,
                            offer_resource: Optional[str] = None, offer_amount: Optional[int] = None,
                            request_resource: Optional[str] = None, request_amount: Optional[int] = None):
        if not all([target, offer_resource, offer_amount, request_resource, request_amount]):
            await self._respond(ctx, content="💰 **Resource Trading**\nUsage: `.trade <user> <offer_resource> <offer_amount> <request_resource> <request_amount>`\nExample: `.trade @user gold 100 food 200`")
            return

        valid_resources = ['gold', 'food', 'wood', 'stone']
        if offer_resource not in valid_resources or request_resource not in valid_resources:
            await self._respond(ctx, content=f"❌ Invalid resource! Choose from: {', '.join(valid_resources)}")
            return

        ok, msg = self._check_cooldown(ctx, "trade")
        if not ok:
            await self._respond(ctx, content=msg, ephemeral=True)
            return

        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await self._respond(ctx, content="❌ You need to start a civilization first! Use `.start <name>`")
            return

        target_id = str(target.id)
        target_civ = self.civ_manager.get_civilization(target_id)
        if not target_civ:
            await self._respond(ctx, content="❌ Target user doesn't have a civilization!")
            return

        if not self.civ_manager.can_afford(user_id, {offer_resource: offer_amount}):
            await self._respond(ctx, content=f"❌ You don't have {offer_amount} {offer_resource} to offer!")
            return

        trade_id = str(random.randint(100000, 999999))
        self.db.save_trade_proposal(trade_id, {
            "proposer_id": user_id,
            "target_id": target_id,
            "offer_resource": offer_resource,
            "offer_amount": offer_amount,
            "request_resource": request_resource,
            "request_amount": request_amount,
            "expires": (datetime.utcnow() + timedelta(minutes=30)).isoformat(),
            "created_at": datetime.utcnow().isoformat(),
        })

        resource_icons = {"gold": "🪙", "food": "🌾", "wood": "🪵", "stone": "🪨"}
        embed = discord.Embed(
            title="💰 Trade Proposal Received!",
            description=f"From **{civ['name']}** (led by {ctx.author.name})",
            color=discord.Color.blue()
        )
        embed.add_field(
            name="Proposed Trade",
            value=(f"They offer: {resource_icons[offer_resource]} {offer_amount} {offer_resource.capitalize()}\n"
                   f"They request: {resource_icons[request_resource]} {request_amount} {request_resource.capitalize()}"),
            inline=False
        )
        embed.add_field(
            name="How to Respond",
            value=(f"Use `.accepttrade {trade_id}` or `/accepttrade`\n"
                   f"Or `.rejecttrade {trade_id}` or `/rejecttrade`\n"
                   "Proposal expires in 30 minutes."),
            inline=False
        )
        await ctx.send(f"<@{target_id}>", embed=embed)
        await self._respond(ctx, content=f"💰 **Trade Proposed!** Your offer has been sent to **{target_civ['name']}**.")
        self.db.log_event(user_id, "trade_proposal", "Trade Proposed",
                          f"Proposed trade to {target_civ['name']}: {offer_amount} {offer_resource} for {request_amount} {request_resource}")

    @commands.hybrid_command(name='accepttrade')
    @app_commands.describe(trade_id="Pending trade proposal ID")
    @app_commands.autocomplete(trade_id=_trade_id_autocomplete)
    async def accept_trade(self, ctx, trade_id: str = None):
        if not trade_id:
            await self._respond(ctx, content="Usage: `.accepttrade <id>` or `/accepttrade <id>`")
            return
        user_id = str(ctx.author.id)
        trade = self.db.get_trade_proposal(trade_id)
        if not trade:
            await self._respond(ctx, content="❌ Invalid or expired trade ID!")
            return
        if user_id != trade["target_id"]:
            await self._respond(ctx, content="❌ This trade proposal isn't for you!")
            return

        ok, msg = self._check_cooldown(ctx, "accepttrade")
        if not ok:
            await self._respond(ctx, content=msg, ephemeral=True)
            return

        if not self.civ_manager.can_afford(trade["proposer_id"], {trade["offer_resource"]: trade["offer_amount"]}):
            await self._respond(ctx, content="❌ The proposer no longer has the offered resources!")
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

        await ctx.send(f"<@{trade['proposer_id']}> 💰 **Trade Accepted!** Your trade proposal has been accepted!")
        await self._respond(ctx, content="💰 **Trade Accepted!** The exchange has been completed.")

        self.db.log_event(user_id, "trade_accept", "Trade Accepted", f"Accepted trade {trade_id}")
        self.db.log_event(trade["proposer_id"], "trade_accept", "Trade Accepted", f"Trade {trade_id} accepted by target")
        self.db.delete_trade_proposal(trade_id)

    @commands.hybrid_command(name='rejecttrade')
    @app_commands.describe(trade_id="Pending trade proposal ID")
    @app_commands.autocomplete(trade_id=_trade_id_autocomplete)
    async def reject_trade(self, ctx, trade_id: str = None):
        if not trade_id:
            await self._respond(ctx, content="Usage: `.rejecttrade <id>` or `/rejecttrade <id>`")
            return
        user_id = str(ctx.author.id)
        trade = self.db.get_trade_proposal(trade_id)
        if not trade:
            await self._respond(ctx, content="❌ Invalid or expired trade ID!")
            return
        if user_id != trade["target_id"]:
            await self._respond(ctx, content="❌ This trade proposal isn't for you!")
            return

        ok, msg = self._check_cooldown(ctx, "rejecttrade")
        if not ok:
            await self._respond(ctx, content=msg, ephemeral=True)
            return

        await ctx.send(f"<@{trade['proposer_id']}> 💰 **Trade Rejected!** Your trade proposal has been rejected.")
        await self._respond(ctx, content="💰 **Trade Rejected!** You've declined the proposal.")

        self.db.log_event(user_id, "trade_reject", "Trade Rejected", f"Rejected trade {trade_id}")
        self.db.log_event(trade["proposer_id"], "trade_reject", "Trade Rejected", f"Trade {trade_id} rejected by target")
        self.db.delete_trade_proposal(trade_id)

    # =================================================================
    #                          MAIL
    # =================================================================

    @commands.hybrid_command(name='mail')
    @app_commands.describe(target="Message recipient", message="Diplomatic message")
    async def send_diplomatic_message(self, ctx, target: Optional[discord.Member] = None, *, message: Optional[str] = None):
        if not target or not message:
            await self._respond(ctx, content="📜 **Diplomatic Mail**\nUsage: `.mail <user> <message>` or `/mail`\nSend diplomatic communications to other civilizations.")
            return

        if len(message) > 500:
            await self._respond(ctx, content="❌ Message too long! Maximum 500 characters.")
            return

        ok, msg = self._check_cooldown(ctx, "mail")
        if not ok:
            await self._respond(ctx, content=msg, ephemeral=True)
            return

        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await self._respond(ctx, content="❌ You need to start a civilization first! Use `.start <name>`")
            return

        target_id = str(target.id)
        target_civ = self.civ_manager.get_civilization(target_id)
        if not target_civ:
            await self._respond(ctx, content="❌ Target user doesn't have a civilization!")
            return

        try:
            success = self.db.send_message(user_id, target_id, message)
            if not success:
                await self._respond(ctx, content="❌ Failed to send message. Please try again.")
                return
        except Exception as e:
            logger.error(f"Error saving message: {e}")
            await self._respond(ctx, content="❌ Failed to send message. Please try again.")
            return

        # Slash: silent, DM target
        if ctx.interaction is not None:
            await self._respond(ctx, content=f"📜 **Message sent to {target_civ['name']}.** Only you can see this.", ephemeral=True)
            try:
                target_user = await self.bot.fetch_user(int(target_id))
                await target_user.send(f"📜 **You've got mail from {civ['name']}!** Check your inbox with `.inbox` or `/inbox`.")
            except Exception:
                pass
        else:
            await ctx.send(f"<@{target_id}> 📜 You've got mail from {civ['name']}! Check your `.inbox` to read it.")
            await ctx.send("📜 **Sent diplomatic message**")

        self.db.log_event(user_id, "diplomatic_message", "Message Sent", f"Sent message to {target_civ['name']}")

    @commands.hybrid_command(name='inbox')
    async def check_inbox(self, ctx):
        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await self._respond(ctx, content="❌ You need to start a civilization first! Use `.start <name>`")
            return

        ok, msg = self._check_cooldown(ctx, "inbox")
        if not ok:
            await self._respond(ctx, content=msg, ephemeral=True)
            return

        embed = discord.Embed(
            title="📬 Inbox",
            description=f"Pending proposals and messages for **{civ['name']}**",
            color=discord.Color.blue()
        )

        # Alliance proposals
        alliance_proposals = []
        for proposal in self.db.get_alliance_proposals_for_user(user_id):
            proposer_civ = self.civ_manager.get_civilization(proposal["proposer_id"])
            if proposer_civ:
                exp_raw = proposal.get("expires")
                exp_dt = datetime.fromisoformat(exp_raw) if isinstance(exp_raw, str) else exp_raw
                exp_ts = int(exp_dt.timestamp()) if exp_dt else 0
                alliance_proposals.append(
                    f"**Alliance ID**: {proposal['id']}\n"
                    f"From: **{proposer_civ['name']}**\n"
                    f"Alliance Name: **{proposal['alliance_name']}**\n"
                    f"Respond with: `.acceptally {proposal['id']}` or `.rejectally {proposal['id']}`\n"
                    f"Expires: <t:{exp_ts}:R>"
                )

        # Trade proposals
        trade_proposals = []
        resource_icons = {"gold": "🪙", "food": "🌾", "wood": "🪵", "stone": "🪨"}
        for trade in self.db.get_trade_proposals_for_user(user_id):
            proposer_civ = self.civ_manager.get_civilization(trade["proposer_id"])
            if proposer_civ:
                exp_raw = trade.get("expires")
                exp_dt = datetime.fromisoformat(exp_raw) if isinstance(exp_raw, str) else exp_raw
                exp_ts = int(exp_dt.timestamp()) if exp_dt else 0
                trade_proposals.append(
                    f"**Trade ID**: {trade['id']}\n"
                    f"From: **{proposer_civ['name']}**\n"
                    f"Offers: {resource_icons[trade['offer_resource']]} {trade['offer_amount']} {trade['offer_resource'].capitalize()}\n"
                    f"Requests: {resource_icons[trade['request_resource']]} {trade['request_amount']} {trade['request_resource'].capitalize()}\n"
                    f"Respond with: `.accepttrade {trade['id']}` or `.rejecttrade {trade['id']}`\n"
                    f"Expires: <t:{exp_ts}:R>"
                )

        # Diplomatic messages
        diplomatic_messages = []
        try:
            messages = self.db.get_messages(user_id)
            for m in messages:
                sender_civ = self.civ_manager.get_civilization(m['sender_id'])
                if sender_civ:
                    timestamp = m['created_at']
                    if isinstance(timestamp, str):
                        timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    diplomatic_messages.append(
                        f"**From**: {sender_civ['name']}\n"
                        f"**Message**: {m['message']}\n"
                        f"**Received**: <t:{int(timestamp.timestamp())}:R>"
                    )
        except Exception as e:
            logger.error(f"Error fetching messages: {e}")
            diplomatic_messages.append("⚠️ Could not load messages")

        embed.add_field(
            name="Alliance Proposals",
            value="\n\n".join(alliance_proposals) if alliance_proposals else "No pending alliance proposals.",
            inline=False
        )
        embed.add_field(
            name="Trade Proposals",
            value="\n\n".join(trade_proposals) if trade_proposals else "No pending trade proposals.",
            inline=False
        )
        embed.add_field(
            name="Diplomatic Messages",
            value="\n\n".join(diplomatic_messages) if diplomatic_messages else "No diplomatic messages received.",
            inline=False
        )

        # Inbox is private — always ephemeral on slash
        await self._respond(ctx, embed=embed, ephemeral=True)

    # =================================================================
    #                          COALITION
    # =================================================================

    @commands.hybrid_command(name='coalition')
    @app_commands.describe(target_alliance="Target alliance name")
    async def form_coalition(self, ctx, target_alliance: str = None):
        if not target_alliance:
            await self._respond(ctx, content="⚔️ **Coalition Warfare**\nUsage: `.coalition <target_alliance_name>`\nForm a coalition to declare war on another alliance.")
            return

        ok, msg = self._check_cooldown(ctx, "coalition")
        if not ok:
            await self._respond(ctx, content=msg, ephemeral=True)
            return

        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await self._respond(ctx, content="❌ You need to start a civilization first! Use `.start <name>`")
            return

        user_alliance_doc = None
        for doc in self.db.client.collection("alliances").where("members", "array_contains", user_id).stream():
            user_alliance_doc = doc
            break

        if not user_alliance_doc:
            await self._respond(ctx, content="❌ You must be in an alliance to form a coalition!")
            return

        user_alliance_data = user_alliance_doc.to_dict()
        target_alliance_data = self.db.get_alliance_by_name(target_alliance)
        if not target_alliance_data:
            await self._respond(ctx, content=f"❌ Alliance '{target_alliance}' not found!")
            return
        if user_alliance_data['name'] == target_alliance:
            await self._respond(ctx, content="❌ You cannot form a coalition against your own alliance!")
            return

        user_members = user_alliance_data.get("members", [])
        target_members = target_alliance_data.get("members", [])
        success_chance = min(0.8, len(user_members) / max(1, len(target_members)))

        if random.random() < success_chance:
            embed = discord.Embed(
                title="⚔️ Coalition Formed!",
                description=f"**{user_alliance_data['name']}** has formed a coalition against **{target_alliance}**!",
                color=discord.Color.red()
            )
            embed.add_field(
                name="Coalition Effects",
                value="• All members can attack target alliance\n• Reduced diplomatic penalties\n• Coordinated military bonuses",
                inline=False
            )
            all_affected = user_members + target_members
            for member_id in all_affected:
                if member_id != user_id:
                    if member_id in user_members:
                        await ctx.send(f"<@{member_id}> ⚔️ **Coalition Formed!** Your alliance has formed a coalition against {target_alliance}!")
                    else:
                        await ctx.send(f"<@{member_id}> ⚔️ **Coalition Against You!** {user_alliance_data['name']} has formed a coalition against your alliance!")
            await self._respond(ctx, embed=embed)
        else:
            embed = discord.Embed(
                title="⚔️ Coalition Failed",
                description=f"Your attempt to form a coalition against **{target_alliance}** has failed.",
                color=discord.Color.red()
            )
            embed.add_field(name="Consequence", value="Failed diplomacy has consequences. (-10 happiness)", inline=False)
            self.civ_manager.update_population(user_id, {"happiness": -10})
            await self._respond(ctx, embed=embed)

        self.db.log_event(user_id, "coalition_failed", "Coalition Failed", f"Failed coalition against {target_alliance}")


async def setup(bot):
    await bot.add_cog(DiplomacyCommands(bot))
