import logging
from datetime import datetime, timezone
import discord
from discord.ext import commands

from bot.commands.territory import PROVINCES, PROVINCE_TO_SUBREGION, PROVINCE_AREAS

logger = logging.getLogger(__name__)

UNION_FINE = 500


def now_iso():
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat()


class UnionCommands(commands.Cog):
    """Union and peaceful post-war annexation system."""

    def __init__(self, bot):
        self.bot = bot
        self.db = bot.db
        self.civ_manager = bot.civ_manager

    def _civ_ref(self, uid):
        return self.db.client.collection("civilizations").document(str(uid))

    def _get_civ(self, uid):
        return self.db.get_civilization(str(uid))

    def _get_union(self, uid):
        civ = self._get_civ(uid)
        return (civ or {}).get("union")

    def _all_union_members(self, uid):
        union = self._get_union(uid)
        return list((union or {}).get("members", []))

    def _set_civ_union(self, uid, union):
        self._civ_ref(uid).update({"union": union, "last_active": now_iso()})

    def _clear_civ_union(self, uid):
        self._civ_ref(uid).update({"union": None, "last_active": now_iso()})

    def _set_civ_name(self, uid, name):
        self._civ_ref(uid).update({"name": name, "last_active": now_iso()})

    def _set_land(self, uid, amount):
        self._civ_ref(uid).update({"territory.land_size": max(0, int(amount)), "last_active": now_iso()})

    def _add_resources(self, uid, changes):
        civ = self._get_civ(uid)
        if not civ:
            return
        resources = dict(civ.get("resources", {}))
        for key, value in changes.items():
            resources[key] = max(0, resources.get(key, 0) + value)
        self._civ_ref(uid).update({"resources": resources, "last_active": now_iso()})

    def _remove_gold(self, uid, amount):
        civ = self._get_civ(uid)
        gold = (civ or {}).get("resources", {}).get("gold", 0)
        if gold < amount:
            return False
        self._add_resources(uid, {"gold": -amount})
        return True

    def _proposal_ref(self, collection, proposal_id):
        return self.db.client.collection(collection).document(proposal_id)

    @commands.command(name="unite")
    async def unite(self, ctx, member: discord.Member = None, *, new_country: str = None):
        if not member or not new_country:
            await ctx.send("❌ Usage: `.unite @player <new country name>`")
            return
        uid = str(ctx.author.id)
        target = str(member.id)
        if uid == target:
            await ctx.send("❌ You can't unite with yourself.")
            return
        if not self._get_civ(uid) or not self._get_civ(target):
            await ctx.send("❌ Both players need an active civilization.")
            return
        if self._get_union(uid) or self._get_union(target):
            await ctx.send("❌ Both players must be independent before forming a new union.")
            return
        new_country = new_country.strip()
        if len(new_country) < 2 or len(new_country) > 50:
            await ctx.send("❌ The new country name must be 2-50 characters.")
            return

        ref = self.db.client.collection("union_requests").document()
        ref.set({
            "requester_id": uid,
            "target_id": target,
            "new_country": new_country,
            "status": "pending",
            "created_at": now_iso(),
        })
        await ctx.send(
            f"🤝 {member.mention}, **{ctx.author.display_name}** wants to unite with you as **{new_country}**.\n"
            f"Accept with `.acceptunite {ref.id}` or decline with `.declineunite {ref.id}`."
        )

    @commands.command(name="acceptunite")
    async def accept_unite(self, ctx, request_id: str = None):
        if not request_id:
            await ctx.send("❌ Usage: `.acceptunite <request id>`")
            return
        uid = str(ctx.author.id)
        ref = self._proposal_ref("union_requests", request_id)
        snap = ref.get()
        if not snap.exists:
            await ctx.send("❌ Union request not found.")
            return
        req = snap.to_dict()
        if req.get("status") != "pending" or req.get("target_id") != uid:
            await ctx.send("❌ This union request is not waiting for you.")
            return
        requester = req["requester_id"]
        if self._get_union(uid) or self._get_union(requester):
            await ctx.send("❌ One of you is already in a union.")
            return

        a = self._get_civ(requester)
        b = self._get_civ(uid)
        members = [requester, uid]
        land = (a.get("territory", {}).get("land_size", 0) + b.get("territory", {}).get("land_size", 0))
        union = {
            "id": ref.id,
            "name": req["new_country"],
            "members": members,
            "created_at": now_iso(),
        }
        for member_id in members:
            self._set_civ_union(member_id, union)
            self._set_civ_name(member_id, req["new_country"])
            self._set_land(member_id, land)
        ref.update({"status": "accepted", "accepted_at": now_iso()})
        await ctx.send(f"🤝 **{req['new_country']}** has been formed! {ctx.author.mention} and <@{requester}> now act as one country.")

    @commands.command(name="declineunite")
    async def decline_unite(self, ctx, request_id: str = None):
        if not request_id:
            await ctx.send("❌ Usage: `.declineunite <request id>`")
            return
        uid = str(ctx.author.id)
        ref = self._proposal_ref("union_requests", request_id)
        snap = ref.get()
        if not snap.exists or snap.to_dict().get("target_id") != uid or snap.to_dict().get("status") != "pending":
            await ctx.send("❌ Union request not found or not addressed to you.")
            return
        ref.update({"status": "declined", "resolved_at": now_iso()})
        await ctx.send("❌ Union request declined.")

    @commands.command(name="leave")
    async def leave_union(self, ctx):
        uid = str(ctx.author.id)
        union = self._get_union(uid)
        if not union:
            await ctx.send("❌ You aren't in a union.")
            return
        if not self._remove_gold(uid, UNION_FINE):
            await ctx.send(f"❌ Leaving costs **{UNION_FINE} gold**. You don't have enough gold.")
            return
        members = list(union.get("members", []))
        members = [m for m in members if m != uid]
        old_name = (self._get_civ(uid) or {}).get("original_union_name") or (self._get_civ(uid) or {}).get("name", "Independent Nation")
        self._clear_civ_union(uid)
        self._civ_ref(uid).update({"original_union_name": None})
        self._set_civ_name(uid, old_name)

        if len(members) <= 1:
            for m in members:
                self._clear_civ_union(m)
                civ = self._get_civ(m)
                restore = civ.get("original_union_name") or civ.get("name", "Independent Nation")
                self._civ_ref(m).update({"name": restore, "original_union_name": None})
        else:
            for m in members:
                self._set_civ_union(m, {**union, "members": members})
        await ctx.send(f"🚪 You left **{union['name']}** and paid the **{UNION_FINE} gold** separation fine.")

    @commands.command(name="annex")
    async def annex(self, ctx, *, country: str = None):
        if not country:
            await ctx.send("❌ Usage: `.annex <country>`")
            return
        uid = str(ctx.author.id)
        civ = self._get_civ(uid)
        if not civ:
            await ctx.send("❌ You need an active civilization.")
            return
        normalized = country.strip().lower()
        province = next((p for p in PROVINCE_TO_SUBREGION if p.lower() == normalized), None)
        if not province:
            province = next((p for p in PROVINCE_TO_SUBREGION if normalized in p.lower()), None)
        if not province:
            await ctx.send("❌ That country/province doesn't exist.")
            return
        owner = self.db.get_territory_owner(province)
        if not owner:
            await ctx.send("❌ Nobody currently owns that territory.")
            return
        if owner == uid:
            await ctx.send("❌ You already own that territory.")
            return
        target_civ = self._get_civ(owner)
        target_name = target_civ.get("name", "Unknown") if target_civ else "Unknown"

        ref = self.db.client.collection("annex_requests").document()
        ref.set({
            "requester_id": uid,
            "target_id": str(owner),
            "territory": province,
            "status": "pending",
            "created_at": now_iso(),
        })
        await ctx.send(
            f"📜 **Annexation request:** {ctx.author.mention} wants **{province}** from **{target_name}**.\n"
            f"<@{owner}>, accept with `.acceptannex {ref.id}` or decline with `.declineannex {ref.id}`."
        )

    @commands.command(name="acceptannex")
    async def accept_annex(self, ctx, request_id: str = None):
        if not request_id:
            await ctx.send("❌ Usage: `.acceptannex <request id>`")
            return
        uid = str(ctx.author.id)
        ref = self._proposal_ref("annex_requests", request_id)
        snap = ref.get()
        if not snap.exists:
            await ctx.send("❌ Annex request not found.")
            return
        req = snap.to_dict()
        if req.get("status") != "pending" or req.get("target_id") != uid:
            await ctx.send("❌ This annex request is not waiting for you.")
            return
        territory = req.get("territory")
        if self.db.get_territory_owner(territory) != uid:
            await ctx.send("❌ You no longer own that territory, so the request expired.")
            ref.update({"status": "expired", "resolved_at": now_iso()})
            return
        requester = req["requester_id"]
        target_civ = self._get_civ(uid)
        requester_civ = self._get_civ(requester)
        if not target_civ or not requester_civ:
            await ctx.send("❌ One of the civilizations no longer exists.")
            return

        # Transfer the territory using the existing territory database method.
        if not self.db.conquer_territory(requester, uid, territory):
            await ctx.send("❌ The territory transfer failed. Nothing was changed.")
            return
        area = PROVINCE_AREAS.get(territory, 1000)
        self.civ_manager.update_territory(requester, {"land_size": area})
        self.civ_manager.update_territory(uid, {"land_size": -area})
        ref.update({"status": "accepted", "resolved_at": now_iso()})
        await ctx.send(f"🗺️ **{territory}** has been peacefully annexed by <@{requester}>.")

    @commands.command(name="declineannex")
    async def decline_annex(self, ctx, request_id: str = None):
        if not request_id:
            await ctx.send("❌ Usage: `.declineannex <request id>`")
            return
        uid = str(ctx.author.id)
        ref = self._proposal_ref("annex_requests", request_id)
        snap = ref.get()
        if not snap.exists:
            await ctx.send("❌ Annex request not found.")
            return
        req = snap.to_dict()
        if req.get("status") != "pending" or req.get("target_id") != uid:
            await ctx.send("❌ This annex request is not waiting for you.")
            return
        ref.update({"status": "declined", "resolved_at": now_iso()})
        await ctx.send("❌ Annexation request declined.")


async def setup(bot):
    await bot.add_cog(UnionCommands(bot))
