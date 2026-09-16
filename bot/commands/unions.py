import logging
from datetime import datetime, timezone
import discord
from discord.ext import commands
from bot.commands.territory import PROVINCE_TO_SUBREGION, PROVINCE_AREAS

logger = logging.getLogger(__name__)
UNION_FINE = 500

def now_iso():
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat()

class UnionCommands(commands.Cog):
    def __init__(self, bot):
        self.bot, self.db, self.civ_manager = bot, bot.db, bot.civ_manager

    def _ref(self, uid): return self.db.client.collection("civilizations").document(str(uid))
    def _civ(self, uid): return self.db.get_civilization(str(uid))
    def _union(self, uid): return (self._civ(uid) or {}).get("union")
    def _set_land(self, uid, n): self._ref(uid).update({"territory.land_size": max(0, int(n)), "last_active": now_iso()})
    def _remove_gold(self, uid, amount):
        civ = self._civ(uid) or {}
        resources = dict(civ.get("resources", {}))
        if resources.get("gold", 0) < amount:
            return False
        resources["gold"] -= amount
        self._ref(uid).update({"resources": resources, "last_active": now_iso()})
        return True
    def _own_land(self, uid):
        return sum(PROVINCE_AREAS.get(p, 1000) for p in self.db.get_player_territories(uid))
    def _proposal(self, collection, rid): return self.db.client.collection(collection).document(rid)

    @commands.command(name="unite")
    async def unite(self, ctx, member: discord.Member = None, *, new_country: str = None):
        if not member or not new_country:
            return await ctx.send("❌ Usage: `.unite @player <new country name>`")
        uid, target = str(ctx.author.id), str(member.id)
        if uid == target: return await ctx.send("❌ You can't unite with yourself.")
        if not self._civ(uid) or not self._civ(target): return await ctx.send("❌ Both players need an active civilization.")
        if self._union(uid) or self._union(target): return await ctx.send("❌ Both players must be independent before forming a new union.")
        new_country = new_country.strip()
        if not 2 <= len(new_country) <= 50: return await ctx.send("❌ The new country name must be 2-50 characters.")
        ref = self.db.client.collection("union_requests").document()
        ref.set({"requester_id": uid, "target_id": target, "new_country": new_country, "status": "pending", "created_at": now_iso()})
        await ctx.send(f"🤝 {member.mention}, **{ctx.author.display_name}** wants to unite with you as **{new_country}**.\nAccept with `.acceptunite {ref.id}` or decline with `.declineunite {ref.id}`.")

    @commands.command(name="acceptunite")
    async def accept_unite(self, ctx, request_id: str = None):
        if not request_id: return await ctx.send("❌ Usage: `.acceptunite <request id>`")
        uid = str(ctx.author.id); ref = self._proposal("union_requests", request_id); snap = ref.get()
        if not snap.exists: return await ctx.send("❌ Union request not found.")
        req = snap.to_dict(); requester = req.get("requester_id")
        if req.get("status") != "pending" or req.get("target_id") != uid: return await ctx.send("❌ This union request is not waiting for you.")
        if self._union(uid) or self._union(requester): return await ctx.send("❌ One of you is already in a union.")
        a, b = self._civ(requester), self._civ(uid)
        if not a or not b: return await ctx.send("❌ One of the civilizations no longer exists.")
        members = [requester, uid]
        land = a.get("territory", {}).get("land_size", 0) + b.get("territory", {}).get("land_size", 0)
        union = {"id": ref.id, "name": req["new_country"], "members": members, "created_at": now_iso()}
        for mid, civ in ((requester, a), (uid, b)):
            self._ref(mid).update({"union": union, "original_union_name": civ.get("name", "Independent Nation"), "name": req["new_country"], "territory.land_size": int(land), "last_active": now_iso()})
        ref.update({"status": "accepted", "accepted_at": now_iso()})
        await ctx.send(f"🤝 **{req['new_country']}** has been formed! {ctx.author.mention} and <@{requester}> now act as one country.")

    @commands.command(name="declineunite")
    async def decline_unite(self, ctx, request_id: str = None):
        if not request_id: return await ctx.send("❌ Usage: `.declineunite <request id>`")
        ref = self._proposal("union_requests", request_id); snap = ref.get()
        if not snap.exists: return await ctx.send("❌ Union request not found.")
        req = snap.to_dict()
        if req.get("status") != "pending" or req.get("target_id") != str(ctx.author.id): return await ctx.send("❌ Union request not found or not addressed to you.")
        ref.update({"status": "declined", "resolved_at": now_iso()}); await ctx.send("❌ Union request declined.")

    @commands.command(name="leave")
    async def leave_union(self, ctx):
        uid = str(ctx.author.id); union = self._union(uid)
        if not union: return await ctx.send("❌ You aren't in a union.")
        if not self._remove_gold(uid, UNION_FINE): return await ctx.send(f"❌ Leaving costs **{UNION_FINE} gold**. You don't have enough gold.")
        members = [m for m in union.get("members", []) if m != uid]
        civ = self._civ(uid) or {}; old_name = civ.get("original_union_name") or "Independent Nation"
        self._ref(uid).update({"union": None, "name": old_name, "original_union_name": None, "territory.land_size": int(self._own_land(uid)), "last_active": now_iso()})
        for m in members:
            mciv = self._civ(m) or {}
            if len(members) == 1:
                restore = mciv.get("original_union_name") or "Independent Nation"
                self._ref(m).update({"union": None, "name": restore, "original_union_name": None, "territory.land_size": int(self._own_land(m)), "last_active": now_iso()})
            else:
                self._ref(m).update({"union": {**union, "members": members}, "territory.land_size": int(self._own_land(m)), "last_active": now_iso()})
        await ctx.send(f"🚪 You left **{union['name']}** and paid the **{UNION_FINE} gold** separation fine.")

    @commands.command(name="annex")
    async def annex(self, ctx, *, country: str = None):
        if not country: return await ctx.send("❌ Usage: `.annex <country>`")
        uid = str(ctx.author.id)
        if not self._civ(uid): return await ctx.send("❌ You need an active civilization.")
        q = country.strip().lower(); province = next((p for p in PROVINCE_TO_SUBREGION if p.lower() == q), None)
        if not province: province = next((p for p in PROVINCE_TO_SUBREGION if q in p.lower()), None)
        if not province: return await ctx.send("❌ That country/province doesn't exist.")
        owner = self.db.get_territory_owner(province)
        if not owner: return await ctx.send("❌ Nobody currently owns that territory.")
        if str(owner) == uid: return await ctx.send("❌ You already own that territory.")
        ref = self.db.client.collection("annex_requests").document()
        ref.set({"requester_id": uid, "target_id": str(owner), "territory": province, "status": "pending", "created_at": now_iso()})
        target = self._civ(owner) or {}
        await ctx.send(f"📜 **Annexation request:** {ctx.author.mention} wants **{province}** from **{target.get('name', 'Unknown')}**.\n<@{owner}>, accept with `.acceptannex {ref.id}` or decline with `.declineannex {ref.id}`.")

    @commands.command(name="acceptannex")
    async def accept_annex(self, ctx, request_id: str = None):
        if not request_id: return await ctx.send("❌ Usage: `.acceptannex <request id>`")
        uid = str(ctx.author.id); ref = self._proposal("annex_requests", request_id); snap = ref.get()
        if not snap.exists: return await ctx.send("❌ Annex request not found.")
        req = snap.to_dict()
        if req.get("status") != "pending" or req.get("target_id") != uid: return await ctx.send("❌ This annex request is not waiting for you.")
        territory = req.get("territory")
        if self.db.get_territory_owner(territory) != uid:
            ref.update({"status": "expired", "resolved_at": now_iso()}); return await ctx.send("❌ You no longer own that territory, so the request expired.")
        requester = req["requester_id"]
        if not self._civ(requester): return await ctx.send("❌ The requesting civilization no longer exists.")
        if not self.db.conquer_territory(requester, uid, territory): return await ctx.send("❌ The territory transfer failed. Nothing was changed.")
        area = PROVINCE_AREAS.get(territory, 1000)
        self.civ_manager.update_territory(requester, {"land_size": area}); self.civ_manager.update_territory(uid, {"land_size": -area})
        ref.update({"status": "accepted", "resolved_at": now_iso()}); await ctx.send(f"🗺️ **{territory}** has been peacefully annexed by <@{requester}>.")

    @commands.command(name="declineannex")
    async def decline_annex(self, ctx, request_id: str = None):
        if not request_id: return await ctx.send("❌ Usage: `.declineannex <request id>`")
        ref = self._proposal("annex_requests", request_id); snap = ref.get()
        if not snap.exists: return await ctx.send("❌ Annex request not found.")
        req = snap.to_dict()
        if req.get("status") != "pending" or req.get("target_id") != str(ctx.author.id): return await ctx.send("❌ This annex request is not waiting for you.")
        ref.update({"status": "declined", "resolved_at": now_iso()}); await ctx.send("❌ Annexation request declined.")

async def setup(bot):
    await bot.add_cog(UnionCommands(bot))
