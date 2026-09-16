import logging
from copy import deepcopy
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
        self._install_shared_state_hooks()

    def _ref(self, uid):
        return self.db.client.collection("civilizations").document(str(uid))

    def _civ(self, uid):
        return self.db.get_civilization(str(uid))

    def _union(self, uid):
        return (self._civ(uid) or {}).get("union")

    def _set_land(self, uid, n):
        self._ref(uid).update({"territory.land_size": max(0, int(n)), "last_active": now_iso()})

    def _remove_gold(self, uid, amount):
        civ = self._civ(uid) or {}
        resources = dict(civ.get("resources", {}))
        if resources.get("gold", 0) < amount):
            return False
        resources["gold"] -= amount
        self._ref(uid).update({"resources": resources, "last_active": now_iso()})
        return True

    def _own_land(self, uid):
        return sum(PROVINCE_AREAS.get(p, 1000) for p in self.db.get_player_territories(uid))

    def _proposal(self, collection, rid):
        return self.db.client.collection(collection).document(rid)

    # ------------------------------------------------------------------
    # UNION SHARED STATE
    # ------------------------------------------------------------------
    def _install_shared_state_hooks(self):
        """Make a union behave like one civilization for stats/resources.

        Existing commands all go through CivilizationManager for resource,
        population, military and territory updates. Wrapping those methods
        here lets both members use one shared pool without rewriting every cog.
        """
        manager = self.civ_manager
        if getattr(manager, "_union_hooks_installed", False):
            return

        original_get = manager.get_civilization
        original_resources = manager.update_resources
        original_population = manager.update_population
        original_military = manager.update_military
        original_employment = manager.update_employment
        original_territory = manager.update_territory
        self._original_get = original_get

        def union_members(uid):
            civ = self.db.get_civilization(str(uid)) or {}
            union = civ.get("union") or {}
            return [str(x) for x in union.get("members", [])] if union else []

        def merged_civ(uid):
            base = original_get(str(uid))
            if not base:
                return base
            members = union_members(uid)
            if len(members) < 2:
                return base

            civs = [(m, self.db.get_civilization(m) or {}) for m in members]
            out = deepcopy(base)
            first_union = (civs[0][1].get("union") or {})
            out["name"] = first_union.get("name", base.get("name"))
            out["union"] = first_union
            out["leaders"] = members

            # Resources are one shared pool. Use the sum of the stored member
            # values, then update hooks keep both member documents identical.
            resource_keys = set()
            for _, c in civs:
                resource_keys.update((c.get("resources") or {}).keys())
            out["resources"] = {
                k: sum((c.get("resources") or {}).get(k, 0) for _, c in civs)
                for k in resource_keys
            }

            # Military is also shared. Tech level is a single union level.
            military_keys = set()
            for _, c in civs:
                military_keys.update((c.get("military") or {}).keys())
            out["military"] = {
                k: (max((c.get("military") or {}).get(k, 0) for _, c in civs)
                    if k == "tech_level" else
                    sum((c.get("military") or {}).get(k, 0) for _, c in civs))
                for k in military_keys
            }

            pops = [(c.get("population") or {}) for _, c in civs]
            out["population"] = dict(pops[0]) if pops else out.get("population", {})
            if pops:
                out["population"]["citizens"] = sum(p.get("citizens", 0) for p in pops)
                out["population"]["employed"] = sum(p.get("employed", 0) for p in pops)
                out["population"]["happiness"] = round(sum(p.get("happiness", 0) for p in pops) / len(pops))
                out["population"]["hunger"] = round(sum(p.get("hunger", 0) for p in pops) / len(pops))

            # Territory is represented by the actual Firestore provinces, not
            # the duplicated land_size field stored on each member.
            out.setdefault("territory", {})["land_size"] = sum(self._own_land(m) for m in members)
            return out

        def get_hook(uid):
            return merged_civ(uid)

        def resources_hook(uid, changes):
            members = union_members(uid)
            if len(members) < 2:
                return original_resources(uid, changes)
            current = merged_civ(uid)["resources"]
            for resource, change in changes.items():
                if resource in current:
                    current[resource] = max(0, current[resource] + change)
            for m in members:
                self._ref(m).update({"resources": dict(current), "last_active": now_iso()})
                manager._invalidate_civ(m)
            return True

        def population_hook(uid, changes):
            members = union_members(uid)
            if len(members) < 2:
                return original_population(uid, changes)
            current = merged_civ(uid)["population"]
            for stat, change in changes.items():
                if stat in current:
                    if stat == "happiness":
                        current[stat] = max(-100, min(100, current[stat] + change))
                    elif stat == "hunger":
                        current[stat] = max(0, min(100, current[stat] + change))
                    elif stat == "citizens":
                        current[stat] = max(0, current[stat] + change)
                        current["employed"] = min(current.get("employed", 0), current["citizens"])
                    else:
                        current[stat] = max(0, current[stat] + change)
            for m in members:
                self._ref(m).update({"population": dict(current), "last_active": now_iso()})
                manager._invalidate_civ(m)
            return True

        def military_hook(uid, changes):
            members = union_members(uid)
            if len(members) < 2:
                return original_military(uid, changes)
            current = merged_civ(uid)["military"]
            old_tech = current.get("tech_level", 1)
            for stat, change in changes.items():
                if stat in current:
                    if stat == "tech_level":
                        current[stat] = min(10, max(1, current[stat] + change))
                    else:
                        current[stat] = max(0, current[stat] + change)
            for m in members:
                self._ref(m).update({"military": dict(current), "last_active": now_iso()})
                manager._invalidate_civ(m)
            if current.get("tech_level", 1) > old_tech:
                for m in members:
                    self.db.generate_card_selection(m, current["tech_level"])
            return True

        def employment_hook(uid, change):
            members = union_members(uid)
            if len(members) < 2:
                return original_employment(uid, change)
            current = merged_civ(uid)["population"]
            current["employed"] = max(0, min(current["citizens"], current.get("employed", 0) + change))
            for m in members:
                self._ref(m).update({"population": dict(current), "last_active": now_iso()})
                manager._invalidate_civ(m)
            return True

        def territory_hook(uid, changes):
            members = union_members(uid)
            if len(members) < 2:
                return original_territory(uid, changes)
            # Land changes should affect the union's displayed total. Keep the
            # duplicated member field synchronized to the actual province total.
            result = original_territory(uid, changes)
            total = sum(self._own_land(m) for m in members)
            for m in members:
                self._ref(m).update({"territory.land_size": int(total), "last_active": now_iso()})
                manager._invalidate_civ(m)
            return result

        manager.get_civilization = get_hook
        manager.update_resources = resources_hook
        manager.update_population = population_hook
        manager.update_military = military_hook
        manager.update_employment = employment_hook
        manager.update_territory = territory_hook
        manager._union_hooks_installed = True

    def _sync_union(self, members):
        """Normalize an existing union so both member documents share state."""
        members = [str(m) for m in members]
        if len(members) < 2:
            return
        civs = [self._civ(m) or {} for m in members]
        resources = {}
        for c in civs:
            for k, v in (c.get("resources") or {}).items():
                resources[k] = resources.get(k, 0) + v
        military = {}
        keys = set()
        for c in civs:
            keys.update((c.get("military") or {}).keys())
        for k in keys:
            vals = [(c.get("military") or {}).get(k, 0) for c in civs]
            military[k] = max(vals) if k == "tech_level" else sum(vals)
        populations = [c.get("population") or {} for c in civs]
        population = dict(populations[0]) if populations else {}
        if populations:
            population["citizens"] = sum(p.get("citizens", 0) for p in populations)
            population["employed"] = sum(p.get("employed", 0) for p in populations)
            population["happiness"] = round(sum(p.get("happiness", 0) for p in populations) / len(populations))
            population["hunger"] = round(sum(p.get("hunger", 0) for p in populations) / len(populations))
        land = sum(self._own_land(m) for m in members)
        union = (civs[0].get("union") or {})
        for m in members:
            self._ref(m).update({
                "resources": dict(resources),
                "military": dict(military),
                "population": dict(population),
                "territory.land_size": int(land),
                "name": union.get("name", civs[0].get("name", "Union")),
                "union": {**union, "members": members},
                "last_active": now_iso(),
            })
            self.civ_manager._invalidate_civ(m)

    @commands.command(name="unite")
    async def unite(self, ctx, member: discord.Member = None, *, new_country: str = None):
        if not member or not new_country:
            return await ctx.send("❌ Usage: `.unite @player <new country name>`")
        uid, target = str(ctx.author.id), str(member.id)
        if uid == target:
            return await ctx.send("❌ You can't unite with yourself.")
        if not self._civ(uid) or not self._civ(target):
            return await ctx.send("❌ Both players need an active civilization.")
        if self._union(uid) or self._union(target):
            return await ctx.send("❌ Both players must be independent before forming a new union.")
        new_country = new_country.strip()
        if not 2 <= len(new_country) <= 50:
            return await ctx.send("❌ The new country name must be 2-50 characters.")
        ref = self.db.client.collection("union_requests").document()
        ref.set({"requester_id": uid, "target_id": target, "new_country": new_country, "status": "pending", "created_at": now_iso()})
        await ctx.send(f"🤝 {member.mention}, **{ctx.author.display_name}** wants to unite with you as **{new_country}**.\nAccept with `.acceptunite {ref.id}` or decline with `.declineunite {ref.id}`.")

    @commands.command(name="acceptunite")
    async def accept_unite(self, ctx, request_id: str = None):
        if not request_id:
            return await ctx.send("❌ Usage: `.acceptunite <request id>`")
        uid = str(ctx.author.id)
        ref = self._proposal("union_requests", request_id)
        snap = ref.get()
        if not snap.exists:
            return await ctx.send("❌ Union request not found.")
        req = snap.to_dict()
        requester = req.get("requester_id")
        if req.get("status") != "pending" or req.get("target_id") != uid:
            return await ctx.send("❌ This union request is not waiting for you.")
        if self._union(uid) or self._union(requester):
            return await ctx.send("❌ One of you is already in a union.")
        a, b = self._civ(requester), self._civ(uid)
        if not a or not b:
            return await ctx.send("❌ One of the civilizations no longer exists.")
        members = [requester, uid]
        union = {"id": ref.id, "name": req["new_country"], "members": members, "created_at": now_iso()}
        for mid, civ in ((requester, a), (uid, b)):
            self._ref(mid).update({
                "union": union,
                "original_union_name": civ.get("name", "Independent Nation"),
                "name": req["new_country"],
                "leaders": members,
                "last_active": now_iso()
            })
        # Merge the two existing pools exactly once, then keep them synchronized.
        self._sync_union(members)
        ref.update({"status": "accepted", "accepted_at": now_iso()})
        await ctx.send(f"🤝 **{req['new_country']}** has been formed! {ctx.author.mention} and <@{requester}> now act as one country.")

    @commands.command(name="declineunite")
    async def decline_unite(self, ctx, request_id: str = None):
        if not request_id:
            return await ctx.send("❌ Usage: `.declineunite <request id>`")
        ref = self._proposal("union_requests", request_id)
        snap = ref.get()
        if not snap.exists:
            return await ctx.send("❌ Union request not found.")
        req = snap.to_dict()
        if req.get("status") != "pending" or req.get("target_id") != str(ctx.author.id):
            return await ctx.send("❌ Union request not found or not addressed to you.")
        ref.update({"status": "declined", "resolved_at": now_iso()})
        await ctx.send("❌ Union request declined.")

    @commands.command(name="leave")
    async def leave_union(self, ctx):
        uid = str(ctx.author.id)
        union = self._union(uid)
        if not union:
            return await ctx.send("❌ You aren't in a union.")
        if not self._remove_gold(uid, UNION_FINE):
            return await ctx.send(f"❌ Leaving costs **{UNION_FINE} gold**. You don't have enough gold.")
        members = [m for m in union.get("members", []) if m != uid]
        civ = self._civ(uid) or {}
        old_name = civ.get("original_union_name") or "Independent Nation"
        shared = self._civ(uid) or {}
        if members:
            # Split the shared pools evenly when one member leaves.
            resources = dict(shared.get("resources", {}))
            military = dict(shared.get("military", {}))
            population = dict(shared.get("population", {}))
            for key in resources:
                resources[key] //= 2
            for key in ("soldiers", "spies"):
                if key in military:
                    military[key] //= 2
            for key in ("citizens", "employed"):
                if key in population:
                    population[key] //= 2
            self._ref(uid).update({"union": None, "name": old_name, "original_union_name": None, "resources": resources, "military": military, "population": population, "territory.land_size": int(self._own_land(uid)), "last_active": now_iso()})
            self._ref(members[0]).update({"union": None, "name": (self._civ(members[0]) or {}).get("original_union_name", "Independent Nation"), "original_union_name": None, "resources": resources, "military": military, "population": population, "territory.land_size": int(self._own_land(members[0])), "last_active": now_iso()})
            self.civ_manager._invalidate_civ(uid)
            self.civ_manager._invalidate_civ(members[0])
        else:
            self._ref(uid).update({"union": None, "name": old_name, "original_union_name": None, "territory.land_size": int(self._own_land(uid)), "last_active": now_iso()})
            self.civ_manager._invalidate_civ(uid)
        await ctx.send(f"🚪 You left **{union['name']}** and paid the **{UNION_FINE} gold** separation fine.")

    @commands.command(name="annex")
    async def annex(self, ctx, *, country: str = None):
        if not country:
            return await ctx.send("❌ Usage: `.annex <country>`")
        uid = str(ctx.author.id)
        if not self._civ(uid):
            return await ctx.send("❌ You need an active civilization.")
        q = country.strip().lower()
        province = next((p for p in PROVINCE_TO_SUBREGION if p.lower() == q), None)
        if not province:
            province = next((p for p in PROVINCE_TO_SUBREGION if q in p.lower()), None)
        if not province:
            return await ctx.send("❌ That country/province doesn't exist.")
        owner = self.db.get_territory_owner(province)
        if not owner:
            return await ctx.send("❌ Nobody currently owns that territory.")
        if str(owner) == uid or str(owner) in [str(x) for x in ((self._union(uid) or {}).get("members", []))]:
            return await ctx.send("❌ You already own that territory.")
        ref = self.db.client.collection("annex_requests").document()
        ref.set({"requester_id": uid, "target_id": str(owner), "territory": province, "status": "pending", "created_at": now_iso()})
        target = self._civ(owner) or {}
        await ctx.send(f"📜 **Annexation request:** {ctx.author.mention} wants **{province}** from **{target.get('name', 'Unknown')}**.\n<@{owner}>, accept with `.acceptannex {ref.id}` or decline with `.declineannex {ref.id}`.")

    @commands.command(name="acceptannex")
    async def accept_annex(self, ctx, request_id: str = None):
        if not request_id:
            return await ctx.send("❌ Usage: `.acceptannex <request id>`")
        uid = str(ctx.author.id)
        ref = self._proposal("annex_requests", request_id)
        snap = ref.get()
        if not snap.exists:
            return await ctx.send("❌ Annex request not found.")
        req = snap.to_dict()
        if req.get("status") != "pending" or req.get("target_id") != uid:
            return await ctx.send("❌ This annex request is not waiting for you.")
        territory = req.get("territory")
        if self.db.get_territory_owner(territory) != uid:
            ref.update({"status": "expired", "resolved_at": now_iso()})
            return await ctx.send("❌ You no longer own that territory, so the request expired.")
        requester = req["requester_id"]
        if not self._civ(requester):
            return await ctx.send("❌ The requesting civilization no longer exists.")
        if not self.db.conquer_territory(requester, uid, territory):
            return await ctx.send("❌ The territory transfer failed. Nothing was changed.")
        area = PROVINCE_AREAS.get(territory, 1000)
        self.civ_manager.update_territory(requester, {"land_size": area})
        self.civ_manager.update_territory(uid, {"land_size": -area})
        ref.update({"status": "accepted", "resolved_at": now_iso()})
        await ctx.send(f"🗺️ **{territory}** has been peacefully annexed by <@{requester}>.")

    @commands.command(name="declineannex")
    async def decline_annex(self, ctx, request_id: str = None):
        if not request_id:
            return await ctx.send("❌ Usage: `.declineannex <request id>`")
        ref = self._proposal("annex_requests", request_id)
        snap = ref.get()
        if not snap.exists:
            return await ctx.send("❌ Annex request not found.")
        req = snap.to_dict()
        if req.get("status") != "pending" or req.get("target_id") != str(ctx.author.id):
            return await ctx.send("❌ This annex request is not waiting for you.")
        ref.update({"status": "declined", "resolved_at": now_iso()})
        await ctx.send("❌ Annexation request declined.")


async def setup(bot):
    await bot.add_cog(UnionCommands(bot))
