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
        bot.remove_command("status")
        self._install_shared_state_hooks()

    def _ref(self, uid): return self.db.client.collection("civilizations").document(str(uid))
    def _civ(self, uid): return self.db.get_civilization(str(uid))
    def _union(self, uid): return (self._civ(uid) or {}).get("union")
    def _members(self, uid): return [str(x) for x in ((self._union(uid) or {}).get("members", []))]
    def _own_land(self, uid): return sum(PROVINCE_AREAS.get(p, 1000) for p in self.db.get_player_territories(uid))
    def _proposal(self, collection, rid): return self.db.client.collection(collection).document(rid)

    def _remove_gold(self, uid, amount):
        civ=self._civ(uid) or {}; r=dict(civ.get("resources", {}))
        if r.get("gold",0) < amount: return False
        r["gold"]-=amount; self._ref(uid).update({"resources":r,"last_active":now_iso()}); return True

    def _reps(self, members):
        reps=[]; seen=set()
        for m in dict.fromkeys(str(x) for x in members):
            u=self._union(m) or {}; key=str(u.get("id")) if u.get("id") else m
            if key not in seen: seen.add(key); reps.append(m)
        return reps

    def _aggregate(self, members):
        reps=self._reps(members); civs=[self._civ(m) or {} for m in reps]
        if not civs: return {}
        out=deepcopy(civs[0])
        for field in ("resources","military"):
            result={}; keys=set()
            for c in civs: keys.update((c.get(field) or {}).keys())
            for k in keys:
                vals=[(c.get(field) or {}).get(k,0) for c in civs]
                result[k]=max(vals) if k=="tech_level" else sum(v for v in vals if isinstance(v,(int,float)))
            out[field]=result
        pops=[c.get("population") or {} for c in civs]
        if pops:
            p=deepcopy(pops[0]); p["citizens"]=sum(x.get("citizens",0) for x in pops); p["employed"]=sum(x.get("employed",0) for x in pops); p["happiness"]=round(sum(x.get("happiness",0) for x in pops)/len(pops)); p["hunger"]=round(sum(x.get("hunger",0) for x in pops)/len(pops)); out["population"]=p
        for field in ("bonuses","policies","corporations","megaprojects"):
            merged={}
            for c in civs:
                v=c.get(field) or {}
                if isinstance(v,dict):
                    for k,x in v.items(): merged[k]=merged.get(k,0)+x if isinstance(x,(int,float)) else deepcopy(x)
            if merged: out[field]=merged
        for field in ("hyper_items","selected_cards","purchased_cards","owned_territories","black_market_history"):
            vals=[]; seen=set()
            for c in civs:
                for x in (c.get(field) or []):
                    marker=repr(x)
                    if marker not in seen: seen.add(marker); vals.append(deepcopy(x))
            if vals: out[field]=vals
        out.setdefault("territory",{})["land_size"]=sum(self._own_land(m) for m in members)
        return out

    def _combine_aggregates(self, a, b, members):
        out=deepcopy(a or b or {})
        for field in ("resources","military"):
            x=(a or {}).get(field,{}) ; y=(b or {}).get(field,{}) ; keys=set(x)|set(y)
            out[field]={k:(max(x.get(k,0),y.get(k,0)) if k=="tech_level" else x.get(k,0)+y.get(k,0)) for k in keys}
        ap=(a or {}).get("population",{}); bp=(b or {}).get("population",{}); p=deepcopy(ap or bp)
        p["citizens"]=ap.get("citizens",0)+bp.get("citizens",0); p["employed"]=min(p["citizens"],ap.get("employed",0)+bp.get("employed",0)); total=p["citizens"] or 1
        p["happiness"]=round((ap.get("happiness",0)*ap.get("citizens",0)+bp.get("happiness",0)*bp.get("citizens",0))/total); p["hunger"]=round((ap.get("hunger",0)*ap.get("citizens",0)+bp.get("hunger",0)*bp.get("citizens",0))/total); out["population"]=p
        for field in ("bonuses","policies","corporations","megaprojects"):
            x=(a or {}).get(field,{}) ; y=(b or {}).get(field,{}) ; merged=deepcopy(x)
            for k,v in y.items(): merged[k]=merged.get(k,0)+v if isinstance(v,(int,float)) and isinstance(merged.get(k),int|float) else deepcopy(v)
            if merged: out[field]=merged
        for field in ("hyper_items","selected_cards","purchased_cards","owned_territories","black_market_history"):
            vals=[]; seen=set()
            for c in (a or {},b or {}):
                for x in c.get(field,[]) or []:
                    marker=repr(x)
                    if marker not in seen: seen.add(marker); vals.append(deepcopy(x))
            if vals: out[field]=vals
        out.setdefault("territory",{})["land_size"]=sum(self._own_land(m) for m in members); return out

    def _write_union(self, members, union, agg):
        for m in members:
            self._ref(m).update({"resources":deepcopy(agg.get("resources",{})),"military":deepcopy(agg.get("military",{})),"population":deepcopy(agg.get("population",{})),"territory.land_size":int(agg.get("territory",{}).get("land_size",0)),"name":union.get("name",agg.get("name","Union")),"union":union,"leaders":members,"last_active":now_iso()}); self.civ_manager._invalidate_civ(m)

    def _sync_union(self,members,agg=None):
        members=list(dict.fromkeys(str(x) for x in members))
        if len(members)<2:return
        agg=agg or self._aggregate(members); u=self._union(members[0]) or {}; u={**u,"members":members}; self._write_union(members,u,agg)

    def _install_shared_state_hooks(self):
        manager=self.civ_manager
        if getattr(manager,"_union_hooks_installed",False):return
        original_get=manager.get_civilization; original_resources=manager.update_resources; original_population=manager.update_population; original_military=manager.update_military; original_employment=manager.update_employment; original_territory=manager.update_territory
        def merged(uid):
            base=original_get(str(uid)); members=self._members(uid)
            if not base or len(members)<2:return base
            out=self._aggregate(members); u=self._union(members[0]) or {}; out["name"]=u.get("name",out.get("name")); out["union"]=u; out["leaders"]=members; return out
        def shared_update(uid,field,changes,original):
            members=self._members(uid)
            if len(members)<2:return original(uid,changes)
            current=merged(uid).get(field,{})
            for k,v in changes.items():
                if k in current and isinstance(v,(int,float)):current[k]=max(0,current[k]+v)
            for m in members:self._ref(m).update({field:dict(current),"last_active":now_iso()});manager._invalidate_civ(m)
            return True
        def rh(uid,c):return shared_update(uid,"resources",c,original_resources)
        def ph(uid,c):
            members=self._members(uid)
            if len(members)<2:return original_population(uid,c)
            cur=merged(uid).get("population",{})
            for k,v in c.items():
                if k in cur and isinstance(v,(int,float)):cur[k]=max(-100,min(100,cur[k]+v)) if k=="happiness" else max(0,cur[k]+v)
            cur["employed"]=min(cur.get("employed",0),cur.get("citizens",0))
            for m in members:self._ref(m).update({"population":dict(cur),"last_active":now_iso()});manager._invalidate_civ(m)
            return True
        def mh(uid,c):return shared_update(uid,"military",c,original_military)
        def eh(uid,c):
            members=self._members(uid)
            if len(members)<2:return original_employment(uid,c)
            cur=merged(uid).get("population",{});cur["employed"]=max(0,min(cur.get("citizens",0),cur.get("employed",0)+c))
            for m in members:self._ref(m).update({"population":dict(cur),"last_active":now_iso()});manager._invalidate_civ(m)
            return True
        def th(uid,c):
            members=self._members(uid)
            if len(members)<2:return original_territory(uid,c)
            result=original_territory(uid,c);total=sum(self._own_land(m) for m in members)
            for m in members:self._ref(m).update({"territory.land_size":int(total),"last_active":now_iso()});manager._invalidate_civ(m)
            return result
        manager.get_civilization=merged;manager.update_resources=rh;manager.update_population=ph;manager.update_military=mh;manager.update_employment=eh;manager.update_territory=th;manager._union_hooks_installed=True

    @commands.command(name="status")
    async def status(self,ctx):
        uid=str(ctx.author.id);civ=self.civ_manager.get_civilization(uid)
        if not civ:return await ctx.send("❌ You need to start a civilization first! Use `.start <name>`.")
        members=self._members(uid) or [uid];names=[]
        for m in members:
            try:names.append((await self.bot.fetch_user(int(m))).name)
            except Exception:names.append(m)
        r,p,mi=civ.get("resources",{}),civ.get("population",{}),civ.get("military",{});land=civ.get("territory",{}).get("land_size",self._own_land(uid))
        e=discord.Embed(title=f"🏛️ {civ.get('name','Unknown')}",description=f"**Leaders**: {' & '.join(names)}\n**Ideology**: {str(civ.get('ideology') or 'None').capitalize()}\n**Region**: {civ.get('region','Not selected')}",color=0x0099ff)
        e.add_field(name="💰 Resources",value=f"🪙 Gold: {r.get('gold',0):,}\n🌾 Food: {r.get('food',0):,}\n🪨 Stone: {r.get('stone',0):,}\n🪵 Wood: {r.get('wood',0):,}",inline=True);e.add_field(name="👥 Population",value=f"Citizens: {p.get('citizens',0):,}\nEmployed: {p.get('employed',0):,}\nHappiness: {p.get('happiness',0)}%\nHunger: {p.get('hunger',0)}%",inline=True);e.add_field(name="⚔️ Military",value=f"Soldiers: {mi.get('soldiers',0):,}\nSpies: {mi.get('spies',0):,}\nTech: {mi.get('tech_level',1)}",inline=True);e.add_field(name="🗺️ Territory",value=f"Land: {land:,} km²\nMembers: {len(members)}",inline=True);await ctx.send(embed=e)

    @commands.command(name="unite")
    async def unite(self,ctx,member:discord.Member=None,*,new_country:str=None):
        if not member or not new_country:return await ctx.send("❌ Usage: `.unite @player <new country name>`")
        uid,target=str(ctx.author.id),str(member.id)
        if uid==target:return await ctx.send("❌ You can't unite with yourself.")
        if not self._civ(uid) or not self._civ(target):return await ctx.send("❌ Both players need an active civilization.")
        if target in self._members(uid):return await ctx.send("❌ That player is already in your union.")
        new_country=new_country.strip()
        if not 2<=len(new_country)<=50:return await ctx.send("❌ The new country name must be 2-50 characters.")
        ref=self.db.client.collection("union_requests").document();ref.set({"requester_id":uid,"target_id":target,"new_country":new_country,"status":"pending","created_at":now_iso()});await ctx.send(f"🤝 {member.mention}, **{ctx.author.display_name}** wants to unite with you as **{new_country}**.\nAccept with `.acceptunite {ref.id}` or decline with `.declineunite {ref.id}`.")

    @commands.command(name="acceptunite")
    async def accept_unite(self,ctx,request_id:str=None):
        if not request_id:return await ctx.send("❌ Usage: `.acceptunite <request id>`")
        uid=str(ctx.author.id);ref=self._proposal("union_requests",request_id);snap=ref.get()
        if not snap.exists:return await ctx.send("❌ Union request not found.")
        req=snap.to_dict();requester=str(req.get("requester_id"));target=str(req.get("target_id"))
        if req.get("status")!="pending" or target!=uid:return await ctx.send("❌ This union request is not waiting for you.")
        a=self._members(requester) or [requester];b=self._members(uid) or [uid];members=list(dict.fromkeys(a+b))
        a_agg=self._aggregate(a);b_agg=self._aggregate(b) if b!=a else {};agg=self._combine_aggregates(a_agg,b_agg,members) if a_agg and b_agg else (a_agg or b_agg)
        old_union=self._union(requester) or self._union(uid) or {};union_id=old_union.get("id") or ref.id;union={"id":union_id,"name":req["new_country"],"members":members,"created_at":old_union.get("created_at",now_iso())}
        self._write_union(members,union,agg);ref.update({"status":"accepted","accepted_at":now_iso()});await ctx.send(f"🤝 **{req['new_country']}** has expanded into a **{len(members)}-member union**! All members now share one country, economy, military, population and territory.")

    @commands.command(name="declineunite")
    async def decline_unite(self,ctx,request_id:str=None):
        if not request_id:return await ctx.send("❌ Usage: `.declineunite <request id>`")
        ref=self._proposal("union_requests",request_id);snap=ref.get()
        if not snap.exists:return await ctx.send("❌ Union request not found.")
        req=snap.to_dict()
        if req.get("status")!="pending" or req.get("target_id")!=str(ctx.author.id):return await ctx.send("❌ Union request not found or not addressed to you.")
        ref.update({"status":"declined","resolved_at":now_iso()});await ctx.send("❌ Union request declined.")

    @commands.command(name="leave")
    async def leave_union(self,ctx):
        uid=str(ctx.author.id);union=self._union(uid)
        if not union:return await ctx.send("❌ You aren't in a union.")
        if not self._remove_gold(uid,UNION_FINE):return await ctx.send(f"❌ Leaving costs **{UNION_FINE} gold**. You don't have enough gold.")
        members=[m for m in union.get("members",[]) if str(m)!=uid];old=(self._civ(uid) or {}).get("original_union_name") or "Independent Nation";shared=self._aggregate(union.get("members",[]));count=max(1,len(union.get("members",[])))
        if members:
            dr={k:v//count for k,v in shared.get("resources",{}).items()};rr={k:max(0,v-dr.get(k,0)) for k,v in shared.get("resources",{}).items()};dp=dict(shared.get("population",{}));dp["citizens"]//=count;dp["employed"]=min(dp.get("employed",0),dp["citizens"]);rp=dict(shared.get("population",{}));rp["citizens"]=max(0,rp.get("citizens",0)-dp["citizens"]);rp["employed"]=min(rp.get("employed",0),rp["citizens"])
            self._ref(uid).update({"union":None,"name":old,"original_union_name":None,"resources":dr,"population":dp,"territory.land_size":int(self._own_land(uid)),"last_active":now_iso()});self.civ_manager._invalidate_civ(uid)
            newu={**union,"members":members}
            for m in members:self._ref(m).update({"resources":rr,"population":rp,"union":newu,"leaders":members,"territory.land_size":int(sum(self._own_land(x) for x in members)),"last_active":now_iso()});self.civ_manager._invalidate_civ(m)
        else:self._ref(uid).update({"union":None,"name":old,"original_union_name":None,"last_active":now_iso()});self.civ_manager._invalidate_civ(uid)
        await ctx.send(f"🚪 You left **{union['name']}** and paid the **{UNION_FINE} gold** separation fine.")

    @commands.command(name="annex")
    async def annex(self,ctx,*,country:str=None):
        if not country:return await ctx.send("❌ Usage: `.annex <country>`")
        uid=str(ctx.author.id)
        if not self._civ(uid):return await ctx.send("❌ You need an active civilization.")
        q=country.strip().lower();province=next((p for p in PROVINCE_TO_SUBREGION if p.lower()==q),None) or next((p for p in PROVINCE_TO_SUBREGION if q in p.lower()),None)
        if not province:return await ctx.send("❌ That country/province doesn't exist.")
        owner=self.db.get_territory_owner(province);members=self._members(uid)
        if not owner:return await ctx.send("❌ Nobody currently owns that territory.")
        if str(owner)==uid or str(owner) in members:return await ctx.send("❌ You already own that territory.")
        ref=self.db.client.collection("annex_requests").document();ref.set({"requester_id":uid,"target_id":str(owner),"territory":province,"status":"pending","created_at":now_iso()});target=self._civ(owner) or {};await ctx.send(f"📜 **Annexation request:** {ctx.author.mention} wants **{province}** from **{target.get('name','Unknown')}**.\n<@{owner}>, accept with `.acceptannex {ref.id}` or decline with `.declineannex {ref.id}`.")

    @commands.command(name="acceptannex")
    async def accept_annex(self,ctx,request_id:str=None):
        if not request_id:return await ctx.send("❌ Usage: `.acceptannex <request id>`")
        uid=str(ctx.author.id);ref=self._proposal("annex_requests",request_id);snap=ref.get()
        if not snap.exists:return await ctx.send("❌ Annex request not found.")
        req=snap.to_dict()
        if req.get("status")!="pending" or req.get("target_id")!=uid:return await ctx.send("❌ This annex request is not waiting for you.")
        territory=req.get("territory")
        if self.db.get_territory_owner(territory)!=uid:ref.update({"status":"expired","resolved_at":now_iso()});return await ctx.send("❌ You no longer own that territory, so the request expired.")
        requester=req["requester_id"]
        if not self._civ(requester) or not self.db.conquer_territory(requester,uid,territory):return await ctx.send("❌ The territory transfer failed. Nothing was changed.")
        ref.update({"status":"accepted","resolved_at":now_iso()});await ctx.send(f"🗺️ **{territory}** has been peacefully annexed by <@{requester}>.")

    @commands.command(name="declineannex")
    async def decline_annex(self,ctx,request_id:str=None):
        if not request_id:return await ctx.send("❌ Usage: `.declineannex <request id>`")
        ref=self._proposal("annex_requests",request_id);snap=ref.get()
        if not snap.exists:return await ctx.send("❌ Annex request not found.")
        req=snap.to_dict()
        if req.get("status")!="pending" or req.get("target_id")!=str(ctx.author.id):return await ctx.send("❌ This annex request is not waiting for you.")
        ref.update({"status":"declined","resolved_at":now_iso()});await ctx.send("❌ Annexation request declined.")

async def setup(bot):
    await bot.add_cog(UnionCommands(bot))