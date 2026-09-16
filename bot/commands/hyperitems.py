import random
import asyncio
import discord as guilded
from discord.ext import commands
from discord import app_commands
import logging
from typing import Optional
from bot.utils import format_number, create_embed
from bot import config

logger = logging.getLogger(__name__)


class HyperItemCommands(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.db = bot.db
        self.civ_manager = bot.civ_manager

    def _has_hyperitem(self, user_id: str, item_name: str) -> bool:
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            return False
        return item_name in civ.get('hyper_items', [])

    def _is_allied(self, user_id: str, target_id: str) -> bool:
        """True if the two users share an alliance."""
        try:
            for doc in self.db.client.collection("alliances").where("members", "array_contains", user_id).stream():
                if target_id in doc.to_dict().get("members", []):
                    return True
        except Exception as e:
            logger.error(f"_is_allied error: {e}")
        return False

    def _tech_mult(self, civ: dict) -> float:
        """Power-curve scaling helper: 1x at tech 1, ~3x at tech 10."""
        tech = civ['military']['tech_level']
        return 1 + (tech * 0.20)

    def _pop_mult(self, civ: dict) -> float:
        """Population scaling: 1x at 1000 citizens, grows with pop."""
        return 1 + (civ['population']['citizens'] / 5000)

    async def _block_with_shield(self, ctx, target_id: str, target_civ, attacker_civ, attack_type: str):
        self.civ_manager.use_hyper_item(target_id, "Anti-Nuke Shield")
        embed = create_embed(
            "🛡️ Attack Completely Blocked!",
            f"**{target_civ['name']}**'s Anti-Nuke Shield nullified the {attack_type} from **{attacker_civ['name']}**!",
            guilded.Color.blue()
        )
        embed.add_field(name="Result", value="Zero damage taken. Shield consumed on activation.", inline=False)
        await ctx.send(embed=embed)
        try:
            target_user = await self.bot.fetch_user(int(target_id))
            await target_user.send(f"🛡️ **Shield Popped Off!** Your Anti-Nuke Shield blocked that {attack_type} from {attacker_civ['name']}.")
        except Exception:
            pass

    async def _reflect_with_mirror(self, ctx, target_id: str, target_civ, attacker_civ, attack_type: str):
        self.civ_manager.use_hyper_item(target_id, "Mirror")
        embed = create_embed(
            "🪞 ATTACK REFLECTED!",
            f"**{target_civ['name']}**'s Mirror reflected the {attack_type} back at **{attacker_civ['name']}**!",
            guilded.Color.purple()
        )
        embed.add_field(name="Result", value="Attack completely reflected back to the attacker! Mirror consumed.", inline=False)
        await ctx.send(embed=embed)
        try:
            target_user = await self.bot.fetch_user(int(target_id))
            await target_user.send(f"🪞 **Attack Reflected!** Your Mirror reflected the {attack_type} back at {attacker_civ['name']}!")
        except Exception:
            pass
        try:
            attacker_user = await self.bot.fetch_user(int(attacker_civ['user_id']))
            await attacker_user.send(f"🪞 **ATTACK REFLECTED!** Your {attack_type} was reflected back at you by {target_civ['name']}'s Mirror!")
        except Exception:
            pass

    async def _announce_global_attack(self, ctx, attacker_name: str, target_name: str, attack_type: str):
        embed = create_embed(
            f"🌍 GLOBAL ALERT: {attack_type.upper()}",
            f"**{attacker_name}** has launched a {attack_type} against **{target_name}**!",
            guilded.Color.red()
        )
        embed.add_field(name="⚠️ World Event", value="This attack affects the global balance of power!", inline=False)
        await ctx.send(embed=embed)

    def _check_defenses(self, target_id: str, attack_type: str):
        if self._has_hyperitem(target_id, "Mirror"):
            return "mirror"
        elif self._has_hyperitem(target_id, "Anti-Nuke Shield"):
            return "shield"
        return None

    # =================================================================
    # LAST STAND
    # =================================================================
    @commands.command(name='laststand')
    async def last_stand(self, ctx):
        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need to start a civilization first!")
            return
        if not self._has_hyperitem(user_id, "Last Stand"):
            await ctx.send("❌ You need a **Last Stand** HyperItem to use this command!")
            return
        if civ['resources']['gold'] >= 500:
            await ctx.send("❌ **Last Stand** can only be used when you have less than 500 gold!")
            return
        self.civ_manager.use_hyper_item(user_id, "Last Stand")
        poverty_factor = max(0.1, (500 - civ['resources']['gold']) / 500)
        military_boost_multiplier = 3.0 + (poverty_factor * 7.0)
        soldiers_boost = int(civ['military']['soldiers'] * military_boost_multiplier)
        spies_boost = int(civ['military']['spies'] * military_boost_multiplier)
        tech_boost = random.randint(3, 8)
        self.civ_manager.update_military(user_id, {
            "soldiers": soldiers_boost,
            "spies": spies_boost,
            "tech_level": tech_boost
        })
        self.civ_manager.update_population(user_id, {"happiness": 40})
        embed = create_embed(
            "💥 LAST STAND ACTIVATED!",
            f"**{civ['name']}** makes a desperate final stand with nothing left to lose!",
            guilded.Color.dark_red()
        )
        embed.add_field(name="Desperation Bonus",
                        value=f"Poverty: {poverty_factor:.1f}x → Total Boost: {military_boost_multiplier:.1f}x",
                        inline=False)
        embed.add_field(name="Reinforcements",
                        value=f"⚔️ {format_number(soldiers_boost)} soldiers\n🕵️ {format_number(spies_boost)} spies\n🔬 +{tech_boost} tech",
                        inline=True)
        embed.add_field(name="Morale Surge", value="😊 +40 Happiness", inline=True)
        await ctx.send(embed=embed)

    # =================================================================
    # SACRIFICE
    # =================================================================
    @commands.command(name='sacrifice')
    @app_commands.describe(target="Target civilization leader")
    async def mutual_destruction(self, ctx, target: Optional[guilded.Member] = None):
        if not target:
            await ctx.send("💀 **MUTUAL DESTRUCTION**\nUsage: `.sacrifice <user>`\nRequires: Sacrifice HyperItem\n⚠️ DESTROYS BOTH CIVILIZATIONS!")
            return
        user_id = str(ctx.author.id)
        if not self._has_hyperitem(user_id, "Sacrifice"):
            await ctx.send("❌ You need a **Sacrifice** HyperItem to use this command!")
            return
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need to start a civilization first!")
            return
        target_id = str(target.id)
        if target_id == user_id:
            await ctx.send("❌ You cannot sacrifice with yourself!")
            return
        target_civ = self.civ_manager.get_civilization(target_id)
        if not target_civ:
            await ctx.send("❌ Target user doesn't have a civilization!")
            return

        defense = self._check_defenses(target_id, "sacrifice")
        if defense == "mirror":
            await self._reflect_with_mirror(ctx, target_id, target_civ, civ, "mutual destruction sacrifice")
            try:
                if self.db.delete_civilization(user_id):
                    await ctx.send("💀 **SACRIFICE REFLECTED!** You were destroyed by your own reflected sacrifice!")
                    try:
                        attacker_user = await self.bot.fetch_user(int(user_id))
                        await attacker_user.send("💀 **SACRIFICE REFLECTED!** Your mutual destruction attempt was reflected back at you!")
                    except Exception:
                        pass
                else:
                    await ctx.send("❌ Failed to delete your civilization after reflection.")
            except Exception as e:
                logger.error(f"Error in reflected sacrifice: {e}")
            return

        def check(m):
            return m.author.id == ctx.author.id and m.channel.id == ctx.channel.id and m.content.lower() == 'confirm'

        embed = create_embed(
            "💀 FINAL WARNING: MUTUAL DESTRUCTION",
            f"**This will COMPLETELY DESTROY both {civ['name']} and {target_civ['name']}!**",
            guilded.Color.dark_red()
        )
        embed.add_field(name="CONFIRMATION", value="Type `confirm` in the next 30 seconds.", inline=False)
        embed.add_field(name="Effects", value="• Both civilizations permanently deleted\n• All progress lost", inline=False)
        await ctx.send(embed=embed)
        try:
            await self.bot.wait_for('message', timeout=30.0, check=check)
        except asyncio.TimeoutError:
            await ctx.send("❌ Mutual destruction cancelled.")
            return
        self.civ_manager.use_hyper_item(user_id, "Sacrifice")
        try:
            success1 = self.db.delete_civilization(user_id)
            success2 = self.db.delete_civilization(target_id)
            if not success1 or not success2:
                await ctx.send("❌ Failed to delete one or both civilizations.")
                return
            await self._announce_global_attack(ctx, civ['name'], target_civ['name'], "Mutual Destruction Sacrifice")
            embed = create_embed(
                "💀 MUTUAL DESTRUCTION COMPLETE",
                f"**{civ['name']}** and **{target_civ['name']}** have been annihilated!",
                guilded.Color.dark_red()
            )
            await ctx.send(embed=embed)
            try:
                target_user = await self.bot.fetch_user(int(target_id))
                await target_user.send(f"💀 **MUTUAL DESTRUCTION!** Your civilization was destroyed in a mutual sacrifice with {civ['name']}!")
            except Exception:
                pass
            try:
                user = await self.bot.fetch_user(int(user_id))
                await user.send(f"💀 **SACRIFICE COMPLETE!** You destroyed both yourself and {target_civ['name']}.")
            except Exception:
                pass
            self.db.log_event(user_id, "mutual_destruction", "Mutual Destruction", f"Destroyed both {civ['name']} and {target_civ['name']}")
        except Exception as e:
            logger.error(f"Error in mutual destruction: {e}")
            await ctx.send("❌ Failed to execute mutual destruction.")

    # =================================================================
    # MIRROR
    # =================================================================
    @commands.command(name='mirror')
    async def mirror_status(self, ctx):
        user_id = str(ctx.author.id)
        if not self._has_hyperitem(user_id, "Mirror"):
            await ctx.send("❌ You don't have a **Mirror** HyperItem!")
            return
        civ = self.civ_manager.get_civilization(user_id)
        embed = create_embed(
            "🪞 Ultimate Mirror of Reflection",
            f"**{civ['name']}** is protected by the all-reflecting Mirror!",
            guilded.Color.purple()
        )
        embed.add_field(name="Mirror Status", value="✅ **ACTIVE** - Will reflect the next ANY attack", inline=False)
        embed.add_field(name="God-Tier Reflection",
                        value="• Reflects nukes, obliteration, missiles, assassinations\n"
                              "• Reflects propaganda, spy ops, sacrifice\n"
                              "• Consumed after one reflection",
                        inline=False)
        await ctx.send(embed=embed)

    # =================================================================
    # NUKE
    # =================================================================
    @commands.command(name='nuke')
    @app_commands.describe(target="Target civilization leader")
    async def nuclear_strike(self, ctx, target: Optional[guilded.Member] = None):
        if not target:
            await ctx.send("☢️ **Nuclear Strike**\nUsage: `.nuke <user>`\nRequires: Nuclear Warhead HyperItem")
            return
        user_id = str(ctx.author.id)
        if not self._has_hyperitem(user_id, "Nuclear Warhead"):
            await ctx.send("❌ You need a **Nuclear Warhead** HyperItem!")
            return
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need to start a civilization first!")
            return
        target_id = str(target.id)
        if target_id == user_id:
            await ctx.send("❌ You cannot nuke yourself!")
            return
        target_civ = self.civ_manager.get_civilization(target_id)
        if not target_civ:
            await ctx.send("❌ Target user doesn't have a civilization!")
            return

        defense = self._check_defenses(target_id, "nuclear strike")
        if defense == "mirror":
            await self._reflect_with_mirror(ctx, target_id, target_civ, civ, "nuclear strike")
            population_loss = int(civ['population']['citizens'] * random.uniform(0.4, 0.7))
            military_loss = int(civ['military']['soldiers'] * random.uniform(0.6, 0.9))
            resource_destruction = {
                "gold": int(civ['resources']['gold'] * random.uniform(0.3, 0.6)),
                "food": int(civ['resources']['food'] * random.uniform(0.5, 0.8)),
                "wood": int(civ['resources']['wood'] * random.uniform(0.4, 0.7)),
                "stone": int(civ['resources']['stone'] * random.uniform(0.4, 0.7))
            }
            territory_loss = int(civ['territory']['land_size'] * random.uniform(0.2, 0.4))
            self.civ_manager.update_population(user_id, {"citizens": -population_loss, "happiness": -50, "hunger": 30})
            self.civ_manager.update_military(user_id, {"soldiers": -military_loss, "spies": -int(civ['military']['spies'] * 0.5)})
            self.civ_manager.update_resources(user_id, {r: -a for r, a in resource_destruction.items()})
            self.civ_manager.update_territory(user_id, {"land_size": -territory_loss})
            await ctx.send("💥 **NUCLEAR STRIKE REFLECTED!** Your own nuke was reflected back at you!")
            return
        elif defense == "shield":
            await self._block_with_shield(ctx, target_id, target_civ, civ, "nuclear strike")
            return

        self.civ_manager.use_hyper_item(user_id, "Nuclear Warhead")
        population_loss = int(target_civ['population']['citizens'] * random.uniform(0.4, 0.7))
        military_loss = int(target_civ['military']['soldiers'] * random.uniform(0.6, 0.9))
        resource_destruction = {
            "gold": int(target_civ['resources']['gold'] * random.uniform(0.3, 0.6)),
            "food": int(target_civ['resources']['food'] * random.uniform(0.5, 0.8)),
            "wood": int(target_civ['resources']['wood'] * random.uniform(0.4, 0.7)),
            "stone": int(target_civ['resources']['stone'] * random.uniform(0.4, 0.7))
        }
        territory_loss = int(target_civ['territory']['land_size'] * random.uniform(0.2, 0.4))
        self.civ_manager.update_population(target_id, {"citizens": -population_loss, "happiness": -50, "hunger": 30})
        self.civ_manager.update_military(target_id, {"soldiers": -military_loss, "spies": -int(target_civ['military']['spies'] * 0.5)})
        self.civ_manager.update_resources(target_id, {r: -a for r, a in resource_destruction.items()})
        self.civ_manager.update_territory(target_id, {"land_size": -territory_loss})
        await self._announce_global_attack(ctx, civ['name'], target_civ['name'], "Nuclear Strike")
        embed = create_embed("☢️ NUCLEAR DEVASTATION",
                             f"**{civ['name']}** has nuked **{target_civ['name']}**!",
                             guilded.Color.red())
        damage_text = f"💀 Population: {format_number(population_loss)}\n⚔️ Soldiers: {format_number(military_loss)}\n🏞️ Territory: {format_number(territory_loss)} km²"
        embed.add_field(name="Casualties", value=damage_text, inline=True)
        destruction_text = "\n".join([f"{'🪙' if r == 'gold' else '🌾' if r == 'food' else '🪨' if r == 'stone' else '🪵'} {format_number(a)} {r.capitalize()}" for r, a in resource_destruction.items() if a > 0])
        embed.add_field(name="Resources Destroyed", value=destruction_text, inline=True)
        embed.add_field(name="☢️ Fallout", value="Massive happiness loss, increased hunger", inline=False)
        await ctx.send(embed=embed)
        self.db.log_event(user_id, "nuclear_attack", "Nuclear Strike", f"Nuked {target_civ['name']}")
        self.db.log_event(target_id, "nuclear_victim", "Nuclear Attack Victim", f"Devastated by {civ['name']}")

    # =================================================================
    # OBLITERATE — REWORKED
    # =================================================================
    @commands.command(name='obliterate')
    @app_commands.describe(target="Target civilization leader")
    async def obliterate_civilization(self, ctx, target: Optional[guilded.Member] = None):
        """
        HyperLaser obliteration.

        - If target already has ANY stat at 0 -> instant kill.
        - Otherwise -> randomly zero one of their stats (gold/food/wood/stone/soldiers/citizens)
          OR set happiness to -100 (instant civil war).
        - 30% chance the laser BACKFIRES: your own civilization is obliterated instead.
        """
        if not target:
            await ctx.send("💥 **Total Obliteration**\nUsage: `.obliterate <user>`\nRequires: HyperLaser\n⚠️ 30% backfire chance!")
            return
        user_id = str(ctx.author.id)
        if not self._has_hyperitem(user_id, "HyperLaser"):
            await ctx.send("❌ You need a **HyperLaser** HyperItem!")
            return
        target_id = str(target.id)
        if target_id == user_id:
            await ctx.send("❌ You cannot obliterate yourself!")
            return
        target_civ = self.civ_manager.get_civilization(target_id)
        if not target_civ:
            await ctx.send("❌ Target user doesn't have a civilization!")
            return
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need to start a civilization first!")
            return

        # ---- Defense checks ----
        defense = self._check_defenses(target_id, "HyperLaser obliteration")
        if defense == "mirror":
            await self._reflect_with_mirror(ctx, target_id, target_civ, civ, "HyperLaser obliteration")
            try:
                if self.db.delete_civilization(user_id):
                    await ctx.send("💥 **OBLITERATION REFLECTED!** You were destroyed by your own reflected HyperLaser!")
                    try:
                        u = await self.bot.fetch_user(int(user_id))
                        await u.send("💥 **OBLITERATION REFLECTED!** Your HyperLaser was reflected back at you!")
                    except Exception:
                        pass
            except Exception as e:
                logger.error(f"Error in reflected obliteration: {e}")
            return
        elif defense == "shield":
            await self._block_with_shield(ctx, target_id, target_civ, civ, "HyperLaser obliteration")
            return

        # ---- Check if target has any stat at 0 (easy kill) ----
        target_stats = {
            "gold": target_civ['resources']['gold'],
            "stone": target_civ['resources']['stone'],
            "food": target_civ['resources']['food'],
            "wood": target_civ['resources']['wood'],
            "soldiers": target_civ['military']['soldiers'],
            "citizens": target_civ['population']['citizens']
        }
        has_zero = any(v <= 0 for v in target_stats.values())

        self.civ_manager.use_hyper_item(user_id, "HyperLaser")

        # ---- 30% backfire (only for the "forced" version) ----
        if not has_zero and random.random() < 0.30:
            # BACKFIRE! The HyperLaser overloads and destroys the wielder.
            try:
                if self.db.delete_civilization(user_id):
                    await self._announce_global_attack(ctx, civ['name'], civ['name'], "HyperLaser Backfire")
                    embed = create_embed(
                        "💥 HYPERLASER BACKFIRE!",
                        f"**{civ['name']}**'s HyperLaser exploded in their own face! "
                        f"The forced obliteration of **{target_civ['name']}** catastrophically backfired.",
                        guilded.Color.dark_red()
                    )
                    embed.add_field(name="☠️ Attacker Destroyed", value=f"**{civ['name']}** has been erased from existence.", inline=False)
                    embed.add_field(name="🏳️ Target Survives", value=f"**{target_civ['name']}** sustained no damage.", inline=False)
                    await ctx.send(embed=embed)
                    try:
                        u = await self.bot.fetch_user(int(user_id))
                        await u.send(f"💥 **HYPERLASER BACKFIRE!** Your forced obliteration of {target_civ['name']} backfired. Your civilization has been destroyed.")
                    except Exception:
                        pass
                    try:
                        t = await self.bot.fetch_user(int(target_id))
                        await t.send(f"🏳️ **You survived!** {civ['name']}'s HyperLaser backfired and destroyed them instead.")
                    except Exception:
                        pass
                    self.db.log_event(user_id, "obliteration_backfire", "HyperLaser Backfire", f"Backfired against {target_civ['name']} — wielder destroyed")
                else:
                    await ctx.send("❌ Backfire occurred but failed to delete your civilization. Contact admin.")
            except Exception as e:
                logger.error(f"Error in backfire: {e}")
                await ctx.send("❌ Error processing backfire.")
            return

        # ---- EASY KILL PATH ----
        if has_zero:
            try:
                if not self.db.delete_civilization(target_id):
                    await ctx.send("❌ Failed to obliterate civilization.")
                    return
                await self._announce_global_attack(ctx, civ['name'], target_civ['name'], "HyperLaser Obliteration")
                embed = create_embed(
                    "💥 CIVILIZATION OBLITERATED",
                    f"**{target_civ['name']}** has been erased by **{civ['name']}**'s HyperLaser!",
                    guilded.Color.dark_red()
                )
                embed.add_field(name="💀 Total Annihilation",
                                value="• The target was already weakened (a stat at 0)\n• HyperLaser consumed",
                                inline=False)
                await ctx.send(embed=embed)
                try:
                    t = await self.bot.fetch_user(int(target_id))
                    await t.send(f"💥 **CIVILIZATION OBLITERATED!** Your civilization was destroyed by {civ['name']}'s HyperLaser! Use `.start <name>` to begin anew.")
                except Exception:
                    pass
                self.db.log_event(user_id, "obliteration", "Civilization Obliterated", f"Destroyed {target_civ['name']} (weakness kill)")
            except Exception as e:
                logger.error(f"Error obliterating: {e}")
                await ctx.send("❌ Failed to obliterate civilization.")
            return

        # ---- FORCED RESET PATH (target has no zero stats, survived backfire) ----
        reset_choice = random.choice(["gold", "food", "wood", "stone", "soldiers", "citizens", "happiness"])

        if reset_choice == "happiness":
            # Force happiness to -100 → instant civil war on next check
            current_h = target_civ['population']['happiness']
            self.civ_manager.update_population(target_id, {"happiness": -100 - current_h})
            # Immediately trigger the civil war
            try:
                self.civ_manager.trigger_civil_war(target_id)
            except Exception as e:
                logger.error(f"Failed to auto-trigger civil war after obliterate: {e}")

            embed = create_embed(
                "💥 HYPERLASER STRIKE!",
                f"**{civ['name']}** fired the HyperLaser at **{target_civ['name']}** — the beam struck the population's morale!",
                guilded.Color.dark_red()
            )
            embed.add_field(name="😡 Happiness → -100", value="The nation has spiraled into chaos. **A civil war has erupted!**", inline=False)
            embed.add_field(name="⚔️ Effect", value="The target's territories are now split between loyalists and rebels.", inline=False)
        else:
            # Zero out the chosen resource/unit stat
            if reset_choice in ("gold", "food", "wood", "stone"):
                current = target_civ['resources'][reset_choice]
                self.civ_manager.update_resources(target_id, {reset_choice: -current})
                display = f"{'🪙' if reset_choice == 'gold' else '🌾' if reset_choice == 'food' else '🪨' if reset_choice == 'stone' else '🪵'} {reset_choice.capitalize()}"
            elif reset_choice == "soldiers":
                current = target_civ['military']['soldiers']
                self.civ_manager.update_military(target_id, {"soldiers": -current})
                display = f"⚔️ Soldiers"
            else:  # citizens
                current = target_civ['population']['citizens']
                self.civ_manager.update_population(target_id, {"citizens": -current})
                display = f"👥 Citizens"

            embed = create_embed(
                "💥 HYPERLASER STRIKE!",
                f"**{civ['name']}** fired the HyperLaser at **{target_civ['name']}** — a forced disarmament strike!",
                guilded.Color.dark_red()
            )
            embed.add_field(name=f"🎯 {display} set to 0", value=f"Was **{format_number(current)}**", inline=False)
            embed.add_field(name="⚠️ Vulnerability", value="The target now has a stat at 0. **Next obliterate is an easy kill.**", inline=False)

        await ctx.send(embed=embed)
        try:
            t = await self.bot.fetch_user(int(target_id))
            await t.send(f"💥 **HYPERLASER STRIKE!** {civ['name']} fired a HyperLaser at you! Check `.status` immediately.")
        except Exception:
            pass
        self.db.log_event(user_id, "obliteration_forced", "HyperLaser Forced Reset", f"Reset {reset_choice} on {target_civ['name']}")

    # =================================================================
    # SHIELD
    # =================================================================
    @commands.command(name='shield')
    async def activate_shield(self, ctx):
        user_id = str(ctx.author.id)
        if not self._has_hyperitem(user_id, "Anti-Nuke Shield"):
            await ctx.send("❌ You don't have an **Anti-Nuke Shield** HyperItem!")
            return
        civ = self.civ_manager.get_civilization(user_id)
        embed = create_embed("🛡️ Ultimate Anti-Nuke Shield",
                             f"**{civ['name']}** is locked down with the Anti-Nuke Shield!",
                             guilded.Color.blue())
        embed.add_field(name="Shield Status", value="✅ **ACTIVE** - Auto-blocks the next ANY attack", inline=False)
        embed.add_field(name="God-Tier Protection",
                        value="• Blocks nukes, obliteration, missiles, assassinations, propaganda, spy ops\n• Consumed after one block",
                        inline=False)
        await ctx.send(embed=embed)

    # =================================================================
    # LUCKY STRIKE
    # =================================================================
    @commands.command(name='luckystrike')
    async def lucky_strike(self, ctx):
        user_id = str(ctx.author.id)
        if not self._has_hyperitem(user_id, "Lucky Charm"):
            await ctx.send("❌ You need a **Lucky Charm** HyperItem!")
            return
        self.civ_manager.use_hyper_item(user_id, "Lucky Charm")
        civ = self.civ_manager.get_civilization(user_id)
        bonuses = civ.get('bonuses', {})
        bonuses['next_action_critical'] = True
        self.civ_manager.db.update_civilization(user_id, {"bonuses": bonuses})
        embed = create_embed("🍀 Lucky Charm Activated!",
                             f"**{civ['name']}** radiates with mystical fortune!",
                             guilded.Color.gold())
        embed.add_field(name="Effect", value="Your next combat action, resource gathering, or diplomacy attempt will have guaranteed critical success!", inline=False)
        await ctx.send(embed=embed)

    # =================================================================
    # PROPAGANDA
    # =================================================================
    @commands.command(name='propaganda')
    @app_commands.describe(target="Target civilization leader")
    async def propaganda_campaign(self, ctx, target: Optional[guilded.Member] = None):
        if not target:
            await ctx.send("📢 **Propaganda Campaign**\nUsage: `.propaganda <user>`")
            return
        user_id = str(ctx.author.id)
        if not self._has_hyperitem(user_id, "Propaganda Kit"):
            await ctx.send("❌ You need a **Propaganda Kit** HyperItem!")
            return
        target_id = str(target.id)
        target_civ = self.civ_manager.get_civilization(target_id)
        if not target_civ:
            await ctx.send("❌ Target user doesn't have a civilization!")
            return
        civ = self.civ_manager.get_civilization(user_id)

        defense = self._check_defenses(target_id, "propaganda campaign")
        if defense == "mirror":
            await self._reflect_with_mirror(ctx, target_id, target_civ, civ, "propaganda campaign")
            soldiers_stolen = max(1, int(civ['military']['soldiers'] * random.uniform(0.15, 0.35)))
            self.civ_manager.update_military(user_id, {"soldiers": -soldiers_stolen})
            await ctx.send(f"📢 **PROPAGANDA REFLECTED!** Your own propaganda turned {soldiers_stolen} of your soldiers against you!")
            return
        elif defense == "shield":
            await self._block_with_shield(ctx, target_id, target_civ, civ, "propaganda campaign")
            return

        self.civ_manager.use_hyper_item(user_id, "Propaganda Kit")
        target_soldiers = target_civ['military']['soldiers']
        soldiers_stolen = max(1, int(target_soldiers * random.uniform(0.15, 0.35)))
        propaganda_modifier = self.civ_manager.get_ideology_modifier(user_id, "propaganda_success")
        soldiers_stolen = int(soldiers_stolen * propaganda_modifier)

        self.civ_manager.update_military(target_id, {"soldiers": -soldiers_stolen})
        self.civ_manager.update_military(user_id, {"soldiers": soldiers_stolen})

        embed = create_embed("📢 Propaganda Campaign Success!",
                             f"**{civ['name']}** swayed enemy soldiers to defect!",
                             guilded.Color.purple())
        embed.add_field(name="Defectors", value=f"⚔️ {format_number(soldiers_stolen)} soldiers joined you!", inline=True)
        embed.add_field(name="Target", value=f"**{target_civ['name']}** lost {format_number(soldiers_stolen)} soldiers", inline=True)
        await ctx.send(embed=embed)
        try:
            t = await self.bot.fetch_user(int(target_id))
            await t.send(f"📢 **Propaganda Attack!** {civ['name']} convinced {soldiers_stolen} of your soldiers to defect!")
        except Exception:
            pass

    # =================================================================
    # HIRE MERCS — scaled
    # =================================================================
    @commands.command(name='hiremercs')
    async def hire_mercenaries(self, ctx):
        user_id = str(ctx.author.id)
        if not self._has_hyperitem(user_id, "Mercenary Contract"):
            await ctx.send("❌ You need a **Mercenary Contract** HyperItem!")
            return
        civ = self.civ_manager.get_civilization(user_id)
        self.civ_manager.use_hyper_item(user_id, "Mercenary Contract")

        # Power-curve scaling: base + tech mult + pop mult
        base_soldiers = 50 + (civ['military']['soldiers'] // 4)
        tech_mult = self._tech_mult(civ)
        pop_mult = self._pop_mult(civ)
        mercenaries_hired = int(base_soldiers * tech_mult * pop_mult)
        spies_hired = max(5, int((10 + civ['military']['spies'] // 3) * tech_mult))

        self.civ_manager.update_military(user_id, {
            "soldiers": mercenaries_hired,
            "spies": spies_hired
        })
        embed = create_embed("⚔️ Mercenaries Hired!",
                             f"**{civ['name']}** recruited professional military units!",
                             guilded.Color.orange())
        embed.add_field(name="Forces Recruited",
                        value=f"⚔️ {format_number(mercenaries_hired)} Elite Soldiers\n🕵️ {format_number(spies_hired)} Professional Spies",
                        inline=False)
        embed.add_field(name="Scaling", value=f"Tech: {tech_mult:.2f}x | Pop: {pop_mult:.2f}x", inline=True)
        await ctx.send(embed=embed)

    # =================================================================
    # BOOST TECH
    # =================================================================
    @commands.command(name='boosttech')
    async def boost_technology(self, ctx):
        user_id = str(ctx.author.id)
        if not self._has_hyperitem(user_id, "Ancient Scroll"):
            await ctx.send("❌ You need an **Ancient Scroll** HyperItem!")
            return
        civ = self.civ_manager.get_civilization(user_id)
        self.civ_manager.use_hyper_item(user_id, "Ancient Scroll")

        tech_advance = random.randint(2, 4)
        self.civ_manager.update_military(user_id, {"tech_level": tech_advance})
        embed = create_embed("📜 Ancient Knowledge Unlocked!",
                             f"**{civ['name']}** unlocked the secrets of an Ancient Scroll!",
                             guilded.Color.gold())
        embed.add_field(name="Technology Advancement", value=f"🔬 Tech Level increased by {tech_advance}!", inline=False)
        await ctx.send(embed=embed)

    # =================================================================
    # MINT GOLD — POWER-CURVE SCALED
    # =================================================================
    @commands.command(name='mintgold')
    async def mint_gold(self, ctx):
        user_id = str(ctx.author.id)
        if not self._has_hyperitem(user_id, "Gold Mint"):
            await ctx.send("❌ You need a **Gold Mint** HyperItem!")
            return
        civ = self.civ_manager.get_civilization(user_id)
        self.civ_manager.use_hyper_item(user_id, "Gold Mint")

        tech_mult = self._tech_mult(civ)
        pop_mult = self._pop_mult(civ)
        # Base scales with your PEAK gold, not a fixed number
        current_gold = civ['resources']['gold']
        base_gold = 5000 + (current_gold * 0.15)
        total_gold = int(base_gold * tech_mult * pop_mult)
        total_gold = min(total_gold, 100_000_000)

        self.civ_manager.update_resources(user_id, {"gold": total_gold})
        embed = create_embed("🪙 Gold Mint Activated!",
                             f"**{civ['name']}** has struck it rich with their Gold Mint!",
                             guilded.Color.gold())
        embed.add_field(name="Gold Generated", value=f"🪙 {format_number(total_gold)} Gold", inline=False)
        embed.add_field(name="Scaling", value=f"Tech: {tech_mult:.2f}x | Pop: {pop_mult:.2f}x | Base: {format_number(int(base_gold))}", inline=True)
        await ctx.send(embed=embed)

    # =================================================================
    # SUPER HARVEST — POWER-CURVE SCALED
    # =================================================================
    @commands.command(name='superharvest')
    async def super_harvest(self, ctx):
        user_id = str(ctx.author.id)
        if not self._has_hyperitem(user_id, "Harvest Engine"):
            await ctx.send("❌ You need a **Harvest Engine** HyperItem!")
            return
        civ = self.civ_manager.get_civilization(user_id)
        self.civ_manager.use_hyper_item(user_id, "Harvest Engine")

        tech_mult = self._tech_mult(civ)
        pop_mult = self._pop_mult(civ)
        land = civ['territory']['land_size']
        base_food = 8000 + int(land * 3)
        total_food = int(base_food * tech_mult * pop_mult)
        total_food = min(total_food, 100_000_000)

        self.civ_manager.update_resources(user_id, {"food": total_food})
        self.civ_manager.update_population(user_id, {"happiness": 15, "hunger": -50})
        embed = create_embed("🌾 Super Harvest Complete!",
                             f"**{civ['name']}**'s Harvest Engine produced a massive bounty!",
                             guilded.Color.green())
        embed.add_field(name="Food Produced", value=f"🌾 {format_number(total_food)} Food", inline=True)
        embed.add_field(name="Effects", value="📈 +15 Happiness\n🍽️ Hunger eliminated", inline=True)
        await ctx.send(embed=embed)

    # =================================================================
    # SUPER SPY
    # =================================================================
    @commands.command(name='superspy')
    @app_commands.describe(target="Target civilization leader")
    async def super_spy_mission(self, ctx, target: Optional[guilded.Member] = None):
        if not target:
            await ctx.send("🕵️ **Elite Spy Mission**\nUsage: `.superspy <user>`")
            return
        user_id = str(ctx.author.id)
        if not self._has_hyperitem(user_id, "Spy Network"):
            await ctx.send("❌ You need a **Spy Network** HyperItem!")
            return
        target_id = str(target.id)
        target_civ = self.civ_manager.get_civilization(target_id)
        if not target_civ:
            await ctx.send("❌ Target user doesn't have a civilization!")
            return
        civ = self.civ_manager.get_civilization(user_id)

        defense = self._check_defenses(target_id, "super spy mission")
        if defense == "mirror":
            await self._reflect_with_mirror(ctx, target_id, target_civ, civ, "super spy mission")
            self.civ_manager.update_military(user_id, {"tech_level": -1})
            self.civ_manager.update_resources(user_id, {"gold": -int(civ['resources']['gold'] * 0.15)})
            await ctx.send("🕵️ **SPY MISSION REFLECTED!** Your spies were turned against you!")
            return
        elif defense == "shield":
            await self._block_with_shield(ctx, target_id, target_civ, civ, "super spy mission")
            return

        self.civ_manager.use_hyper_item(user_id, "Spy Network")
        if random.random() < 0.9:
            effects = []
            if random.random() < 0.7:
                tech_stolen = 1
                self.civ_manager.update_military(user_id, {"tech_level": tech_stolen})
                self.civ_manager.update_military(target_id, {"tech_level": -tech_stolen})
                effects.append(f"🔬 Stole {tech_stolen} tech level")
            stolen_gold = int(target_civ['resources']['gold'] * random.uniform(0.15, 0.30))
            if stolen_gold > 0:
                self.civ_manager.update_resources(user_id, {"gold": stolen_gold})
                self.civ_manager.update_resources(target_id, {"gold": -stolen_gold})
                effects.append(f"🪙 Stole {format_number(stolen_gold)} gold")
            if random.random() < 0.5:
                sabotaged = int(target_civ['military']['soldiers'] * random.uniform(0.10, 0.20))
                self.civ_manager.update_military(target_id, {"soldiers": -sabotaged})
                effects.append(f"⚔️ Sabotaged {format_number(sabotaged)} enemy soldiers")
            embed = create_embed("🕵️ Elite Spy Mission Success!",
                                 f"**{civ['name']}**'s operatives infiltrated **{target_civ['name']}**!",
                                 guilded.Color.dark_blue())
            embed.add_field(name="Mission Results", value="\n".join(effects), inline=False)
            await ctx.send(embed=embed)
        else:
            embed = create_embed("🕵️ Mission Compromised!",
                                 f"Elite spy mission against **{target_civ['name']}** was detected!",
                                 guilded.Color.red())
            await ctx.send(embed=embed)

    # =================================================================
    # MEGA INVENT
    # =================================================================
    @commands.command(name='megainvent')
    async def mega_invention(self, ctx):
        user_id = str(ctx.author.id)
        if not self._has_hyperitem(user_id, "Tech Core"):
            await ctx.send("❌ You need a **Tech Core** HyperItem!")
            return
        civ = self.civ_manager.get_civilization(user_id)
        self.civ_manager.use_hyper_item(user_id, "Tech Core")
        tech_levels = random.randint(5, 10)
        self.civ_manager.update_military(user_id, {"tech_level": tech_levels})
        embed = create_embed("🔬 TECHNOLOGICAL BREAKTHROUGH!",
                             f"**{civ['name']}** achieved a revolutionary advancement!",
                             guilded.Color.purple())
        embed.add_field(name="Advancement", value=f"🚀 Tech Level increased by {tech_levels}!", inline=False)
        await ctx.send(embed=embed)

    # =================================================================
    # BACKSTAB — NERFED, ALLY-FOCUSED
    # =================================================================
    @commands.command(name='backstab')
    @app_commands.describe(target="Target civilization leader")
    async def assassination_attempt(self, ctx, target: Optional[guilded.Member] = None):
        """Dagger assassination. MUCH stronger when target is your ally — this is a betrayal weapon.

        Allied target: 60% gold stolen, happiness forced to 0.
        Non-ally target: only 10% gold stolen, happiness -30.
        """
        if not target:
            await ctx.send("🗡️ **Assassination**\nUsage: `.backstab <user>`\nRequires: Dagger\n💀 **Doubles as an ally-killer — much stronger on allies.**")
            return
        user_id = str(ctx.author.id)
        if not self._has_hyperitem(user_id, "Dagger"):
            await ctx.send("❌ You need a **Dagger** HyperItem!")
            return
        target_id = str(target.id)
        if target_id == user_id:
            await ctx.send("❌ You cannot stab yourself!")
            return
        target_civ = self.civ_manager.get_civilization(target_id)
        if not target_civ:
            await ctx.send("❌ Target user doesn't have a civilization!")
            return
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need to start a civilization first!")
            return

        defense = self._check_defenses(target_id, "assassination attempt")
        if defense == "mirror":
            await self._reflect_with_mirror(ctx, target_id, target_civ, civ, "assassination attempt")
            self.civ_manager.update_population(user_id, {
                "happiness": -30,
                "citizens": -int(civ['population']['citizens'] * 0.1)
            })
            self.civ_manager.update_military(user_id, {
                "soldiers": -int(civ['military']['soldiers'] * 0.2),
                "spies": -int(civ['military']['spies'] * 0.3)
            })
            await ctx.send("🗡️ **ASSASSINATION REFLECTED!** Your own attempt backfired!")
            return
        elif defense == "shield":
            await self._block_with_shield(ctx, target_id, target_civ, civ, "assassination attempt")
            return

        self.civ_manager.use_hyper_item(user_id, "Dagger")

        if random.random() < 0.6:
            is_allied = self._is_allied(user_id, target_id)

            if is_allied:
                # ---- BETRAYAL: STRONG ----
                gold_pct = 0.60
                gold_lost = int(target_civ['resources']['gold'] * gold_pct)
                current_happiness = target_civ['population']['happiness']
                happiness_delta = -current_happiness  # force to 0
                citizen_loss = int(target_civ['population']['citizens'] * 0.10)
                soldier_loss = int(target_civ['military']['soldiers'] * 0.20)
                spy_loss = int(target_civ['military']['spies'] * 0.30)
                betrayal_text = "\n🗡️ **BETRAYAL!** The victim was your **ally** — the wound cuts deeper."
                header = "🗡️ Assassination Successful — Betrayal!"
            else:
                # ---- NORMAL: NERFED ----
                gold_pct = 0.10
                gold_lost = int(target_civ['resources']['gold'] * gold_pct)
                happiness_delta = -30  # just a delta
                citizen_loss = int(target_civ['population']['citizens'] * 0.03)
                soldier_loss = int(target_civ['military']['soldiers'] * 0.05)
                spy_loss = int(target_civ['military']['spies'] * 0.10)
                betrayal_text = "\n*Tip: this weapon is far deadlier against allies.*"
                header = "🗡️ Assassination Successful"

            self.civ_manager.update_population(target_id, {
                "happiness": happiness_delta,
                "citizens": -citizen_loss,
            })
            self.civ_manager.update_military(target_id, {
                "soldiers": -soldier_loss,
                "spies": -spy_loss,
            })
            self.civ_manager.update_resources(target_id, {"gold": -gold_lost})

            happiness_display = "**set to 0**" if is_allied else f"{happiness_delta}"

            embed = create_embed(
                header,
                f"**{civ['name']}**'s assassin struck down key leaders in **{target_civ['name']}**!{betrayal_text}",
                guilded.Color.dark_red() if is_allied else guilded.Color.orange()
            )
            embed.add_field(
                name="Damage",
                value=(
                    f"• 😡 Happiness {happiness_display}\n"
                    f"• 💀 {format_number(citizen_loss)} citizens killed\n"
                    f"• ⚔️ {format_number(soldier_loss)} soldiers lost\n"
                    f"• 🕵️ {format_number(spy_loss)} spies lost\n"
                    f"• 🪙 {format_number(gold_lost)} gold stolen (**{int(gold_pct*100)}%**)"
                ),
                inline=False
            )
            if not is_allied:
                embed.set_footer(text="🗡️ Non-ally targets take far less damage. Use this on allies for maximum betrayal.")
            await ctx.send(embed=embed)

            try:
                t = await self.bot.fetch_user(int(target_id))
                if is_allied:
                    await t.send(f"🗡️ **BETRAYAL!** {civ['name']} — **your ally** — assassinated your leaders! You lost {format_number(gold_lost)} gold and your happiness crashed to 0.")
                else:
                    await t.send(f"🗡️ **Assassination!** {civ['name']} struck at your leaders. You lost {format_number(gold_lost)} gold.")
            except Exception:
                pass
        else:
            self.civ_manager.update_population(user_id, {"happiness": -15})
            embed = create_embed("🗡️ Assassination Failed!",
                                 f"The attempt against **{target_civ['name']}** was thwarted!",
                                 guilded.Color.red())
            embed.add_field(name="Consequences", value="International outrage! (-15 happiness)", inline=False)
            await ctx.send(embed=embed)
            try:
                t = await self.bot.fetch_user(int(target_id))
                await t.send(f"🗡️ **Assassination Attempt!** {civ['name']} tried to assassinate your leaders but failed!")
            except Exception:
                pass

    # =================================================================
    # BOMB
    # =================================================================
    @commands.command(name='bomb')
    @app_commands.describe(target="Target civilization leader")
    async def missile_strike(self, ctx, target: Optional[guilded.Member] = None):
        if not target:
            await ctx.send("🚀 **Missile Strike**\nUsage: `.bomb <user>`\nRequires: Missiles")
            return
        user_id = str(ctx.author.id)
        if not self._has_hyperitem(user_id, "Missiles"):
            await ctx.send("❌ You need **Missiles** HyperItem!")
            return
        target_id = str(target.id)
        target_civ = self.civ_manager.get_civilization(target_id)
        if not target_civ:
            await ctx.send("❌ Target user doesn't have a civilization!")
            return
        civ = self.civ_manager.get_civilization(user_id)

        defense = self._check_defenses(target_id, "missile strike")
        if defense == "mirror":
            await self._reflect_with_mirror(ctx, target_id, target_civ, civ, "missile strike")
            population_loss = int(civ['population']['citizens'] * random.uniform(0.1, 0.25))
            military_loss = int(civ['military']['soldiers'] * random.uniform(0.2, 0.4))
            resource_damage = {
                "gold": int(civ['resources']['gold'] * random.uniform(0.1, 0.2)),
                "wood": int(civ['resources']['wood'] * random.uniform(0.15, 0.3)),
                "stone": int(civ['resources']['stone'] * random.uniform(0.15, 0.3))
            }
            self.civ_manager.update_population(user_id, {"citizens": -population_loss, "happiness": -20})
            self.civ_manager.update_military(user_id, {"soldiers": -military_loss})
            self.civ_manager.update_resources(user_id, {r: -a for r, a in resource_damage.items()})
            await ctx.send("🚀 **MISSILE STRIKE REFLECTED!**")
            return
        elif defense == "shield":
            await self._block_with_shield(ctx, target_id, target_civ, civ, "missile strike")
            return

        self.civ_manager.use_hyper_item(user_id, "Missiles")
        population_loss = int(target_civ['population']['citizens'] * random.uniform(0.1, 0.25))
        military_loss = int(target_civ['military']['soldiers'] * random.uniform(0.2, 0.4))
        resource_damage = {
            "gold": int(target_civ['resources']['gold'] * random.uniform(0.1, 0.2)),
            "wood": int(target_civ['resources']['wood'] * random.uniform(0.15, 0.3)),
            "stone": int(target_civ['resources']['stone'] * random.uniform(0.15, 0.3))
        }
        self.civ_manager.update_population(target_id, {"citizens": -population_loss, "happiness": -20})
        self.civ_manager.update_military(target_id, {"soldiers": -military_loss})
        self.civ_manager.update_resources(target_id, {r: -a for r, a in resource_damage.items()})

        embed = create_embed("🚀 Missile Strike Successful!",
                             f"**{civ['name']}** launched a devastating missile attack on **{target_civ['name']}**!",
                             guilded.Color.orange())
        damage_text = f"💀 {format_number(population_loss)} citizens\n⚔️ {format_number(military_loss)} soldiers"
        embed.add_field(name="Casualties", value=damage_text, inline=True)
        destruction_text = "\n".join([f"{'🪙' if r == 'gold' else '🪨' if r == 'stone' else '🪵'} {format_number(a)} {r.capitalize()}" for r, a in resource_damage.items() if a > 0])
        embed.add_field(name="Infrastructure Destroyed", value=destruction_text, inline=True)
        await ctx.send(embed=embed)
        try:
            t = await self.bot.fetch_user(int(target_id))
            await t.send(f"🚀 **Missile Attack!** Your civilization has been bombed by {civ['name']}!")
        except Exception:
            pass

    # =================================================================
    # CLONE — Bio-Replicator
    # =================================================================
    @commands.command(name='clone')
    async def bio_replicator(self, ctx):
        """Use Bio-Replicator. Creates a Shadow Army that mirrors your current military.

        Effect: instantly adds a mirror force equal to 50% of your current soldiers
        and 40% of your spies. The shadow troops are unstable — costs happiness.
        """
        user_id = str(ctx.author.id)
        if not self._has_hyperitem(user_id, "Bio-Replicator"):
            await ctx.send("❌ You need a **Bio-Replicator** HyperItem!")
            return
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need to start a civilization first!")
            return

        self.civ_manager.use_hyper_item(user_id, "Bio-Replicator")

        tech_mult = self._tech_mult(civ)
        mirror_soldiers = int(civ['military']['soldiers'] * 0.50 * tech_mult)
        mirror_spies = int(civ['military']['spies'] * 0.40 * tech_mult)

        self.civ_manager.update_military(user_id, {
            "soldiers": mirror_soldiers,
            "spies": mirror_spies,
        })
        # Shadow army is unstable — costs morale
        self.civ_manager.update_population(user_id, {"happiness": -15})

        embed = create_embed(
            "🧬 Bio-Replicator Activated!",
            f"**{civ['name']}** has cloned its own military! A Shadow Army now marches alongside your forces.",
            guilded.Color.dark_purple()
        )
        embed.add_field(
            name="Shadow Army Manifested",
            value=f"⚔️ +{format_number(mirror_soldiers)} shadow soldiers\n🕵️ +{format_number(mirror_spies)} shadow spies",
            inline=False
        )
        embed.add_field(name="Tech Multiplier", value=f"{tech_mult:.2f}x", inline=True)
        embed.add_field(name="Cost", value="😡 -15 happiness (they are... unsettling)", inline=True)
        embed.set_footer(text="Bio-Replicator consumed.")
        await ctx.send(embed=embed)

    # =================================================================
    # FAKE FLAG — Decoy Banner
    # =================================================================
    @commands.command(name='fakeflag')
    @app_commands.describe(target="The user you want to frame for your next attack")
    async def decoy_banner(self, ctx, target: Optional[guilded.Member] = None):
        """Use Decoy Banner. Your NEXT attack is attributed to another country.

        Sets a decoy flag on your civ. The next combat action you take will appear
        to come from the chosen country instead of you.
        """
        user_id = str(ctx.author.id)
        if not self._has_hyperitem(user_id, "Decoy Banner"):
            await ctx.send("❌ You need a **Decoy Banner** HyperItem!")
            return

        if not target:
            await ctx.send("🎭 **Decoy Banner**\nUsage: `.fakeflag <user>`\nYour next attack will be attributed to that user.")
            return

        target_id = str(target.id)
        if target_id == user_id:
            await ctx.send("❌ You cannot frame yourself!")
            return

        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need to start a civilization first!")
            return

        target_civ = self.civ_manager.get_civilization(target_id)
        if not target_civ:
            await ctx.send("❌ That user doesn't have a civilization!")
            return

        self.civ_manager.use_hyper_item(user_id, "Decoy Banner")

        # Persist the flag on the civ
        flags = civ.get('hyperitem_flags', {})
        flags['decoy_banner_attacker'] = target_id
        flags['decoy_banner_attacker_name'] = target_civ['name']
        self.db.update_civilization(user_id, {"hyperitem_flags": flags})

        embed = create_embed(
            "🎭 Decoy Banner Set!",
            f"**{civ['name']}** has prepared a false flag operation.\n"
            f"Your **next attack** will be attributed to **{target_civ['name']}**.",
            guilded.Color.dark_teal()
        )
        embed.add_field(name="How it works", value="When you attack, the target will think the assault came from the decoy nation.", inline=False)
        embed.set_footer(text="Decoy Banner consumed. Effect lasts until your next attack.")
        await ctx.send(embed=embed)

    # =================================================================
    # GLITCH PROTOCOL — Corrupted
    # =================================================================
    @commands.command(name='glitch_protocol')
    async def glitch_protocol(self, ctx):
        """Use Corrupted (Glitch Protocol). Randomizes the outcome of the next attack used against you.

        Sets a flag that the next incoming attack will be rerolled via pure RNG —
        no calculation, no stats, just chaos. Pure 💀📈 terror.
        """
        user_id = str(ctx.author.id)
        if not self._has_hyperitem(user_id, "Corrupted"):
            await ctx.send("❌ You need a **Corrupted** HyperItem!")
            return
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need to start a civilization first!")
            return

        self.civ_manager.use_hyper_item(user_id, "Corrupted")

        flags = civ.get('hyperitem_flags', {})
        flags['glitch_protocol_active'] = True
        self.db.update_civilization(user_id, {"hyperitem_flags": flags})

        embed = create_embed(
            "💀 GLITCH PROTOCOL ACTIVE",
            f"**{civ['name']}** is now protected by pure, unfiltered chaos.",
            guilded.Color.dark_red()
        )
        embed.add_field(
            name="Effect",
            value="The **next attack** against you will have its outcome fully randomized — "
                  "attacker wins, defender wins, or mutual disaster — rolled by pure RNG.",
            inline=False
        )
        embed.add_field(name="Odds", value="🎲 50% attack fails harmlessly\n🎲 30% attack backfires on the attacker\n🎲 20% attack succeeds normally", inline=False)
        embed.set_footer(text="Corrupted consumed. Effect lasts until your next incoming attack.")
        await ctx.send(embed=embed)


async def setup(bot):
    await bot.add_cog(HyperItemCommands(bot))
