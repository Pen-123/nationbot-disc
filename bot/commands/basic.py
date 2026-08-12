import random
import discord
import os
import aiohttp
import asyncio
import logging
import json
from typing import Literal, Optional
from datetime import datetime, timedelta
from collections import defaultdict, deque
from discord.ext import commands
from discord import app_commands
from bot.utils import format_number, get_ascii_art, create_embed
from bot import config

# Import subregion data from territory.py
from bot.commands.territory import PROVINCES, SUBREGION_TO_CONTINENT, SUBREGION_DATA, ALL_SUBREGIONS, FORBIDDEN_START_PROVINCES

logger = logging.getLogger(__name__)

MAX_CONVERSATION_HISTORY = 100
CONVERSATION_TIMEOUT = 1800

class BasicCommands(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.db = bot.db
        self.civ_manager = bot.civ_manager
        self.groq_key = os.getenv('GROQ_API_KEY')
        self.openrouter_key = os.getenv('OPENROUTER')
        self.openai_key = os.getenv('OPENAI_API_KEY')
        self.current_model = "llama-3.3-70b-versatile"
        self.model_switch_time = None
        self.rate_limited = False
        self.conversations = defaultdict(deque)
        self.last_interaction = {}
        self.saved_chats = set()

    # ---- Firestore-based territory helper ----
    def _get_all_owned_provinces(self) -> list:
        territories = self.db.get_all_territories()
        all_provinces = []
        for territory_name, data in territories.items():
            owner = data.get("owner_id")
            if owner:
                all_provinces.append(territory_name)
        return all_provinces

    def _get_conversation_history(self, user_id):
        history = []
        for msg in self.conversations[user_id]:
            history.append({
                "role": "user" if msg['is_user'] else "assistant",
                "content": msg['content']
            })
        return history

    def _update_conversation(self, user_id, is_user, content):
        now = datetime.now()
        self.last_interaction[user_id] = now
        self.conversations[user_id].append({
            "is_user": is_user,
            "content": content,
            "timestamp": now
        })
        if len(self.conversations[user_id]) > MAX_CONVERSATION_HISTORY:
            self.conversations[user_id].clear()
            return False
        if user_id not in self.saved_chats:
            expired_users = []
            for uid, last_time in list(self.last_interaction.items()):
                if (now - last_time).total_seconds() > CONVERSATION_TIMEOUT:
                    expired_users.append(uid)
            for uid in expired_users:
                try:
                    del self.conversations[uid]
                    del self.last_interaction[uid]
                except KeyError:
                    pass
        return True

    @commands.command(name='reset')
    async def reset_civilization(self, ctx):
        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You don't have a civilization to reset!")
            return
        embed = discord.Embed(
            title="⚠️ CIVILIZATION RESET CONFIRMATION",
            description="**This action is PERMANENT and cannot be undone!**",
            color=0xff0000
        )
        embed.add_field(
            name="You will lose:",
            value="• All resources and progress\n• Your military and population\n• Your territory and items\n• Your region and ideology",
            inline=False
        )
        embed.add_field(
            name="Confirmation Required:",
            value="Type `CONFIRM RESET` exactly as shown to reset your civilization.",
            inline=False
        )
        embed.set_footer(text="This action cannot be reversed!")
        await ctx.send(embed=embed)

        def check(m):
            return m.author.id == ctx.author.id and m.channel.id == ctx.channel.id
        try:
            msg = await self.bot.wait_for('message', timeout=30.0, check=check)
            if msg.content == "CONFIRM RESET":
                if self.civ_manager.reset_civilization(user_id):
                    if user_id in self.saved_chats:
                        self.saved_chats.remove(user_id)
                    if user_id in self.conversations:
                        del self.conversations[user_id]
                    if user_id in self.last_interaction:
                        del self.last_interaction[user_id]
                    success_embed = discord.Embed(
                        title="🗑️ Civilization Reset",
                        description="Your civilization has been completely reset.",
                        color=0x00ff00
                    )
                    success_embed.add_field(
                        name="What's Next?",
                        value="Use `.start <name>` to create a new civilization and begin your journey again!",
                        inline=False
                    )
                    await ctx.send(embed=success_embed)
                else:
                    await ctx.send("❌ Failed to reset civilization. Please try again later.")
            else:
                await ctx.send("🛑 Reset cancelled. Your civilization is safe.")
        except asyncio.TimeoutError:
            await ctx.send("🕒 Reset confirmation timed out. Your civilization is safe.")

    @commands.command(name='sv')
    async def start_saved_chat(self, ctx):
        user_id = str(ctx.author.id)
        if user_id in self.saved_chats:
            await ctx.send("💾 You already have a saved chat running! Use `.svc` to close it.")
            return
        self.saved_chats.add(user_id)
        if user_id not in self.conversations:
            self.conversations[user_id] = deque()
            self.last_interaction[user_id] = datetime.now()
        embed = discord.Embed(
            title="💾 Saved Chat Started",
            description="Your conversation will now be saved until you use `.svc` to close it.",
            color=0x00ff00
        )
        embed.add_field(
            name="Features:",
            value="• No 30-minute timeout\n• Persistent across bot restarts\n• Up to 100 messages\n• Use `.svc` to close and delete",
            inline=False
        )
        await ctx.send(embed=embed)

    @commands.command(name='svc')
    async def close_saved_chat(self, ctx):
        user_id = str(ctx.author.id)
        if user_id not in self.saved_chats:
            await ctx.send("❌ You don't have a saved chat running! Use `.sv` to start one.")
            return
        if user_id in self.conversations:
            del self.conversations[user_id]
        if user_id in self.last_interaction:
            del self.last_interaction[user_id]
        self.saved_chats.remove(user_id)
        embed = discord.Embed(
            title="🗑️ Saved Chat Closed",
            description="Your saved chat has been closed and all conversation history deleted.",
            color=0x00ff00
        )
        await ctx.send(embed=embed)

    @commands.Cog.listener()
    async def on_message(self, message):
        if message.author.bot:
            return
        user_id = str(message.author.id)
        content = (message.content or "").strip()

        is_reply = False
        replied_message = None
        if getattr(message, "reference", None) and getattr(message.reference, "message_id", None):
            try:
                replied_message = await message.channel.fetch_message(message.reference.message_id)
                if replied_message and replied_message.author.id == self.bot.user.id:
                    is_reply = True
            except Exception as e:
                logger.error(f"Error fetching replied message: {e}")

        bot_mentioned = False
        try:
            mentions = getattr(message, "mentions", []) or []
            bot_mentioned = any((getattr(u, "id", None) == self.bot.user.id) for u in mentions)
        except Exception:
            bot_mentioned = False

        if not (bot_mentioned or is_reply):
            return

        if bot_mentioned:
            try:
                content = content.replace(f'<@{self.bot.user.id}>', '').strip()
            except Exception:
                pass

        if is_reply and user_id in self.conversations and len(self.conversations[user_id]) >= MAX_CONVERSATION_HISTORY:
            try:
                await message.reply("Chat limit reached! Starting new conversation.", mention_author=False)
            except Exception:
                logger.error("Failed to send chat limit message")
            if user_id in self.conversations:
                del self.conversations[user_id]
            if user_id in self.last_interaction:
                del self.last_interaction[user_id]
            return

        if bot_mentioned and not is_reply and user_id not in self.saved_chats:
            self.conversations[user_id] = deque()
            self.last_interaction[user_id] = datetime.now()

        if not content:
            if bot_mentioned:
                try:
                    await message.reply(embed=create_embed(
                        "🤖 NationBot Assistant",
                        "Hello! I'm here to help you with NationBot. Ask me about:\n"
                        "- Starting your civilization (`.start`)\n"
                        "- Managing resources (`.status`)\n"
                        "- Military commands (`.warhelp`)\n"
                        "- Ideologies and strategies\n\n"
                        "Try asking: 'How do I declare war?' or 'What does fascism do?'",
                        discord.Color.blue()
                    ), mention_author=False)
                    self._update_conversation(user_id, False, "Hello! How can I assist with NationBot today?")
                except Exception:
                    logger.exception("Failed to send default mention reply")
            return

        civ = None
        try:
            civ = self.civ_manager.get_civilization(user_id)
        except Exception:
            logger.exception("Failed to fetch civ for context")
            civ = None

        civ_status = ""
        if civ:
            try:
                civ_status = (
                    f"Player's Civilization: {civ['name']} (Ideology: {civ.get('ideology', 'none')})\n"
                    f"Resources: 🪙{format_number(civ['resources'].get('gold',0))} "
                    f"🌾{format_number(civ['resources'].get('food',0))} "
                    f"🪨{format_number(civ['resources'].get('stone',0))} "
                    f"🪵{format_number(civ['resources'].get('wood',0))}\n"
                    f"Military: ⚔️{format_number(civ['military'].get('soldiers',0))} "
                    f"🕵️{format_number(civ['military'].get('spies',0))}\n"
                )
            except Exception:
                civ_status = ""

        # ----- COMPREHENSIVE SYSTEM PROMPT - UPDATED WITH ALL SYSTEMS -----
        system_prompt = f"""You are NationBot, an AI assistant for a nation simulation game. 
Players build civilizations, manage resources, wage wars, and form alliances. 
Your role is to help players understand game mechanics and strategies.

{civ_status}

**CURRENT BALANCE (from config.py):**
- Economy gains use config.ECONOMY values (e.g., gather base 5-20, cap 500k).
- Tax is nerfed: base 1 per citizen, happiness penalty -5, higher population loss.
- Soldier cost: 20 gold each.
- Expansion: large provinces (≥1M km²) cost 1 soldier per 2,000 km² (small: 1 per 595 km²). Navy gives 25% off, airforce gives 50% off (land only).
- Territory factor: max 3.0x.
- Starting resources: {config.STARTING_RESOURCES}

**NEW SYSTEMS:**
- **Corporations**: Build up to 5 corporations (100k each, upgrade for 200k) for passive income. Tech 5 unlocks full potential.
- **Megaprojects**: Build world-changing projects (Great Wall, Space Program, Global Bank, AI Network, Green Energy). Requires Tech 4-8, costs millions.
- **Policies**: Enable/upgrade policies (Military Service, Agricultural Subsidies, Trade Agreements, Public Education, Environmental Protection, Industrial Innovation). Costs gold/resources, gives permanent bonuses.
- **Mid-game buffs**: Harvest, Drill, Labor, Raidcaravan now scale with population, tech, and territory.
- **Buy Tech**: `.buytech` (or `.buylevel`) – 2000 gold per tech level (max 10).
- **Cheer Up**: `.cheerup` – 2000 gold for 50% happiness boost.
- **Cards**: Buy cards with `.buycard` (500 gold), view with `.cards view`, use with `.cards use "Card Name"`.
- **Testing Mode**: Admins can toggle `/testmode on|off` – sets everyone's resources to 999,999,999 (restores on disable).

**KEY GAME CONCEPTS:**
- Resources: gold, food, stone, wood (capped per command)
- Military: soldiers, spies, tech_level (max 10), navy ships, airforce planes
- Population: citizens, happiness (0-100), hunger (0-100)
- Territory: land_size (km²) – affects resource gains via territory factor
- Ideologies: fascism, democracy, communism, theocracy, anarchy, destruction, pacifist, socialism, terrorism, capitalism, federalism, monarchy
- Regions: Subregions with unique provinces (one per player)
- Countryballs: Collectible factions unlocked by completing subregions – give passive bonuses and synergies
- Industrial Revolution: Permanent micromanagement challenge – reach 1000 Industrial Power (once per player)
- HyperItems: Powerful one-time items from Black Market or rare drops
- Borders: Defensive structures that boost battle defense
- Navy & Airforce: Used for expansion – navy required for overseas expansion (different continent). Navy gives 25% cost reduction, airforce gives 50% reduction for land expansions.

**FULL COMMAND LIST (alphabetical by category):**

**Basic Commands:**
`.start <name>` – Start a new civilization with cinematic intro
`.status` – View your civilization status
`.ideology <type>` – Choose your government ideology (12 options)
`.regions [subregion]` – View or select your region (e.g., `.regions western europe`)
`.reset` – Permanently reset your civilization (requires confirmation)
`.sv` – Start a saved chat (no timeout)
`.svc` – Close and delete saved chat
`.warhelp [category]` – Show command categories or list commands in a category
`.updates` – View the roadmap and update log

**Economy Commands (all resource gains use config.ECONOMY, tax is nerfed):**
`.gather` – Gather random resources – 1min cooldown
`.work <amount>` – Employ citizens to gain gold – 1min cooldown
`.farm` – Farm food – 1min cooldown
`.mine` – Mine stone and wood – 1min cooldown
`.harvest` – Large harvest, scales with pop & happiness – no cooldown
`.drill` – Extract rare minerals (Tech 2), scales with tech & pop – 1min cooldown
`.fish` – Fish for food or treasure – 1min cooldown
`.tax` – Collect taxes (nerfed) – 5min cooldown
`.lottery <bet>` – Gamble gold – 1min cooldown
`.invest <amount>` – Invest gold (returns after 2 hours) – 5min cooldown
`.raidcaravan` – Raid caravans, scales with soldiers & territory – 5min cooldown
`.labor` – Forced labor, scales with pop & tech (Tech 3) – 5min cooldown
`.advertise` – Attract new citizens – 10min cooldown
`.census` – Display your stats
`.recruit <number>` – Convert citizens to soldiers
`.buysoldiers <amount>` – Buy soldiers (20 gold each)
`.sell <item>` – Sell hyper items
`.burn` – Reduce all resources to 1000
`.immigration` – Open borders (risk of riots) – 10min cooldown
`.cheerup` – 50% happiness boost for 2000 gold – 10min cooldown
`.buytech` – Buy one tech level for 2000 gold (max 10)

**Corporations (passive income):**
`.corporation` – View your corporations
`.corporation build` – Build a new corp (100k gold, max 5)
`.corporation upgrade <number>` – Upgrade a corp (200k gold)
`.corporation list` – List all corps

**Megaprojects (late-game):**
`.megaproject` – List available projects
`.megaproject build <name>` – Build a megaproject (costs millions, requires tech)
`.megaproject list` – List available projects

**Policies (permanent bonuses):**
`.policy` – View your active policies
`.policy enable <name>` – Enable a policy (costs resources)
`.policy upgrade <name>` – Upgrade a policy to next level
`.policy disable <name>` – Disable a policy
`.policieshelp` – Show detailed policy help

**Military & War Commands:**
`.train <soldiers|spies> <amount>` – Train units (2min cooldown)
`.find` – Find wandering soldiers (1min cooldown)
`.declare <user>` – Declare war
`.attack <user> <level>` – Direct attack (level 1-10, 3min cooldown)
`.siege <user>` – Lay siege (10min cooldown, needs 50 soldiers)
`.stealthbattle <user>` – Spy stealth attack (4min cooldown, needs 3 spies)
`.peace <user>` – Offer peace
`.accept_peace <user>` – Accept peace
`.addborder` – Build defensive border (5min cooldown)
`.removeborder` – Remove border (2min cooldown)
`.rectract <percentage>` – Assign soldiers to border (1min)
`.retrieve <percentage>` – Retrieve soldiers from border (1min)
`.borderinfo` – Check border status (1min)
`.buildship <type> <amount>` – Build navy ships (no cooldown)
`.buildplane <type> <amount>` – Build airforce planes (no cooldown)
`.tech <ground|naval|air> [amount]` – Upgrade military tech (500 gold/level)
`.trainboost [amount]` – Increase training level (max 3, 500 gold/level)
`.navy` – View navy fleet
`.airforce` – View airforce fleet
`.cards [view|use] <card_name>` – View or use purchased cards
`.buycard` – Buy a random card for 500 gold

**Diplomacy Commands:**
`.ally <user> <alliance_name>` – Propose an alliance
`.acceptally <alliance_id>` – Accept alliance
`.rejectally <alliance_id>` – Reject alliance
`.break` – Break current alliance
`.send <user> <resource> <amount>` – Send resources to ally
`.trade <user> <offer> <offer_amount> <request> <request_amount>` – Propose trade
`.accepttrade <trade_id>` – Accept trade
`.rejecttrade <trade_id>` – Reject trade
`.mail <user> <message>` – Send diplomatic message
`.inbox` – Check pending proposals
`.coalition <target_alliance>` – Form coalition

**Store & HyperItems:**
`.store [item]` – View or purchase upgrades
`.blackmarket` – Enter Black Market (1000 gold)
`.inventory` – View HyperItems and upgrades
`.market` – Black Market info
`.laststand` – Use Last Stand (under 500 gold)
`.sacrifice <user>` – Mutual destruction (24h cooldown)
`.mirror` – Display Mirror status
`.nuke <user>` – Nuclear strike
`.obliterate <user>` – Obliterate civilization
`.shield` – Display Anti-Nuke Shield status
`.luckystrike` – Use Lucky Charm
`.propaganda <user>` – Steal enemy soldiers
`.hiremercs` – Hire mercenaries
`.boosttech` – Advance technology
`.mintgold` – Generate large gold
`.superharvest` – Massive food
`.superspy <user>` – Elite espionage
`.megainvent` – Advance multiple tech levels
`.backstab <user>` – Assassination attempt
`.bomb <user>` – Missile strike

**Territory Commands:**
`.territories` – List owned provinces
`.expand <province>` – Claim a province (overseas requires navy, navy 25% off, airforce 50% off land)
`.rapidexpansion <province>` – Claim using soldiers (2× cost, cap 10000)
`.map` – Show world map

**Countryball Commands:**
`.openpacks` – Unlock countryballs for completed subregions
`.evolve` – Manually check evolution
`.packs` – View collection
`.activate <ball_name>` – Activate a countryball (max 3)
`.deactivate <ball_name>` – Deactivate
`.synergies` – Show active synergies

**Industrial Revolution (once per player):**
`.industrial_start` – Begin (requires `yes` confirmation)
`.industrial_status` – View all stats
`.industrial_build` – Build a factory
`.industrial_tech` – Research technology
`.industrial_workers` – Train workers
`.industrial_cleanup` – Reduce pollution
`.industrial_railway` – Build railways
`.industrial_transport` – Improve transport
`.industrial_army` – Raise military protection
`.industrial_policy` – Enact a new policy
`.industrial_import` – Import raw materials
`.industrial_export` – Export goods
`.industrial_steam` – Research steam power
`.industrial_mine` – Build a mine
`.industrial_hospital` – Build a hospital
`.industrial_school` – Build a school
`.industrial_law` – Enforce law and order
`.industrial_trade` – Diplomatic trade
`.industrial_aid` – Request foreign aid
`.industrial_suppress` – Suppress revolts
`.industrial_bribe` – Bribe workers
`.industrial_automate` – Automate factories
`.industrial_upgrade` – Upgrade factories
`.industrial_relief` – Disaster relief
`.industrial_expand` – Expand cities
`.industrial_banking` – Invest in banking (max 5 uses)
`.industrial_nationalize` – Nationalize industry
`.indushelp` – Show all commands

**ExtraEconomy (gambling & jobs):**
`.extrawork` – Work for gold (job required)
`.extrastore [buy <item>]` – Buy items
`.extrainventory` – Show inventory
`.extragamble <amount>` – Gamble gold
`.extracards <amount>` – Play cards
`.slots <amount>` – Slots
`.blackjack <amount>` – Blackjack
`.jobs` – List jobs
`.job <job_type>` – Apply for job
`.arrest <user>` – Arrest (police)
`.rob <user>` – Rob (criminal)
`.code <project>` – Start coding project
`.darkweb <item>` – Dark web purchase
`.setbalance <amount>` – Admin: set gold

**Admin Commands:**
`.sync [scope]` – Sync slash commands (owner only)
`.exportdb` – Export database
`.testmode <on|off>` – Toggle testing mode (admin only)

**STRATEGY TIPS:**
- Build navy before overseas expansion – gives 25% off and is required.
- Airforce gives 50% off for land expansions – build planes for cheaper land grabs.
- Use `.buysoldiers` (20 gold each) to quickly raise armies.
- `.cheerup` gives 50% happiness for 2000 gold – use when morale is low.
- Buy tech levels (`.buytech`) for permanent global modifiers.
- Corporations give passive income – build them early.
- Megaprojects are late-game resource sinks with huge permanent bonuses.
- Policies give stacking bonuses – enable and upgrade them over time.
- Complete subregions to unlock countryballs for passive bonuses and synergies.
- Industrial Revolution gives huge rewards but every command has 30% disaster chance.
- Black Market has pity system: guaranteed Uncommon every 3, Rare every 6, Legendary every 10 purchases.

You are helpful, encouraging, and strategic. Keep responses concise and focused on gameplay.
If asked about non-game topics, politely decline. Use brief Discord-style formatting.
Address the player as 'President' and keep a confident, commanding tone.
When appropriate, include tactical suggestions and short examples.

IMPORTANT: Use Discord markdown formatting in your responses:
- **Bold** for emphasis
- *Italics* for subtle emphasis
- __Underline__ for important points
- `Inline code` for commands and code references
- > Blockquotes for special notes
- --- for dividers
- Use emoji where appropriate: 🏛️ ⚔️ 🪙 🌾 🪨 🪵 👥 🕵️

Remember to keep responses engaging but focused on the game.
"""

        try:
            messages = [{"role": "system", "content": system_prompt}]
            if user_id in self.conversations and self.conversations[user_id]:
                history = self._get_conversation_history(user_id)
                messages.extend(history)
            messages.append({"role": "user", "content": content})

            response = await self.generate_ai_response(messages)

            update_success = self._update_conversation(user_id, True, content)
            if not update_success:
                response += "\n\n💬 *Note: Chat history limit reached. Starting a new conversation.*"
                if user_id not in self.saved_chats:
                    self.conversations[user_id] = deque()
                    self.last_interaction[user_id] = datetime.now()

            self._update_conversation(user_id, False, response)

            try:
                await message.reply(response, mention_author=False)
            except Exception:
                try:
                    await message.channel.send(response)
                except Exception:
                    logger.exception("Failed to send AI response to channel")
        except Exception as e:
            logger.error(f"AI response error: {e}", exc_info=True)
            try:
                await message.reply("I'm having trouble thinking right now. Please try again later!", mention_author=False)
            except Exception:
                pass

    async def generate_ai_response(self, messages):
        if self.groq_key:
            headers = {
                "Authorization": f"Bearer {self.groq_key}",
                "Content-Type": "application/json"
            }
            payload = {
                "model": self.current_model,
                "messages": messages,
                "max_tokens": 500,
                "temperature": 0.7
            }
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        "https://api.groq.com/openai/v1/chat/completions",
                        headers=headers,
                        json=payload,
                        timeout=60
                    ) as response:
                        text = await response.text()
                        if response.status == 200:
                            data = await response.json()
                            return data['choices'][0]['message']['content']
                        raise Exception(f"Groq API error {response.status}: {text}")
            except Exception:
                logger.exception("Groq request failed")

        if self.openrouter_key:
            headers = {
                "Authorization": f"Bearer {self.openrouter_key}",
                "Content-Type": "application/json"
            }
            if self.rate_limited and self.model_switch_time and datetime.now() < self.model_switch_time:
                model = "moonshotai/kimi-k2:free"
            else:
                model = self.current_model
                self.rate_limited = False

            payload = {
                "model": model,
                "messages": messages,
                "max_tokens": 500
            }

            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post("https://openrouter.ai/api/v1/chat/completions",
                                            headers=headers, json=payload, timeout=60) as response:
                        text = await response.text()
                        if response.status == 200:
                            data = await response.json()
                            return data['choices'][0]['message']['content']
                        elif response.status == 429:
                            self.rate_limited = True
                            self.model_switch_time = datetime.now() + timedelta(hours=24)
                            logger.warning("OpenRouter rate limited; switching to fallback model for 24 hours")
                            payload["model"] = "moonshotai/kimi-k2:free"
                            async with session.post("https://openrouter.ai/api/v1/chat/completions",
                                                    headers=headers, json=payload, timeout=60) as fallback_response:
                                if fallback_response.status == 200:
                                    data = await fallback_response.json()
                                    return data['choices'][0]['message']['content']
                                errtxt = await fallback_response.text()
                                raise Exception(f"Fallback model failed: {fallback_response.status} - {errtxt}")
                        else:
                            raise Exception(f"OpenRouter API error {response.status}: {text}")
            except Exception:
                logger.exception("OpenRouter failed, will try OpenAI if available")

        if self.openai_key:
            headers = {
                "Authorization": f"Bearer {self.openai_key}",
                "Content-Type": "application/json"
            }
            payload = {
                "model": "gpt-3.5-turbo",
                "messages": messages,
                "max_tokens": 500,
                "temperature": 0.7
            }
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post("https://api.openai.com/v1/chat/completions",
                                            headers=headers, json=payload, timeout=60) as response:
                        text = await response.text()
                        if response.status == 200:
                            data = await response.json()
                            return data['choices'][0]['message']['content']
                        raise Exception(f"OpenAI API error {response.status}: {text}")
            except Exception:
                logger.exception("OpenAI request failed")

        logger.error("No configured AI provider available or all providers failed")
        return ("AI is unavailable right now. Please make sure the bot has an API key set "
                "via GROQ_API_KEY, OPENROUTER, or OPENAI_API_KEY, and try again later.")

    # ---------- WARHELP (UPDATED with chunking to avoid 1024 char limit) ----------
    @commands.command(name='warhelp')
    async def warhelp(self, ctx, category: str = None):
        """
        Show command categories or list commands for a specific category.
        Usage: .warhelp             -> lists all categories
               .warhelp <category>  -> lists commands in that category
        """
        categories = {
            "basic": {
                "name": "🏛️ Basic Commands",
                "description": "Essential civilization management",
                "commands": {
                    "ideology": "Choose your civilization's government ideology",
                    "regions": "View or select your civilization's region",
                    "reset": "Reset your civilization (irreversible!)",
                    "start": "Start a new civilization with a cinematic intro",
                    "status": "View your civilization status",
                    "sv": "Start a saved chat with the AI (no timeout)",
                    "svc": "Close and delete your saved chat",
                    "warhelp": "Show this help menu",
                    "updates": "View the roadmap and update log"
                }
            },
            "diplomacy": {
                "name": "🤝 Diplomacy Commands",
                "description": "Alliances, trade, and messages",
                "commands": {
                    "acceptally": "Accept a pending alliance proposal",
                    "accepttrade": "Accept a pending trade proposal",
                    "ally": "Propose an alliance with another civilization",
                    "break": "Break your current alliance",
                    "coalition": "Form a coalition against another alliance",
                    "inbox": "Check your pending alliance, trade proposals, and diplomatic messages",
                    "mail": "Send a diplomatic message to another civilization",
                    "rejectally": "Reject a pending alliance proposal",
                    "rejecttrade": "Reject a pending trade proposal",
                    "send": "Send resources to an ally",
                    "trade": "Propose a resource trade with another civilization"
                }
            },
            "extraeconomy": {
                "name": "🎲 ExtraEconomy",
                "description": "Jobs, gambling, and extra economy commands",
                "commands": {
                    "arrest": "Arrest a target (police job)",
                    "blackjack": "Play a game of blackjack",
                    "code": "Start a coding project",
                    "darkweb": "Purchase risky items from the dark web",
                    "extracards": "Play a card game against the bot",
                    "extragamble": "Gamble gold for a chance to win",
                    "extrainventory": "Show your inventory",
                    "extrastore": "Buy items from the store",
                    "extrawork": "Work to earn gold",
                    "job": "Apply for a specific job",
                    "jobs": "List available jobs",
                    "rob": "Attempt to rob another user",
                    "setbalance": "Set your gold balance (admin only)",
                    "slots": "Play the slot machine"
                }
            },
            "economy": {
                "name": "💰 Economy",
                "description": "Resource gathering, taxes, immigration, and soldier buying",
                "commands": {
                    "advertise": "Run promotional campaigns to attract new citizens",
                    "burn": "Burn excess resources down to 1000 each",
                    "buysoldiers": "Buy soldiers with gold at 20 gold each",
                    "census": "Display current gold and population status",
                    "cheer": "Spread cheer to boost citizen happiness",
                    "cheerup": "50% happiness boost for 2000 gold",
                    "drill": "Extract rare minerals with advanced drilling (Tech 2)",
                    "drive": "Unemploy citizens, freeing them from work",
                    "farm": "Farm food for your civilization",
                    "festival": "Hold a grand festival to boost happiness",
                    "fish": "Fish for food or occasionally find treasure",
                    "gather": "Gather random resources from your territory",
                    "harvest": "Large harvest that scales with population and happiness",
                    "immigration": "Open borders to gain citizens (risk of protests/riots)",
                    "invest": "Invest gold for delayed profit",
                    "labor": "Forced labor for wood/stone (Tech 3, scales with pop)",
                    "lottery": "Gamble gold for a chance at the jackpot",
                    "mine": "Mine stone and wood from your territory",
                    "raidcaravan": "Raid NPC merchant caravans for loot",
                    "recruit": "Convert citizens into soldiers",
                    "sell": "Sell hyper items to wandering merchants",
                    "tax": "Collect taxes from your citizens (now nerfed)",
                    "work": "Employ citizens to work and gain immediate gold",
                    "buytech": "Purchase one tech level for 2000 gold (max 10)"
                }
            },
            "corporations": {
                "name": "🏢 Corporations",
                "description": "Build and manage corporations for passive income",
                "commands": {
                    "corporation": "View your corporations",
                    "corporation build": "Build a new corporation (100k gold, max 5)",
                    "corporation upgrade <number>": "Upgrade a corporation (200k gold)",
                    "corporation list": "List all your corporations"
                }
            },
            "megaprojects": {
                "name": "🏗️ Megaprojects",
                "description": "Build world-changing projects with permanent bonuses",
                "commands": {
                    "megaproject": "List available megaprojects",
                    "megaproject build <name>": "Build a megaproject (costs millions, requires tech)",
                    "megaproject list": "List available projects"
                }
            },
            "policies": {
                "name": "📜 Policies",
                "description": "Enable and upgrade policies for permanent bonuses",
                "commands": {
                    "policy": "View your active policies",
                    "policy enable <name>": "Enable a policy at level 1",
                    "policy upgrade <name>": "Upgrade a policy to the next level",
                    "policy disable <name>": "Disable an active policy",
                    "policieshelp": "Show detailed policy information"
                }
            },
            "hyperitems": {
                "name": "💎 HyperItem Commands",
                "description": "Powerful one‑time items",
                "commands": {
                    "backstab": "Use Dagger for assassination attempt",
                    "bomb": "Use Missiles for mid‑tier military strike",
                    "boosttech": "Use Ancient Scroll to instantly advance technology",
                    "hiremercs": "Use Mercenary Contract to hire professional soldiers",
                    "laststand": "Use Last Stand when under 500 gold",
                    "luckystrike": "Use Lucky Charm for guaranteed critical success",
                    "megainvent": "Use Tech Core to advance multiple technology levels",
                    "mintgold": "Use Gold Mint to generate large amounts of gold",
                    "mirror": "Display Mirror status – reflects ANY attack",
                    "nuke": "Launch a devastating nuclear attack (Warhead required)",
                    "obliterate": "Completely obliterate a civilization (HyperLaser)",
                    "propaganda": "Use Propaganda Kit to steal enemy soldiers",
                    "sacrifice": "Destroy both your civilization and another (mutual destruction)",
                    "shield": "Display Anti‑Nuke Shield status",
                    "superharvest": "Use Harvest Engine for massive food",
                    "superspy": "Use Spy Network for elite espionage"
                }
            },
            "military": {
                "name": "⚔️ Military Commands",
                "description": "War, borders, navy, airforce, and cards",
                "commands": {
                    "accept_peace": "Accept a peace offer from another civilization",
                    "addborder": "Build a defensive border (5min cooldown)",
                    "airforce": "View your airforce fleet",
                    "attack": "Launch a direct attack (3min cooldown)",
                    "borderinfo": "Check your border status (1min cooldown)",
                    "buildplane": "Build airforce planes (no cooldown)",
                    "buildship": "Build navy ships (no cooldown)",
                    "cards": "View or use your purchased cards",
                    "declare": "Declare war on another civilization",
                    "find": "Search for wandering soldiers (1min cooldown)",
                    "navy": "View your navy fleet",
                    "peace": "Offer peace to an enemy civilization",
                    "rectract": "Assign soldiers to the border (1min cooldown)",
                    "removeborder": "Remove your border and retrieve soldiers (2min)",
                    "retrieve": "Retrieve soldiers from the border (1min cooldown)",
                    "siege": "Lay siege to an enemy (10min cooldown)",
                    "stealthbattle": "Conduct a spy‑based stealth attack (4min)",
                    "tech": "Upgrade military tech (500 gold per level)",
                    "train": "Train military units (2min cooldown)",
                    "trainboost": "Increase soldier training level (max 3)"
                }
            },
            "store": {
                "name": "🏪 Store Commands",
                "description": "Upgrades and black market",
                "commands": {
                    "blackmarket": "Purchase random HyperItems (no cooldown)",
                    "buycard": "Purchase a random card for 500 gold",
                    "inventory": "View your HyperItems and store upgrades",
                    "market": "Display information about the Black Market",
                    "store": "View the civilization store and purchase upgrades"
                }
            },
            "industrial": {
                "name": "🏭 Industrial Revolution",
                "description": "Micromanagement challenge (once per player)",
                "commands": {
                    "industrial_start": "Begin the revolution (confirmation required)",
                    "industrial_status": "View all 25+ stats",
                    "industrial_build": "Build a factory",
                    "industrial_tech": "Research technology",
                    "industrial_workers": "Train workers",
                    "industrial_cleanup": "Reduce pollution",
                    "industrial_railway": "Build railways",
                    "industrial_transport": "Improve transport",
                    "industrial_army": "Raise military protection",
                    "industrial_policy": "Enact a new policy",
                    "industrial_import": "Import raw materials",
                    "industrial_export": "Export goods",
                    "industrial_steam": "Research steam power",
                    "industrial_mine": "Build a mine",
                    "industrial_hospital": "Build a hospital",
                    "industrial_school": "Build a school",
                    "industrial_law": "Enforce law and order",
                    "industrial_trade": "Diplomatic trade",
                    "industrial_aid": "Request foreign aid",
                    "industrial_suppress": "Suppress revolts",
                    "industrial_bribe": "Bribe workers",
                    "industrial_automate": "Automate factories",
                    "industrial_upgrade": "Upgrade factories",
                    "industrial_relief": "Disaster relief",
                    "industrial_expand": "Expand cities",
                    "industrial_banking": "Invest in banking (max 5 uses)",
                    "industrial_nationalize": "Nationalize industry",
                    "indushelp": "Show all Industrial Revolution commands"
                }
            },
            "territory": {
                "name": "🗺️ Territory & Countryballs",
                "description": "Expansion, maps, and countryball collection (config-driven costs)",
                "commands": {
                    "activate": "Activate a countryball as a manager",
                    "deactivate": "Deactivate a countryball manager",
                    "evolve": "Manually check evolution for countryballs",
                    "expand": "Claim a province. Overseas expansion requires navy. Navy: 25% off, Airforce: 50% off (land only). Cost uses config.EXPANSION.",
                    "map": "Show the world map",
                    "openpacks": "Unlock countryballs for completed subregions",
                    "packs": "View your countryball collection",
                    "rapidexpansion": "Claim a province using only soldiers (2× cost, cap 10000, reductions apply)",
                    "synergies": "Show active synergy bonuses",
                    "territories": "List your owned provinces"
                }
            },
            "other": {
                "name": "📌 Other",
                "description": "Miscellaneous commands",
                "commands": {
                    "help": "Shows this message",
                    "updates": "View the roadmap and update log"
                }
            }
        }

        def chunk_value(text: str, max_len: int = 1000) -> List[str]:
            """Split a string into chunks of max_len characters."""
            if len(text) <= max_len:
                return [text]
            chunks = []
            while text:
                if len(text) <= max_len:
                    chunks.append(text)
                    break
                # Try to break at a newline or comma
                split_pos = text[:max_len].rfind('\n')
                if split_pos == -1:
                    split_pos = text[:max_len].rfind(',')
                if split_pos == -1:
                    split_pos = max_len
                chunks.append(text[:split_pos])
                text = text[split_pos:].lstrip()
            return chunks

        if category is None:
            embed = discord.Embed(
                title="🤖 NationBot – Command Categories",
                description="Use `.warhelp <category>` to see commands in that category.\nAvailable categories:",
                color=discord.Color.blue()
            )
            for key, data in categories.items():
                embed.add_field(
                    name=data["name"],
                    value=f"*{data['description']}*",
                    inline=False
                )
            embed.set_footer(text="Example: .warhelp military")
            await ctx.send(embed=embed)
            return

        category_key = category.lower()
        if category_key not in categories:
            await ctx.send(f"❌ Unknown category `{category}`. Use `.warhelp` to see all categories.")
            return

        cat = categories[category_key]
        cmd_list = []
        for cmd, desc in cat["commands"].items():
            cmd_list.append(f"`{cmd}` – {desc}")

        full_text = "\n".join(cmd_list)

        # Chunk the text to avoid exceeding 1024 characters per field
        chunks = chunk_value(full_text, 1000)
        embed = discord.Embed(
            title=f"{cat['name']}",
            description=f"*{cat['description']}*",
            color=discord.Color.green()
        )

        if len(chunks) == 1:
            embed.add_field(name="Commands", value=chunks[0], inline=False)
        else:
            for i, chunk in enumerate(chunks, 1):
                embed.add_field(name=f"Commands (Part {i})", value=chunk, inline=False)

        embed.set_footer(text="Use .warhelp for categories")
        await ctx.send(embed=embed)

    # ---------- UPDATES COMMAND (Roadmap & Update Log) ----------
    @commands.command(name='updates')
    async def show_updates(self, ctx):
        """View the NationBot roadmap and update log."""
        embed = discord.Embed(
            title="📋 NationBot – Roadmap & Update Log",
            description="Here's what's been added and what's coming next.",
            color=discord.Color.gold()
        )

        # ---- Roadmap ----
        roadmap = (
            "**🟢 Recent Additions (v1.5):**\n"
            "• Mid-game buffs: Harvest, Drill, Labor, Raidcaravan now scale with progress\n"
            "• Corporation system: Build & upgrade for passive income\n"
            "• Megaprojects: World-changing projects with permanent bonuses\n"
            "• Policies: Enable/upgrade policies for stacking buffs\n"
            "• Cheer Up: 50% happiness boost for 2000 gold\n"
            "• Buy Tech: Purchase tech levels for 2000 gold each\n"
            "• Buy Cards: Purchase random cards for 500 gold\n"
            "• Testing mode for admins\n\n"
            "**🟡 Upcoming Features (v2.0):**\n"
            "• State-level expansion (instead of whole countries)\n"
            "• AI nations that expand, form alliances, and defend\n"
            "• Full-scale war system with troop movement and state capture\n"
            "• More megaprojects and policies\n"
            "• Advanced diplomacy (treaties, trade routes)\n"
            "• Economy rework: more late-game sinks\n"
            "• Performance optimisations"
        )
        embed.add_field(name="🚀 Roadmap", value=roadmap, inline=False)

        # ---- Update Log ----
        update_log = (
            "**v1.5 (Current) – 2026-08-12**\n"
            "• Mid-game commands now scale with population, tech, and territory\n"
            "• Corporation system with passive income (build/upgrade/list)\n"
            "• Megaprojects (Great Wall, Space Program, Global Bank, AI Network, Green Energy)\n"
            "• Policies system (Military Service, Agricultural Subsidies, Trade Agreements, Public Education, Environmental Protection, Industrial Innovation)\n"
            "• `.cheerup` command (50% happiness for 2000 gold)\n"
            "• `.buytech` command (buy tech levels for 2000 gold)\n"
            "• `.buycard` command (buy random cards for 500 gold)\n"
            "• Testing mode for admins (`/testmode on|off`)\n"
            "• `.warhelp` now splits long fields to avoid Discord limits\n"
            "• All balance parameters moved to `config.py`\n"
            "• Navy and airforce now persist correctly\n"
            "• Cooldowns removed from build commands\n\n"
            "**v1.4 – 2026-08-10**\n"
            "• Fixed expansion costs (1 per 2000 km² for large provinces)\n"
            "• Buffed economy (tax nerfed, mid-game buffed)\n"
            "• Added `.immigration`, `.labor`, `.buysoldiers`\n"
            "• Improved AI system prompt\n\n"
            "**v1.3 – 2026-08-08**\n"
            "• Countryball system added\n"
            "• Industrial Revolution rework\n"
            "• Territory expansion rework\n"
            "• Map generation fixed"
        )
        embed.add_field(name="📜 Update Log", value=update_log, inline=False)

        embed.set_footer(text="Check back often for new features! | Use .warhelp for commands")
        await ctx.send(embed=embed)

    # ---------- REGIONS COMMAND ----------
    @commands.command(name='regions')
    @app_commands.describe(region_name="Subregion to select (e.g., 'western europe')")
    async def regions_command(self, ctx, *, region_name: str = None):
        """
        Choose a subregion for your civilization.
        Usage: .regions <subregion_name>
        Examples: .regions western europe, .regions east asia, .regions brazil
        """
        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need to start a civilization first! Use `.start <name>`")
            return

        if not region_name:
            embed = discord.Embed(
                title="🌍 Available Subregions",
                description="Choose a subregion to start your civilization. Each subregion provides unique bonuses based on its continent and provinces are **unique** – once a province is taken, no one else can claim it.\n\nTo select one, use `.regions <subregion_name>` (e.g., `.regions western europe`).",
                color=0x00ff00
            )
            grouped = {}
            for sub in ALL_SUBREGIONS:
                continent = SUBREGION_TO_CONTINENT.get(sub, "Unknown")
                grouped.setdefault(continent, []).append(sub)
            for continent, sublist in grouped.items():
                available_subregions = []
                for sub in sorted(sublist):
                    provinces = PROVINCES.get(sub, [])
                    all_owned = self._get_all_owned_provinces()
                    available = [p for p in provinces if p not in all_owned and p not in FORBIDDEN_START_PROVINCES]
                    if available:
                        available_subregions.append(sub)
                if available_subregions:
                    embed.add_field(name=continent, value=", ".join(available_subregions), inline=False)
                else:
                    embed.add_field(name=continent, value="*All subregions in this continent are fully claimed or not startable.*", inline=False)
            embed.set_footer(text=f"Your current region: {civ.get('region', 'None')}")
            await ctx.send(embed=embed)
            return

        input_name = region_name.strip().lower()
        matched_subregion = None
        for sub in ALL_SUBREGIONS:
            if sub.lower() == input_name:
                matched_subregion = sub
                break
        if not matched_subregion:
            for sub in ALL_SUBREGIONS:
                if input_name in sub.lower():
                    matched_subregion = sub
                    break
        if not matched_subregion:
            await ctx.send(f"❌ Unknown subregion: `{region_name}`. Use `.regions` to see available subregions.")
            return

        if civ.get('region'):
            await ctx.send(f"❌ You've already selected the **{civ['region']}** region. Region selection cannot be changed.")
            return

        provinces_in_subregion = PROVINCES.get(matched_subregion, [])
        if not provinces_in_subregion:
            await ctx.send(f"❌ Subregion **{matched_subregion}** has no provinces.")
            return

        all_owned = self._get_all_owned_provinces()
        available_provinces = [p for p in provinces_in_subregion if p not in all_owned and p not in FORBIDDEN_START_PROVINCES]

        if not available_provinces:
            await ctx.send(f"❌ All startable provinces in **{matched_subregion}** have already been claimed. Choose another subregion.")
            return

        chosen_province = random.choice(available_provinces)

        continent = SUBREGION_TO_CONTINENT.get(matched_subregion, "Unknown")
        continent_bonuses = {
            "Europe": {"gold": 300, "tech_level": 1},
            "Asia": {"food": 200, "population": 50},
            "Africa": {"stone": 150, "wood": 150},
            "North America": {"gold": 200, "food": 200},
            "South America": {"food": 300, "wood": 100},
            "Oceania": {"food": 250, "happiness": 15},
            "Antarctica": {"research": 25},
            "Unknown": {"gold": 100, "food": 100}
        }
        bonuses = continent_bonuses.get(continent, continent_bonuses["Unknown"])

        updated_resources = civ['resources'].copy()
        updated_population = civ['population'].copy()
        for resource, amount in bonuses.items():
            if resource in updated_resources:
                updated_resources[resource] += amount
            elif resource == "population":
                updated_population['citizens'] += amount
            elif resource == "happiness":
                updated_population['happiness'] = min(100, updated_population['happiness'] + amount)
            elif resource == "research":
                current_bonuses = civ.get('bonuses', {})
                current_bonuses['research_speed'] = current_bonuses.get('research_speed', 0) + amount
                self.db.update_civilization(user_id, {'bonuses': current_bonuses})

        update_data = {
            'region': matched_subregion,
            'resources': updated_resources,
            'population': updated_population,
        }

        if self.db.update_civilization(user_id, update_data):
            territory_cog = self.bot.get_cog("TerritoryCog")
            if territory_cog:
                success = territory_cog._add_province(user_id, chosen_province)
                if not success:
                    await ctx.send("❌ Failed to assign starting province. Please contact an admin.")
                    return
            else:
                await ctx.send("❌ Territory system not available. Please contact an admin.")
                return

            area = territory_cog.province_areas.get(chosen_province, 1000) if territory_cog else 1000
            bonus_text = ", ".join([f"+{amount} {resource}" for resource, amount in bonuses.items()])
            embed = discord.Embed(
                title=f"🌍 Region Selected: {matched_subregion}",
                description=f"Your civilization is now established in **{matched_subregion}** ({continent}).",
                color=0x00ff00
            )
            embed.add_field(name="Bonuses Applied", value=bonus_text, inline=False)
            embed.add_field(name="🏛️ Starting Province", value=f"You have been granted **{chosen_province}** (area: {area:,} km²).", inline=False)
            embed.add_field(name="🎉 Nation Complete!", value="Use `.status` to view your complete stats and `.warhelp` to see all available commands.", inline=False)
            await ctx.send(embed=embed)
        else:
            await ctx.send("❌ Failed to update your region. Please try again later.")

    # ---------- START COMMAND ----------
    @commands.command(name='start')
    @app_commands.describe(civ_name="Name of your civilization")
    async def start_civilization(self, ctx, *, civ_name: str = None):
        """Start a new civilization with a cinematic intro."""
        if not civ_name:
            await ctx.send("❌ Please provide a civilization name: `.start <civilization_name>`")
            return
        user_id = str(ctx.author.id)
        if self.civ_manager.get_civilization(user_id):
            await ctx.send("❌ You already have a civilization! Use `.status` to view it.")
            return
        intro_art = get_ascii_art("civilization_start")
        founding_events = [
            ("🏛️ **Golden Dawn**: Your people discovered ancient gold deposits!", {"gold": 200}),
            ("🌾 **Fertile Lands**: Blessed with rich soil for farming!", {"food": 300}),
            ("🏗️ **Master Builders**: Your citizens are natural architects!", {"stone": 150, "wood": 150}),
            ("👥 **Population Boom**: Word of your great leadership spreads!", {"population": 50}),
            ("⚡ **Lightning Strike**: A divine sign brings good fortune!", {"gold": 100, "happiness": 20})
        ]
        event_text, bonus_resources = random.choice(founding_events)
        name_bonuses = {}
        special_message = ""
        if "ink" in civ_name.lower():
            name_bonuses["luck_bonus"] = 5
            special_message = "🖋️ *The pen will never forget your work.* (+5% luck)"
        elif "pen" in civ_name.lower():
            name_bonuses["diplomacy_bonus"] = 5
            special_message = "🖋️ *The pen is mightier than the sword.* (+5% diplomacy success)"
        hyper_item = None
        if random.random() < 0.05:
            common_items = ["Lucky Charm", "Propaganda Kit", "Mercenary Contract"]
            hyper_item = random.choice(common_items)
        self.civ_manager.create_civilization(user_id, civ_name, bonus_resources, name_bonuses, hyper_item)
        embed = discord.Embed(title=f"🏛️ The Founding of {civ_name}", description=f"{intro_art}\n\n{event_text}\n{special_message}", color=0x00ff00)
        if hyper_item:
            embed.add_field(name="🎁 Rare Discovery!", value=f"Your scouts found a **{hyper_item}**! This powerful item unlocks special abilities.", inline=False)
        embed.add_field(name="📋 Next Steps", value="Choose your government ideology with `.ideology <type>`\nSelect your region with `.regions`\nView your status with `.status`", inline=False)
        await ctx.send(embed=embed)

    @commands.command(name='ideology')
    @app_commands.describe(ideology_type="Government ideology")
    @app_commands.choices(ideology_type=[
        app_commands.Choice(name="fascism", value="fascism"),
        app_commands.Choice(name="democracy", value="democracy"),
        app_commands.Choice(name="communism", value="communism"),
        app_commands.Choice(name="theocracy", value="theocracy"),
        app_commands.Choice(name="anarchy", value="anarchy"),
        app_commands.Choice(name="destruction", value="destruction"),
        app_commands.Choice(name="pacifist", value="pacifist"),
        app_commands.Choice(name="socialism", value="socialism"),
        app_commands.Choice(name="terrorism", value="terrorism"),
        app_commands.Choice(name="capitalism", value="capitalism"),
        app_commands.Choice(name="federalism", value="federalism"),
        app_commands.Choice(name="monarchy", value="monarchy"),
    ])
    async def choose_ideology(self, ctx, ideology_type: Optional[Literal["fascism", "democracy", "communism", "theocracy", "anarchy", "destruction", "pacifist", "socialism", "terrorism", "capitalism", "federalism", "monarchy"]] = None):
        if not ideology_type:
            ideologies = {
                "fascism": "+25% soldier training speed, -15% diplomacy success, -10% luck",
                "democracy": "+20% happiness, +10% trade profit, slower soldier training (-15%)",
                "communism": "Equal resource distribution (+10% citizen productivity), -10% tech speed",
                "theocracy": "+15% propaganda success, +5% happiness, -10% tech speed",
                "anarchy": "Random events happen twice as often, 0 soldier upkeep, -20% spy success",
                "destruction": "+35% combat strength, +40% soldier training, -25% resources, -30% happiness, -50% diplomacy",
                "pacifist": "+35% happiness, +25% population growth, +20% trade profit, -60% soldier training, -40% combat, +25% diplomacy",
                "socialism": "+15% citizen productivity, +10% happiness from welfare, -10% trade profit",
                "terrorism": "+40% guerrilla/raid effectiveness, +30% spy success, -50% diplomacy, increases unrest",
                "capitalism": "+20% trade profit, +15% gold generation, -10% happiness due to inequality",
                "federalism": "+10% stability, +10% diplomacy, +5% regional production, minor tech tradeoffs",
                "monarchy": "+10% loyalty/happiness, +10% soldier morale, -10% reform speed"
            }
            embed = discord.Embed(title="🏛️ Government Ideologies", color=0x0099ff)
            for name, description in ideologies.items():
                embed.add_field(name=name.capitalize(), value=description, inline=False)
            embed.add_field(name="Usage", value="`.ideology <type>`", inline=False)
            await ctx.send(embed=embed)
            return
        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You need to start a civilization first! Use `.start <name>`")
            return
        if civ.get('ideology'):
            await ctx.send("❌ You have already chosen an ideology! It cannot be changed.")
            return
        ideology_type = ideology_type.lower()
        valid_ideologies = ["fascism", "democracy", "communism", "theocracy", "anarchy", "destruction", "pacifist", "socialism", "terrorism", "capitalism", "federalism", "monarchy"]
        if ideology_type not in valid_ideologies:
            await ctx.send(f"❌ Invalid ideology! Choose from: {', '.join(valid_ideologies)}")
            return
        self.civ_manager.set_ideology(user_id, ideology_type)
        ideology_descriptions = {
            "fascism": "⚔️ **Fascism**: Your military grows strong, but diplomacy suffers.",
            "democracy": "🗳️ **Democracy**: Your people are happy and trade flourishes.",
            "communism": "🏭 **Communism**: Workers unite for the collective good.",
            "theocracy": "⛪ **Theocracy**: Divine blessing guides your civilization.",
            "anarchy": "💥 **Anarchy**: Chaos reigns, but freedom has no limits.",
            "destruction": "💥 **Destruction**: Y o u. m o n s t e r.",
            "pacifist": "🕊️ **Pacifist**: Your civilization thrives in peace and harmony.",
            "socialism": "🤝 **Socialism**: Welfare and shared prosperity — steady growth, modest trade penalties.",
            "terrorism": "🔥 **Terrorism**: Operates from the shadows — excels at raids and covert ops but ruins diplomacy.",
            "capitalism": "💹 **Capitalism**: Commerce and wealth generation reign; inequality can lower happiness.",
            "federalism": "🏛️ **Federalism**: Regions manage themselves well — improved stability and diplomacy.",
            "monarchy": "👑 **Monarchy**: Tradition and loyalty strengthen your rule; reforms are slower."
        }
        embed = discord.Embed(title=f"🏛️ Ideology Chosen: {ideology_type.capitalize()}", description=ideology_descriptions[ideology_type], color=0x00ff00)
        embed.add_field(name="🎉 Nation Almost Complete!", value="Your civilization is nearly ready! **Select your region with `.regions`** to complete your nation setup and receive regional bonuses.", inline=False)
        await ctx.send(embed=embed)

    @commands.command(name='status')
    async def civilization_status(self, ctx):
        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        if not civ:
            await ctx.send("❌ You don't have a civilization yet! Use `.start <name>` to begin.")
            return
        embed = discord.Embed(title=f"🏛️ {civ['name']}", description=f"**Leader**: {ctx.author.name}\n**Ideology**: {civ['ideology'].capitalize() if civ.get('ideology') else 'None'}\n**Region**: {civ.get('region', 'Not selected')}", color=0x0099ff)
        resources = civ['resources']
        embed.add_field(name="💰 Resources", value=f"🪙 Gold: {format_number(resources['gold'])}\n🌾 Food: {format_number(resources['food'])}\n🪨 Stone: {format_number(resources['stone'])}\n🪵 Wood: {format_number(resources['wood'])}", inline=True)
        population = civ['population']
        military = civ['military']
        embed.add_field(name="👥 Population & Military", value=f"👤 Citizens: {format_number(population['citizens'])}\n😊 Happiness: {population['happiness']}%\n🍽️ Hunger: {population['hunger']}%\n⚔️ Soldiers: {format_number(military['soldiers'])}\n🕵️ Spies: {format_number(military['spies'])}", inline=True)
        territory = civ['territory']
        hyper_items = civ.get('hyper_items', [])
        embed.add_field(name="🗺️ Territory & Items", value=f"🏞️ Land Size: {format_number(territory['land_size'])} km²\n🎁 HyperItems: {len(hyper_items)}\n" + ("\n".join(f"• {item}" for item in hyper_items[:5]) + ("..." if len(hyper_items) > 5 else "")), inline=True)
        await ctx.send(embed=embed)


async def setup(bot):
    await bot.add_cog(BasicCommands(bot))
