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

        # ----- COMPREHENSIVE SYSTEM PROMPT - UPDATED FOR CONFIG AND BALANCE -----
        system_prompt = f"""You are NationBot, an AI assistant for a nation simulation game. 
Players build civilizations, manage resources, wage wars, and form alliances. 
Your role is to help players understand game mechanics and strategies.

{civ_status}

**CURRENT BALANCE (from config.py):**
- Economy gains use config.ECONOMY values (e.g., gather base 5-20, cap 500k).
- Tax is nerfed: base 1 per citizen, happiness penalty -5, higher population loss.
- Soldier cost: 20 gold each.
- Expansion: large provinces (≥1M km²) cost 1 soldier per {config.EXPANSION['soldier_per_area_large']:,} km² (small: 1 per {config.EXPANSION['soldier_per_area_small']} km²). Navy gives 25% off, airforce gives 50% off (land only).
- Territory factor: max {config.TERRITORY_FACTOR['max']}x, coefficient {config.TERRITORY_FACTOR['coefficient']}.
- Starting resources: {config.STARTING_RESOURCES}

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
- Cards: Purchasable for 500 gold (`.buycard`) – give bonuses or one-time effects
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

**Economy Commands (all resource gains use config.ECONOMY, tax is nerfed):**
`.gather` – Gather random resources (gold, wood, stone, food) – 1min cooldown
`.work <amount>` – Employ citizens to gain gold – 1min cooldown
`.farm` – Farm food – 1min cooldown
`.mine` – Mine stone and wood – 1min cooldown
`.harvest` – Large harvest (no cooldown but longer)
`.drill` – Extract rare minerals (requires Tech Level 2) – 1min cooldown
`.fish` – Fish for food or treasure – 1min cooldown
`.tax` – Collect taxes (5min cooldown, now heavily nerfed: lower yield, -5 happiness, higher population loss chance)
`.lottery <bet>` – Gamble gold for jackpot – 1min cooldown
`.invest <amount>` – Invest gold (returns after 2 hours) – 5min cooldown
`.raidcaravan` – Raid NPC caravans for loot (5min cooldown, needs 5 soldiers)
`.labor` – Forced labor for wood/stone (5min cooldown, -15 happiness, needs 5 soldiers)
`.advertise` – Attract new citizens (10min cooldown)
`.census` – Display your gold and population stats
`.recruit <number>` – Convert citizens into soldiers (risk of failure)
`.buysoldiers <amount>` – Buy soldiers with gold at 20 gold each (no cooldown)
`.sell <item>` – Sell hyper items to merchants
`.burn` – Reduce all resources to 1000 if above that
`.immigration` – Open borders to gain citizens (10min cooldown, risk of protests/riots)

**Military & War Commands:**
`.train <soldiers|spies> <amount>` – Train military units (2min cooldown)
`.find` – Search for wandering soldiers (1min cooldown)
`.declare <user>` – Declare war on another civilization
`.attack <user> <level>` – Launch direct attack (level 1-10, 3min cooldown)
`.siege <user>` – Lay siege to enemy (10min cooldown, needs 50 soldiers)
`.stealthbattle <user>` – Spy-based stealth attack (4min cooldown, needs 3 spies)
`.peace <user>` – Offer peace to enemy
`.accept_peace <user>` – Accept a peace offer
`.addborder` – Build defensive border (5min cooldown, costs resources)
`.removeborder` – Remove border and retrieve soldiers (2min cooldown)
`.rectract <percentage>` – Assign soldiers to border (1min cooldown)
`.retrieve <percentage>` – Retrieve soldiers from border (1min cooldown)
`.borderinfo` – Check border status (1min cooldown)
`.buildship <type> <amount>` – Build navy ships (no cooldown)
`.buildplane <type> <amount>` – Build airforce planes (no cooldown, requires Industrial Revolution + Air Tech 3 for attackers/bombers)
`.tech <ground|naval|air> [amount]` – Upgrade military tech (500 gold per level, max 10)
`.trainboost [amount]` – Increase soldier training level (max 3, 500 gold per level)
`.navy` – View your navy fleet
`.airforce` – View your airforce fleet
`.cards [view|use] <card_name>` – View or use your purchased cards

**Diplomacy Commands:**
`.ally <user> <alliance_name>` – Propose an alliance
`.acceptally <alliance_id>` – Accept alliance proposal
`.rejectally <alliance_id>` – Reject alliance proposal
`.break` – Break your current alliance
`.send <user> <resource> <amount>` – Send resources to an ally
`.trade <user> <offer_resource> <offer_amount> <request_resource> <request_amount>` – Propose a trade
`.accepttrade <trade_id>` – Accept a trade
`.rejecttrade <trade_id>` – Reject a trade
`.mail <user> <message>` – Send diplomatic message
`.inbox` – Check pending proposals and messages
`.coalition <target_alliance>` – Form a coalition against another alliance

**Store & HyperItems:**
`.store [item]` – View or purchase permanent upgrades
`.blackmarket` – Enter Black Market (1000 gold entry) – get random HyperItem (no cooldown)
`.inventory` – View your HyperItems and upgrades
`.market` – Display Black Market info
`.laststand` – Use Last Stand (under 500 gold, 60min cooldown)
`.sacrifice <user>` – Mutual destruction (24h cooldown, requires Sacrifice)
`.mirror` – Display Mirror status (reflects ANY attack)
`.nuke <user>` – Nuclear strike (5min cooldown, requires Nuclear Warhead)
`.obliterate <user>` – Obliterate civilization (13min cooldown, requires HyperLaser)
`.shield` – Display Anti-Nuke Shield status
`.luckystrike` – Use Lucky Charm (60min cooldown)
`.propaganda <user>` – Steal enemy soldiers (3min cooldown, requires Propaganda Kit)
`.hiremercs` – Hire professional soldiers (10min cooldown, requires Mercenary Contract)
`.boosttech` – Advance technology (5min cooldown, requires Ancient Scroll)
`.mintgold` – Generate large gold (10min cooldown, requires Gold Mint)
`.superharvest` – Massive food production (10min cooldown, requires Harvest Engine)
`.superspy <user>` – Elite espionage (10min cooldown, requires Spy Network)
`.megainvent` – Advance multiple tech levels (5min cooldown, requires Tech Core)
`.backstab <user>` – Assassination attempt (180min cooldown, requires Dagger)
`.bomb <user>` – Missile strike (1min cooldown, requires Missiles)
`.buycard` – Purchase a random card for 500 gold (no cooldown)

**Territory Commands (config-driven expansion costs):**
`.territories` – List your owned provinces
`.expand <province>` – Claim a province. 
   - Cost formula: uses config.EXPANSION (1 per {config.EXPANSION['soldier_per_area_large']:,} km² for large provinces, 1 per {config.EXPANSION['soldier_per_area_small']} for small).
   - Overseas (different continent) requires navy.
   - Navy gives 25% off, airforce gives 50% off (land only).
`.rapidexpansion <province>` – Claim using only soldiers (2× normal cost, cap {config.EXPANSION['rapid_max_soldier_cost']}, reductions apply).
`.map` – Show the world map with civilization ownership

**Countryball Commands:**
`.openpacks` – Unlock countryballs for completed subregions
`.evolve` – Manually check evolution for all your countryballs
`.packs` – View your countryball collection
`.activate <ball_name>` – Activate a countryball as a manager (max 3)
`.deactivate <ball_name>` – Deactivate a countryball manager
`.synergies` – Show active synergy bonuses

**Industrial Revolution Commands (once per player):**
`.industrial_start` – Begin the revolution (requires `yes` confirmation)
`.industrial_status` – View all 25+ stats
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
`.industrial_banking` – Invest in banking
`.industrial_nationalize` – Nationalize industry
`.indushelp` – Show all Industrial Revolution commands

**ExtraEconomy (gambling & jobs):**
`.extrawork` – Work for gold (job required, 5min cooldown)
`.extrastore [buy <item>]` – Buy items from store
`.extrainventory` – Show inventory
`.extragamble <amount>` – Gamble gold (1min cooldown)
`.extracards <amount>` – Play a card game against bot (1min cooldown)
`.slots <amount>` – Play slot machine (1min cooldown)
`.blackjack <amount>` – Play blackjack (1min cooldown)
`.jobs` – List available jobs
`.job <job_type>` – Apply for a job (1min cooldown)
`.arrest <user>` – Arrest target (police job, 1min cooldown)
`.rob <user>` – Rob target (criminal job, 1min cooldown)
`.code <project>` – Start a coding project (virus/website/messenger)
`.darkweb <item>` – Purchase risky items from dark web (50% scam)
`.setbalance <amount>` – Admin: set your gold balance

**Admin Commands:**
`.sync [scope]` – Sync slash commands (owner only)
`.exportdb` – Export the database file (owner only)

**STRATEGY TIPS:**
- Build a navy before expanding overseas – it gives 25% off and is required for non‑neighbour provinces.
- Airforce gives 50% off for land expansions (neighbouring subregions) – build planes for cheaper land grabs.
- Use `.buysoldiers` to quickly raise an army (20 gold each).
- `.immigration` gives citizens but can trigger riots – use with care.
- Tax is now weak – focus on active gathering and raids.
- Buy cards for permanent bonuses – they cost 500 gold each.
- Complete subregions to unlock countryballs for passive bonuses and synergies.
- The Industrial Revolution gives huge rewards but is risky – every command has 30% disaster chance.
- Black Market has pity system: guaranteed Uncommon every 3, Rare every 6, Legendary every 10 purchases.
- Always declare war before attacking.

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

    # ---------- WARHELP (updated with all categories) ----------
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
                    "warhelp": "Show this help menu"
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
                    "drill": "Extract rare minerals with advanced drilling",
                    "drive": "Unemploy citizens, freeing them from work",
                    "farm": "Farm food for your civilization",
                    "festival": "Hold a grand festival to boost happiness",
                    "fish": "Fish for food or occasionally find treasure",
                    "gather": "Gather random resources from your territory",
                    "harvest": "Large harvest with longer cooldown",
                    "immigration": "Open borders to gain citizens (risk of protests/riots)",
                    "invest": "Invest gold for delayed profit",
                    "labor": "Forced labor for wood/stone (costs happiness)",
                    "lottery": "Gamble gold for a chance at the jackpot",
                    "mine": "Mine stone and wood from your territory",
                    "raidcaravan": "Raid NPC merchant caravans for loot",
                    "recruit": "Convert citizens into soldiers",
                    "sell": "Sell hyper items to wandering merchants",
                    "tax": "Collect taxes from your citizens (now nerfed)",
                    "work": "Employ citizens to work and gain immediate gold"
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
                    "industrial_banking": "Invest in banking",
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
                    "help": "Shows this message"
                }
            }
        }

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

        if len(cmd_list) > 25:
            cmd_list = cmd_list[:25] + ["… and more"]

        embed = discord.Embed(
            title=f"{cat['name']}",
            description=f"*{cat['description']}*",
            color=discord.Color.green()
        )
        embed.add_field(name="Commands", value="\n".join(cmd_list), inline=False)
        embed.set_footer(text="Use .warhelp for categories")
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
