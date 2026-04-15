import random
import discord as guilded
import os
import aiohttp
import asyncio
import logging
from typing import Literal, Optional
from datetime import datetime, timedelta
from collections import defaultdict, deque
from discord.ext import commands
from discord import app_commands
from bot.utils import format_number, get_ascii_art, create_embed

logger = logging.getLogger(__name__)

# Constants
MAX_CONVERSATION_HISTORY = 100  # Increased to 100 messages max
CONVERSATION_TIMEOUT = 1800  # 30 minutes in seconds

class BasicCommands(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.db = bot.db
        self.civ_manager = bot.civ_manager
        self.groq_key = os.getenv('GROQ_API_KEY')
        self.openrouter_key = os.getenv('OPENROUTER')
        self.openai_key = os.getenv('OPENAI_API_KEY')  # fallback option
        self.current_model = "llama-3.3-70b-versatile"
        self.model_switch_time = None
        self.rate_limited = False
        
        # Conversation tracking
        self.conversations = defaultdict(deque)  # user_id: deque of messages
        self.last_interaction = {}  # user_id: timestamp
        self.saved_chats = set()  # user_ids with saved chats

        self.help_categories = {
            "basic": {
                "title": "🏛️ Basic Commands",
                "description": "Essential civilization management",
                "commands": {
                    "/start <name>": "Begin your civilization journey",
                    "/ideology <type>": "Choose government type",
                    "/status": "View your civilization stats",
                    "/regions": "Select your region for bonuses",
                    "/reset": "⚠️ Reset your civilization (irreversible!)",
                    "/sv": "💾 Start saved chat with AI",
                    "/svc": "🗑️ Close saved chat"
                }
            },
            "economy": {
                "title": "💰 Economy Commands",
                "description": "Resource management & jobs",
                "commands": {
                    "/extrawork": "Work to earn gold (5min cd)",
                    "/extrastore": "View special items shop",
                    "/extrainventory": "Check your inventory",
                    "/farm /mine /fish": "Gather resources",
                    "/tax": "Collect taxes from citizens",
                    "/invest <amt>": "Invest for future profit",
                    "/job <type>": "Apply for special jobs"
                }
            },
            "military": {
                "title": "⚔️ Military Commands",
                "description": "Warfare and defense",
                "commands": {
                    "/train soldiers/spies <amt>": "Train military units",
                    "/declare <user>": "Declare war on another civ",
                    "/attack <user>": "Launch direct attack",
                    "/siege <user>": "Lay siege to territory",
                    "/find": "Recruit wandering soldiers",
                    "/addborder /removeborder": "Manage defenses",
                    "/cards": "Use unlocked battle cards"
                }
            },
            "diplomacy": {
                "title": "🤝 Diplomacy Commands",
                "description": "Alliances and trade",
                "commands": {
                    "/ally <user>": "Propose alliance",
                    "/trade <user> ...": "Trade resources",
                    "/peace <user>": "Offer peace treaty",
                    "/accept_peace <user>": "Accept peace offer",
                    "/mail <user> <msg>": "Send diplomatic message",
                    "/inbox": "Check pending requests"
                }
            },
            "items": {
                "title": "💎 HyperItem Commands",
                "description": "Powerful special items",
                "commands": {
                    "/inventory": "View your HyperItems",
                    "/blackmarket": "Risky item marketplace",
                    "/nuke <user>": "Nuclear attack (Warhead)",
                    "/shield": "Anti-nuke defense (Shield)",
                    "/propaganda <user>": "Steal soldiers (Kit)",
                    "/luckystrike": "Guaranteed crit (Charm)"
                }
            }
        }

    def _get_conversation_history(self, user_id):
        """Get formatted conversation history for a user"""
        history = []
        for msg in self.conversations[user_id]:
            history.append({
                "role": "user" if msg['is_user'] else "assistant",
                "content": msg['content']
            })
        return history

    def _update_conversation(self, user_id, is_user, content):
        """Update conversation history for a user"""
        now = datetime.now()
        self.last_interaction[user_id] = now
        
        # Add new message to history
        self.conversations[user_id].append({
            "is_user": is_user,
            "content": content,
            "timestamp": now
        })
        
        # Check if we've reached the 100 message limit
        if len(self.conversations[user_id]) > MAX_CONVERSATION_HISTORY:
            # Clear the conversation and notify user
            self.conversations[user_id].clear()
            return False
            
        # Clean up expired conversations (only for non-saved chats)
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
        """Reset your civilization (irreversible!)"""
        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        
        if not civ:
            await ctx.send("❌ You don't have a civilization to reset!")
            return
            
        # Create confirmation embed
        embed = guilded.Embed(
            title="⚠️ **CIVILIZATION RESET CONFIRMATION** ⚠️",
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
        
        # Wait for confirmation
        def check(m):
            return m.author.id == ctx.author.id and m.channel.id == ctx.channel.id
            
        try:
            msg = await self.bot.wait_for('message', timeout=30.0, check=check)
            
            if msg.content == "CONFIRM RESET":
                # Reset civilization
                if self.civ_manager.reset_civilization(user_id):
                    # Also clear any saved chat
                    if user_id in self.saved_chats:
                        self.saved_chats.remove(user_id)
                    if user_id in self.conversations:
                        del self.conversations[user_id]
                    if user_id in self.last_interaction:
                        del self.last_interaction[user_id]
                        
                    success_embed = guilded.Embed(
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
        """Start a saved chat with the AI (no timeout)"""
        user_id = str(ctx.author.id)
        
        if user_id in self.saved_chats:
            await ctx.send("💾 You already have a saved chat running! Use `.svc` to close it.")
            return
            
        self.saved_chats.add(user_id)
        
        # Initialize or preserve conversation
        if user_id not in self.conversations:
            self.conversations[user_id] = deque()
            self.last_interaction[user_id] = datetime.now()
        
        embed = guilded.Embed(
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
        """Close and delete your saved chat"""
        user_id = str(ctx.author.id)
        
        if user_id not in self.saved_chats:
            await ctx.send("❌ You don't have a saved chat running! Use `.sv` to start one.")
            return
            
        # Clear conversation data
        if user_id in self.conversations:
            del self.conversations[user_id]
        if user_id in self.last_interaction:
            del self.last_interaction[user_id]
            
        self.saved_chats.remove(user_id)
        
        embed = guilded.Embed(
            title="🗑️ Saved Chat Closed",
            description="Your saved chat has been closed and all conversation history deleted.",
            color=0x00ff00
        )
        
        await ctx.send(embed=embed)

    @commands.Cog.listener()
    async def on_message(self, message):
        """Respond to mentions with AI assistance"""
        # Skip if message is from bot
        try:
            if message.author.bot:
                return
        except Exception:
            # If message object doesn't have .author or .bot attribute as expected, bail
            return
            
        user_id = str(message.author.id)
        content = (message.content or "").strip()
        
        # Check if this is a reply to the bot
        is_reply = False
        replied_message = None
        
        if getattr(message, "reference", None) and getattr(message.reference, "message_id", None):
            try:
                replied_message = await message.channel.fetch_message(message.reference.message_id)
                if replied_message and replied_message.author.id == self.bot.user.id:
                    is_reply = True
            except Exception as e:
                logger.error(f"Error fetching replied message: {e}")
        
        # Check if our bot is mentioned
        bot_mentioned = False
        try:
            mentions = getattr(message, "mentions", []) or []
            bot_mentioned = any((getattr(u, "id", None) == self.bot.user.id) for u in mentions)
        except Exception:
            bot_mentioned = False
        
        # Only respond to direct mentions or replies to our messages
        if not (bot_mentioned or is_reply):
            return
            
        # Handle mentions
        if bot_mentioned:
            # Remove mention text if present
            try:
                content = content.replace(f'<@{self.bot.user.id}>', '').strip()
            except Exception:
                pass
            
        # Handle replies - check if we've reached message limit
        if is_reply and user_id in self.conversations and len(self.conversations[user_id]) >= MAX_CONVERSATION_HISTORY:
            try:
                await message.reply("Chat limit reached! Starting new conversation.", mention_author=False)
            except Exception:
                logger.error("Failed to send chat limit message")
            # Clear the conversation
            if user_id in self.conversations:
                del self.conversations[user_id]
            if user_id in self.last_interaction:
                del self.last_interaction[user_id]
            return
            
        # Reset conversation if it's a new mention (not a reply) and not saved chat
        if bot_mentioned and not is_reply and user_id not in self.saved_chats:
            self.conversations[user_id] = deque()
            self.last_interaction[user_id] = datetime.now()
            
        # Handle empty content
        if not content:
            if bot_mentioned:
                # Default response for just a mention
                try:
                    await message.reply(embed=create_embed(
                        "🤖 NationBot Assistant",
                        "Hello! I'm here to help you with NationBot. Ask me about:\n"
                        "- Starting your civilization (`.start`)\n"
                        "- Managing resources (`.status`)\n"
                        "- Military commands (`.warhelp`)\n"
                        "- Ideologies and strategies\n\n"
                        "Try asking: 'How do I declare war?' or 'What does fascism do?'",
                        guilded.Color.blue()
                    ), mention_author=False)
                    self._update_conversation(user_id, False, "Hello! How can I assist with NationBot today?")
                except Exception:
                    logger.exception("Failed to send default mention reply")
            return
            
        # Get user's civilization status for context
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
        
        # Prepare system prompt with Discord markdown info
        system_prompt = f"""You are NationBot, an AI assistant for a nation simulation game. 
Players build civilizations, manage resources, wage wars, and form alliances. 
Your role is to help players understand game mechanics and strategies.

{civ_status}
Key Game Concepts:
- Resources: gold, food, stone, wood
- Military: soldiers, spies, tech_level
- Population: citizens, happiness, hunger
- Territory: land_size
- Ideologies: fascism, democracy, communism, theocracy, anarchy, destruction, pacifist, socialism, terrorism, capitalism, federalism, monarchy

**NEW COMMANDS:**
- `.reset` - Reset your civilization (irreversible!)
- `.sv` - Start saved chat (no timeout)
- `.svc` - Close saved chat

BasicCommands:
  ideology      Choose your civilization's government ideology
  start         Start a new civilization with a cinematic intro
  status        View your civilization status
  warhelp       Display help information
  regions       View or select your civilization's region

EconomyCommands: (short)
  extrawork, extrastore, extrainventory, extragamble, extracards, slots, blackjack, give, setbalance

MilitaryCommands & Diplomacy:
  train         Train soldiers or spies
  find          Search for wandering soldiers
  declare       Declare war on another civilization
  attack        Launch direct attack
  siege         Lay siege to enemy territory
  stealthbattle Spy-based stealth attack
  cards         View/use unlocked cards (20% chance from military commands)
  peace         Offer peace
  accept_peace  Accept peace offer
  addborder     Build defensive border
  removeborder  Remove border and retrieve soldiers
  rectract      Assign percentage of soldiers to border
  retrieve      Retrieve percentage of soldiers from border
  borderinfo    Check border status

Border Management:
  - Borders provide defensive bonuses in battles
  - Soldiers assigned to border increase border strength
  - Strategic trade-off between border defense and offensive capability

Card System:
  - Cards unlock with 20% chance after military commands
  - Cards provide powerful but risky effects
  - Use `.cards` to view and use unlocked cards

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
        
        # Generate AI response with conversation history
        try:
            # Build messages with conversation history
            messages = [{"role": "system", "content": system_prompt}]
            
            # Add conversation history if available
            if user_id in self.conversations and self.conversations[user_id]:
                history = self._get_conversation_history(user_id)
                messages.extend(history)
            
            # Add current user message
            messages.append({"role": "user", "content": content})
            
            # Generate response
            response = await self.generate_ai_response(messages)
            
            # Check if we reached message limit during update
            update_success = self._update_conversation(user_id, True, content)
            if not update_success:
                # We reached the limit, add a note to the response
                response += "\n\n💬 *Note: Chat history limit reached. Starting a new conversation.*"
                # Clear and restart conversation (unless saved chat)
                if user_id not in self.saved_chats:
                    self.conversations[user_id] = deque()
                    self.last_interaction[user_id] = datetime.now()
            
            # Update with AI response
            self._update_conversation(user_id, False, response)
            
            # Send response
            try:
                await message.reply(response, mention_author=False)
            except Exception:
                # fallback to sending as plain text if reply fails
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
        """Generate response using Groq or fallback providers."""
        # PRIORITY: Groq -> OpenRouter -> OpenAI -> local fallback message
        # messages should be a list of dicts with 'role' and 'content'
        headers = {}

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
    @commands.command(name='warhelp')
    @app_commands.describe(category="Optional help category")
    @app_commands.choices(category=[
        app_commands.Choice(name="basic", value="basic"),
        app_commands.Choice(name="economy", value="economy"),
        app_commands.Choice(name="military", value="military"),
        app_commands.Choice(name="diplomacy", value="diplomacy"),
        app_commands.Choice(name="items", value="items"),
    ])
    async def warbot_help_command(
        self,
        ctx,
        category: Optional[Literal["basic", "economy", "military", "diplomacy", "items"]] = None
    ):
        """Display simplified, organized help menu"""
        categories = self.help_categories

        # If no category specified, show main menu
        if not category:
            embed = guilded.Embed(
                title="🤖 NationBot Help Menu",
                description="**Use `/warhelp <category>` for detailed commands**\nExample: `/warhelp basic`",
                color=0x1e90ff
            )
            
            for cat_name, cat_data in categories.items():
                embed.add_field(
                    name=cat_data["title"],
                    value=f"*{cat_data['description']}*\n`{cat_name}`",
                    inline=True
                )
            
            embed.add_field(
                name="💡 Quick Tips",
                value="• Mention me or reply for AI help\n• Use `/sv` for persistent chats\n• Slash commands show built-in suggestions",
                inline=False
            )
            
            await ctx.send(embed=embed)
            return

        # Show specific category
        category = category.lower()
        if category in categories:
            cat_data = categories[category]
            
            embed = guilded.Embed(
                title=cat_data["title"],
                description=cat_data["description"],
                color=0x1e90ff
            )
            
            for cmd, desc in cat_data["commands"].items():
                embed.add_field(name=cmd, value=desc, inline=False)
            
            embed.set_footer(text=f"Use /warhelp for main menu | Total categories: {len(categories)}")
            
        else:
            embed = guilded.Embed(
                title="❌ Category Not Found",
                description=f"Available categories: {', '.join(categories.keys())}",
                color=0xff0000
            )
        
        await ctx.send(embed=embed)

    # ... rest of your existing commands (regions, start, ideology, status) remain the same ...
    @commands.command(name='regions')
    @app_commands.describe(region_name="Region to select (optional)")
    @app_commands.choices(region_name=[
        app_commands.Choice(name="asia", value="asia"),
        app_commands.Choice(name="europe", value="europe"),
        app_commands.Choice(name="africa", value="africa"),
        app_commands.Choice(name="north_america", value="north_america"),
        app_commands.Choice(name="south_america", value="south_america"),
        app_commands.Choice(name="middle_east", value="middle_east"),
        app_commands.Choice(name="oceania", value="oceania"),
        app_commands.Choice(name="antarctica", value="antarctica"),
    ])
    async def regions_command(
        self,
        ctx,
        region_name: Optional[
            Literal[
                "asia", "europe", "africa", "north_america",
                "south_america", "middle_east", "oceania", "antarctica"
            ]
        ] = None
    ):
        """View or select your civilization's region"""
        # [Existing regions command code remains unchanged]
        # Define available regions with bonuses (using underscores for names)
        regions = {
            "asia": {
                "name": "Asia",
                "bonuses": {"food": 200, "population": 50},
                "description": "🌏 **Asia**: Fertile lands with abundant resources and large population capacity."
            },
            "europe": {
                "name": "Europe", 
                "bonuses": {"gold": 300, "tech_level": 1},
                "description": "🇪🇺 **Europe**: Advanced technological development and economic strength."
            },
            "africa": {
                "name": "Africa",
                "bonuses": {"stone": 150, "wood": 150},
                "description": "🌍 **Africa**: Rich in natural resources and mineral wealth."
            },
            "north_america": {
                "name": "North America",
                "bonuses": {"gold": 200, "food": 200},
                "description": "🇺🇸 **North America**: Balanced economy with strong agricultural and financial sectors."
            },
            "south_america": {
                "name": "South America", 
                "bonuses": {"food": 300, "wood": 100},
                "description": "🇧🇷 **South America**: Lush rainforests and abundant agricultural potential."
            },
            "middle_east": {
                "name": "Middle East",
                "bonuses": {"gold": 400},
                "description": "🌅 **Middle East**: Vast oil reserves creating immense wealth."
            },
            "oceania": {
                "name": "Oceania",
                "bonuses": {"food": 250, "happiness": 15},
                "description": "🇦🇺 **Oceania**: Island paradise with high quality of life and abundant seafood."
            },
            "antarctica": {
                "name": "Antarctica",
                "bonuses": {"research": 25},
                "description": "🇦🇶 **Antarctica**: Harsh environment but unique research opportunities. +25% research speed."
            }
        }
        
        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        
        if not civ:
            await ctx.send("❌ You need to start a civilization first! Use `.start <name>`")
            return
            
        # If no region specified, show available regions
        if not region_name:
            embed = guilded.Embed(
                title="🌍 Available Regions",
                description="Choose a region for your civilization. Each region provides unique bonuses:",
                color=0x00ff00
            )
            
            for region_id, region_data in regions.items():
                bonus_text = ", ".join([f"+{amount} {resource}" for resource, amount in region_data["bonuses"].items()])
                embed.add_field(
                    name=region_data["name"],
                    value=f"{region_data['description']}\n**Bonuses:** {bonus_text}",
                    inline=False
                )
            
            embed.add_field(
                name="Usage",
                value="Use `.regions <region_name>` to select a region (e.g., `.regions asia`)\nAvailable regions: asia, europe, africa, north_america, south_america, middle_east, oceania, antarctica",
                inline=False
            )
            
            if civ.get('region'):
                current_region = next((r for r in regions.values() if r['name'].lower() == civ.get('region').lower()), None)
                if current_region:
                    bonus_text = ", ".join([f"+{amount} {resource}" for resource, amount in current_region["bonuses"].items()])
                    embed.add_field(
                        name="Current Region",
                        value=f"**{current_region['name']}**: {bonus_text}",
                        inline=False
                    )
            
            await ctx.send(embed=embed)
            return
            
        # Check if region is valid
        region_name = region_name.lower()
        if region_name not in regions:
            await ctx.send(f"❌ Invalid region! Available regions: {', '.join(regions.keys())}")
            return
            
        # Check if region is already set
        if civ.get('region'):
            if civ['region'].lower() == region_name:
                await ctx.send(f"❌ Your civilization is already in the {regions[region_name]['name']} region!")
                return
            else:
                await ctx.send(f"❌ You've already selected the {civ['region']} region. Region selection cannot be changed.")
                return
                
        # Apply region bonuses
        region_bonuses = regions[region_name]['bonuses']
        updated_resources = civ['resources'].copy()
        updated_population = civ['population'].copy()
        
        for resource, amount in region_bonuses.items():
            if resource in updated_resources:
                updated_resources[resource] += amount
            elif resource == "population":
                updated_population['citizens'] += amount
            elif resource == "happiness":
                updated_population['happiness'] = min(100, updated_population['happiness'] + amount)
            elif resource == "research":
                # Special bonus for Antarctica - stored in bonuses
                current_bonuses = civ.get('bonuses', {})
                current_bonuses['research_speed'] = current_bonuses.get('research_speed', 0) + amount
                self.db.update_civilization(user_id, {'bonuses': current_bonuses})
        
        # Update civilization with region and bonuses
        update_data = {
            'region': regions[region_name]['name'],
            'resources': updated_resources,
            'population': updated_population
        }
        
        if self.db.update_civilization(user_id, update_data):
            bonus_text = ", ".join([f"+{amount} {resource}" for resource, amount in region_bonuses.items()])
            
            embed = guilded.Embed(
                title=f"🌍 Region Selected: {regions[region_name]['name']}",
                description=regions[region_name]['description'],
                color=0x00ff00
            )
            
            embed.add_field(
                name="Bonuses Applied",
                value=bonus_text,
                inline=False
            )
            
            embed.add_field(
                name="🎉 Nation Complete!",
                value="Your civilization is now fully established! Use `.status` to view your complete stats and `.warhelp` to see all available commands.",
                inline=False
            )
            
            await ctx.send(embed=embed)
        else:
            await ctx.send("❌ Failed to update your region. Please try again later.")

    @commands.command(name='start')
    @app_commands.describe(civ_name="Name of your civilization")
    async def start_civilization(self, ctx, civ_name: str = None):
        """Start a new civilization with a cinematic intro"""
        if not civ_name:
            await ctx.send("❌ Please provide a civilization name: `.start <civilization_name>`")
            return
            
        user_id = str(ctx.author.id)
        
        # Check if user already has a civilization
        if self.civ_manager.get_civilization(user_id):
            await ctx.send("❌ You already have a civilization! Use `.status` to view it.")
            return
            
        # Show cinematic intro
        intro_art = get_ascii_art("civilization_start")
        
        # Random founding event
        founding_events = [
            ("🏛️ **Golden Dawn**: Your people discovered ancient gold deposits!", {"gold": 200}),
            ("🌾 **Fertile Lands**: Blessed with rich soil for farming!", {"food": 300}),
            ("🏗️ **Master Builders**: Your citizens are natural architects!", {"stone": 150, "wood": 150}),
            ("👥 **Population Boom**: Word of your great leadership spreads!", {"population": 50}),
            ("⚡ **Lightning Strike**: A divine sign brings good fortune!", {"gold": 100, "happiness": 20})
        ]
        
        event_text, bonus_resources = random.choice(founding_events)
        
        # Special name bonuses
        name_bonuses = {}
        special_message = ""
        if "ink" in civ_name.lower():
            name_bonuses["luck_bonus"] = 5
            special_message = "🖋️ *The pen will never forget your work.* (+5% luck)"
        elif "pen" in civ_name.lower():
            name_bonuses["diplomacy_bonus"] = 5
            special_message = "🖋️ *The pen is mightier than the sword.* (+5% diplomacy success)"
            
        # 5% chance for random HyperItem
        hyper_item = None
        if random.random() < 0.05:
            common_items = ["Lucky Charm", "Propaganda Kit", "Mercenary Contract"]
            hyper_item = random.choice(common_items)
            
        # Create civilization
        self.civ_manager.create_civilization(user_id, civ_name, bonus_resources, name_bonuses, hyper_item)
        
        # Send intro message
        embed = guilded.Embed(
            title=f"🏛️ The Founding of {civ_name}",
            description=f"{intro_art}\n\n{event_text}\n{special_message}",
            color=0x00ff00
        )
        
        if hyper_item:
            embed.add_field(
                name="🎁 Rare Discovery!",
                value=f"Your scouts found a **{hyper_item}**! This powerful item unlocks special abilities.",
                inline=False
            )
            
        embed.add_field(
            name="📋 Next Steps",
            value="Choose your government ideology with `.ideology <type>`\nSelect your region with `.regions`\nView your status with `.status`",
            inline=False
        )
        
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
    async def choose_ideology(
        self,
        ctx,
        ideology_type: Optional[
            Literal[
                "fascism", "democracy", "communism", "theocracy", "anarchy",
                "destruction", "pacifist", "socialism", "terrorism",
                "capitalism", "federalism", "monarchy"
            ]
        ] = None
    ):
        """Choose your civilization's government ideology"""
        if not ideology_type:
            ideologies = {
                "fascism": "+25% soldier training speed, -15% diplomacy success, -10% luck",
                "democracy": "+20% happiness, +10% trade profit, slower soldier training (-15%)", 
                "communism": "Equal resource distribution (+10% citizen productivity), -10% tech speed",
                "theocracy": "+15% propaganda success, +5% happiness, -10% tech speed",
                "anarchy": "Random events happen twice as often, 0 soldier upkeep, -20% spy success",
                # NEW IDEOLOGIES
                "destruction": "+35% combat strength, +40% soldier training, -25% resources, -30% happiness, -50% diplomacy",
                "pacifist": "+35% happiness, +25% population growth, +20% trade profit, -60% soldier training, -40% combat, +25% diplomacy", 
                "socialism": "+15% citizen productivity, +10% happiness from welfare, -10% trade profit",
                "terrorism": "+40% guerrilla/raid effectiveness, +30% spy success, -50% diplomacy, increases unrest",
                "capitalism": "+20% trade profit, +15% gold generation, -10% happiness due to inequality",
                "federalism": "+10% stability, +10% diplomacy, +5% regional production, minor tech tradeoffs",
                "monarchy": "+10% loyalty/happiness, +10% soldier morale, -10% reform speed"
            }
            
            embed = guilded.Embed(title="🏛️ Government Ideologies", color=0x0099ff)
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
        # UPDATED valid ideologies list
        valid_ideologies = ["fascism", "democracy", "communism", "theocracy", "anarchy", "destruction", "pacifist", "socialism", "terrorism", "capitalism", "federalism", "monarchy"]
        
        if ideology_type not in valid_ideologies:
            await ctx.send(f"❌ Invalid ideology! Choose from: {', '.join(valid_ideologies)}")
            return
            
        # Apply ideology
        self.civ_manager.set_ideology(user_id, ideology_type)
        
        ideology_descriptions = {
            "fascism": "⚔️ **Fascism**: Your military grows strong, but diplomacy suffers.",
            "democracy": "🗳️ **Democracy**: Your people are happy and trade flourishes.", 
            "communism": "🏭 **Communism**: Workers unite for the collective good.",
            "theocracy": "⛪ **Theocracy**: Divine blessing guides your civilization.",
            "anarchy": "💥 **Anarchy**: Chaos reigns, but freedom has no limits.",
            # NEW IDEOLOGY DESCRIPTIONS
            "destruction": "💥 **Destruction**: Y o u. m o n s t e r.",
            "pacifist": "🕊️ **Pacifist**: Your civilization thrives in peace and harmony.",
            "socialism": "🤝 **Socialism**: Welfare and shared prosperity — steady growth, modest trade penalties.", 
            "terrorism": "🔥 **Terrorism**: Operates from the shadows — excels at raids and covert ops but ruins diplomacy.",
            "capitalism": "💹 **Capitalism**: Commerce and wealth generation reign; inequality can lower happiness.",
            "federalism": "🏛️ **Federalism**: Regions manage themselves well — improved stability and diplomacy.",
            "monarchy": "👑 **Monarchy**: Tradition and loyalty strengthen your rule; reforms are slower."
        }
        
        embed = guilded.Embed(
            title=f"🏛️ Ideology Chosen: {ideology_type.capitalize()}",
            description=ideology_descriptions[ideology_type],
            color=0x00ff00
        )
        embed.add_field(
            name="🎉 Nation Almost Complete!",
            value="Your civilization is nearly ready! **Select your region with `.regions`** to complete your nation setup and receive regional bonuses.",
            inline=False
        )
        
        await ctx.send(embed=embed)

    @commands.command(name='status')
    async def civilization_status(self, ctx):
        """View your civilization status"""
        user_id = str(ctx.author.id)
        civ = self.civ_manager.get_civilization(user_id)
        
        if not civ:
            await ctx.send("❌ You don't have a civilization yet! Use `.start <name>` to begin.")
            return
            
        # Create status embed
        embed = guilded.Embed(
            title=f"🏛️ {civ['name']}",
            description=f"**Leader**: {ctx.author.name}\n**Ideology**: {civ['ideology'].capitalize() if civ.get('ideology') else 'None'}\n**Region**: {civ.get('region', 'Not selected')}",
            color=0x0099ff
        )
        
        # Resources
        resources = civ['resources']
        embed.add_field(
            name="💰 Resources",
            value=f"🪙 Gold: {format_number(resources['gold'])}\n🌾 Food: {format_number(resources['food'])}\n🪨 Stone: {format_number(resources['stone'])}\n🪵 Wood: {format_number(resources['wood'])}",
            inline=True
        )
        
        # Population & Military
        population = civ['population']
        military = civ['military']
        embed.add_field(
            name="👥 Population & Military",
            value=f"👤 Citizens: {format_number(population['citizens'])}\n😊 Happiness: {population['happiness']}%\n🍽️ Hunger: {population['hunger']}%\n⚔️ Soldiers: {format_number(military['soldiers'])}\n🕵️ Spies: {format_number(military['spies'])}",
            inline=True
        )
        
        # Territory & Items
        territory = civ['territory']
        hyper_items = civ.get('hyper_items', [])
        embed.add_field(
            name="🗺️ Territory & Items",
            value=f"🏞️ Land Size: {format_number(territory['land_size'])} km²\n🎁 HyperItems: {len(hyper_items)}\n" + ("\n".join(f"• {item}" for item in hyper_items[:5]) + ("..." if len(hyper_items) > 5 else "")),
            inline=True
        )
        
        await ctx.send(embed=embed)

async def setup(bot):
    await bot.add_cog(BasicCommands(bot))
