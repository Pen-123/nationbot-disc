import functools
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Callable, Any, Optional
import discord

logger = logging.getLogger(__name__)

def format_number(number: int) -> str:
    """Format large numbers with appropriate suffixes"""
    if number < 1000:
        return str(number)
    elif number < 1000000:
        return f"{number/1000:.1f}K"
    elif number < 1000000000:
        return f"{number/1000000:.1f}M"
    else:
        return f"{number/1000000000:.1f}B"


def create_embed(title: str, description: str, color: discord.Color = None) -> discord.Embed:
    """Create a standardized embed for bot responses"""
    if color is None:
        color = discord.Color.blue()
        
    embed = discord.Embed(
        title=title,
        description=description,
        color=color,
        timestamp=datetime.now()
    )
    
    return embed


async def run_in_executor(func: Callable, *args, **kwargs) -> Any:
    """Run a blocking function in a threadpool and return the result."""
    return await asyncio.to_thread(func, *args, **kwargs)


# Helper utilities for hybrid command compatibility
def is_interaction(ctx_or_interaction) -> bool:
    """Return True when the invocation object is a discord.Interaction."""
    return isinstance(ctx_or_interaction, discord.Interaction)


def get_invoker_id(ctx_or_interaction) -> str:
    """Return the user id (string) of the invoking user for Context or Interaction."""
    if is_interaction(ctx_or_interaction):
        return str(ctx_or_interaction.user.id)
    return str(getattr(ctx_or_interaction, "author").id)


async def send_response(ctx_or_interaction, content: Any = None, **kwargs):
    """Send a message that works for both prefix Context and Interaction.

    - For Interaction: uses response.send_message (or followup) and supports ephemeral kwarg.
    - For Context: falls back to ctx.send.
    """
    if is_interaction(ctx_or_interaction):
        try:
            # prefer initial response when available
            if not ctx_or_interaction.response.is_done():
                await ctx_or_interaction.response.send_message(content, **kwargs)
            else:
                await ctx_or_interaction.followup.send(content, **kwargs)
        except Exception:
            # best-effort fallback: try to DM the invoker
            try:
                await ctx_or_interaction.user.send(content, **kwargs)
            except Exception:
                logger.exception("Failed to deliver interaction response")
    else:
        await ctx_or_interaction.send(content, **kwargs)


def check_cooldown_decorator(minutes: int = 5):
    """Decorator to add cooldown functionality to both prefix commands (Context)
    and slash/app commands (discord.Interaction).

    The decorator will call blocking DB functions in a thread to avoid blocking the event loop,
    and will send ephemeral replies for interactions where appropriate.
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(self, ctx_or_interaction, *args, **kwargs):
            # Determine whether this was called from an Interaction or a Context
            is_inter = isinstance(ctx_or_interaction, discord.Interaction)

            # Extract user id in a safe way
            if is_inter:
                user_id = str(ctx_or_interaction.user.id)
            else:
                user_id = str(getattr(ctx_or_interaction, "author").id)

            command_name = func.__name__

            # Use executor for DB calls to avoid blocking
            try:
                last_used = await run_in_executor(self.db.get_command_cooldown, user_id, command_name)
            except Exception as e:
                logger.exception("Failed to fetch cooldown from DB")
                last_used = None

            if last_used:
                cooldown_expiry = last_used + timedelta(minutes=minutes)
                time_left = cooldown_expiry - datetime.utcnow()
                if time_left.total_seconds() > 0:
                    # Format time remaining
                    time_str = format_time_duration(time_left)
                    embed = create_embed(
                        "⏰ Command on Cooldown",
                        f"You must wait **{time_str}** before using this command again.",
                        discord.Color.orange()
                    )
                    try:
                        if is_inter:
                            # Try to respond ephemeral if possible
                            try:
                                if not ctx_or_interaction.response.is_done():
                                    await ctx_or_interaction.response.send_message(embed=embed, ephemeral=True)
                                else:
                                    await ctx_or_interaction.followup.send(embed=embed, ephemeral=True)
                            except Exception:
                                # Fallback to sending via channel if interaction cannot be replied to
                                try:
                                    channel = await run_in_executor(self.bot.fetch_channel, ctx_or_interaction.channel_id)
                                    if channel:
                                        await channel.send(embed=embed)
                                except Exception:
                                    logger.exception("Failed to deliver cooldown message for interaction")
                        else:
                            await ctx_or_interaction.send(embed=embed)
                    except Exception:
                        logger.exception("Failed to send cooldown message")
                    return

            # Execute the command
            try:
                result = await func(self, ctx_or_interaction, *args, **kwargs)

                # Set cooldown only if command succeeded
                try:
                    await run_in_executor(self.db.set_command_cooldown, user_id, command_name, datetime.utcnow())
                except Exception:
                    logger.exception("Failed to set cooldown in DB")

                return result

            except Exception as e:
                logger.exception(f"Error in command {command_name}: {e}")

                # Don't set cooldown if command failed; send a friendly message
                embed = create_embed(
                    "❌ Command Error",
                    "An error occurred while executing this command. Please try again or contact an admin.",
                    discord.Color.red()
                )
                try:
                    if is_inter:
                        try:
                            if not ctx_or_interaction.response.is_done():
                                await ctx_or_interaction.response.send_message(embed=embed, ephemeral=True)
                            else:
                                await ctx_or_interaction.followup.send(embed=embed, ephemeral=True)
                        except Exception:
                            # last resort: DM the user (best-effort)
                            try:
                                await ctx_or_interaction.user.send(embed=embed)
                            except Exception:
                                logger.exception("Failed to deliver error message to interaction user")
                    else:
                        await ctx_or_interaction.send(embed=embed)
                except Exception:
                    logger.exception("Failed to send command error message")

        return wrapper
    return decorator


def format_time_duration(delta: timedelta) -> str:
    """Format a timedelta into a readable string"""
    total_seconds = int(delta.total_seconds())
    
    if total_seconds < 60:
        return f"{total_seconds} seconds"
    elif total_seconds < 3600:
        minutes = total_seconds // 60
        seconds = total_seconds % 60
        if seconds > 0:
            return f"{minutes} minutes, {seconds} seconds"
        return f"{minutes} minutes"
    else:
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        if minutes > 0:
            return f"{hours} hours, {minutes} minutes"
        return f"{hours} hours"


def get_ascii_art(art_type: str) -> str:
    """Get ASCII art for various occasions"""
    art_collection = {
        "civilization_start": """
    ╔══════════════════════════════════════╗
    ║        🏛️  CIVILIZATION BORN  🏛️        ║
    ║                                      ║
    ║    From humble beginnings arise      ║
    ║       great civilizations...        ║
    ║                                      ║
    ║         ⚡ ⭐ DESTINY AWAITS ⭐ ⚡        ║
    ╚══════════════════════════════════════╝
        """,
        
        "war_declaration": """
    ⚔️ ═══════════════════════════════════ ⚔️
       🔥 THE DRUMS OF WAR THUNDER 🔥
        
         Armies march to battle!
         Steel clashes with steel!
         Only one shall prevail!
         
    ⚔️ ═══════════════════════════════════ ⚔️
        """,
        
        "victory": """
    🏆 ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓ 🏆
       
         ⭐ GLORIOUS VICTORY! ⭐
           The battle is won!
         
         "History is written by
          the victorious!"
       
    🏆 ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓ 🏆
        """,
        
        "nuclear_blast": """
    ☢️ ████████████████████████████████ ☢️
      
       💥 NUCLEAR DEVASTATION 💥
      
         The atom is split!
         Cities turn to ash!
         The world trembles!
      
    ☢️ ████████████████████████████████ ☢️
        """,
        
        "black_market": """
    🕴️ ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░ 🕴️
      
        💀 BLACK MARKET DEALINGS 💀
         
          "Psst... Looking for
           something special?"
      
          💰 Gold for Power 💰
      
    🕴️ ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░ 🕴️
        """,
        
        "alliance": """
    🤝 ╭─────────────────────────────────╮ 🤝
      │                                 │
      │    ⚖️ DIPLOMATIC ALLIANCE ⚖️     │
      │                                 │
      │     "United we stand,           │
      │      divided we fall"           │
      │                                 │
    🤝 ╰─────────────────────────────────╯ 🤝
        """,
        
        "technology": """
    🔬 ┌───────────────────────────────┐ 🔬
      │                               │
      │    ⚡ TECHNOLOGICAL LEAP ⚡     │
      │                               │
      │      Knowledge is power!      │
      │     Progress never stops!     │
      │                               │
    🔬 └───────────────────────────────┘ 🔬
        """
    }
    
    return art_collection.get(art_type, "")


def calculate_percentage_change(old_value: int, new_value: int) -> str:
    """Calculate and format percentage change between two values"""
    if old_value == 0:
        return "+∞%" if new_value > 0 else "0%"
        
    change = ((new_value - old_value) / old_value) * 100
    
    if change > 0:
        return f"+{change:.1f}%"
    else:
        return f"{change:.1f}%"


def get_civilization_rank(power_score: int) -> tuple[str, str]:
    """Get civilization rank and title based on power score"""
    if power_score < 500:
        return "Hamlet", "🏘️"
    elif power_score < 1500:
        return "Village", "🏡"
    elif power_score < 3000:
        return "Town", "🏘️"
    elif power_score < 6000:
        return "City", "🏙️"
    elif power_score < 12000:
        return "City-State", "🏛️"
    elif power_score < 25000:
        return "Kingdom", "👑"
    elif power_score < 50000:
        return "Empire", "⚜️"
    elif power_score < 100000:
        return "Superpower", "🌟"
    else:
        return "Galactic Empire", "🌌"


def get_happiness_status(happiness: int) -> tuple[str, str]:
    """Get happiness status description and emoji"""
    if happiness >= 90:
        return "Ecstatic", "🤩"
    elif happiness >= 80:
        return "Very Happy", "😄"
    elif happiness >= 70:
        return "Happy", "😊"
    elif happiness >= 60:
        return "Content", "😐"
    elif happiness >= 50:
        return "Neutral", "😑"
    elif happiness >= 40:
        return "Unhappy", "😞"
    elif happiness >= 30:
        return "Very Unhappy", "😢"
    elif happiness >= 20:
        return "Miserable", "😭"
    else:
        return "Revolt Risk", "😡"


def get_hunger_status(hunger: int) -> tuple[str, str]:
    """Get hunger status description and emoji"""
    if hunger <= 10:
        return "Well Fed", "😋"
    elif hunger <= 25:
        return "Satisfied", "🙂"
    elif hunger <= 50:
        return "Hungry", "😕"
    elif hunger <= 75:
        return "Very Hungry", "😰"
    else:
        return "Starving", "💀"


def get_military_strength_description(soldiers: int, spies: int, tech_level: int) -> str:
    """Get description of military strength"""
    total_strength = soldiers + (spies * 2) + (tech_level * 50)
    
    if total_strength < 100:
        return "Defenseless"
    elif total_strength < 300:
        return "Weak"
    elif total_strength < 600:
        return "Modest"
    elif total_strength < 1200:
        return "Strong"
    elif total_strength < 2500:
        return "Formidable"
    elif total_strength < 5000:
        return "Mighty"
    else:
        return "Legendary"


def validate_user_mention(mention: str) -> Optional[str]:
    """Extract user ID from mention string"""
    if mention.startswith('<@') and mention.endswith('>'):
        user_id = mention[2:-1]
        if user_id.startswith('!'):
            user_id = user_id[1:]
        return user_id
    return None


def get_resource_efficiency_bonus(ideology: str, action_type: str) -> float:
    """Get resource efficiency bonus based on ideology and action type"""
    ideology_bonuses = {
        "fascism": {
            "military": 1.15,
            "resource_extraction": 1.05
        },
        "democracy": {
            "trade": 1.20,
            "happiness": 1.15,
            "taxation": 1.10
        },
        "communism": {
            "production": 1.15,
            "citizen_efficiency": 1.10
        },
        "theocracy": {
            "happiness": 1.10,
            "propaganda": 1.15
        },
        "anarchy": {
            "chaos_resistance": 1.25,
            "unpredictability": 2.0
        }
    }
    
    return ideology_bonuses.get(ideology, {}).get(action_type, 1.0)


def format_civilization_summary(civ_data: dict) -> str:
    """Format a civilization summary for display"""
    resources = civ_data.get('resources', {})
    population = civ_data.get('population', {})
    military = civ_data.get('military', {})

    gold = resources.get('gold', 0)
    citizens = population.get('citizens', 0)
    happiness = population.get('happiness', 50)
    soldiers = military.get('soldiers', 0)
    tech_level = military.get('tech_level', 0)

    power_score = (
        sum(resources.values()) + 
        citizens * 2 + 
        soldiers * 5 + 
        tech_level * 100
    )
    
    rank, rank_emoji = get_civilization_rank(power_score)
    happiness_status, happiness_emoji = get_happiness_status(happiness)

    return (
        f"{rank_emoji} **{civ_data.get('name','Unknown')}** ({rank})\n"
        f"💰 {format_number(gold)} Gold | 👤 {format_number(citizens)} Citizens | {happiness_emoji} {happiness_status}\n"
        f"⚔️ Soldiers: {format_number(soldiers)} | 🔬 Tech Level: {tech_level} | Power: {format_number(power_score)}"
    )


def create_progress_bar(current: int, maximum: int, length: int = 10) -> str:
    """Create a visual progress bar"""
    if maximum <= 0:
        return "▓" * length
        
    filled = int((current / maximum) * length)
    filled = max(0, min(length, filled))
    
    bar = "▓" * filled + "░" * (length - filled)
    return f"[{bar}] {current}/{maximum}"


def get_random_flavor_text(category: str) -> str:
    """Get random flavor text for various situations"""
    flavor_texts = {
        "victory": [
            "Victory belongs to the bold!",
            "Another triumph for the history books!",
            "The sweet taste of victory!",
            "Glory to the victorious!",
            "Conquest achieved!"
        ],
        "defeat": [
            "Even the mighty can fall...",
            "A temporary setback!",
            "Defeat is but a lesson in disguise.",
            "The wheel of fortune turns...",
            "Rise again, stronger than before!"
        ],
        "trade": [
            "Commerce is the lifeblood of civilization!",
            "A deal beneficial to all!",
            "Trade winds blow favorably!",
            "Prosperity through cooperation!",
            "The market is pleased!"
        ],
        "diplomacy": [
            "The pen truly is mightier than the sword.",
            "Diplomacy opens new possibilities!",
            "Words can move mountains!",
            "Peace through understanding!",
            "A new chapter in international relations!"
        ]
    }
    
    import random
    return random.choice(flavor_texts.get(category, ["Fortune favors the prepared!"]))


class CooldownManager:
    """Advanced cooldown management for complex scenarios"""
    
    def __init__(self, db):
        self.db = db
        
    def set_dynamic_cooldown(self, user_id: str, command: str, base_minutes: int, modifiers: dict = None):
        """Set cooldown with dynamic modifiers"""
        final_minutes = base_minutes
        
        if modifiers:
            # Apply ideology modifiers
            if modifiers.get('ideology') == 'fascism' and 'military' in command:
                final_minutes = int(final_minutes * 0.8)  # 20% faster military actions
            elif modifiers.get('ideology') == 'democracy' and 'trade' in command:
                final_minutes = int(final_minutes * 0.9)  # 10% faster trade
                
            # Apply tech level modifiers
            tech_level = modifiers.get('tech_level', 1)
            if tech_level >= 5:
                final_minutes = int(final_minutes * 0.9)  # Advanced tech reduces cooldowns
                
        self.db.set_cooldown(user_id, command, final_minutes)
        
    def get_cooldown_with_context(self, user_id: str, command: str) -> dict:
        """Get cooldown information with additional context"""
        expiry = self.db.check_cooldown(user_id, command)
        
        if not expiry:
            return {"on_cooldown": False}
            
        time_left = expiry - datetime.now()
        
        if time_left.total_seconds() <= 0:
            return {"on_cooldown": False}
            
        return {
            "on_cooldown": True,
            "time_left": time_left,
            "formatted_time": format_time_duration(time_left),
            "expires_at": expiry
        }
