import functools
import logging
from datetime import datetime, timedelta
from typing import Callable, Any
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

def check_cooldown_decorator(minutes: int = 5):
    """Decorator to add cooldown functionality to commands"""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(self, ctx, *args, **kwargs):
            user_id = str(ctx.author.id)
            command_name = func.__name__
            
            # Check if user is on cooldown
            last_used = self.db.get_command_cooldown(user_id, command_name)

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
                    await ctx.send(embed=embed)
                    return
                    
            # Execute the command
            try:
                result = await func(self, ctx, *args, **kwargs)
                
                # Set cooldown only if command succeeded (didn't return early with error)
                self.db.set_command_cooldown(user_id, command_name, datetime.utcnow())
                
                return result
                
            except Exception as e:
                logger.error(f"Error in command {command_name}: {e}")
                
                # Don't set cooldown if command failed
                embed = create_embed(
                    "❌ Command Error",
                    "An error occurred while executing this command. Please try again.",
                    discord.Color.red()
                )
                await ctx.send(embed=embed)
                
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
    🏆 ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓ 🏆
       
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

def validate_user_mention(mention: str) -> str:
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
    resources = civ_data['resources']
    population = civ_data['population']
    military = civ_data['military']
    
    power_score = (
        sum(resources.values()) + 
        population['citizens'] * 2 + 
        military['soldiers'] * 5 + 
        military['tech_level'] * 100
    )
    
    rank, rank_emoji = get_civilization_rank(power_score)
    happiness_status, happiness_emoji = get_happiness_status(population['happiness'])
    
    return f"{rank_emoji} **{civ_data['name']}** ({rank})\n💰 {format_number(resources['gold'])} Gold | 👤 {format_number(population['citizens'])} Citizens | {happiness_emoji} {happiness_status}"

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
