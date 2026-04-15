import random
import asyncio
import logging
import discord
from bot.utils import format_number, create_embed

logger = logging.getLogger(__name__)

class EventManager:
    def __init__(self, db):
        self.db = db
        self.running = False
        
        # Global events that affect all or multiple civilizations
        self.global_events = [
            {
                "name": "Solar Flare",
                "description": "Cosmic radiation disrupts technology worldwide",
                "effects": {"tech_level": -1},
                "probability": 0.05,
                "global": True
            },
            {
                "name": "Meteor Shower",
                "description": "Meteors bring rare minerals to Earth",
                "effects": {"stone": 500, "gold": 200},
                "probability": 0.03,
                "global": False
            },
            {
                "name": "Divine Blessing",
                "description": "The gods smile upon civilization",
                "effects": {"happiness": 15, "food": 300},
                "probability": 0.02,
                "global": False
            },
            {
                "name": "Pandemic Outbreak",
                "description": "Disease spreads across the land",
                "effects": {"citizens": -50, "happiness": -20},
                "probability": 0.04,
                "global": True
            },
            {
                "name": "Golden Age",
                "description": "A period of unprecedented prosperity",
                "effects": {"gold": 1000, "happiness": 20, "citizens": 100},
                "probability": 0.01,
                "global": False
            }
        ]
        
        # Local events that affect individual civilizations
        self.local_events = [
            {
                "name": "Bandit Raid",
                "description": "Bandits attack your territory",
                "effects": {"gold": -200, "food": -100, "soldiers": -5},
                "probability": 0.08
            },
            {
                "name": "Merchant Caravan",
                "description": "Wealthy merchants visit your city",
                "effects": {"gold": 300, "happiness": 5},
                "probability": 0.10
            },
            {
                "name": "Natural Disaster",
                "description": "Earthquake destroys infrastructure",
                "effects": {"stone": -150, "wood": -100, "citizens": -30},
                "probability": 0.06
            },
            {
                "name": "Population Boom",
                "description": "Your cities attract new inhabitants",
                "effects": {"citizens": 75, "happiness": 10},
                "probability": 0.07
            },
            {
                "name": "Technology Breakthrough",
                "description": "Scientists make an important discovery",
                "effects": {"tech_level": 1},
                "probability": 0.05
            },
            {
                "name": "Spy Infiltration",
                "description": "Enemy spies are caught in your territory",
                "effects": {"spies": 3, "happiness": -5},
                "probability": 0.06
            },
            {
                "name": "Harvest Festival",
                "description": "A bountiful harvest brings joy",
                "effects": {"food": 400, "happiness": 15},
                "probability": 0.09
            },
            {
                "name": "Royal Wedding",
                "description": "A noble wedding boosts morale",
                "effects": {"happiness": 20, "gold": -100},
                "probability": 0.04
            },
            {
                "name": "Military Desertion",
                "description": "Some soldiers abandon their posts",
                "effects": {"soldiers": -15, "happiness": -10},
                "probability": 0.05
            },
            {
                "name": "Ancient Ruins Discovered",
                "description": "Archaeologists uncover valuable artifacts",
                "effects": {"gold": 500, "stone": 200, "tech_level": 1},
                "probability": 0.03
            },
            {
                "name": "Forest Fire",
                "description": "Wildfires consume wooden structures",
                "effects": {"wood": -300, "happiness": -8},
                "probability": 0.06
            },
            {
                "name": "Trade Route Established",
                "description": "New trade connections boost economy",
                "effects": {"gold": 400, "food": 200},
                "probability": 0.08
            },
            {
                "name": "Plague of Locusts",
                "description": "Insects destroy crops",
                "effects": {"food": -500, "happiness": -15},
                "probability": 0.05
            },
            {
                "name": "Military Academy Founded",
                "description": "Better training improves your army",
                "effects": {"soldiers": 25, "gold": -200},
                "probability": 0.04
            },
            {
                "name": "Diplomatic Summit",
                "description": "International relations improve",
                "effects": {"happiness": 12, "gold": 150},
                "probability": 0.06
            }
        ]
        
        # Ideology-specific events
        self.ideology_events = {
            "fascism": [
                {
                    "name": "Military Parade",
                    "description": "Grand military display boosts nationalist pride",
                    "effects": {"happiness": 15, "soldiers": 20},
                    "probability": 0.12
                },
                {
                    "name": "Political Purge",
                    "description": "Internal enemies are eliminated",
                    "effects": {"happiness": -10, "spies": 10},
                    "probability": 0.08
                }
            ],
            "democracy": [
                {
                    "name": "Free Elections",
                    "description": "Democratic process strengthens society",
                    "effects": {"happiness": 20, "citizens": 50},
                    "probability": 0.10
                },
                {
                    "name": "Parliamentary Debate",
                    "description": "Political discourse slows decision-making",
                    "effects": {"happiness": 5, "gold": -100},
                    "probability": 0.09
                }
            ],
            "communism": [
                {
                    "name": "Worker's Revolution",
                    "description": "The proletariat rises with renewed vigor",
                    "effects": {"citizens": 100, "happiness": 10},
                    "probability": 0.11
                },
                {
                    "name": "Five Year Plan",
                    "description": "Central planning boosts production",
                    "effects": {"stone": 300, "wood": 300, "food": 400},
                    "probability": 0.08
                }
            ],
            "theocracy": [
                {
                    "name": "Divine Revelation",
                    "description": "Religious vision inspires the faithful",
                    "effects": {"happiness": 25, "tech_level": 1},
                    "probability": 0.09
                },
                {
                    "name": "Religious Festival",
                    "description": "Holy celebration unites the people",
                    "effects": {"happiness": 18, "food": 200},
                    "probability": 0.12
                }
            ],
            "anarchy": [
                {
                    "name": "Chaos Erupts",
                    "description": "Lawlessness brings both opportunity and danger",
                    "effects": {"gold": 300, "happiness": -15, "soldiers": -10},
                    "probability": 0.15
                },
                {
                    "name": "Spontaneous Organization",
                    "description": "Citizens self-organize for mutual benefit",
                    "effects": {"citizens": 60, "happiness": 12},
                    "probability": 0.10
                }
            ]
        }

    async def start_random_events(self, bot):
        """Start the random events loop"""
        if self.running:
            return
            
        self.running = True
        logger.info("Random events system started")
        
        while self.running:
            try:
                await asyncio.sleep(1800)  # Check every 30 minutes
                await self.process_random_events(bot)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in random events loop: {e}")
                await asyncio.sleep(300)  # Wait 5 minutes before retrying

    def stop_random_events(self):
        """Stop the random events loop"""
        self.running = False
        logger.info("Random events system stopped")

    async def process_random_events(self, bot):
        """Process random events for all civilizations"""
        try:
            # Get all active civilizations
            civilizations = self.db.get_all_civilizations()
            
            if not civilizations:
                return
                
            # Check for global events first
            await self._check_global_events(bot, civilizations)
            
            # Process local events for each civilization
            for civ in civilizations:
                await self._check_local_events(bot, civ)
                
        except Exception as e:
            logger.error(f"Error processing random events: {e}")

    async def _check_global_events(self, bot, civilizations):
        """Check and process global events"""
        for event in self.global_events:
            if random.random() < event["probability"]:
                if event.get("global", False):
                    # Apply to all civilizations
                    affected_civs = []
                    for civ in civilizations:
                        self._apply_event_effects(civ['user_id'], event["effects"])
                        affected_civs.append(civ['name'])
                        
                    # Log global event
                    self.db.log_event(None, "global_event", event["name"], event["description"])
                    
                    # Announce globally (simplified)
                    logger.info(f"Global event triggered: {event['name']} - {len(affected_civs)} civilizations affected")
                    
                else:
                    # Apply to random civilization
                    target_civ = random.choice(civilizations)
                    self._apply_event_effects(target_civ['user_id'], event["effects"])
                    self.db.log_event(target_civ['user_id'], "global_event", event["name"], event["description"])
                    
                    # Try to notify the affected user
                    await self._notify_user_of_event(bot, target_civ['user_id'], event)
                    
                break  # Only one global event per cycle

    async def _check_local_events(self, bot, civ):
        """Check and process local events for a civilization"""
        user_id = civ['user_id']
        ideology = civ.get('ideology', '')
        
        # Apply ideology modifier to event frequency
        base_chance = 0.15  # 15% base chance per 30-minute cycle
        
        if ideology == 'anarchy':
            event_modifier = self._get_anarchy_modifier(civ)
            base_chance *= event_modifier
            
        if random.random() < base_chance:
            # Choose event type
            available_events = self.local_events.copy()
            
            # Add ideology-specific events
            if ideology in self.ideology_events:
                available_events.extend(self.ideology_events[ideology])
                
            # Weight events by probability
            event = self._select_weighted_event(available_events)
            
            if event:
                # Apply effects
                self._apply_event_effects(user_id, event["effects"])
                
                # Log event
                self.db.log_event(user_id, "random_event", event["name"], event["description"])
                
                # Notify user
                await self._notify_user_of_event(bot, user_id, event)

    def _select_weighted_event(self, events):
        """Select an event based on probability weights"""
        weighted_events = []
        for event in events:
            weight = int(event["probability"] * 1000)  # Convert to integer weight
            weighted_events.extend([event] * weight)
            
        return random.choice(weighted_events) if weighted_events else None

    def _apply_event_effects(self, user_id, effects):
        """Apply event effects to a civilization"""
        try:
            civ = self.db.get_civilization(user_id)
            if not civ:
                return
                
            # Separate effects by category
            resource_effects = {}
            population_effects = {}
            military_effects = {}
            territory_effects = {}
            
            for effect, value in effects.items():
                if effect in ['gold', 'food', 'wood', 'stone']:
                    resource_effects[effect] = value
                elif effect in ['citizens', 'happiness', 'hunger']:
                    population_effects[effect] = value
                elif effect in ['soldiers', 'spies', 'tech_level']:
                    military_effects[effect] = value
                elif effect in ['land_size']:
                    territory_effects[effect] = value
                    
            # Apply effects using civilization manager methods
            from bot.civilization import CivilizationManager
            civ_manager = CivilizationManager(self.db)
            
            if resource_effects:
                civ_manager.update_resources(user_id, resource_effects)
            if population_effects:
                civ_manager.update_population(user_id, population_effects)
            if military_effects:
                civ_manager.update_military(user_id, military_effects)
            if territory_effects:
                # Handle territory updates
                territory = civ['territory']
                for effect, value in territory_effects.items():
                    territory[effect] = max(100, territory.get(effect, 1000) + value)  # Minimum 100 km²
                self.db.update_civilization(user_id, {"territory": territory})
                
        except Exception as e:
            logger.error(f"Error applying event effects for user {user_id}: {e}")

    def _get_anarchy_modifier(self, civ):
        """Get event frequency modifier for anarchy ideology"""
        # Anarchy causes events to happen twice as often
        return 2.0

    async def _notify_user_of_event(self, bot, user_id, event):
        """Notify a user about a random event"""
        try:
            user = await bot.fetch_user(int(user_id))
            
            # Determine event color based on effects
            color = self._get_event_color(event["effects"])
            
            embed = create_embed(
                f"📰 Random Event: {event['name']}",
                event["description"],
                color
            )
            
            # Add effects description
            effects_text = self._format_event_effects(event["effects"])
            embed.add_field(name="Effects", value=effects_text, inline=False)
            
            await user.send(embed=embed)
            
        except Exception as e:
            logger.debug(f"Could not notify user {user_id} of event: {e}")

    def _get_event_color(self, effects):
        """Determine embed color based on event effects"""
        positive_score = 0
        negative_score = 0
        
        positive_effects = ['gold', 'food', 'wood', 'stone', 'citizens', 'happiness', 'soldiers', 'spies', 'tech_level', 'land_size']
        negative_effects = ['hunger']
        
        for effect, value in effects.items():
            if effect in positive_effects and value > 0:
                positive_score += 1
            elif effect in positive_effects and value < 0:
                negative_score += 1
            elif effect in negative_effects and value > 0:
                negative_score += 1
            elif effect in negative_effects and value < 0:
                positive_score += 1
                
        if positive_score > negative_score:
            return discord.Color.green()
        elif negative_score > positive_score:
            return discord.Color.red()
        else:
            return discord.Color.blue()

    def _format_event_effects(self, effects):
        """Format event effects for display"""
        effect_lines = []
        
        effect_icons = {
            'gold': '🪙', 'food': '🌾', 'wood': '🪵', 'stone': '🪨',
            'citizens': '👤', 'happiness': '😊', 'hunger': '🍽️',
            'soldiers': '⚔️', 'spies': '🕵️', 'tech_level': '🔬',
            'land_size': '🏞️'
        }
        
        for effect, value in effects.items():
            icon = effect_icons.get(effect, '📊')
            if value > 0:
                effect_lines.append(f"{icon} +{format_number(value)} {effect.replace('_', ' ').title()}")
            else:
                effect_lines.append(f"{icon} {format_number(value)} {effect.replace('_', ' ').title()}")
                
        return '\n'.join(effect_lines) if effect_lines else "No direct effects"

    async def trigger_manual_event(self, bot, user_id, event_name):
        """Manually trigger a specific event (for admin use)"""
        try:
            # Find event in all event lists
            all_events = self.global_events + self.local_events
            for ideology_events in self.ideology_events.values():
                all_events.extend(ideology_events)
                
            target_event = None
            for event in all_events:
                if event["name"].lower() == event_name.lower():
                    target_event = event
                    break
                    
            if not target_event:
                return False
                
            # Apply event
            self._apply_event_effects(user_id, target_event["effects"])
            self.db.log_event(user_id, "manual_event", target_event["name"], target_event["description"])
            
            # Notify user
            await self._notify_user_of_event(bot, user_id, target_event)
            
            return True
            
        except Exception as e:
            logger.error(f"Error triggering manual event: {e}")
            return False

    def get_event_statistics(self):
        """Get statistics about recent events"""
        try:
            events = self.db.get_recent_events(100)
            
            # Count events by type
            event_types = {}
            for event in events:
                event_type = event['event_type']
                event_types[event_type] = event_types.get(event_type, 0) + 1
                
            # Count events by name
            event_names = {}
            for event in events:
                name = event['title']
                event_names[name] = event_names.get(name, 0) + 1
                
            return {
                "total_events": len(events),
                "event_types": event_types,
                "event_names": event_names,
                "recent_events": events[:10]  # Last 10 events
            }
            
        except Exception as e:
            logger.error(f"Error getting event statistics: {e}")
            return {}
