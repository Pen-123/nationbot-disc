# Civilization.py
import random
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
from bot.database import Database

logger = logging.getLogger(__name__)

class CivilizationManager:
    def __init__(self, db: Database):
        self.db = db
        # Expanded ideology modifiers to include socialism, terrorism, capitalism, federalism, monarchy
        self.ideology_modifiers = {
            "fascism": {
                "soldier_training_speed": 1.25,
                "diplomacy_success": 0.85,
                "luck_modifier": 0.90
            },
            "democracy": {
                "happiness_boost": 1.20,
                "trade_profit": 1.10,
                "soldier_training_speed": 0.85
            },
            "communism": {
                "citizen_productivity": 1.10,
                "tech_speed": 0.90
            },
            "theocracy": {
                "propaganda_success": 1.15,
                "happiness_boost": 1.05,
                "tech_speed": 0.90
            },
            "anarchy": {
                "random_event_frequency": 2.0,
                "soldier_upkeep": 0.0,
                "spy_success": 0.80
            },
            "destruction": {
                "combat_strength": 1.35,
                "resource_production": 0.75,
                "soldier_training_speed": 1.40,
                "happiness_boost": 0.70,
                "diplomacy_success": 0.50
            },
            "pacifist": {
                "happiness_boost": 1.35,
                "population_growth": 1.25,
                "trade_profit": 1.20,
                "soldier_training_speed": 0.40,
                "combat_strength": 0.60,
                "diplomacy_success": 1.25
            },
            # New ideologies
            "socialism": {
                "citizen_productivity": 1.15,
                "happiness_boost": 1.10,
                "trade_profit": 0.90
            },
            "terrorism": {
                # terrorism is treated as a shadow/guerrilla ideology: strong raids/spy ops,
                # poor diplomacy and unstable resources
                "guerrilla_effectiveness": 1.40,
                "spy_success": 1.30,
                "diplomacy_success": 0.50,
                "resource_production": 0.80,
                "unrest_multiplier": 1.25
            },
            "capitalism": {
                "trade_profit": 1.20,
                "gold_generation": 1.15,
                "happiness_boost": 0.90  # inequality can lower happiness
            },
            "federalism": {
                "stability": 1.10,
                "diplomacy_success": 1.10,
                "regional_production": 1.05
            },
            "monarchy": {
                "loyalty": 1.10,
                "soldier_morale": 1.10,
                "reform_speed": 0.90,
                "happiness_boost": 1.10
            }
        }

        # Region-specific modifiers
        self.region_modifiers = {
            "Asia": {"food_production": 1.20, "population_capacity": 1.25},
            "Europe": {"tech_research": 1.25, "gold_production": 1.15},
            "Africa": {"mining_efficiency": 1.30, "stone_production": 1.20},
            "North America": {"balanced_production": 1.10, "trade_efficiency": 1.15},
            "South America": {"food_production": 1.25, "wood_production": 1.15},
            "Middle East": {"gold_production": 1.40, "oil_resources": 1.30},
            "Oceania": {"happiness": 1.15, "naval_advantage": 1.20},
            "Antarctica": {"research_speed": 1.25, "unique_discoveries": 1.30}
        }

    def create_civilization(self, user_id: str, name: str, bonus_resources: Dict = None, bonuses: Dict = None, hyper_item: str = None) -> bool:
        """Create a new civilization"""
        try:
            return self.db.create_civilization(user_id, name, bonus_resources, bonuses, hyper_item)
        except Exception as e:
            logger.error(f"Error creating civilization for {user_id}: {e}")
            return False

    def get_civilization(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Get civilization data with proper error handling"""
        try:
            civ = self.db.get_civilization(user_id)
            if civ and 'employed' not in civ['population']:
                civ['population']['employed'] = civ['population']['citizens'] // 2
                # Use a separate method to update employment to avoid recursion
                self._update_employment_only(user_id, civ['population']['employed'])
            return civ
        except Exception as e:
            logger.error(f"Error getting civilization for {user_id}: {e}")
            return None

    def reset_civilization(self, user_id: str) -> bool:
        """Completely reset a user's civilization"""
        try:
            civ = self.get_civilization(user_id)
            if not civ:
                return False
                
            if self.db.delete_civilization(user_id):
                logger.info(f"Civilization reset for user {user_id}")
                return True
            else:
                logger.error(f"Failed to reset civilization for user {user_id}")
                return False
                
        except Exception as e:
            logger.error(f"Error resetting civilization for {user_id}: {e}")
            return False

    def _update_employment_only(self, user_id: str, employed: int) -> bool:
        """Update only the employment field without recursion"""
        try:
            civ = self.get_civilization(user_id)
            if not civ:
                return False
                
            population = civ['population'].copy()
            population['employed'] = employed
            return self.db.update_civilization(user_id, {"population": population})
        except Exception as e:
            logger.error(f"Error updating employment for {user_id}: {e}")
            return False

    def check_civil_war_risk(self, user_id: str) -> bool:
        """Check if a civil war occurs based on happiness level"""
        try:
            civ = self.get_civilization(user_id)
            if not civ:
                return False
            
            happiness = civ['population']['happiness']
            
            # Only check if happiness is below 50%
            if happiness >= 50:
                return False
            
            # Calculate civil war chance: higher risk the lower the happiness
            # At 0 happiness: 40% chance, at 49 happiness: 1% chance
            civil_war_chance = (50 - happiness) * 0.8  # 0.8% per happiness point below 50
            
            # Add additional risk factors
            if civ.get('ideology') == 'terrorism':
                civil_war_chance *= 1.5  # Terrorism has higher unrest
            elif civ.get('ideology') == 'anarchy':
                civil_war_chance *= 1.3  # Anarchy is unstable
            
            # Check if civil war occurs
            if random.random() * 100 < civil_war_chance:
                self.trigger_civil_war(user_id)
                return True
            
            return False
        except Exception as e:
            logger.error(f"Error checking civil war risk for {user_id}: {e}")
            return False

    def trigger_civil_war(self, user_id: str):
        """Trigger a civil war event"""
        try:
            civ = self.get_civilization(user_id)
            if not civ:
                return
            
            # Calculate losses: 50% of resources
            resources = civ['resources']
            resource_losses = {}
            for resource, amount in resources.items():
                if amount > 0:
                    loss = amount // 2  # 50% loss
                    resource_losses[resource] = -loss
            
            # Apply resource losses
            if resource_losses:
                self.update_resources(user_id, resource_losses)
            
            # Additional population and military losses
            population_loss = max(1, civ['population']['citizens'] // 20)  # 5% population loss
            soldier_loss = max(1, civ['military']['soldiers'] // 10)  # 10% soldier loss
            
            self.update_population(user_id, {"citizens": -population_loss, "happiness": -15})
            self.update_military(user_id, {"soldiers": -soldier_loss})
            
            # Log the civil war event
            self.db.log_event(
                user_id, 
                "civil_war", 
                "Civil War Erupts!", 
                f"A devastating civil war has broken out! Lost 50% of resources, {population_loss} citizens, and {soldier_loss} soldiers due to internal conflict."
            )
            
            logger.info(f"Civil war triggered for {user_id}. Lost 50% of resources.")
            
        except Exception as e:
            logger.error(f"Error triggering civil war for {user_id}: {e}")

    def set_ideology(self, user_id: str, ideology: str) -> bool:
        """Set civilization ideology"""
        try:
            return self.db.update_civilization(user_id, {"ideology": ideology})
        except Exception as e:
            logger.error(f"Error setting ideology for {user_id}: {e}")
            return False

    def set_region(self, user_id: str, region: str) -> bool:
        """Set civilization region"""
        try:
            return self.db.update_civilization(user_id, {"region": region})
        except Exception as e:
            logger.error(f"Error setting region for {user_id}: {e}")
            return False

    def update_resources(self, user_id: str, resource_changes: Dict[str, int]) -> bool:
        """Update civilization resources"""
        try:
            civ = self.get_civilization(user_id)
            if not civ:
                return False
                
            resources = civ['resources']
            for resource, change in resource_changes.items():
                if resource in resources:
                    resources[resource] = max(0, resources[resource] + change)
            
            return self.db.update_civilization(user_id, {"resources": resources})
        except Exception as e:
            logger.error(f"Error updating resources for {user_id}: {e}")
            return False

    def update_population(self, user_id: str, population_changes: Dict[str, int]) -> bool:
        """Update civilization population stats"""
        try:
            civ = self.get_civilization(user_id)
            if not civ:
                return False
                
            population = civ['population']
            for stat, change in population_changes.items():
                if stat in population:
                    if stat in ['happiness', 'hunger']:
                        population[stat] = max(0, min(100, population[stat] + change))
                    elif stat == 'citizens':
                        population['citizens'] = max(0, population['citizens'] + change)
                        population['employed'] = min(population.get('employed', 0), population['citizens'])
                    else:
                        population[stat] = max(0, population[stat] + change)
            
            return self.db.update_civilization(user_id, {"population": population})
        except Exception as e:
            logger.error(f"Error updating population for {user_id}: {e}")
            return False

    def update_military(self, user_id: str, military_changes: Dict[str, int]) -> bool:
        """Update civilization military stats, checking for tech level increase"""
        try:
            civ = self.get_civilization(user_id)
            if not civ:
                return False
                
            military = civ['military']
            old_tech_level = military['tech_level']
            
            for stat, change in military_changes.items():
                if stat in military:
                    if stat == 'tech_level':
                        military[stat] = min(10, max(1, military[stat] + change))  # Cap at 10
                    else:
                        military[stat] = max(0, military[stat] + change)
            
            new_tech_level = military['tech_level']
            result = self.db.update_civilization(user_id, {"military": military})
            
            if result and new_tech_level > old_tech_level and new_tech_level <= 10:
                self.db.generate_card_selection(user_id, new_tech_level)
                self.db.log_event(user_id, "tech_advance", "Tech Level Increased",
                                f"Reached tech level {new_tech_level}. New card selection available!")
            
            return result
        except Exception as e:
            logger.error(f"Error updating military for {user_id}: {e}")
            return False

    def update_employment(self, user_id: str, change: int) -> bool:
        """Update employed citizens"""
        try:
            civ = self.get_civilization(user_id)
            if not civ:
                return False
            
            population = civ['population']
            employed = population.get('employed', 0) + change
            employed = max(0, min(population['citizens'], employed))
            
            population['employed'] = employed
            return self.db.update_civilization(user_id, {"population": population})
        except Exception as e:
            logger.error(f"Error updating employment for {user_id}: {e}")
            return False

    def update_territory(self, user_id: str, territory_changes: Dict[str, int]) -> bool:
        """Update civilization territory stats"""
        try:
            civ = self.get_civilization(user_id)
            if not civ:
                return False
                
            territory = civ['territory']
            for stat, change in territory_changes.items():
                if stat in territory:
                    territory[stat] = max(0, territory[stat] + change)
            
            return self.db.update_civilization(user_id, {"territory": territory})
        except Exception as e:
            logger.error(f"Error updating territory for {user_id}: {e}")
            return False

    def get_employment_rate(self, user_id: str) -> float:
        """Get employment rate percentage"""
        try:
            civ = self.get_civilization(user_id)
            if not civ:
                return 0.0
            
            population = civ['population']
            citizens = population['citizens']
            employed = population.get('employed', 0)
            return (employed / citizens * 100) if citizens > 0 else 0.0
        except Exception as e:
            logger.error(f"Error getting employment rate for {user_id}: {e}")
            return 0.0

    def add_hyper_item(self, user_id: str, item: str) -> bool:
        """Add a HyperItem to civilization"""
        try:
            civ = self.get_civilization(user_id)
            if not civ:
                return False
                
            hyper_items = civ['hyper_items']
            hyper_items.append(item)
            
            return self.db.update_civilization(user_id, {"hyper_items": hyper_items})
        except Exception as e:
            logger.error(f"Error adding hyper item for {user_id}: {e}")
            return False

    def use_hyper_item(self, user_id: str, item: str) -> bool:
        """Use/consume a HyperItem"""
        try:
            civ = self.get_civilization(user_id)
            if not civ:
                return False
                
            hyper_items = civ['hyper_items']
            if item not in hyper_items:
                return False
                
            hyper_items.remove(item)
            return self.db.update_civilization(user_id, {"hyper_items": hyper_items})
        except Exception as e:
            logger.error(f"Error using hyper item for {user_id}: {e}")
            return False

    def apply_card_effect(self, user_id: str, card: Dict) -> bool:
        """Apply the effect of a selected card"""
        try:
            civ = self.get_civilization(user_id)
            if not civ:
                return False
                
            effect = card['effect']
            card_type = card['type']
            
            if card_type == "bonus":
                bonuses = civ['bonuses']
                for key, value in effect.items():
                    bonuses[key] = bonuses.get(key, 0) + value
                self.db.update_civilization(user_id, {"bonuses": bonuses})
            
            elif card_type == "one_time":
                if "gold" in effect or "food" in effect or "stone" in effect or "wood" in effect:
                    self.update_resources(user_id, effect)
                elif "soldiers" in effect or "spies" in effect or "tech_level" in effect:
                    self.update_military(user_id, effect)
                elif "citizens" in effect or "happiness" in effect or "hunger" in effect:
                    self.update_population(user_id, effect)
            
            selected_cards = civ['selected_cards']
            selected_cards.append(card['name'])
            self.db.update_civilization(user_id, {"selected_cards": selected_cards})
            
            self.db.log_event(user_id, "card_selected", f"Card Selected: {card['name']}",
                             card['description'], effect)
            return True
        except Exception as e:
            logger.error(f"Error applying card effect for {user_id}: {e}")
            return False

    def calculate_resource_income(self, user_id: str) -> Dict[str, int]:
        """Calculate passive resource income with region modifiers and tech level gold multiplier"""
        try:
            civ = self.get_civilization(user_id)
            if not civ:
                return {}
                
            population = civ['population']
            territory = civ['territory']
            military = civ['military']
            ideology = civ.get('ideology', '')
            bonuses = civ['bonuses']
            employment_rate = self.get_employment_rate(user_id)
            employment_modifier = employment_rate / 100
            
            # Get region modifier if region is set
            region_modifier = 1.0
            region = civ.get('region')
            if region and region in self.region_modifiers:
                region_bonus = self.region_modifiers[region]
                if region_bonus.get('food_production'):
                    region_modifier *= region_bonus['food_production']
                if region_bonus.get('gold_production'):
                    region_modifier *= region_bonus['gold_production']
                if region_bonus.get('mining_efficiency'):
                    region_modifier *= region_bonus['mining_efficiency']
                if region_bonus.get('balanced_production'):
                    region_modifier *= region_bonus['balanced_production']

            base_gold = int(population['citizens'] * 0.1 * (territory['land_size'] / 1000) * employment_modifier)
            base_food = int(population['citizens'] * 0.2 * employment_modifier)
            
            # Apply tech level gold multiplier: 0.5x per tech level
            tech_level = military['tech_level']
            tech_gold_multiplier = 0.5 * tech_level
            base_gold = int(base_gold * (1 + tech_gold_multiplier))
            
            resource_modifier = 1.0
            # Existing ideology adjustments
            if ideology == 'communism':
                resource_modifier *= self.ideology_modifiers['communism']['citizen_productivity']
            elif ideology == 'democracy':
                resource_modifier *= self.ideology_modifiers['democracy']['trade_profit']
            elif ideology == 'destruction':
                resource_modifier *= self.ideology_modifiers['destruction']['resource_production']
            elif ideology == 'pacifist':
                resource_modifier *= self.ideology_modifiers['pacifist']['trade_profit']
            # New ideology adjustments
            elif ideology == 'socialism':
                resource_modifier *= self.ideology_modifiers['socialism']['citizen_productivity']
            elif ideology == 'capitalism':
                # capitalism favors gold/trade more than raw production
                resource_modifier *= self.ideology_modifiers['capitalism']['trade_profit']
                base_gold = int(base_gold * self.ideology_modifiers['capitalism']['gold_generation'])
            elif ideology == 'federalism':
                resource_modifier *= self.ideology_modifiers['federalism']['regional_production']
            elif ideology == 'monarchy':
                resource_modifier *= 1.0  # monarchy primarily affects morale/happiness, not base resource yield
            elif ideology == 'terrorism':
                resource_modifier *= self.ideology_modifiers['terrorism']['resource_production']
            
            resource_modifier *= (1 + bonuses.get('resource_production', 0) / 100)
            
            # Apply region modifier
            resource_modifier *= region_modifier
            
            return {
                "gold": int(base_gold * resource_modifier),
                "food": int(base_food * resource_modifier),
                "stone": random.randint(0, 5),
                "wood": random.randint(0, 5)
            }
        except Exception as e:
            logger.error(f"Error calculating resource income for {user_id}: {e}")
            return {}

    def calculate_upkeep_costs(self, user_id: str) -> Dict[str, int]:
        """Calculate military and population upkeep costs"""
        try:
            civ = self.get_civilization(user_id)
            if not civ:
                return {}
                
            population = civ['population']
            military = civ['military']
            ideology = civ.get('ideology', '')
            
            food_consumption = int(population['citizens'] * 0.3)
            soldier_upkeep = military['soldiers'] * 2
            spy_upkeep = military['spies'] * 5
            
            if ideology == 'anarchy':
                soldier_upkeep = 0
            # terrorism increases use of spies/guerrilla ops -> higher spy upkeep
            if ideology == 'terrorism':
                spy_upkeep = int(spy_upkeep * 1.3)
            # monarchy + fascism may improve soldier morale but not reduce upkeep by default
            # socialism may add minor upkeep changes via state support; keep base values
            
            return {
                "food": food_consumption,
                "gold": soldier_upkeep + spy_upkeep
            }
        except Exception as e:
            logger.error(f"Error calculating upkeep costs for {user_id}: {e}")
            return {}

    def apply_happiness_effects(self, user_id: str):
        """Apply effects based on civilization happiness"""
        try:
            civ = self.get_civilization(user_id)
            if not civ:
                return
                
            population = civ['population']
            happiness = population['happiness']
            bonuses = civ['bonuses']
            ideology = civ.get('ideology', '')
            
            # Apply region happiness bonus if applicable
            region = civ.get('region')
            if region and region in self.region_modifiers and self.region_modifiers[region].get('happiness'):
                happiness = int(happiness * self.region_modifiers[region]['happiness'])
            
            happiness_modifier = 1 + bonuses.get('happiness_boost', 0) / 100
            # apply ideology intrinsic happiness boosts if present
            if ideology in self.ideology_modifiers and 'happiness_boost' in self.ideology_modifiers[ideology]:
                # some ideologies express happiness_boost as a multiplier (e.g., 1.10)
                ide_happy = self.ideology_modifiers[ideology]['happiness_boost']
                # if ide_happy looks like a multiplier (>1), convert to additive percent boost
                if ide_happy > 1.0:
                    happiness_modifier *= ide_happy
                else:
                    happiness_modifier += ide_happy
            
            happiness = int(happiness * happiness_modifier)
            
            if happiness < 20:
                if random.random() < 0.1:
                    revolt_loss = int(population['citizens'] * 0.05)
                    self.update_population(user_id, {"citizens": -revolt_loss})
                    self.db.log_event(user_id, "revolt", "Population Revolt",
                                    f"Low happiness caused {revolt_loss} citizens to leave!")
            
            elif happiness > 80:
                growth_rate = bonuses.get('population_growth', 0) / 100
                if ideology == 'pacifist':
                    growth_rate += self.ideology_modifiers['pacifist']['population_growth'] - 1
                # socialism and monarchy can add to growth/happiness effects
                if ideology == 'socialism':
                    growth_rate += (self.ideology_modifiers['socialism'].get('citizen_productivity', 1.0) - 1.0)
                if ideology == 'monarchy':
                    growth_rate += (self.ideology_modifiers['monarchy'].get('loyalty', 1.0) - 1.0) * 0.25
                if random.random() < (0.15 + growth_rate):
                    growth = int(population['citizens'] * (0.03 + growth_rate))
                    self.update_population(user_id, {"citizens": growth})
                    self.db.log_event(user_id, "growth", "Population Boom",
                                    f"High happiness attracted {growth} new citizens!")
        except Exception as e:
            logger.error(f"Error applying happiness effects for {user_id}: {e}")

    def process_hunger(self, user_id: str):
        """Process hunger effects on population"""
        try:
            civ = self.get_civilization(user_id)
            if not civ:
                return
                
            population = civ['population']
            resources = civ['resources']
            
            food_needed = int(population['citizens'] * 0.2)
            
            if resources['food'] < food_needed:
                hunger_increase = min(20, food_needed - resources['food'])
                self.update_population(user_id, {"hunger": hunger_increase})
                
                if population['hunger'] > 80:
                    starvation_loss = int(population['citizens'] * 0.02)
                    self.update_population(user_id, {"citizens": -starvation_loss, "happiness": -10})
                    self.db.log_event(user_id, "famine", "Famine Strikes",
                                    f"Severe hunger caused {starvation_loss} citizens to perish!")
            else:
                self.update_resources(user_id, {"food": -food_needed})
                if population['hunger'] > 0:
                    self.update_population(user_id, {"hunger": -5})
        except Exception as e:
            logger.error(f"Error processing hunger for {user_id}: {e}")

    def get_ideology_modifier(self, user_id: str, modifier_type: str) -> float:
        """Get ideology modifier for specific action"""
        try:
            civ = self.get_civilization(user_id)
            if not civ or not civ.get('ideology'):
                return 1.0
                
            ideology = civ['ideology']
            modifiers = self.ideology_modifiers.get(ideology, {})
            base_modifier = modifiers.get(modifier_type, 1.0)
            
            # For common action types combine base modifier with civ bonuses
            if modifier_type in ['soldier_training_speed', 'combat_strength', 'trade_profit', 'population_growth', 'citizen_productivity']:
                return base_modifier + (civ['bonuses'].get(modifier_type, 0) / 100)
            return base_modifier
        except Exception as e:
            logger.error(f"Error getting ideology modifier for {user_id}: {e}")
            return 1.0

    def get_region_modifier(self, user_id: str, modifier_type: str) -> float:
        """Get region modifier for specific action"""
        try:
            civ = self.get_civilization(user_id)
            if not civ or not civ.get('region'):
                return 1.0
                
            region = civ['region']
            modifiers = self.region_modifiers.get(region, {})
            return modifiers.get(modifier_type, 1.0)
        except Exception as e:
            logger.error(f"Error getting region modifier for {user_id}: {e}")
            return 1.0

    def get_name_bonus(self, user_id: str, bonus_type: str) -> float:
        """Get name-based bonus"""
        try:
            civ = self.get_civilization(user_id)
            if not civ:
                return 0.0
                
            bonuses = civ.get('bonuses', {})
            return bonuses.get(f"{bonus_type}_bonus", 0.0) / 100.0
        except Exception as e:
            logger.error(f"Error getting name bonus for {user_id}: {e}")
            return 0.0

    def calculate_total_modifier(self, user_id: str, action_type: str) -> float:
        """Calculate total modifier for an action including region effects"""
        try:
            base_modifier = 1.0
            ideology_modifier = self.get_ideology_modifier(user_id, action_type)
            region_modifier = self.get_region_modifier(user_id, action_type)
            name_bonus = 0.0
            
            if action_type == "luck":
                name_bonus = self.get_name_bonus(user_id, "luck")
            elif action_type == "diplomacy":
                name_bonus = self.get_name_bonus(user_id, "diplomacy")
                
            return base_modifier * ideology_modifier * region_modifier + name_bonus
        except Exception as e:
            logger.error(f"Error calculating total modifier for {user_id}: {e}")
            return 1.0

    def can_afford(self, user_id: str, costs: Dict[str, int]) -> bool:
        """Check if civilization can afford given costs"""
        try:
            civ = self.get_civilization(user_id)
            if not civ:
                return False
                
            resources = civ['resources']
            for resource, cost in costs.items():
                if resource in resources and resources[resource] < cost:
                    return False
                    
            return True
        except Exception as e:
            logger.error(f"Error checking affordability for {user_id}: {e}")
            return False

    def spend_resources(self, user_id: str, costs: Dict[str, int]) -> bool:
        """Spend resources if affordable"""
        try:
            if not self.can_afford(user_id, costs):
                return False
                
            negative_costs = {resource: -cost for resource, cost in costs.items()}
            return self.update_resources(user_id, negative_costs)
        except Exception as e:
            logger.error(f"Error spending resources for {user_id}: {e}")
            return False

    def get_civilization_power(self, user_id: str) -> int:
        """Calculate civilization's total power score"""
        try:
            civ = self.get_civilization(user_id)
            if not civ:
                return 0
                
            resources = civ['resources']
            population = civ['population']
            military = civ['military']
            territory = civ['territory']
            bonuses = civ['bonuses']
            
            resource_power = sum(resources.values()) // 10
            population_power = population['citizens'] * 2
            military_power = military['soldiers'] * 5 + military['spies'] * 10
            tech_power = military['tech_level'] * 100
            territory_power = territory['land_size'] // 100
            happiness_power = population['happiness']
            
            defense_bonus = bonuses.get('defense_strength', 0)
            total_power = (resource_power + population_power + military_power +
                          tech_power + territory_power + happiness_power)
            
            total_power = int(total_power * (1 + defense_bonus / 100))
            return total_power
        except Exception as e:
            logger.error(f"Error calculating civilization power for {user_id}: {e}")
            return 0
