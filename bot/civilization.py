# Civilization.py
import random
import logging
import requests
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from bot.database import Database
from bot.utils import get_territory_modifier

logger = logging.getLogger(__name__)


class CivilizationManager:
    CIV_CACHE_TTL = 5.0  # seconds

    def __init__(self, db: Database):
        self.db = db
        self._civ_cache = {}   # user_id -> (timestamp, civ_dict)
        self.ideology_modifiers = {
            "fascism": {"soldier_training_speed": 1.25, "diplomacy_success": 0.85, "luck_modifier": 0.90},
            "democracy": {"happiness_boost": 1.20, "trade_profit": 1.10, "soldier_training_speed": 0.85},
            "communism": {"citizen_productivity": 1.10, "tech_speed": 0.90},
            "theocracy": {"propaganda_success": 1.15, "happiness_boost": 1.05, "tech_speed": 0.90},
            "anarchy": {"random_event_frequency": 2.0, "soldier_upkeep": 0.0, "spy_success": 0.80},
            "destruction": {"combat_strength": 1.35, "resource_production": 0.75, "soldier_training_speed": 1.40, "happiness_boost": 0.70, "diplomacy_success": 0.50},
            "pacifist": {"happiness_boost": 1.35, "population_growth": 1.25, "trade_profit": 1.20, "soldier_training_speed": 0.40, "combat_strength": 0.60, "diplomacy_success": 1.25},
            "socialism": {"citizen_productivity": 1.15, "happiness_boost": 1.10, "trade_profit": 0.90},
            "terrorism": {"guerrilla_effectiveness": 1.40, "spy_success": 1.30, "diplomacy_success": 0.50, "resource_production": 0.80, "unrest_multiplier": 1.25},
            "capitalism": {"trade_profit": 1.20, "gold_generation": 1.15, "happiness_boost": 0.90},
            "federalism": {"stability": 1.10, "diplomacy_success": 1.10, "regional_production": 1.05},
            "monarchy": {"loyalty": 1.10, "soldier_morale": 1.10, "reform_speed": 0.90, "happiness_boost": 1.10}
        }
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

    # ---------- CACHE ----------
    def _invalidate_civ(self, user_id: str):
        self._civ_cache.pop(str(user_id), None)

    # ---------- EASTER EGGS ----------
    def _get_easter_egg_bonuses(self, civ_name: str) -> Dict[str, Any]:
        result = {}
        name_lower = civ_name.lower()
        if "ncsw" in name_lower:
            result["bonuses"] = {"soldier_training_speed": 20, "happiness_boost": -5}
            result["hyper_item"] = "Confederate Battle Flag"
            result["message"] = "⚔️ The spirit of the Confederacy lives on! (+20% soldier training, -5% happiness)"
        elif "confederate democracy" in name_lower:
            result["bonuses"] = {"diplomacy_success": 10, "happiness_boost": 10}
            result["message"] = "📜 A unique blend of Southern charm and democratic ideals! (+10% diplomacy, +10% happiness)"
        elif "uspr" in name_lower:
            result["bonuses"] = {"resource_production": 15, "trade_profit": -10}
            result["hyper_item"] = "Red Banner"
            result["message"] = "☭ The people's republic rises! (+15% resource production, -10% trade profit)"
        return result

    # ---------- CRUD ----------
    def create_civilization(self, user_id: str, name: str, bonus_resources: Dict = None,
                            bonuses: Dict = None, hyper_item: str = None) -> bool:
        try:
            egg = self._get_easter_egg_bonuses(name)
            if egg:
                if egg.get("bonuses"):
                    if bonuses is None:
                        bonuses = {}
                    bonuses.update(egg["bonuses"])
                if egg.get("hyper_item") and hyper_item is None:
                    hyper_item = egg["hyper_item"]
                logger.info(f"Easter egg activated for {user_id}: {egg.get('message', '')}")
            return self.db.create_civilization(user_id, name, bonus_resources, bonuses, hyper_item)
        except Exception as e:
            logger.error(f"Error creating civilization for {user_id}: {e}")
            return False

    def get_civilization(self, user_id: str, force: bool = False) -> Optional[Dict[str, Any]]:
        import time
        user_id = str(user_id)
        now = time.time()
        entry = self._civ_cache.get(user_id)
        if entry:
            ts, civ = entry
            if not force and now - ts < self.CIV_CACHE_TTL:
                return civ
        try:
            civ = self.db.get_civilization(user_id)
            if civ and 'employed' not in civ['population']:
                civ['population']['employed'] = civ['population']['citizens'] // 2
                self._update_employment_only(user_id, civ['population']['employed'])
            if civ:
                self._civ_cache[user_id] = (now, civ)
            return civ
        except Exception as e:
            logger.error(f"Error getting civilization for {user_id}: {e}")
            return None

    def reset_civilization(self, user_id: str) -> bool:
        try:
            civ = self.get_civilization(user_id)
            if not civ:
                return False
            if self.db.delete_civilization(user_id):
                self._invalidate_civ(user_id)
                logger.info(f"Civilization reset for user {user_id}")
                return True
            logger.error(f"Failed to reset civilization for user {user_id}")
            return False
        except Exception as e:
            logger.error(f"Error resetting civilization for {user_id}: {e}")
            return False

    def _update_employment_only(self, user_id: str, employed: int) -> bool:
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

    # =================================================================
    # CIVIL WAR SYSTEM
    # =================================================================

    def check_civil_war_risk(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Check if a civil war occurs. Returns civil-war context dict or None.

        30-minute per-user cooldown. Chance capped at 60%.
        If the player has only 1 territory, never starts a real civil war —
        the caller should show the classic warning instead.
        """
        try:
            civ = self.get_civilization(user_id)
            if not civ:
                return None

            # Already in a civil war? Don't start another.
            cw = civ.get('civil_war') or {}
            if cw.get('active'):
                return None

            # 30-minute cooldown
            last_cw = civ.get('last_civil_war_at')
            if last_cw:
                try:
                    last_dt = datetime.fromisoformat(last_cw)
                    if (datetime.utcnow() - last_dt).total_seconds() < 1800:
                        return None
                except (ValueError, TypeError):
                    pass

            happiness = civ['population']['happiness']
            if happiness >= 50:
                return None

            # Player must own more than 1 territory for a real civil war
            owned = self.db.get_player_territories(user_id)
            if len(owned) < 2:
                # Single-territory nation → classic warning only, no split
                return {"single_territory": True, "warning": True}

            # Base chance, capped at 40% before ideology
            chance = min(40.0, (50 - happiness) * 0.8)
            if civ.get('ideology') == 'terrorism':
                chance *= 1.5
            elif civ.get('ideology') == 'anarchy':
                chance *= 1.3
            chance = min(60.0, chance)

            if random.random() * 100 < chance:
                return self.trigger_civil_war(user_id)
            return None
        except Exception as e:
            logger.error(f"Error checking civil war risk for {user_id}: {e}")
            return None

    def trigger_civil_war(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Start a real civil war: split territories into loyalist/rebel halves."""
        try:
            civ = self.get_civilization(user_id, force=True)
            if not civ:
                return None

            owned = self.db.get_player_territories(user_id)
            if len(owned) < 2:
                return {"single_territory": True, "warning": True}

            # Randomly split territories
            shuffled = owned[:]
            random.shuffle(shuffled)
            split_idx = max(1, len(shuffled) // 2)
            loyalist = shuffled[:split_idx]
            rebel = shuffled[split_idx:]
            if not rebel:
                return {"single_territory": True, "warning": True}

            # Rebel strength scales with the player's military
            soldiers = civ['military']['soldiers']
            rebel_strength = max(10, int(soldiers * random.uniform(0.35, 0.55)))

            cw_state = {
                "active": True,
                "started_at": datetime.utcnow().isoformat(),
                "loyalist_territories": loyalist,
                "rebel_territories": rebel,
                "rebel_strength": rebel_strength,
                "player_wins": 0,
                "rebel_wins": 0,
            }

            self.db.update_civilization(user_id, {
                "civil_war": cw_state,
                "last_civil_war_at": datetime.utcnow().isoformat(),
            })
            self._invalidate_civ(user_id)

            # Immediate damage
            population_loss = max(1, civ['population']['citizens'] // 20)
            soldier_loss = max(1, soldiers // 10)
            self.update_population(user_id, {"citizens": -population_loss, "happiness": -15})
            self.update_military(user_id, {"soldiers": -soldier_loss})

            self.db.log_event(
                user_id, "civil_war", "Civil War Erupts!",
                f"A devastating civil war has broken out! "
                f"Rebels seized {len(rebel)} territories. "
                f"Lost {population_loss} citizens and {soldier_loss} soldiers."
            )

            return {
                "civil_war": True,
                "state": cw_state,
                "loyalist": loyalist,
                "rebel": rebel,
                "rebel_strength": rebel_strength,
            }
        except Exception as e:
            logger.error(f"Error triggering civil war for {user_id}: {e}")
            return None

    def get_civil_war_state(self, user_id: str) -> Optional[Dict[str, Any]]:
        civ = self.get_civilization(user_id, force=True)
        if not civ:
            return None
        cw = civ.get('civil_war') or {}
        return cw if cw.get('active') else None

    def fight_civil_war_battle(self, user_id: str, territory: str = None) -> Dict[str, Any]:
        """Player attacks a rebel-held territory. 30% offensive boost applies.

        Returns a result dict with victory/defeat, territory, losses, etc.
        """
        try:
            civ = self.get_civilization(user_id)
            if not civ:
                return {"error": "no_civ"}

            cw = civ.get('civil_war') or {}
            if not cw.get('active'):
                return {"error": "no_active_civil_war"}

            rebel_territories = cw.get('rebel_territories', [])
            if not rebel_territories:
                return self.end_civil_war(user_id, victory=True)

            # Pick target
            target = territory if territory in rebel_territories else random.choice(rebel_territories)

            # --- Combat math ---
            soldiers = civ['military']['soldiers']
            if soldiers < 5:
                return {"error": "not_enough_soldiers", "min": 5}

            # 30% offensive boost for the player (loyalists fight on home soil)
            OFFENSIVE_BOOST = 1.30
            player_power = soldiers * OFFENSIVE_BOOST * random.uniform(0.85, 1.15)
            rebel_power = cw.get('rebel_strength', 50) * random.uniform(0.85, 1.15)

            if player_power > rebel_power:
                # Victory — reclaim territory
                rebel_territories.remove(target)
                loyalist = cw.get('loyalist_territories', [])
                loyalist.append(target)
                cw['loyalist_territories'] = loyalist
                cw['rebel_territories'] = rebel_territories
                cw['player_wins'] = cw.get('player_wins', 0) + 1
                cw['rebel_strength'] = max(5, int(cw.get('rebel_strength', 50) * 0.85))

                self.db.update_civilization(user_id, {"civil_war": cw})
                self._invalidate_civ(user_id)

                # Restore territory ownership in Firestore
                self.db.conquer_territory(user_id, None, target)

                # Small soldier losses
                losses = max(1, int(soldiers * random.uniform(0.05, 0.12)))
                self.update_military(user_id, {"soldiers": -losses})
                self.update_population(user_id, {"happiness": 5})

                result = {
                    "victory": True,
                    "territory": target,
                    "soldier_losses": losses,
                    "rebel_strength": cw['rebel_strength'],
                    "remaining_rebel_territories": rebel_territories,
                }

                if not rebel_territories:
                    self.end_civil_war(user_id, victory=True)
                    result["war_over"] = True
                    result["victory_final"] = True

                self.db.log_event(user_id, "civil_war_battle", "Battle Won",
                                  f"Reclaimed {target} from rebels!")
                return result
            else:
                # Defeat — lose soldiers, rebels strengthen
                losses = max(1, int(soldiers * random.uniform(0.10, 0.20)))
                cw['rebel_wins'] = cw.get('rebel_wins', 0) + 1
                cw['rebel_strength'] = int(cw.get('rebel_strength', 50) * 1.10)

                # Chance the rebels seize another loyalist territory
                loyalist = cw.get('loyalist_territories', [])
                captured = None
                if len(loyalist) > 1 and random.random() < 0.35:
                    captured = random.choice(loyalist)
                    loyalist.remove(captured)
                    cw.setdefault('rebel_territories', []).append(captured)
                    cw['loyalist_territories'] = loyalist
                    self.db.conquer_territory(user_id, None, captured)  # still owned by player doc-wise

                self.db.update_civilization(user_id, {"civil_war": cw})
                self._invalidate_civ(user_id)
                self.update_military(user_id, {"soldiers": -losses})
                self.update_population(user_id, {"happiness": -5})

                self.db.log_event(user_id, "civil_war_battle", "Battle Lost",
                                  f"Rebels repelled the assault on {target}!"
                                  + (f" They captured {captured}!" if captured else ""))

                return {
                    "victory": False,
                    "territory": target,
                    "soldier_losses": losses,
                    "rebel_strength": cw['rebel_strength'],
                    "captured": captured,
                    "remaining_rebel_territories": cw.get('rebel_territories', []),
                }
        except Exception as e:
            logger.error(f"Error in civil war battle for {user_id}: {e}")
            return {"error": "internal", "detail": str(e)}

    def end_civil_war(self, user_id: str, victory: bool) -> Dict[str, Any]:
        """End the civil war. On victory, restore all rebel territories."""
        try:
            civ = self.get_civilization(user_id)
            if not civ:
                return {"error": "no_civ"}

            cw = civ.get('civil_war') or {}
            if not cw.get('active'):
                return {"error": "no_active_civil_war"}

            if victory:
                for t in cw.get('rebel_territories', []):
                    self.db.conquer_territory(user_id, None, t)
                self.update_population(user_id, {"happiness": 20})
                msg = "The rebellion is crushed! All territories reclaimed."
            else:
                # Defeat — lose the rebel territories for real
                for t in cw.get('rebel_territories', []):
                    self.db.client.collection("territories").document(t).delete()
                self.update_population(user_id, {"happiness": -25})
                msg = "The rebellion succeeded. Rebel territories are lost."

            self.db.update_civil_war_state = None  # no-op guard
            self.db.update_civilization(user_id, {"civil_war": None})
            self._invalidate_civ(user_id)

            self.db.log_event(user_id, "civil_war_end", "Civil War Ended", msg)
            return {"ended": True, "victory": victory, "message": msg}
        except Exception as e:
            logger.error(f"Error ending civil war for {user_id}: {e}")
            return {"error": "internal", "detail": str(e)}

    # =================================================================
    # AI DOCUMENTATION (called from cogs via asyncio.to_thread)
    # =================================================================

    def generate_civil_war_article(self, civ_name: str, state: Dict[str, Any],
                                   openrouter_key: str = None) -> Optional[str]:
        """Generate a news article about the civil war using OpenRouter laguna-s-2.1."""
        if not openrouter_key:
            return None
        try:
            prompt = (
                f"Write a short, dramatic news article (3-4 paragraphs) about a civil war "
                f"that just broke out in the nation of {civ_name}. "
                f"The rebels have seized {len(state.get('rebel_territories', []))} territories "
                f"including {', '.join(state.get('rebel_territories', [])[:3])}. "
                f"The loyalist government has {len(state.get('loyalist_territories', []))} territories remaining. "
                f"Rebel strength is estimated at {state.get('rebel_strength', 0)} troops. "
                f"Write it as a wire-service news report. Be dramatic but factual. No markdown headers."
            )
            resp = requests.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {openrouter_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": "poolside/laguna-s-2.1:free",
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 500,
                    "temperature": 0.8,
                },
                timeout=45,
            )
            if resp.status_code != 200:
                logger.error(f"OpenRouter article failed: {resp.status_code} {resp.text[:200]}")
                return None
            data = resp.json()
            return data['choices'][0]['message']['content']
        except Exception as e:
            logger.error(f"generate_civil_war_article error: {e}")
            return None

    def generate_civil_war_image_url(self, civ_name: str, state: Dict[str, Any]) -> str:
        """Return a Pollinations image URL illustrating the civil war."""
        prompt = (
            f"dramatic war illustration, civil war in {civ_name}, "
            f"burning cities, rebel flags, smoky battlefield, "
            f"{len(state.get('rebel_territories', []))} rebel-held regions, "
            f"epic cinematic style, no text"
        )
        encoded = requests.utils.quote(prompt)
        return (
            f"https://image.pollinations.ai/prompt/{encoded}"
            f"?width=1024&height=768&nologo=true&model=flux&seed={random.randint(1, 999999)}"
        )

    # =================================================================
    # UPDATERS
    # =================================================================

    def set_ideology(self, user_id: str, ideology: str) -> bool:
        try:
            ok = self.db.update_civilization(user_id, {"ideology": ideology})
            self._invalidate_civ(user_id)
            return ok
        except Exception as e:
            logger.error(f"Error setting ideology for {user_id}: {e}")
            return False

    def set_region(self, user_id: str, region: str) -> bool:
        try:
            ok = self.db.update_civilization(user_id, {"region": region})
            self._invalidate_civ(user_id)
            return ok
        except Exception as e:
            logger.error(f"Error setting region for {user_id}: {e}")
            return False

    def update_resources(self, user_id: str, resource_changes: Dict[str, int]) -> bool:
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
        try:
            civ = self.get_civilization(user_id)
            if not civ:
                return False
            population = civ['population']
            for stat, change in population_changes.items():
                if stat in population:
                    if stat == 'happiness':
                        population[stat] = max(-100, min(100, population[stat] + change))
                    elif stat == 'hunger':
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
        try:
            civ = self.get_civilization(user_id)
            if not civ:
                return False
            military = civ['military']
            old_tech_level = military['tech_level']
            for stat, change in military_changes.items():
                if stat in military:
                    if stat == 'tech_level':
                        military[stat] = min(10, max(1, military[stat] + change))
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

    # ---------- INCOME / UPKEEP ----------
    def calculate_resource_income(self, user_id: str) -> Dict[str, int]:
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

            region_modifier = 1.0
            region = civ.get('region')
            if region and region in self.region_modifiers:
                rb = self.region_modifiers[region]
                for k in ('food_production', 'gold_production', 'mining_efficiency', 'balanced_production'):
                    if rb.get(k):
                        region_modifier *= rb[k]

            territory_modifier = get_territory_modifier(territory['land_size'])
            base_gold = int(population['citizens'] * 0.1 * territory_modifier * employment_modifier)
            base_food = int(population['citizens'] * 0.2 * employment_modifier)

            tech_level = military['tech_level']
            tech_gold_multiplier = 0.15 * tech_level   # nerfed from 0.5
            base_gold = int(base_gold * (1 + tech_gold_multiplier))

            resource_modifier = 1.0
            if ideology == 'communism':
                resource_modifier *= self.ideology_modifiers['communism']['citizen_productivity']
            elif ideology == 'democracy':
                resource_modifier *= self.ideology_modifiers['democracy']['trade_profit']
            elif ideology == 'destruction':
                resource_modifier *= self.ideology_modifiers['destruction']['resource_production']
            elif ideology == 'pacifist':
                resource_modifier *= self.ideology_modifiers['pacifist']['trade_profit']
            elif ideology == 'socialism':
                resource_modifier *= self.ideology_modifiers['socialism']['citizen_productivity']
            elif ideology == 'capitalism':
                resource_modifier *= self.ideology_modifiers['capitalism']['trade_profit']
                base_gold = int(base_gold * self.ideology_modifiers['capitalism']['gold_generation'])
            elif ideology == 'federalism':
                resource_modifier *= self.ideology_modifiers['federalism']['regional_production']
            elif ideology == 'terrorism':
                resource_modifier *= self.ideology_modifiers['terrorism']['resource_production']

            resource_modifier *= (1 + bonuses.get('resource_production', 0) / 100)
            resource_modifier *= region_modifier

            happiness = population.get('happiness', 50)
            if happiness < 0:
                misery = max(0.0, 1 + (happiness / 100.0))
                resource_modifier *= misery

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
            if ideology == 'terrorism':
                spy_upkeep = int(spy_upkeep * 1.3)
            return {"food": food_consumption, "gold": soldier_upkeep + spy_upkeep}
        except Exception as e:
            logger.error(f"Error calculating upkeep costs for {user_id}: {e}")
            return {}

    # ---------- HAPPINESS EFFECTS ----------
    def apply_happiness_effects(self, user_id: str):
        try:
            civ = self.get_civilization(user_id)
            if not civ:
                return
            population = civ['population']
            happiness = population['happiness']
            bonuses = civ['bonuses']
            ideology = civ.get('ideology', '')

            region = civ.get('region')
            if region and region in self.region_modifiers and self.region_modifiers[region].get('happiness'):
                happiness = int(happiness * self.region_modifiers[region]['happiness'])

            happiness_modifier = 1 + bonuses.get('happiness_boost', 0) / 100
            if ideology in self.ideology_modifiers and 'happiness_boost' in self.ideology_modifiers[ideology]:
                ih = self.ideology_modifiers[ideology]['happiness_boost']
                if ih > 1.0:
                    happiness_modifier *= ih
                else:
                    happiness_modifier += ih
            happiness = int(happiness * happiness_modifier)

            if happiness < 0:
                severity = abs(happiness)
                if random.random() < 0.30 + (severity / 200):
                    flee = max(1, int(population['citizens'] * (0.02 + severity / 1000)))
                    self.update_population(user_id, {"citizens": -flee})
                    self.db.log_event(user_id, "mass_exodus", "Mass Exodus",
                                      f"{flee} citizens fled! (happiness {happiness})")
                military = civ.get('military', {})
                if random.random() < 0.25 + (severity / 250):
                    desertion = max(1, int(military.get('soldiers', 0) * (0.03 + severity / 800)))
                    if desertion > 0:
                        self.update_military(user_id, {"soldiers": -desertion})
                        self.db.log_event(user_id, "desertion", "Military Desertion",
                                          f"{desertion} soldiers abandoned! (happiness {happiness})")
                if random.random() < 0.20 + (severity / 300):
                    resources = civ.get('resources', {})
                    loot = {res: -int(amt * 0.05) for res, amt in resources.items() if amt > 0}
                    if loot:
                        self.update_resources(user_id, loot)
                        self.db.log_event(user_id, "riots", "Riots and Looting",
                                          f"Mobs looted resources! (happiness {happiness})")
            elif happiness < 20:
                if random.random() < 0.1:
                    revolt = int(population['citizens'] * 0.05)
                    self.update_population(user_id, {"citizens": -revolt})
                    self.db.log_event(user_id, "revolt", "Population Revolt",
                                      f"Low happiness caused {revolt} citizens to leave!")
            elif happiness > 80:
                growth_rate = bonuses.get('population_growth', 0) / 100
                if ideology == 'pacifist':
                    growth_rate += self.ideology_modifiers['pacifist']['population_growth'] - 1
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
                    starvation = int(population['citizens'] * 0.02)
                    self.update_population(user_id, {"citizens": -starvation, "happiness": -10})
                    self.db.log_event(user_id, "famine", "Famine Strikes",
                                      f"Severe hunger killed {starvation} citizens!")
            else:
                self.update_resources(user_id, {"food": -food_needed})
                if population['hunger'] > 0:
                    self.update_population(user_id, {"hunger": -5})
        except Exception as e:
            logger.error(f"Error processing hunger for {user_id}: {e}")

    # ---------- MODIFIER HELPERS ----------
    def get_ideology_modifier(self, user_id: str, modifier_type: str) -> float:
        try:
            civ = self.get_civilization(user_id)
            if not civ or not civ.get('ideology'):
                return 1.0
            modifiers = self.ideology_modifiers.get(civ['ideology'], {})
            base_modifier = modifiers.get(modifier_type, 1.0)
            if modifier_type in ['soldier_training_speed', 'combat_strength', 'trade_profit', 'population_growth', 'citizen_productivity']:
                return base_modifier + (civ['bonuses'].get(modifier_type, 0) / 100)
            return base_modifier
        except Exception as e:
            logger.error(f"Error getting ideology modifier for {user_id}: {e}")
            return 1.0

    def get_region_modifier(self, user_id: str, modifier_type: str) -> float:
        try:
            civ = self.get_civilization(user_id)
            if not civ or not civ.get('region'):
                return 1.0
            return self.region_modifiers.get(civ['region'], {}).get(modifier_type, 1.0)
        except Exception as e:
            logger.error(f"Error getting region modifier for {user_id}: {e}")
            return 1.0

    def get_name_bonus(self, user_id: str, bonus_type: str) -> float:
        try:
            civ = self.get_civilization(user_id)
            if not civ:
                return 0.0
            val = civ.get('bonuses', {}).get(f"{bonus_type}_bonus", 0.0)
            if isinstance(val, str):
                val = float(val)
            return val / 100.0
        except Exception as e:
            logger.error(f"Error getting name bonus for {user_id}: {e}")
            return 0.0

    def calculate_total_modifier(self, user_id: str, action_type: str) -> float:
        try:
            base = 1.0
            ideo = self.get_ideology_modifier(user_id, action_type)
            region = self.get_region_modifier(user_id, action_type)
            name_bonus = 0.0
            if action_type == "luck":
                name_bonus = self.get_name_bonus(user_id, "luck")
            elif action_type == "diplomacy":
                name_bonus = self.get_name_bonus(user_id, "diplomacy")
            if isinstance(name_bonus, str):
                name_bonus = float(name_bonus)
            return base * ideo * region + name_bonus
        except Exception as e:
            logger.error(f"Error calculating total modifier for {user_id}: {e}")
            return 1.0

    def can_afford(self, user_id: str, costs: Dict[str, int]) -> bool:
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
        try:
            if not self.can_afford(user_id, costs):
                return False
            return self.update_resources(user_id, {r: -c for r, c in costs.items()})
        except Exception as e:
            logger.error(f"Error spending resources for {user_id}: {e}")
            return False

    def get_civilization_power(self, user_id: str) -> int:
        try:
            civ = self.get_civilization(user_id)
            if not civ:
                return 0
            resources = civ['resources']
            population = civ['population']
            military = civ['military']
            territory = civ['territory']
            bonuses = civ['bonuses']

            resource_power = sum(resources.values()) // 25
            population_power = population['citizens'] * 1
            military_power = military['soldiers'] * 3 + military['spies'] * 5
            tech_power = military['tech_level'] * 50
            territory_power = territory['land_size'] // 500
            happiness_power = max(0, population['happiness'])

            defense_bonus = bonuses.get('defense_strength', 0)
            total = (resource_power + population_power + military_power +
                     tech_power + territory_power + happiness_power)
            return int(total * (1 + defense_bonus / 100))
        except Exception as e:
            logger.error(f"Error calculating civilization power for {user_id}: {e}")
            return 0
