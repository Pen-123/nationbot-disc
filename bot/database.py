"""
database.py — Firebase Realtime Database backend for Nation Bot
================================================================
DROP-IN REPLACEMENT for the old SQLite + Dropbox database.py.

All method signatures are identical.  All return types are identical.
No changes needed in any other bot file.  Just replace this file,
install firebase-admin, set one environment variable, and run.

QUICK SETUP (3 steps):
  1. pip install firebase-admin
  2. Download service account JSON from Firebase Console
  3. Set env var: FIREBASE_SERVICE_ACCOUNT_PATH=/path/to/that/file.json

That's it.  No Dropbox, no .db files, no thread-local connections.

BUGS FIXED:
  • Territory conquest now saves named territories for EVERY player
    (not just the victor's land_size number).
  • Added conquer_territory(), get_player_territories(), get_territory_owner(),
    get_all_territories(), get_territory_history().
  • No more "only one player's country gets saved" after wars.

FIREBASE DATA STRUCTURE:
  /civilizations/{user_id}        — all civ data (native dicts, not JSON strings)
  /cooldowns/{user_id}/{command}  — ISO timestamp string
  /cards/{user_id}/{tech_level}   — card selection data
  /alliances/{push_id}            — alliance data
  /wars/{push_id}                 — war data
  /peace_offers/{push_id}         — peace offer data
  /messages/{push_id}             — message data
  /trade_requests/{push_id}       — trade request data
  /events/{push_id}               — event log
  /alliance_invitations/{push_id} — invite data
  /territories/{name}             — global territory ownership (NEW)
  /territory_history/{user_id}/{push_id} — per-player conquest log (NEW)
  /industrial_revolutions/{user_id} — revolution data
  /global_settings/{key}          — key-value settings
"""

import os
import json
import random
import logging
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple

import firebase_admin
from firebase_admin import credentials, db

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════
# FIREBASE INITIALISATION
# ═══════════════════════════════════════════════════════════════════════════

def _init_firebase(database_url: str) -> bool:
    """
    Initialise Firebase Admin SDK.
    Credential priority:
      1. FIREBASE_SERVICE_ACCOUNT_JSON  (raw JSON string — best for cloud hosting)
      2. FIREBASE_SERVICE_ACCOUNT_PATH  (path to JSON file — best for local/VPS)
      3. GOOGLE_APPLICATION_CREDENTIALS (GCP default)
    """
    if firebase_admin._apps:
        return True  # already initialised

    cred = None
    source = None

    # 1. Raw JSON from env
    raw = os.environ.get("FIREBASE_SERVICE_ACCOUNT_JSON")
    if raw:
        try:
            cred = credentials.Certificate(json.loads(raw))
            source = "FIREBASE_SERVICE_ACCOUNT_JSON"
        except Exception as e:
            logger.error(f"Failed to parse FIREBASE_SERVICE_ACCOUNT_JSON: {e}")

    # 2. Path to JSON file
    if cred is None:
        path = os.environ.get("FIREBASE_SERVICE_ACCOUNT_PATH")
        if path and os.path.isfile(path):
            try:
                cred = credentials.Certificate(path)
                source = f"FIREBASE_SERVICE_ACCOUNT_PATH ({path})"
            except Exception as e:
                logger.error(f"Failed to load {path}: {e}")

    # 3. GCP standard
    if cred is None:
        gcp = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        if gcp and os.path.isfile(gcp):
            try:
                cred = credentials.Certificate(gcp)
                source = f"GOOGLE_APPLICATION_CREDENTIALS ({gcp})"
            except Exception as e:
                logger.error(f"Failed to load {gcp}: {e}")

    if cred is None:
        logger.critical(
            "NO FIREBASE CREDENTIALS FOUND.\n"
            "Set one of: FIREBASE_SERVICE_ACCOUNT_JSON, "
            "FIREBASE_SERVICE_ACCOUNT_PATH, or GOOGLE_APPLICATION_CREDENTIALS"
        )
        return False

    try:
        firebase_admin.initialize_app(cred, {"databaseURL": database_url})
        logger.info(f"Firebase initialised (via {source})")
        return True
    except Exception as e:
        logger.critical(f"Firebase init failed: {e}")
        return False


# ═══════════════════════════════════════════════════════════════════════════
# DATABASE CLASS
# ═══════════════════════════════════════════════════════════════════════════

class Database:
    """Firebase Realtime Database wrapper.  Same API as the old SQLite class."""

    DEFAULT_URL = (
        "https://nation-bot-disc-default-rtdb"
        ".asia-southeast1.firebasedatabase.app"
    )

    def __init__(self, db_path: str = "warbot.db", database_url: str = None):
        """
        db_path:       IGNORED (kept for compatibility — old code passed 'warbot.db')
        database_url:  Override the Firebase URL if needed
        """
        url = (
            database_url
            or os.environ.get("FIREBASE_DATABASE_URL")
            or self.DEFAULT_URL
        )

        if not _init_firebase(url):
            raise RuntimeError("Firebase initialisation failed. Check credentials.")

        self.db_url = url
        self.init_database()
        self.setup_cleanup_scheduler()
        logger.info(f"Database ready: {url}")

    # ────────────────────────────────────────────────────────────────
    # Firebase helpers
    # ────────────────────────────────────────────────────────────────

    @staticmethod
    def _ref(path: str):
        return db.reference(path)

    @staticmethod
    def _get(path: str):
        return db.reference(path).get()

    @staticmethod
    def _set(path: str, data):
        db.reference(path).set(data)

    @staticmethod
    def _update(path: str, data: dict):
        db.reference(path).update(data)

    @staticmethod
    def _push(path: str, data) -> str:
        ref = db.reference(path).push(data)
        return ref.key

    @staticmethod
    def _delete(path: str):
        db.reference(path).delete()

    @staticmethod
    def _now() -> str:
        return datetime.utcnow().isoformat()

    @staticmethod
    def _push_key() -> str:
        return db.reference("/").push().key

    # ────────────────────────────────────────────────────────────────
    # Backward-compatibility stubs
    # ────────────────────────────────────────────────────────────────

    def _ensure_db_writable(self):
        pass  # Firebase handles this

    def get_connection(self):
        pass  # No SQLite connection needed

    def upload_to_dropbox(self) -> bool:
        return True  # Firebase IS the remote store

    def backup_to_dropbox(self) -> bool:
        return True

    def close_connections(self):
        pass

    # ────────────────────────────────────────────────────────────────
    # Database initialisation
    # ────────────────────────────────────────────────────────────────

    def init_database(self):
        """No-op — Firebase creates paths on write.  Kept for compatibility."""
        logger.info("Firebase database initialised.")

    def setup_cleanup_scheduler(self):
        """Run cleanup of expired items every 24 hours."""

        def _task():
            logger.info("Scheduled cleanup running...")
            self.cleanup_expired_requests()
            t = threading.Timer(86400, _task)
            t.daemon = True
            t.start()

        t0 = threading.Timer(60, _task)
        t0.daemon = True
        t0.start()
        logger.info("Cleanup scheduler started.")

    # ────────────────────────────────────────────────────────────────
    # Civilization CRUD
    # ────────────────────────────────────────────────────────────────

    def create_civilization(
        self,
        user_id: str,
        name: str,
        bonus_resources: Dict = None,
        bonuses: Dict = None,
        hyper_item: str = None,
    ) -> bool:
        try:
            if self._get(f"/civilizations/{user_id}") is not None:
                logger.warning(f"User {user_id} already has a civilization")
                return False

            resources = {"gold": 500, "food": 300, "stone": 100, "wood": 100}
            if bonus_resources:
                for k, v in bonus_resources.items():
                    if k in resources:
                        resources[k] += v

            pop_bonus = bonus_resources.get("population", 0) if bonus_resources else 0
            hap_bonus = bonus_resources.get("happiness", 0) if bonus_resources else 0

            population = {
                "citizens": 100 + pop_bonus,
                "happiness": 50 + hap_bonus,
                "hunger": 0,
                "employed": 50,
            }

            military = {"soldiers": 10, "spies": 2, "tech_level": 1}
            territory = {"land_size": 1000}
            now = self._now()

            data = {
                "name": name,
                "ideology": None,
                "resources": resources,
                "population": population,
                "military": military,
                "territory": territory,
                "hyper_items": [hyper_item] if hyper_item else [],
                "bonuses": bonuses or {},
                "selected_cards": [],
                "region": None,
                "black_market_history": {},
                "job": "Unemployed",
                "owned_territories": [],        # ← NEW FIELD
                "created_at": now,
                "last_active": now,
            }

            self._set(f"/civilizations/{user_id}", data)
            self.generate_card_selection(user_id, 1)
            logger.info(f"Created civilization '{name}' for {user_id}")
            return True

        except Exception as e:
            logger.error(f"create_civilization error: {e}")
            return False

    def delete_civilization(self, user_id: str) -> bool:
        try:
            civ = self.get_civilization(user_id)

            # Release owned territories
            if civ and civ.get("owned_territories"):
                for tname in civ["owned_territories"]:
                    self._delete(f"/territories/{tname}")

            # Delete core paths
            for p in [
                f"/civilizations/{user_id}",
                f"/cooldowns/{user_id}",
                f"/cards/{user_id}",
                f"/industrial_revolutions/{user_id}",
                f"/territory_history/{user_id}",
            ]:
                self._delete(p)

            # Clean alliances
            alliances = self._get("/alliances") or {}
            for aid, al in alliances.items():
                members = list(al.get("members", []))
                join_reqs = list(al.get("join_requests", []))
                changed = False
                if user_id in members:
                    members.remove(user_id)
                    changed = True
                if user_id in join_reqs:
                    join_reqs.remove(user_id)
                    changed = True
                if changed:
                    self._update(f"/alliances/{aid}", {
                        "members": members,
                        "join_requests": join_reqs,
                    })

            # Clean wars, peace offers, messages, trades, invites
            for collection, fields in [
                ("wars", ["attacker_id", "defender_id"]),
                ("peace_offers", ["offerer_id", "receiver_id"]),
                ("messages", ["sender_id", "recipient_id"]),
                ("trade_requests", ["sender_id", "recipient_id"]),
                ("alliance_invitations", ["sender_id", "recipient_id"]),
            ]:
                items = self._get(f"/{collection}") or {}
                for item_id, item in items.items():
                    if any(item.get(f) == user_id for f in fields):
                        self._delete(f"/{collection}/{item_id}")

            # Anonymise events
            events = self._get("/events") or {}
            for eid, ev in events.items():
                if ev.get("user_id") == user_id:
                    self._update(f"/events/{eid}", {"user_id": None})

            logger.info(f"Deleted civilization and related data for {user_id}")
            return True
        except Exception as e:
            logger.error(f"delete_civilization error: {e}")
            return False

    def get_civilization(self, user_id: str) -> Optional[Dict[str, Any]]:
        """
        Returns native Python dict (same format as old code's return value
        after json.loads).  No json.loads needed by caller.
        """
        try:
            data = self._get(f"/civilizations/{user_id}")
            if data is None:
                return None
            # Ensure all fields exist with defaults
            data.setdefault("hyper_items", [])
            data.setdefault("bonuses", {})
            data.setdefault("selected_cards", [])
            data.setdefault("black_market_history", {})
            data.setdefault("owned_territories", [])
            data.setdefault("region", None)
            data.setdefault("ideology", None)
            data.setdefault("job", "Unemployed")
            data.setdefault("resources", {})
            data.setdefault("population", {})
            data.setdefault("military", {})
            data.setdefault("territory", {})
            data["user_id"] = user_id
            return data
        except Exception as e:
            logger.error(f"get_civilization error for {user_id}: {e}")
            return None

    def update_civilization(self, user_id: str, updates: Dict[str, Any]) -> bool:
        """
        Merge updates into civilization.  Works with native dicts —
        the old code's json.dumps/json.loads dance is no longer needed.
        """
        try:
            updates["last_active"] = self._now()
            self._update(f"/civilizations/{user_id}", updates)
            return True
        except Exception as e:
            logger.error(f"update_civilization error for {user_id}: {e}")
            return False

    # ────────────────────────────────────────────────────────────────
    # TERRITORY MANAGEMENT  (THE BIG FIX)
    # ────────────────────────────────────────────────────────────────

    def conquer_territory(
        self,
        victor_id: str,
        loser_id: str,
        territory_name: str,
    ) -> bool:
        """
        Transfer a named territory from loser to victor.
        Updates BOTH players and the global territory registry atomically.
        This is the fix for "only one player's country gets saved".
        """
        try:
            now = self._now()
            victor = self.get_civilization(victor_id)
            loser = self.get_civilization(loser_id) if loser_id else None

            if not victor:
                logger.error(f"conquer_territory: victor {victor_id} not found")
                return False

            victor_territories = list(victor.get("owned_territories", []))
            loser_territories = (
                list(loser.get("owned_territories", [])) if loser else []
            )

            # Move territory
            if territory_name in loser_territories:
                loser_territories.remove(territory_name)
            if territory_name not in victor_territories:
                victor_territories.append(territory_name)

            # Current owner for history
            current = self._get(f"/territories/{territory_name}") or {}
            prev_owner = current.get("owner_id")

            # Build atomic update
            multi = {
                f"/territories/{territory_name}": {
                    "owner_id": victor_id,
                    "conquered_at": now,
                    "previous_owner": prev_owner,
                },
                f"/civilizations/{victor_id}/owned_territories": victor_territories,
                f"/civilizations/{victor_id}/last_active": now,
                f"/territory_history/{victor_id}/{self._push_key()}": {
                    "territory_name": territory_name,
                    "claimed_at": now,
                    "action": "conquered",
                },
            }

            if loser and loser_id:
                multi[f"/civilizations/{loser_id}/owned_territories"] = loser_territories
                multi[f"/civilizations/{loser_id}/last_active"] = now
                if territory_name not in loser_territories:
                    multi[f"/territory_history/{loser_id}/{self._push_key()}"] = {
                        "territory_name": territory_name,
                        "claimed_at": now,
                        "action": "lost",
                    }

            db.reference("/").update(multi)
            logger.info(f"Territory '{territory_name}': {loser_id or 'unowned'} → {victor_id}")
            return True

        except Exception as e:
            logger.error(f"conquer_territory error: {e}")
            return False

    def get_player_territories(self, user_id: str) -> List[str]:
        """Return list of territory NAMES owned by the player."""
        civ = self.get_civilization(user_id)
        return civ.get("owned_territories", []) if civ else []

    def get_territory_owner(self, territory_name: str) -> Optional[str]:
        """Get the current owner ID of a territory from the global registry."""
        data = self._get(f"/territories/{territory_name}")
        return data.get("owner_id") if data else None

    def get_all_territories(self) -> Dict[str, Any]:
        """Return the full /territories registry."""
        return self._get("/territories") or {}

    def get_territory_history(self, user_id: str) -> List[Dict]:
        """Return a player's conquest/loss history, newest first."""
        data = self._get(f"/territory_history/{user_id}") or {}
        history = list(data.values())
        history.sort(key=lambda x: x.get("claimed_at", ""), reverse=True)
        return history

    # ────────────────────────────────────────────────────────────────
    # Cooldowns
    # ────────────────────────────────────────────────────────────────

    def get_command_cooldown(
        self, user_id: str, command: str
    ) -> Optional[datetime]:
        try:
            val = self._get(f"/cooldowns/{user_id}/{command}")
            return datetime.fromisoformat(val) if val else None
        except Exception as e:
            logger.error(f"get_command_cooldown error: {e}")
            return None

    def check_cooldown(
        self, user_id: str, command: str
    ) -> Optional[datetime]:
        return self.get_command_cooldown(user_id, command)

    def set_command_cooldown(
        self,
        user_id: str,
        command: str,
        timestamp: datetime = None,
    ) -> bool:
        try:
            ts = (timestamp or datetime.utcnow()).isoformat()
            self._set(f"/cooldowns/{user_id}/{command}", ts)
            return True
        except Exception as e:
            logger.error(f"set_command_cooldown error: {e}")
            return False

    def update_cooldown(
        self,
        user_id: str,
        command: str,
        timestamp: datetime = None,
    ) -> bool:
        return self.set_command_cooldown(user_id, command, timestamp)

    # ────────────────────────────────────────────────────────────────
    # Cards
    # ────────────────────────────────────────────────────────────────

    def generate_card_selection(self, user_id: str, tech_level: int) -> bool:
        try:
            card_pool = [
                {"name": "Resource Boost", "type": "bonus",
                 "effect": {"resource_production": 10},
                 "description": "+10% resource production"},
                {"name": "Military Training", "type": "bonus",
                 "effect": {"soldier_training_speed": 15},
                 "description": "+15% soldier training speed"},
                {"name": "Trade Advantage", "type": "bonus",
                 "effect": {"trade_profit": 10},
                 "description": "+10% trade profit"},
                {"name": "Population Surge", "type": "bonus",
                 "effect": {"population_growth": 10},
                 "description": "+10% population growth"},
                {"name": "Tech Breakthrough", "type": "one_time",
                 "effect": {"tech_level": 1},
                 "description": "+1 tech level (max 10)"},
                {"name": "Gold Cache", "type": "one_time",
                 "effect": {"gold": 500},
                 "description": "Gain 500 gold"},
                {"name": "Food Reserves", "type": "one_time",
                 "effect": {"food": 300},
                 "description": "Gain 300 food"},
                {"name": "Mercenary Band", "type": "one_time",
                 "effect": {"soldiers": 20},
                 "description": "Recruit 20 soldiers"},
                {"name": "Spy Network", "type": "one_time",
                 "effect": {"spies": 5},
                 "description": "Recruit 5 spies"},
                {"name": "Fortification", "type": "bonus",
                 "effect": {"defense_strength": 15},
                 "description": "+15% defense strength"},
                {"name": "Stone Quarry", "type": "one_time",
                 "effect": {"stone": 200},
                 "description": "Gain 200 stone"},
                {"name": "Lumber Mill", "type": "one_time",
                 "effect": {"wood": 200},
                 "description": "Gain 200 wood"},
                {"name": "Intelligence Agency", "type": "bonus",
                 "effect": {"spy_effectiveness": 20},
                 "description": "+20% spy effectiveness"},
                {"name": "Economic Boom", "type": "one_time",
                 "effect": {"gold": 800, "happiness": 10},
                 "description": "Gain 800 gold and +10 happiness"},
                {"name": "Military Academy", "type": "bonus",
                 "effect": {"soldier_training_speed": 25},
                 "description": "+25% soldier training speed"},
            ]
            available = random.sample(card_pool, min(5, len(card_pool)))
            self._set(f"/cards/{user_id}/{tech_level}", {
                "available_cards": available,
                "status": "pending",
                "created_at": self._now(),
            })
            return True
        except Exception as e:
            logger.error(f"generate_card_selection error: {e}")
            return False

    def get_card_selection(
        self, user_id: str, tech_level: int
    ) -> Optional[Dict]:
        try:
            data = self._get(f"/cards/{user_id}/{tech_level}")
            if data and data.get("status") == "pending":
                return data
            return None
        except Exception as e:
            logger.error(f"get_card_selection error: {e}")
            return None

    def select_card(
        self, user_id: str, tech_level: int, card_name: str
    ) -> Optional[Dict]:
        try:
            selection = self.get_card_selection(user_id, tech_level)
            if not selection:
                return None
            chosen = next(
                (c for c in selection["available_cards"]
                 if c["name"].lower() == card_name.lower()),
                None,
            )
            if not chosen:
                return None
            self._update(f"/cards/{user_id}/{tech_level}", {"status": "selected"})
            return chosen
        except Exception as e:
            logger.error(f"select_card error: {e}")
            return None

    # ────────────────────────────────────────────────────────────────
    # Bulk reads
    # ────────────────────────────────────────────────────────────────

    def get_all_civilizations(self) -> List[Dict[str, Any]]:
        try:
            data = self._get("/civilizations") or {}
            civs = []
            for uid, civ in data.items():
                civ["user_id"] = uid
                civ.setdefault("hyper_items", [])
                civ.setdefault("bonuses", {})
                civ.setdefault("selected_cards", [])
                civ.setdefault("black_market_history", {})
                civ.setdefault("owned_territories", [])
                civ.setdefault("resources", {})
                civ.setdefault("population", {})
                civ.setdefault("military", {})
                civ.setdefault("territory", {})
                civs.append(civ)
            civs.sort(key=lambda x: x.get("last_active", ""), reverse=True)
            return civs
        except Exception as e:
            logger.error(f"get_all_civilizations error: {e}")
            return []

    # ────────────────────────────────────────────────────────────────
    # Alliances
    # ────────────────────────────────────────────────────────────────

    def create_alliance(
        self, name: str, leader_id: str, description: str = ""
    ) -> bool:
        try:
            if self.get_alliance_by_name(name):
                logger.warning(f"Alliance '{name}' already exists")
                return False
            self._push("/alliances", {
                "name": name,
                "leader_id": leader_id,
                "description": description,
                "members": [leader_id],
                "join_requests": [],
                "created_at": self._now(),
            })
            logger.info(f"Created alliance '{name}' by {leader_id}")
            return True
        except Exception as e:
            logger.error(f"create_alliance error: {e}")
            return False

    def get_alliance(self, alliance_id: str) -> Optional[Dict]:
        try:
            data = self._get(f"/alliances/{alliance_id}")
            if data:
                data["id"] = alliance_id
                data.setdefault("members", [])
                data.setdefault("join_requests", [])
            return data
        except Exception as e:
            logger.error(f"get_alliance error: {e}")
            return None

    def get_alliance_by_name(self, name: str) -> Optional[Dict]:
        try:
            all_a = self._get("/alliances") or {}
            for aid, al in all_a.items():
                if al.get("name", "").lower() == name.lower():
                    al["id"] = aid
                    al.setdefault("members", [])
                    al.setdefault("join_requests", [])
                    return al
            return None
        except Exception as e:
            logger.error(f"get_alliance_by_name error: {e}")
            return None

    def add_alliance_member(self, alliance_id: str, user_id: str) -> bool:
        try:
            alliance = self.get_alliance(alliance_id)
            if not alliance:
                return False
            if user_id in alliance["members"]:
                return True
            members = alliance["members"] + [user_id]
            join_requests = [
                u for u in alliance.get("join_requests", []) if u != user_id
            ]
            self._update(f"/alliances/{alliance_id}", {
                "members": members,
                "join_requests": join_requests,
            })
            return True
        except Exception as e:
            logger.error(f"add_alliance_member error: {e}")
            return False

    # ────────────────────────────────────────────────────────────────
    # Events
    # ────────────────────────────────────────────────────────────────

    def log_event(
        self,
        user_id: str,
        event_type: str,
        title: str,
        description: str,
        effects: Dict = None,
    ):
        try:
            self._push("/events", {
                "user_id": user_id,
                "event_type": event_type,
                "title": title,
                "description": description,
                "effects": effects or {},
                "timestamp": self._now(),
            })
        except Exception as e:
            logger.error(f"log_event error: {e}")

    def get_recent_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        try:
            all_e = self._get("/events") or {}
            events = []
            for eid, ev in all_e.items():
                ev["id"] = eid
                uid = ev.get("user_id")
                if uid:
                    civ = self._get(f"/civilizations/{uid}")
                    ev["civ_name"] = civ["name"] if civ else "Unknown"
                else:
                    ev["civ_name"] = "System"
                events.append(ev)
            events.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
            return events[:limit]
        except Exception as e:
            logger.error(f"get_recent_events error: {e}")
            return []

    # ────────────────────────────────────────────────────────────────
    # Trade requests
    # ────────────────────────────────────────────────────────────────

    def create_trade_request(
        self,
        sender_id: str,
        recipient_id: str,
        offer: Dict,
        request: Dict,
    ) -> bool:
        try:
            self._push("/trade_requests", {
                "sender_id": sender_id,
                "recipient_id": recipient_id,
                "offer": offer,
                "request": request,
                "created_at": self._now(),
                "expires_at": (datetime.utcnow() + timedelta(days=1)).isoformat(),
            })
            return True
        except Exception as e:
            logger.error(f"create_trade_request error: {e}")
            return False

    def get_trade_requests(self, user_id: str) -> List[Dict]:
        try:
            all_t = self._get("/trade_requests") or {}
            now = datetime.utcnow()
            result = []
            for tid, tr in all_t.items():
                if tr.get("recipient_id") != user_id:
                    continue
                expires = tr.get("expires_at", "")
                if expires:
                    try:
                        if datetime.fromisoformat(expires) <= now:
                            continue
                    except ValueError:
                        pass
                sender_civ = self._get(
                    f"/civilizations/{tr.get('sender_id', '')}"
                )
                tr["sender_name"] = sender_civ["name"] if sender_civ else "Unknown"
                tr["id"] = tid
                result.append(tr)
            return result
        except Exception as e:
            logger.error(f"get_trade_requests error: {e}")
            return []

    def get_trade_request_by_id(self, request_id) -> Optional[Dict]:
        try:
            rid = str(request_id)
            tr = self._get(f"/trade_requests/{rid}")
            if not tr:
                return None
            expires = tr.get("expires_at", "")
            if expires:
                try:
                    if datetime.fromisoformat(expires) <= datetime.utcnow():
                        return None
                except ValueError:
                    pass
            tr["id"] = rid
            return tr
        except Exception as e:
            logger.error(f"get_trade_request_by_id error: {e}")
            return None

    def delete_trade_request(self, request_id) -> bool:
        try:
            rid = str(request_id)
            exists = self._get(f"/trade_requests/{rid}") is not None
            if exists:
                self._delete(f"/trade_requests/{rid}")
            return exists
        except Exception as e:
            logger.error(f"delete_trade_request error: {e}")
            return False

    # ────────────────────────────────────────────────────────────────
    # Alliance invitations
    # ────────────────────────────────────────────────────────────────

    def create_alliance_invite(
        self, alliance_id, sender_id: str, recipient_id: str
    ) -> bool:
        try:
            self._push("/alliance_invitations", {
                "alliance_id": str(alliance_id),
                "sender_id": sender_id,
                "recipient_id": recipient_id,
                "created_at": self._now(),
                "expires_at": (datetime.utcnow() + timedelta(days=1)).isoformat(),
            })
            return True
        except Exception as e:
            logger.error(f"create_alliance_invite error: {e}")
            return False

    def get_alliance_invites(self, user_id: str) -> List[Dict]:
        try:
            all_i = self._get("/alliance_invitations") or {}
            now = datetime.utcnow()
            result = []
            for iid, inv in all_i.items():
                if inv.get("recipient_id") != user_id:
                    continue
                expires = inv.get("expires_at", "")
                if expires:
                    try:
                        if datetime.fromisoformat(expires) <= now:
                            continue
                    except ValueError:
                        pass
                al = self._get(f"/alliances/{inv.get('alliance_id', '')}")
                inv["alliance_name"] = al["name"] if al else "Unknown"
                inv["id"] = iid
                result.append(inv)
            return result
        except Exception as e:
            logger.error(f"get_alliance_invites error: {e}")
            return []

    def get_alliance_invite_by_id(self, invite_id) -> Optional[Dict]:
        try:
            iid = str(invite_id)
            inv = self._get(f"/alliance_invitations/{iid}")
            if not inv:
                return None
            expires = inv.get("expires_at", "")
            if expires:
                try:
                    if datetime.fromisoformat(expires) <= datetime.utcnow():
                        return None
                except ValueError:
                    pass
            al = self._get(f"/alliances/{inv.get('alliance_id', '')}")
            inv["alliance_name"] = al["name"] if al else "Unknown"
            inv["id"] = iid
            return inv
        except Exception as e:
            logger.error(f"get_alliance_invite_by_id error: {e}")
            return None

    def delete_alliance_invite(self, invite_id) -> bool:
        try:
            iid = str(invite_id)
            exists = self._get(f"/alliance_invitations/{iid}") is not None
            if exists:
                self._delete(f"/alliance_invitations/{iid}")
            return exists
        except Exception as e:
            logger.error(f"delete_alliance_invite error: {e}")
            return False

    # ────────────────────────────────────────────────────────────────
    # Messages
    # ────────────────────────────────────────────────────────────────

    def send_message(
        self, sender_id: str, recipient_id: str, message: str
    ) -> bool:
        try:
            self._push("/messages", {
                "sender_id": sender_id,
                "recipient_id": recipient_id,
                "message": message,
                "created_at": self._now(),
                "expires_at": (datetime.utcnow() + timedelta(days=1)).isoformat(),
            })
            return True
        except Exception as e:
            logger.error(f"send_message error: {e}")
            return False

    def get_messages(self, user_id: str) -> List[Dict]:
        try:
            all_m = self._get("/messages") or {}
            now = datetime.utcnow()
            result = []
            for mid, msg in all_m.items():
                if msg.get("recipient_id") != user_id:
                    continue
                expires = msg.get("expires_at", "")
                if expires:
                    try:
                        if datetime.fromisoformat(expires) <= now:
                            continue
                    except ValueError:
                        pass
                sender_civ = self._get(
                    f"/civilizations/{msg.get('sender_id', '')}"
                )
                msg["sender_name"] = sender_civ["name"] if sender_civ else "Unknown"
                msg["id"] = mid
                result.append(msg)
            result.sort(key=lambda x: x.get("created_at", ""), reverse=True)
            return result
        except Exception as e:
            logger.error(f"get_messages error: {e}")
            return []

    def delete_message(self, message_id) -> bool:
        try:
            mid = str(message_id)
            exists = self._get(f"/messages/{mid}") is not None
            if exists:
                self._delete(f"/messages/{mid}")
            return exists
        except Exception as e:
            logger.error(f"delete_message error: {e}")
            return False

    # ────────────────────────────────────────────────────────────────
    # Wars
    # ────────────────────────────────────────────────────────────────

    def declare_war(
        self,
        attacker_id: str,
        defender_id: str,
        war_type: str = "standard",
    ) -> Optional[str]:
        """
        NEW method — explicitly declare a war and get back a war ID.
        If your bot uses a different pattern to insert wars, let me know.
        """
        try:
            war_id = self._push("/wars", {
                "attacker_id": attacker_id,
                "defender_id": defender_id,
                "war_type": war_type,
                "declared_at": self._now(),
                "ended_at": None,
                "result": "ongoing",
            })
            return war_id
        except Exception as e:
            logger.error(f"declare_war error: {e}")
            return None

    def get_wars(
        self, user_id: str = None, status: str = "ongoing"
    ) -> List[Dict]:
        try:
            all_w = self._get("/wars") or {}
            result = []
            for wid, war in all_w.items():
                if war.get("result") != status:
                    continue
                if (
                    user_id
                    and war.get("attacker_id") != user_id
                    and war.get("defender_id") != user_id
                ):
                    continue
                atk = self._get(f"/civilizations/{war.get('attacker_id', '')}")
                dfd = self._get(f"/civilizations/{war.get('defender_id', '')}")
                war["attacker_name"] = atk["name"] if atk else "Unknown"
                war["defender_name"] = dfd["name"] if dfd else "Unknown"
                war["id"] = wid
                result.append(war)
            return result
        except Exception as e:
            logger.error(f"get_wars error: {e}")
            return []

    def end_war(
        self, attacker_id: str, defender_id: str, result: str
    ) -> bool:
        try:
            now = self._now()
            all_w = self._get("/wars") or {}
            updated = False
            for wid, war in all_w.items():
                if war.get("result") != "ongoing":
                    continue
                a = war.get("attacker_id")
                d = war.get("defender_id")
                if (a == attacker_id and d == defender_id) or (
                    a == defender_id and d == attacker_id
                ):
                    self._update(f"/wars/{wid}", {
                        "result": result,
                        "ended_at": now,
                    })
                    updated = True
            return updated
        except Exception as e:
            logger.error(f"end_war error: {e}")
            return False

    # ────────────────────────────────────────────────────────────────
    # Peace offers
    # ────────────────────────────────────────────────────────────────

    def create_peace_offer(
        self, offerer_id: str, receiver_id: str
    ) -> Optional[str]:
        try:
            offer_id = self._push("/peace_offers", {
                "offerer_id": offerer_id,
                "receiver_id": receiver_id,
                "status": "pending",
                "offered_at": self._now(),
                "responded_at": None,
            })
            return offer_id
        except Exception as e:
            logger.error(f"create_peace_offer error: {e}")
            return None

    def get_peace_offers(self, user_id: str = None) -> List[Dict]:
        try:
            all_p = self._get("/peace_offers") or {}
            result = []
            for oid, offer in all_p.items():
                if offer.get("status") != "pending":
                    continue
                if (
                    user_id
                    and offer.get("offerer_id") != user_id
                    and offer.get("receiver_id") != user_id
                ):
                    continue
                ofr = self._get(
                    f"/civilizations/{offer.get('offerer_id', '')}"
                )
                rec = self._get(
                    f"/civilizations/{offer.get('receiver_id', '')}"
                )
                offer["offerer_name"] = ofr["name"] if ofr else "Unknown"
                offer["receiver_name"] = rec["name"] if rec else "Unknown"
                offer["id"] = oid
                result.append(offer)
            return result
        except Exception as e:
            logger.error(f"get_peace_offers error: {e}")
            return []

    def update_peace_offer(self, offer_id, status: str) -> bool:
        try:
            oid = str(offer_id)
            exists = self._get(f"/peace_offers/{oid}") is not None
            if not exists:
                return False
            self._update(f"/peace_offers/{oid}", {
                "status": status,
                "responded_at": self._now(),
            })
            return True
        except Exception as e:
            logger.error(f"update_peace_offer error: {e}")
            return False

    # ────────────────────────────────────────────────────────────────
    # Statistics & Leaderboard
    # ────────────────────────────────────────────────────────────────

    def get_user_statistics(self, user_id: str) -> Dict[str, Any]:
        try:
            civ = self.get_civilization(user_id)
            if not civ:
                return {}

            all_w = self._get("/wars") or {}
            war_stats = {
                "total_wars": 0,
                "victories": 0,
                "defeats": 0,
                "peace_treaties": 0,
            }
            for war in all_w.values():
                if (
                    war.get("attacker_id") != user_id
                    and war.get("defender_id") != user_id
                ):
                    continue
                war_stats["total_wars"] += 1
                r = war.get("result", "")
                if r == "victory":
                    war_stats["victories"] += 1
                elif r == "defeat":
                    war_stats["defeats"] += 1
                elif r == "peace":
                    war_stats["peace_treaties"] += 1

            all_e = self._get("/events") or {}
            total_events = sum(
                1 for e in all_e.values() if e.get("user_id") == user_id
            )

            military = civ.get("military", {})
            resources = civ.get("resources", {})
            territory = civ.get("territory", {})

            military_power = (
                military.get("soldiers", 0) * 10
                + military.get("spies", 0) * 5
                + military.get("tech_level", 0) * 50
            )
            economic_power = sum(resources.values())
            territorial_power = territory.get("land_size", 0)
            total_power = military_power + economic_power + territorial_power

            return {
                "civilization": civ,
                "war_statistics": war_stats,
                "total_events": total_events,
                "power_scores": {
                    "military": military_power,
                    "economic": economic_power,
                    "territorial": territorial_power,
                    "total": total_power,
                },
            }
        except Exception as e:
            logger.error(f"get_user_statistics error: {e}")
            return {}

    def get_leaderboard(
        self, category: str = "power", limit: int = 10
    ) -> List[Dict]:
        try:
            all_c = self._get("/civilizations") or {}
            entries = []

            for uid, civ in all_c.items():
                name = civ.get("name", "Unknown")
                military = civ.get("military", {})
                resources = civ.get("resources", {})
                territory = civ.get("territory", {})

                if category == "power":
                    score = (
                        military.get("soldiers", 0) * 10
                        + military.get("spies", 0) * 5
                        + military.get("tech_level", 0) * 50
                        + sum(resources.values())
                        + territory.get("land_size", 0)
                    )
                elif category == "gold":
                    score = resources.get("gold", 0)
                elif category == "military":
                    score = military.get("soldiers", 0) + military.get("spies", 0)
                elif category == "territory":
                    score = territory.get("land_size", 0)
                else:
                    score = 0

                entries.append({"user_id": uid, "name": name, "score": score})

            entries.sort(key=lambda x: x["score"], reverse=True)
            return entries[:limit]
        except Exception as e:
            logger.error(f"get_leaderboard error: {e}")
            return []

    # ────────────────────────────────────────────────────────────────
    # Region management
    # ────────────────────────────────────────────────────────────────

    def is_region_taken(
        self, region_name: str, exclude_user_id: str = None
    ) -> bool:
        try:
            all_c = self._get("/civilizations") or {}
            for uid, civ in all_c.items():
                if exclude_user_id and uid == exclude_user_id:
                    continue
                if civ.get("region") == region_name:
                    return True
            return False
        except Exception as e:
            logger.error(f"is_region_taken error: {e}")
            return False

    # ────────────────────────────────────────────────────────────────
    # Cleanup
    # ────────────────────────────────────────────────────────────────

    def cleanup_expired_requests(self):
        try:
            now = datetime.utcnow()
            deleted = 0
            for collection in [
                "trade_requests",
                "alliance_invitations",
                "messages",
            ]:
                items = self._get(f"/{collection}") or {}
                for item_id, item in items.items():
                    expires = item.get("expires_at", "")
                    if not expires:
                        continue
                    try:
                        if datetime.fromisoformat(expires) <= now:
                            self._delete(f"/{collection}/{item_id}")
                            deleted += 1
                    except ValueError:
                        pass
            logger.info(f"Cleanup: removed {deleted} expired items")
            return True
        except Exception as e:
            logger.error(f"cleanup_expired_requests error: {e}")
            return False

    # ────────────────────────────────────────────────────────────────
    # Backup / Info
    # ────────────────────────────────────────────────────────────────

    def backup_database(self, backup_path: str = None) -> bool:
        """Export entire Firebase DB as a local JSON file."""
        try:
            path = backup_path or (
                f"firebase_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            )
            data = self._get("/")
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, default=str)
            logger.info(f"Database exported to {path}")
            return True
        except Exception as e:
            logger.error(f"backup_database error: {e}")
            return False

    def get_database_info(self) -> Dict[str, Any]:
        try:
            info = {}
            tables = [
                "civilizations", "wars", "peace_offers", "alliances",
                "events", "trade_requests", "messages", "cards",
                "cooldowns", "alliance_invitations", "territories",
                "territory_history", "industrial_revolutions",
            ]
            for t in tables:
                data = self._get(f"/{t}") or {}
                info[f"{t}_count"] = len(data) if isinstance(data, dict) else 0

            week_ago = datetime.utcnow() - timedelta(days=7)
            all_c = self._get("/civilizations") or {}
            active = 0
            for civ in all_c.values():
                last = civ.get("last_active", "")
                if last:
                    try:
                        if datetime.fromisoformat(last) >= week_ago:
                            active += 1
                    except ValueError:
                        pass
            info["active_users_week"] = active
            info["database_url"] = self.db_url
            return info
        except Exception as e:
            logger.error(f"get_database_info error: {e}")
            return {}


# ═══════════════════════════════════════════════════════════════════════════
# Module-level logging
# ═══════════════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
