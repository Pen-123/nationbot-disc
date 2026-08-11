"""
database.py — Firestore backend for Nation Bot
================================================
DROP-IN REPLACEMENT for the old SQLite + Dropbox database.py.

All method signatures and return types are identical.
No changes needed in any other bot file.
Just replace this file, add firebase-admin to requirements.txt,
set one environment variable, and run.

SETUP:
  1. pip install firebase-admin
  2. Download service account JSON from Firebase Console
  3. On Railway: add variable FIREBASE_SERVICE_ACCOUNT_JSON with the full JSON
     OR locally: set FIREBASE_SERVICE_ACCOUNT_PATH to the file path

FIRESTORE STRUCTURE:
  Collection civilizations      doc: {user_id}
  └── subcollection cooldowns   doc: {command}  → { last_used_at: ISO string }
  └── subcollection cards       doc: {tech_level} → { available_cards, status, created_at }
  Collection alliances          doc: (auto-id)   → { name, leader_id, members[], join_requests[], ... }
  Collection messages           doc: (auto-id)
  Collection trade_requests     doc: (auto-id)
  Collection events             doc: (auto-id)
  Collection alliance_invitations doc: (auto-id)
  Collection territories        doc: {territory_name} → { owner_id, conquered_at, previous_owner }
  Collection territory_history  doc: (auto-id) → { user_id, territory_name, action, claimed_at }
  Collection wars               doc: (auto-id)   (if used, kept for compatibility)
  Collection peace_offers       doc: (auto-id)

BUGS FIXED:
  • Territory conquest now tracks NAMED territories for every player.
  • Added new methods: conquer_territory, get_player_territories,
    get_territory_owner, get_all_territories, get_territory_history.
"""

import os
import json
import random
import logging
import threading
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any, Tuple

import firebase_admin
from firebase_admin import credentials, firestore

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# FIREBASE INITIALISATION (thread‑safe)
# ──────────────────────────────────────────────────────────────────────────────

_init_lock = threading.Lock()

def _init_firebase() -> bool:
    """Initialise Firebase Admin SDK using environment variables (thread‑safe)."""
    with _init_lock:
        try:
            firebase_admin.get_app()
            return True  # Already initialised
        except ValueError:
            pass

        cred = None
        source = None

        # 1. Raw JSON string from environment (best for Railway)
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

        # 3. GCP default
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
            firebase_admin.initialize_app(cred)
            logger.info(f"Firebase initialised (via {source})")
            return True
        except Exception as e:
            logger.critical(f"Firebase init failed: {e}")
            return False


# ──────────────────────────────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────────────────────────────

def _utc_now_iso() -> str:
    """Return an ISO‑formatted naive UTC datetime string."""
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat()

def _parse_iso_to_utc(iso_string: str) -> Optional[datetime]:
    """Parse an ISO string to a naive UTC datetime. Returns None on failure."""
    try:
        dt = datetime.fromisoformat(iso_string)
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt
    except Exception:
        return None

# ──────────────────────────────────────────────────────────────────────────────
# DATABASE CLASS
# ──────────────────────────────────────────────────────────────────────────────

class Database:
    """Firestore database wrapper – same API as the old SQLite Database."""

    def __init__(self, db_path: str = "warbot.db", database_url: str = None):
        """
        db_path:       IGNORED (kept for compatibility)
        database_url:  IGNORED for Firestore (project is determined by credentials)
        """
        self.db_path = db_path  # kept for backward compatibility (e.g. backup_database)

        if not _init_firebase():
            raise RuntimeError("Firebase initialisation failed. Check credentials.")

        self.client = firestore.client()
        self.init_database()
        self.setup_cleanup_scheduler()
        logger.info("Firestore database ready")

    # ────────────────────────────────────────────────────────────────
    # Backward‑compatibility stubs
    # ────────────────────────────────────────────────────────────────

    def _ensure_db_writable(self):
        pass

    def get_connection(self):
        pass

    def upload_to_dropbox(self) -> bool:
        return True  # Firestore is the remote store

    def backup_to_dropbox(self) -> bool:
        return True

    def close_connections(self):
        pass

    # ────────────────────────────────────────────────────────────────
    # Database initialisation (no‑op)
    # ────────────────────────────────────────────────────────────────

    def init_database(self):
        logger.info("Firestore database initialised (schema‑less).")

    def setup_cleanup_scheduler(self):
        """Periodically remove expired documents."""

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
    # Civilisation CRUD
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
            doc_ref = self.client.collection("civilizations").document(user_id)
            if doc_ref.get().exists:
                logger.warning(f"User {user_id} already has a civilization")
                return False

            resources = {"gold": 500, "food": 300, "stone": 100, "wood": 100}
            if bonus_resources:
                for k, v in bonus_resources.items():
                    if k in resources:
                        resources[k] += v

            pop_bonus = bonus_resources.get("population", 0) if bonus_resources else 0
            hap_bonus = bonus_resources.get("happiness", 0) if bonus_resources else 0

            now = _utc_now_iso()

            data = {
                "name": name,
                "ideology": None,
                "resources": resources,
                "population": {
                    "citizens": 100 + pop_bonus,
                    "happiness": 50 + hap_bonus,
                    "hunger": 0,
                    "employed": 50,
                },
                "military": {"soldiers": 10, "spies": 2, "tech_level": 1},
                "territory": {"land_size": 1000},
                "hyper_items": [hyper_item] if hyper_item else [],
                "bonuses": bonuses or {},
                "selected_cards": [],
                "region": None,
                "black_market_history": {},
                "job": "Unemployed",
                "owned_territories": [],          # NEW: named territories
                "created_at": now,
                "last_active": now,
            }

            doc_ref.set(data)
            self.generate_card_selection(user_id, 1)
            logger.info(f"Created civilization '{name}' for {user_id}")
            return True
        except Exception as e:
            logger.error(f"create_civilization error: {e}")
            return False

    def delete_civilization(self, user_id: str) -> bool:
        """Delete a civilization and all related data efficiently."""
        try:
            batch = self.client.batch()
            civ_ref = self.client.collection("civilizations").document(user_id)

            # 1. Release owned territories
            civ_doc = civ_ref.get()
            if civ_doc.exists:
                civ_data = civ_doc.to_dict()
                if civ_data.get("owned_territories"):
                    for tname in civ_data["owned_territories"]:
                        self.client.collection("territories").document(tname).delete()

            # 2. Delete subcollections (cooldowns, cards) manually
            for sub in ["cooldowns", "cards"]:
                for d in civ_ref.collection(sub).stream():
                    batch.delete(d.reference)

            # 3. Remove from alliances using array_contains (much faster)
            alliances = self.client.collection("alliances") \
                            .where("members", "array_contains", user_id) \
                            .stream()
            for al_doc in alliances:
                batch.update(al_doc.reference, {
                    "members": firestore.ArrayRemove([user_id]),
                    "join_requests": firestore.ArrayRemove([user_id])
                })

            # 4. Delete messages, trade_requests, invitations, wars, peace_offers
            #    using indexed queries (sender/recipient) to avoid full scans.
            for col_name, sender_field, recipient_field in [
                ("messages", "sender_id", "recipient_id"),
                ("trade_requests", "sender_id", "recipient_id"),
                ("alliance_invitations", "sender_id", "recipient_id"),
                ("wars", "attacker_id", "defender_id"),
                ("peace_offers", "offerer_id", "receiver_id"),
            ]:
                # Delete documents where user is sender
                for doc in self.client.collection(col_name) \
                                   .where(sender_field, "==", user_id) \
                                   .stream():
                    batch.delete(doc.reference)
                # Delete documents where user is recipient
                for doc in self.client.collection(col_name) \
                                   .where(recipient_field, "==", user_id) \
                                   .stream():
                    batch.delete(doc.reference)

            # 5. Anonymise events (set user_id to None)
            events = self.client.collection("events") \
                         .where("user_id", "==", user_id) \
                         .stream()
            for e in events:
                batch.update(e.reference, {"user_id": None})

            # 6. Delete the civilization document itself
            batch.delete(civ_ref)

            batch.commit()
            logger.info(f"Deleted civilization and all related data for {user_id}")
            return True
        except Exception as e:
            logger.error(f"delete_civilization error: {e}")
            return False

    def get_civilization(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Return full civilisation data with sensible defaults."""
        try:
            doc = self.client.collection("civilizations").document(user_id).get()
            if not doc.exists:
                return None
            data = doc.to_dict()
            # Ensure all expected keys exist
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
        Merge updates into a civilisation document.
        Automatically bumps last_active.
        """
        try:
            updates["last_active"] = _utc_now_iso()
            self.client.collection("civilizations").document(user_id).update(updates)
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
        Updates both players’ owned_territories, global territory registry,
        and history, all within a Firestore batch (atomic).
        """
        try:
            batch = self.client.batch()
            now = _utc_now_iso()

            victor_ref = self.client.collection("civilizations").document(victor_id)
            victor_doc = victor_ref.get()
            if not victor_doc.exists:
                logger.error(f"Victor {victor_id} not found")
                return False
            victor_data = victor_doc.to_dict()
            victor_territories = list(victor_data.get("owned_territories", []))

            loser_territories = []
            if loser_id:
                loser_ref = self.client.collection("civilizations").document(loser_id)
                loser_doc = loser_ref.get()
                if loser_doc.exists:
                    loser_data = loser_doc.to_dict()
                    loser_territories = list(loser_data.get("owned_territories", []))

            # Move territory
            if territory_name in loser_territories:
                loser_territories.remove(territory_name)
            if territory_name not in victor_territories:
                victor_territories.append(territory_name)

            # Global territory registry
            territory_ref = self.client.collection("territories").document(territory_name)
            terr_doc = territory_ref.get()
            prev_owner = terr_doc.to_dict().get("owner_id") if terr_doc.exists else None

            batch.set(territory_ref, {
                "owner_id": victor_id,
                "conquered_at": now,
                "previous_owner": prev_owner,
            })

            # Update victor
            batch.update(victor_ref, {
                "owned_territories": victor_territories,
                "last_active": now,
            })

            # Victim history
            victor_history_ref = self.client.collection("territory_history").document()
            batch.set(victor_history_ref, {
                "user_id": victor_id,
                "territory_name": territory_name,
                "action": "conquered",
                "claimed_at": now,
            })

            # Loser updates
            if loser_id:
                loser_ref = self.client.collection("civilizations").document(loser_id)
                batch.update(loser_ref, {
                    "owned_territories": loser_territories,
                    "last_active": now,
                })
                if territory_name not in loser_territories:
                    loser_history_ref = self.client.collection("territory_history").document()
                    batch.set(loser_history_ref, {
                        "user_id": loser_id,
                        "territory_name": territory_name,
                        "action": "lost",
                        "claimed_at": now,
                    })

            batch.commit()
            logger.info(f"Territory '{territory_name}': {loser_id or 'unowned'} → {victor_id}")
            return True

        except Exception as e:
            logger.error(f"conquer_territory error: {e}")
            return False

    def get_player_territories(self, user_id: str) -> List[str]:
        civ = self.get_civilization(user_id)
        return civ.get("owned_territories", []) if civ else []

    def get_territory_owner(self, territory_name: str) -> Optional[str]:
        doc = self.client.collection("territories").document(territory_name).get()
        if doc.exists:
            return doc.to_dict().get("owner_id")
        return None

    def get_all_territories(self) -> Dict[str, Any]:
        result = {}
        for doc in self.client.collection("territories").stream():
            result[doc.id] = doc.to_dict()
        return result

    def get_territory_history(self, user_id: str) -> List[Dict]:
        history = []
        docs = self.client.collection("territory_history") \
                    .where("user_id", "==", user_id) \
                    .order_by("claimed_at", direction=firestore.Query.DESCENDING) \
                    .stream()
        for d in docs:
            history.append(d.to_dict())
        return history

    # ────────────────────────────────────────────────────────────────
    # Cooldowns (stored as subcollection under the civ doc)
    # ────────────────────────────────────────────────────────────────

    def get_command_cooldown(self, user_id: str, command: str) -> Optional[datetime]:
        try:
            doc = self.client.collection("civilizations") \
                       .document(user_id) \
                       .collection("cooldowns") \
                       .document(command) \
                       .get()
            if doc.exists:
                ts = doc.to_dict().get("last_used_at")
                if ts:
                    return _parse_iso_to_utc(ts)
            return None
        except Exception as e:
            logger.error(f"get_command_cooldown error: {e}")
            return None

    def check_cooldown(self, user_id: str, command: str) -> Optional[datetime]:
        return self.get_command_cooldown(user_id, command)

    def set_command_cooldown(self, user_id: str, command: str, timestamp: datetime = None) -> bool:
        try:
            ts = (timestamp or datetime.now(timezone.utc)).replace(tzinfo=None).isoformat()
            self.client.collection("civilizations") \
                .document(user_id) \
                .collection("cooldowns") \
                .document(command) \
                .set({"last_used_at": ts})
            return True
        except Exception as e:
            logger.error(f"set_command_cooldown error: {e}")
            return False

    def update_cooldown(self, user_id: str, command: str, timestamp: datetime = None) -> bool:
        return self.set_command_cooldown(user_id, command, timestamp)

    # ────────────────────────────────────────────────────────────────
    # Cards (subcollection under the civ doc)
    # ────────────────────────────────────────────────────────────────

    def generate_card_selection(self, user_id: str, tech_level: int) -> bool:
        try:
            card_pool = [
                {"name": "Resource Boost", "type": "bonus", "effect": {"resource_production": 10}, "description": "+10% resource production"},
                {"name": "Military Training", "type": "bonus", "effect": {"soldier_training_speed": 15}, "description": "+15% soldier training speed"},
                {"name": "Trade Advantage", "type": "bonus", "effect": {"trade_profit": 10}, "description": "+10% trade profit"},
                {"name": "Population Surge", "type": "bonus", "effect": {"population_growth": 10}, "description": "+10% population growth"},
                {"name": "Tech Breakthrough", "type": "one_time", "effect": {"tech_level": 1}, "description": "+1 tech level (max 10)"},
                {"name": "Gold Cache", "type": "one_time", "effect": {"gold": 500}, "description": "Gain 500 gold"},
                {"name": "Food Reserves", "type": "one_time", "effect": {"food": 300}, "description": "Gain 300 food"},
                {"name": "Mercenary Band", "type": "one_time", "effect": {"soldiers": 20}, "description": "Recruit 20 soldiers"},
                {"name": "Spy Network", "type": "one_time", "effect": {"spies": 5}, "description": "Recruit 5 spies"},
                {"name": "Fortification", "type": "bonus", "effect": {"defense_strength": 15}, "description": "+15% defense strength"},
                {"name": "Stone Quarry", "type": "one_time", "effect": {"stone": 200}, "description": "Gain 200 stone"},
                {"name": "Lumber Mill", "type": "one_time", "effect": {"wood": 200}, "description": "Gain 200 wood"},
                {"name": "Intelligence Agency", "type": "bonus", "effect": {"spy_effectiveness": 20}, "description": "+20% spy effectiveness"},
                {"name": "Economic Boom", "type": "one_time", "effect": {"gold": 800, "happiness": 10}, "description": "Gain 800 gold and +10 happiness"},
                {"name": "Military Academy", "type": "bonus", "effect": {"soldier_training_speed": 25}, "description": "+25% soldier training speed"},
            ]
            available = random.sample(card_pool, min(5, len(card_pool)))
            self.client.collection("civilizations") \
                .document(user_id) \
                .collection("cards") \
                .document(str(tech_level)) \
                .set({
                    "available_cards": available,
                    "status": "pending",
                    "created_at": _utc_now_iso(),
                })
            return True
        except Exception as e:
            logger.error(f"generate_card_selection error: {e}")
            return False

    def get_card_selection(self, user_id: str, tech_level: int) -> Optional[Dict]:
        try:
            doc = self.client.collection("civilizations") \
                       .document(user_id) \
                       .collection("cards") \
                       .document(str(tech_level)) \
                       .get()
            if doc.exists:
                data = doc.to_dict()
                if data.get("status") == "pending":
                    return data
            return None
        except Exception as e:
            logger.error(f"get_card_selection error: {e}")
            return None

    def select_card(self, user_id: str, tech_level: int, card_name: str) -> Optional[Dict]:
        try:
            selection = self.get_card_selection(user_id, tech_level)
            if not selection:
                return None
            chosen = next((c for c in selection["available_cards"] if c["name"].lower() == card_name.lower()), None)
            if not chosen:
                return None
            self.client.collection("civilizations") \
                .document(user_id) \
                .collection("cards") \
                .document(str(tech_level)) \
                .update({"status": "selected"})
            return chosen
        except Exception as e:
            logger.error(f"select_card error: {e}")
            return None

    # ────────────────────────────────────────────────────────────────
    # Bulk reads
    # ────────────────────────────────────────────────────────────────

    def get_all_civilizations(self) -> List[Dict[str, Any]]:
        try:
            civs = []
            for doc in self.client.collection("civilizations").stream():
                data = doc.to_dict()
                data["user_id"] = doc.id
                data.setdefault("hyper_items", [])
                data.setdefault("bonuses", {})
                data.setdefault("selected_cards", [])
                data.setdefault("black_market_history", {})
                data.setdefault("owned_territories", [])
                data.setdefault("resources", {})
                data.setdefault("population", {})
                data.setdefault("military", {})
                data.setdefault("territory", {})
                civs.append(data)
            civs.sort(key=lambda x: x.get("last_active", ""), reverse=True)
            return civs
        except Exception as e:
            logger.error(f"get_all_civilizations error: {e}")
            return []

    # ────────────────────────────────────────────────────────────────
    # Alliances
    # ────────────────────────────────────────────────────────────────

    def create_alliance(self, name: str, leader_id: str, description: str = "") -> bool:
        try:
            # Check duplicate name
            existing = self.get_alliance_by_name(name)
            if existing:
                logger.warning(f"Alliance '{name}' already exists")
                return False
            self.client.collection("alliances").add({
                "name": name,
                "leader_id": leader_id,
                "description": description,
                "members": [leader_id],
                "join_requests": [],
                "created_at": _utc_now_iso(),
            })
            logger.info(f"Created alliance '{name}' by {leader_id}")
            return True
        except Exception as e:
            logger.error(f"create_alliance error: {e}")
            return False

    def get_alliance(self, alliance_id: str) -> Optional[Dict]:
        try:
            doc = self.client.collection("alliances").document(alliance_id).get()
            if doc.exists:
                data = doc.to_dict()
                data["id"] = doc.id
                data.setdefault("members", [])
                data.setdefault("join_requests", [])
                return data
            return None
        except Exception as e:
            logger.error(f"get_alliance error: {e}")
            return None

    def get_alliance_by_name(self, name: str) -> Optional[Dict]:
        try:
            docs = self.client.collection("alliances") \
                       .where("name", "==", name).limit(1).stream()
            for doc in docs:
                data = doc.to_dict()
                data["id"] = doc.id
                data.setdefault("members", [])
                data.setdefault("join_requests", [])
                return data
            return None
        except Exception as e:
            logger.error(f"get_alliance_by_name error: {e}")
            return None

    def add_alliance_member(self, alliance_id: str, user_id: str) -> bool:
        """Atomically add a user to the alliance and remove any join request."""
        try:
            self.client.collection("alliances").document(alliance_id).update({
                "members": firestore.ArrayUnion([user_id]),
                "join_requests": firestore.ArrayRemove([user_id])
            })
            return True
        except Exception as e:
            logger.error(f"add_alliance_member error: {e}")
            return False

    # ────────────────────────────────────────────────────────────────
    # Events
    # ────────────────────────────────────────────────────────────────

    def log_event(self, user_id: str, event_type: str, title: str, description: str, effects: Dict = None):
        try:
            self.client.collection("events").add({
                "user_id": user_id,
                "event_type": event_type,
                "title": title,
                "description": description,
                "effects": effects or {},
                "timestamp": _utc_now_iso(),
            })
        except Exception as e:
            logger.error(f"log_event error: {e}")

    def get_recent_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Return recent events with civilisation names preloaded."""
        try:
            docs = self.client.collection("events") \
                       .order_by("timestamp", direction=firestore.Query.DESCENDING) \
                       .limit(limit) \
                       .stream()
            raw_events = []
            user_ids = set()
            for doc in docs:
                data = doc.to_dict()
                data["id"] = doc.id
                uid = data.get("user_id")
                if uid:
                    user_ids.add(uid)
                raw_events.append(data)

            # Preload civilisation names (1 read per distinct user)
            civ_names = {}
            for uid in user_ids:
                civ = self.get_civilization(uid)
                civ_names[uid] = civ["name"] if civ else "Unknown"

            events = []
            for data in raw_events:
                uid = data.get("user_id")
                data["civ_name"] = civ_names.get(uid, "System") if uid else "System"
                events.append(data)
            return events
        except Exception as e:
            logger.error(f"get_recent_events error: {e}")
            return []

    # ────────────────────────────────────────────────────────────────
    # Trade requests
    # ────────────────────────────────────────────────────────────────

    def create_trade_request(self, sender_id: str, recipient_id: str, offer: Dict, request: Dict) -> bool:
        try:
            self.client.collection("trade_requests").add({
                "sender_id": sender_id,
                "recipient_id": recipient_id,
                "offer": offer,
                "request": request,
                "created_at": _utc_now_iso(),
                "expires_at": (datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(days=1)).isoformat(),
            })
            return True
        except Exception as e:
            logger.error(f"create_trade_request error: {e}")
            return False

    def get_trade_requests(self, user_id: str) -> List[Dict]:
        try:
            now = datetime.now(timezone.utc).replace(tzinfo=None)
            docs = self.client.collection("trade_requests") \
                       .where("recipient_id", "==", user_id) \
                       .stream()
            result = []
            for doc in docs:
                data = doc.to_dict()
                expires = data.get("expires_at", "")
                if expires:
                    exp_dt = _parse_iso_to_utc(expires)
                    if exp_dt and exp_dt <= now:
                        continue
                sender_civ = self.get_civilization(data.get("sender_id", ""))
                data["sender_name"] = sender_civ["name"] if sender_civ else "Unknown"
                data["id"] = doc.id
                result.append(data)
            return result
        except Exception as e:
            logger.error(f"get_trade_requests error: {e}")
            return []

    def get_trade_request_by_id(self, request_id) -> Optional[Dict]:
        try:
            rid = str(request_id)
            doc = self.client.collection("trade_requests").document(rid).get()
            if not doc.exists:
                return None
            data = doc.to_dict()
            expires = data.get("expires_at", "")
            if expires:
                exp_dt = _parse_iso_to_utc(expires)
                if exp_dt and exp_dt <= datetime.now(timezone.utc).replace(tzinfo=None):
                    return None
            data["id"] = rid
            return data
        except Exception as e:
            logger.error(f"get_trade_request_by_id error: {e}")
            return None

    def delete_trade_request(self, request_id) -> bool:
        try:
            rid = str(request_id)
            doc_ref = self.client.collection("trade_requests").document(rid)
            if doc_ref.get().exists:
                doc_ref.delete()
                return True
            return False
        except Exception as e:
            logger.error(f"delete_trade_request error: {e}")
            return False

    # ────────────────────────────────────────────────────────────────
    # Alliance invitations
    # ────────────────────────────────────────────────────────────────

    def create_alliance_invite(self, alliance_id, sender_id: str, recipient_id: str) -> bool:
        try:
            self.client.collection("alliance_invitations").add({
                "alliance_id": str(alliance_id),
                "sender_id": sender_id,
                "recipient_id": recipient_id,
                "created_at": _utc_now_iso(),
                "expires_at": (datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(days=1)).isoformat(),
            })
            return True
        except Exception as e:
            logger.error(f"create_alliance_invite error: {e}")
            return False

    def get_alliance_invites(self, user_id: str) -> List[Dict]:
        try:
            now = datetime.now(timezone.utc).replace(tzinfo=None)
            docs = self.client.collection("alliance_invitations") \
                       .where("recipient_id", "==", user_id) \
                       .stream()
            result = []
            for doc in docs:
                data = doc.to_dict()
                expires = data.get("expires_at", "")
                if expires:
                    exp_dt = _parse_iso_to_utc(expires)
                    if exp_dt and exp_dt <= now:
                        continue
                al = self.get_alliance(data.get("alliance_id", ""))
                data["alliance_name"] = al["name"] if al else "Unknown"
                data["id"] = doc.id
                result.append(data)
            return result
        except Exception as e:
            logger.error(f"get_alliance_invites error: {e}")
            return []

    def get_alliance_invite_by_id(self, invite_id) -> Optional[Dict]:
        try:
            iid = str(invite_id)
            doc = self.client.collection("alliance_invitations").document(iid).get()
            if not doc.exists:
                return None
            data = doc.to_dict()
            expires = data.get("expires_at", "")
            if expires:
                exp_dt = _parse_iso_to_utc(expires)
                if exp_dt and exp_dt <= datetime.now(timezone.utc).replace(tzinfo=None):
                    return None
            al = self.get_alliance(data.get("alliance_id", ""))
            data["alliance_name"] = al["name"] if al else "Unknown"
            data["id"] = iid
            return data
        except Exception as e:
            logger.error(f"get_alliance_invite_by_id error: {e}")
            return None

    def delete_alliance_invite(self, invite_id) -> bool:
        try:
            iid = str(invite_id)
            doc_ref = self.client.collection("alliance_invitations").document(iid)
            if doc_ref.get().exists:
                doc_ref.delete()
                return True
            return False
        except Exception as e:
            logger.error(f"delete_alliance_invite error: {e}")
            return False

    # ────────────────────────────────────────────────────────────────
    # Messages
    # ────────────────────────────────────────────────────────────────

    def send_message(self, sender_id: str, recipient_id: str, message: str) -> bool:
        try:
            self.client.collection("messages").add({
                "sender_id": sender_id,
                "recipient_id": recipient_id,
                "message": message,
                "created_at": _utc_now_iso(),
                "expires_at": (datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(days=1)).isoformat(),
            })
            return True
        except Exception as e:
            logger.error(f"send_message error: {e}")
            return False

    def get_messages(self, user_id: str) -> List[Dict]:
        try:
            now = datetime.now(timezone.utc).replace(tzinfo=None)
            docs = self.client.collection("messages") \
                       .where("recipient_id", "==", user_id) \
                       .order_by("created_at", direction=firestore.Query.DESCENDING) \
                       .stream()
            result = []
            for doc in docs:
                data = doc.to_dict()
                expires = data.get("expires_at", "")
                if expires:
                    exp_dt = _parse_iso_to_utc(expires)
                    if exp_dt and exp_dt <= now:
                        continue
                sender_civ = self.get_civilization(data.get("sender_id", ""))
                data["sender_name"] = sender_civ["name"] if sender_civ else "Unknown"
                data["id"] = doc.id
                result.append(data)
            return result
        except Exception as e:
            logger.error(f"get_messages error: {e}")
            return []

    def delete_message(self, message_id) -> bool:
        try:
            mid = str(message_id)
            doc_ref = self.client.collection("messages").document(mid)
            if doc_ref.get().exists:
                doc_ref.delete()
                return True
            return False
        except Exception as e:
            logger.error(f"delete_message error: {e}")
            return False

    # ────────────────────────────────────────────────────────────────
    # Wars
    # ────────────────────────────────────────────────────────────────

    def declare_war(self, attacker_id: str, defender_id: str, war_type: str = "standard") -> Optional[str]:
        """Optional helper – not present in old code, but needed if bot declares wars directly."""
        try:
            doc_ref = self.client.collection("wars").document()
            doc_ref.set({
                "attacker_id": attacker_id,
                "defender_id": defender_id,
                "war_type": war_type,
                "declared_at": _utc_now_iso(),
                "ended_at": None,
                "result": "ongoing",
            })
            return doc_ref.id
        except Exception as e:
            logger.error(f"declare_war error: {e}")
            return None

    def get_wars(self, user_id: str = None, status: str = "ongoing") -> List[Dict]:
        try:
            docs = self.client.collection("wars") \
                       .where("result", "==", status) \
                       .stream()
            result = []
            for doc in docs:
                data = doc.to_dict()
                if user_id and data.get("attacker_id") != user_id and data.get("defender_id") != user_id:
                    continue
                atk = self.get_civilization(data.get("attacker_id", ""))
                dfd = self.get_civilization(data.get("defender_id", ""))
                data["attacker_name"] = atk["name"] if atk else "Unknown"
                data["defender_name"] = dfd["name"] if dfd else "Unknown"
                data["id"] = doc.id
                result.append(data)
            return result
        except Exception as e:
            logger.error(f"get_wars error: {e}")
            return []

    def end_war(self, attacker_id: str, defender_id: str, result: str) -> bool:
        try:
            now = _utc_now_iso()
            # Find the ongoing war between these two
            docs = self.client.collection("wars") \
                       .where("result", "==", "ongoing") \
                       .stream()
            updated = False
            for doc in docs:
                data = doc.to_dict()
                a = data.get("attacker_id")
                d = data.get("defender_id")
                if (a == attacker_id and d == defender_id) or (a == defender_id and d == attacker_id):
                    doc.reference.update({"result": result, "ended_at": now})
                    updated = True
            return updated
        except Exception as e:
            logger.error(f"end_war error: {e}")
            return False

    # ────────────────────────────────────────────────────────────────
    # Peace offers
    # ────────────────────────────────────────────────────────────────

    def create_peace_offer(self, offerer_id: str, receiver_id: str) -> Optional[str]:
        try:
            doc_ref = self.client.collection("peace_offers").document()
            doc_ref.set({
                "offerer_id": offerer_id,
                "receiver_id": receiver_id,
                "status": "pending",
                "offered_at": _utc_now_iso(),
                "responded_at": None,
            })
            return doc_ref.id
        except Exception as e:
            logger.error(f"create_peace_offer error: {e}")
            return None

    def get_peace_offers(self, user_id: str = None) -> List[Dict]:
        try:
            docs = self.client.collection("peace_offers") \
                       .where("status", "==", "pending") \
                       .stream()
            result = []
            for doc in docs:
                data = doc.to_dict()
                if user_id and data.get("offerer_id") != user_id and data.get("receiver_id") != user_id:
                    continue
                ofr = self.get_civilization(data.get("offerer_id", ""))
                rec = self.get_civilization(data.get("receiver_id", ""))
                data["offerer_name"] = ofr["name"] if ofr else "Unknown"
                data["receiver_name"] = rec["name"] if rec else "Unknown"
                data["id"] = doc.id
                result.append(data)
            return result
        except Exception as e:
            logger.error(f"get_peace_offers error: {e}")
            return []

    def update_peace_offer(self, offer_id, status: str) -> bool:
        try:
            oid = str(offer_id)
            doc_ref = self.client.collection("peace_offers").document(oid)
            if doc_ref.get().exists:
                doc_ref.update({
                    "status": status,
                    "responded_at": _utc_now_iso(),
                })
                return True
            return False
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

            # Count wars using indexed queries (two separate queries)
            war_stats = {"total_wars": 0, "victories": 0, "defeats": 0, "peace_treaties": 0}
            # Attacker side
            for w in self.client.collection("wars") \
                         .where("attacker_id", "==", user_id) \
                         .stream():
                data = w.to_dict()
                war_stats["total_wars"] += 1
                r = data.get("result", "")
                if r == "victory":
                    war_stats["victories"] += 1
                elif r == "defeat":
                    war_stats["defeats"] += 1
                elif r == "peace":
                    war_stats["peace_treaties"] += 1
            # Defender side
            for w in self.client.collection("wars") \
                         .where("defender_id", "==", user_id) \
                         .stream():
                data = w.to_dict()
                war_stats["total_wars"] += 1
                r = data.get("result", "")
                if r == "victory":
                    war_stats["victories"] += 1
                elif r == "defeat":
                    war_stats["defeats"] += 1
                elif r == "peace":
                    war_stats["peace_treaties"] += 1

            # Count events
            events = self.client.collection("events") \
                         .where("user_id", "==", user_id) \
                         .stream()
            total_events = sum(1 for _ in events)

            military = civ.get("military", {})
            resources = civ.get("resources", {})
            territory = civ.get("territory", {})

            military_power = (military.get("soldiers", 0) * 10 +
                              military.get("spies", 0) * 5 +
                              military.get("tech_level", 0) * 50)
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

    def get_leaderboard(self, category: str = "power", limit: int = 10) -> List[Dict]:
        try:
            civs = self.get_all_civilizations()
            entries = []
            for civ in civs:
                military = civ.get("military", {})
                resources = civ.get("resources", {})
                territory = civ.get("territory", {})

                if category == "power":
                    score = (military.get("soldiers", 0) * 10 +
                             military.get("spies", 0) * 5 +
                             military.get("tech_level", 0) * 50 +
                             sum(resources.values()) +
                             territory.get("land_size", 0))
                elif category == "gold":
                    score = resources.get("gold", 0)
                elif category == "military":
                    score = military.get("soldiers", 0) + military.get("spies", 0)
                elif category == "territory":
                    score = territory.get("land_size", 0)
                else:
                    score = 0
                entries.append({
                    "user_id": civ["user_id"],
                    "name": civ["name"],
                    "score": score,
                })
            entries.sort(key=lambda x: x["score"], reverse=True)
            return entries[:limit]
        except Exception as e:
            logger.error(f"get_leaderboard error: {e}")
            return []

    # ────────────────────────────────────────────────────────────────
    # Region management
    # ────────────────────────────────────────────────────────────────

    def is_region_taken(self, region_name: str, exclude_user_id: str = None) -> bool:
        try:
            docs = self.client.collection("civilizations") \
                       .where("region", "==", region_name) \
                       .stream()
            for doc in docs:
                if exclude_user_id and doc.id == exclude_user_id:
                    continue
                return True
            return False
        except Exception as e:
            logger.error(f"is_region_taken error: {e}")
            return False

    # ────────────────────────────────────────────────────────────────
    # Cleanup (efficient, index‑based)
    # ────────────────────────────────────────────────────────────────

    def cleanup_expired_requests(self):
        """Delete all expired documents using range queries (requires single‑field indexes)."""
        try:
            now_iso = _utc_now_iso()
            deleted = 0
            for collection_name in ["messages", "trade_requests", "alliance_invitations"]:
                docs = self.client.collection(collection_name) \
                            .where("expires_at", "<=", now_iso) \
                            .stream()
                batch = self.client.batch()
                count = 0
                for doc in docs:
                    batch.delete(doc.reference)
                    count += 1
                    if count % 500 == 0:   # Firestore batch limit
                        batch.commit()
                        batch = self.client.batch()
                if count % 500 != 0:
                    batch.commit()
                deleted += count
            logger.info(f"Cleanup: removed {deleted} expired items")
            return True
        except Exception as e:
            logger.error(f"cleanup_expired_requests error: {e}")
            return False

    # ────────────────────────────────────────────────────────────────
    # Backup / Info (now includes subcollections)
    # ────────────────────────────────────────────────────────────────

    def backup_database(self, backup_path: str = None) -> bool:
        """Export all Firestore data (including subcollections) as a local JSON file."""
        try:
            path = backup_path or f"firestore_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            all_data = {}

            # Top‑level collections
            top_collections = [
                "alliances", "messages", "trade_requests",
                "events", "alliance_invitations", "territories",
                "territory_history", "wars", "peace_offers",
            ]
            for col_name in top_collections:
                col_data = {}
                for doc in self.client.collection(col_name).stream():
                    doc_data = doc.to_dict()
                    doc_data["_id"] = doc.id
                    col_data[doc.id] = doc_data
                all_data[col_name] = col_data

            # Civilizations + subcollections
            civs_data = {}
            civs_sub = {}
            for doc in self.client.collection("civilizations").stream():
                user_id = doc.id
                civ_data = doc.to_dict()
                civ_data["_id"] = user_id
                civs_data[user_id] = civ_data

                # Subcollections: cooldowns, cards
                sub_data = {}
                for sub_name in ["cooldowns", "cards"]:
                    sub_col = {}
                    for sub_doc in doc.reference.collection(sub_name).stream():
                        sub_doc_data = sub_doc.to_dict()
                        sub_doc_data["_id"] = sub_doc.id
                        sub_col[sub_doc.id] = sub_doc_data
                    sub_data[sub_name] = sub_col
                civs_sub[user_id] = sub_data

            all_data["civilizations"] = civs_data
            all_data["civilizations_subcollections"] = civs_sub

            with open(path, "w", encoding="utf-8") as f:
                json.dump(all_data, f, indent=2, default=str)
            logger.info(f"Database exported to {path}")
            return True
        except Exception as e:
            logger.error(f"backup_database error: {e}")
            return False

    def get_database_info(self) -> Dict[str, Any]:
        try:
            info = {}
            collections = [
                "civilizations", "wars", "peace_offers", "alliances",
                "events", "trade_requests", "messages",
                "alliance_invitations", "territories",
                "territory_history",
            ]
            for col_name in collections:
                docs = self.client.collection(col_name).stream()
                info[f"{col_name}_count"] = sum(1 for _ in docs)

            # Active users in last 7 days
            week_ago = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=7)
            active = 0
            civs = self.client.collection("civilizations").stream()
            for doc in civs:
                last = doc.to_dict().get("last_active", "")
                if last:
                    dt = _parse_iso_to_utc(last)
                    if dt and dt >= week_ago:
                        active += 1
            info["active_users_week"] = active
            info["database_type"] = "Firestore"
            return info
        except Exception as e:
            logger.error(f"get_database_info error: {e}")
            return {}
