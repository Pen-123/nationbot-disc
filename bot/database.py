import sqlite3
import random
import json
import threading
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
import time
import dropbox
from dropbox.exceptions import ApiError, AuthError
import os
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

class Database:
    def __init__(self, db_path: str = 'nationbot.db', dropbox_refresh_token: str = None,
                 dropbox_app_key: str = None, dropbox_app_secret: str = None):
        self.db_path = db_path
        self.local = threading.local()
        self.dropbox_refresh_token = dropbox_refresh_token or os.getenv('DROPBOX_REFRESH_TOKEN')
        self.dropbox_app_key = dropbox_app_key or os.getenv('DROPBOX_APP_KEY')
        self.dropbox_app_secret = dropbox_app_secret or os.getenv('DROPBOX_APP_SECRET')
        self.dropbox_client = None
        self._last_upload = 0
        if self.dropbox_refresh_token and self.dropbox_app_key and self.dropbox_app_secret:
            self.init_dropbox()
        # Only download if local file missing
        if not os.path.exists(self.db_path):
            self.download_database()
        self.init_database()
        self.setup_cleanup_scheduler()

    def init_dropbox(self):
        try:
            dbx = dropbox.Dropbox(
                oauth2_refresh_token=self.dropbox_refresh_token,
                app_key=self.dropbox_app_key,
                app_secret=self.dropbox_app_secret
            )
            dbx.check_user()
            self.dropbox_client = dbx
            logger.info("Dropbox client initialized")
        except AuthError as e:
            logger.error(f"Dropbox auth error: {e}")
            self.dropbox_client = None
        except Exception as e:
            logger.error(f"Error initializing Dropbox: {e}")
            self.dropbox_client = None

    def download_database(self):
        if not self.dropbox_client:
            return
        try:
            dropbox_path = f"/{os.path.basename(self.db_path)}"
            self.dropbox_client.files_download_to_file(self.db_path, dropbox_path)
            logger.info(f"Downloaded database from Dropbox: {dropbox_path}")
        except ApiError as e:
            if e.error.is_path() and e.error.get_path().is_not_found():
                logger.info("No database found in Dropbox, starting fresh")
            else:
                logger.error(f"Error downloading database: {e}")
        except Exception as e:
            logger.error(f"Unexpected error downloading database: {e}")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def upload_database(self):
        if not self.dropbox_client:
            return
        # Throttle to once per 30 seconds
        if time.time() - self._last_upload < 30:
            return
        try:
            cursor = self.get_connection().cursor()
            cursor.execute("PRAGMA integrity_check")
            if cursor.fetchone()[0] != "ok":
                logger.error("Database corrupted, skipping upload")
                return
            dropbox_path = f"/{os.path.basename(self.db_path)}"
            with open(self.db_path, 'rb') as f:
                self.dropbox_client.files_upload(
                    f.read(),
                    dropbox_path,
                    mode=dropbox.files.WriteMode('overwrite')
                )
            self._last_upload = time.time()
            logger.info(f"Uploaded database to Dropbox: {dropbox_path}")
        except Exception as e:
            logger.error(f"Error uploading database to Dropbox: {e}")
            raise

    def get_connection(self):
        if not hasattr(self.local, 'connection'):
            self.local.connection = sqlite3.connect(self.db_path, check_same_thread=False)
            self.local.connection.row_factory = sqlite3.Row
        return self.local.connection

    def setup_cleanup_scheduler(self):
        def cleanup_task():
            logger.info("Running scheduled cleanup...")
            self.cleanup_expired_requests()
            timer = threading.Timer(86400, cleanup_task)
            timer.daemon = True
            timer.start()
        initial_timer = threading.Timer(60, cleanup_task)
        initial_timer.daemon = True
        initial_timer.start()
        logger.info("Scheduled cleanup task initialized")

    def _migrate_civilizations_table(self):
        """Add missing columns to civilizations table if they don't exist."""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(civilizations)")
        columns = [col[1] for col in cursor.fetchall()]
        # Define expected columns with their types and defaults
        expected = {
            'selected_cards': ("TEXT NOT NULL DEFAULT '[]'", "'[]'"),
            'region': ("TEXT", None),
            'black_market_history': ("TEXT NOT NULL DEFAULT '{}'", "'{}'")
        }
        for col, (col_def, default_val) in expected.items():
            if col not in columns:
                try:
                    cursor.execute(f"ALTER TABLE civilizations ADD COLUMN {col} {col_def}")
                    if default_val:
                        cursor.execute(f"UPDATE civilizations SET {col} = {default_val} WHERE {col} IS NULL")
                    conn.commit()
                    logger.info(f"Added missing column '{col}' to civilizations table")
                except Exception as e:
                    logger.error(f"Failed to add column '{col}': {e}")

    def init_database(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS civilizations (
                user_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                ideology TEXT,
                resources TEXT NOT NULL,
                population TEXT NOT NULL,
                military TEXT NOT NULL,
                territory TEXT NOT NULL,
                hyper_items TEXT NOT NULL DEFAULT '[]',
                bonuses TEXT NOT NULL DEFAULT '{}',
                selected_cards TEXT NOT NULL DEFAULT '[]',
                region TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        # Run migration for existing tables
        self._migrate_civilizations_table()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS cooldowns (
                user_id TEXT,
                command TEXT,
                last_used_at TIMESTAMP,
                PRIMARY KEY (user_id, command)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS cards (
                user_id TEXT,
                tech_level INTEGER,
                available_cards TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, tech_level)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS alliances (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                description TEXT,
                leader_id TEXT NOT NULL,
                members TEXT NOT NULL DEFAULT '[]',
                join_requests TEXT NOT NULL DEFAULT '[]',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS wars (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                attacker_id TEXT NOT NULL,
                defender_id TEXT NOT NULL,
                war_type TEXT NOT NULL,
                declared_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                ended_at TIMESTAMP,
                result TEXT DEFAULT 'ongoing'
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS peace_offers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                offerer_id TEXT NOT NULL,
                receiver_id TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                offered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                responded_at TIMESTAMP
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sender_id TEXT NOT NULL,
                recipient_id TEXT NOT NULL,
                message TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP DEFAULT (datetime('now', '+1 day'))
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS trade_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sender_id TEXT NOT NULL,
                recipient_id TEXT NOT NULL,
                offer TEXT NOT NULL,
                request TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP DEFAULT (datetime('now', '+1 day'))
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT,
                event_type TEXT NOT NULL,
                title TEXT NOT NULL,
                description TEXT NOT NULL,
                effects TEXT NOT NULL DEFAULT '{}',
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS global_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS alliance_invitations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                alliance_id INTEGER NOT NULL,
                sender_id TEXT NOT NULL,
                recipient_id TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP DEFAULT (datetime('now', '+1 day'))
            )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_messages_expires ON messages(expires_at)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_trade_expires ON trade_requests(expires_at)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_invites_expires ON alliance_invitations(expires_at)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_wars_ongoing ON wars(result)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_peace_offers_status ON peace_offers(status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_messages_recipient ON messages(recipient_id)')
        conn.commit()
        self.upload_database()
        logger.info("Database initialized")

    def create_civilization(self, user_id: str, name: str, bonus_resources: Dict = None, bonuses: Dict = None, hyper_item: str = None) -> bool:
        try:
            default_resources = {"gold": 500, "food": 300, "stone": 100, "wood": 100}
            if bonus_resources:
                for r, v in bonus_resources.items():
                    if r in default_resources:
                        default_resources[r] += v
            default_population = {
                "citizens": 100 + (bonus_resources.get('population', 0) if bonus_resources else 0),
                "happiness": 50 + (bonus_resources.get('happiness', 0) if bonus_resources else 0),
                "hunger": 0,
                "employed": 50
            }
            default_military = {"soldiers": 10, "spies": 2, "tech_level": 1}
            default_territory = {"land_size": 1000}
            hyper_items = [hyper_item] if hyper_item else []
            bonuses = bonuses or {}
            selected_cards = []
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO civilizations (user_id, name, resources, population, military, territory, hyper_items, bonuses, selected_cards, region)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                user_id, name,
                json.dumps(default_resources),
                json.dumps(default_population),
                json.dumps(default_military),
                json.dumps(default_territory),
                json.dumps(hyper_items),
                json.dumps(bonuses),
                json.dumps(selected_cards),
                None
            ))
            self.generate_card_selection(user_id, 1)
            conn.commit()
            self.upload_database()
            logger.info(f"Created civilization '{name}' for user {user_id}")
            return True
        except sqlite3.IntegrityError:
            logger.warning(f"User {user_id} already has a civilization")
            return False
        except Exception as e:
            logger.error(f"Error creating civilization: {e}")
            return False

    def delete_civilization(self, user_id: str) -> bool:
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('DELETE FROM civilizations WHERE user_id = ?', (user_id,))
            cursor.execute('DELETE FROM cooldowns WHERE user_id = ?', (user_id,))
            cursor.execute('DELETE FROM cards WHERE user_id = ?', (user_id,))
            cursor.execute('DELETE FROM events WHERE user_id = ?', (user_id,))
            cursor.execute('DELETE FROM peace_offers WHERE offerer_id = ? OR receiver_id = ?', (user_id, user_id))
            cursor.execute('DELETE FROM messages WHERE sender_id = ? OR recipient_id = ?', (user_id, user_id))
            cursor.execute('DELETE FROM trade_requests WHERE sender_id = ? OR recipient_id = ?', (user_id, user_id))
            cursor.execute('DELETE FROM alliance_invitations WHERE sender_id = ? OR recipient_id = ?', (user_id, user_id))
            cursor.execute('DELETE FROM wars WHERE attacker_id = ? OR defender_id = ?', (user_id, user_id))
            cursor.execute('SELECT id, members FROM alliances')
            for alliance in cursor.fetchall():
                members = json.loads(alliance['members'])
                if user_id in members:
                    members.remove(user_id)
                    cursor.execute('UPDATE alliances SET members = ? WHERE id = ?', (json.dumps(members), alliance['id']))
            conn.commit()
            self.upload_database()
            logger.info(f"Deleted civilization for user {user_id}")
            return True
        except Exception as e:
            logger.error(f"Error deleting civilization for {user_id}: {e}")
            return False

    def get_civilization(self, user_id: str) -> Optional[Dict[str, Any]]:
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            # Migration already ran in init_database, but double-check columns exist
            cursor.execute("PRAGMA table_info(civilizations)")
            columns = [col[1] for col in cursor.fetchall()]
            if 'selected_cards' not in columns:
                cursor.execute("ALTER TABLE civilizations ADD COLUMN selected_cards TEXT NOT NULL DEFAULT '[]'")
                conn.commit()
            if 'region' not in columns:
                cursor.execute("ALTER TABLE civilizations ADD COLUMN region TEXT")
                conn.commit()
            cursor.execute('SELECT * FROM civilizations WHERE user_id = ?', (user_id,))
            row = cursor.fetchone()
            if not row:
                return None
            civ = dict(row)
            civ['resources'] = json.loads(civ.get('resources', '{}'))
            civ['population'] = json.loads(civ.get('population', '{}'))
            civ['military'] = json.loads(civ.get('military', '{}'))
            civ['territory'] = json.loads(civ.get('territory', '{}'))
            civ['hyper_items'] = json.loads(civ.get('hyper_items', '[]'))
            civ['bonuses'] = json.loads(civ.get('bonuses', '{}'))
            civ['selected_cards'] = json.loads(civ.get('selected_cards', '[]'))
            return civ
        except Exception as e:
            logger.error(f"Error getting civilization for {user_id}: {e}")
            return None

    def update_civilization(self, user_id: str, updates: Dict[str, Any]) -> bool:
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            set_clauses = []
            values = []
            for field, value in updates.items():
                if field in ['resources', 'population', 'military', 'territory', 'hyper_items', 'bonuses', 'selected_cards']:
                    set_clauses.append(f"{field} = ?")
                    values.append(json.dumps(value))
                else:
                    set_clauses.append(f"{field} = ?")
                    values.append(value)
            set_clauses.append("last_active = CURRENT_TIMESTAMP")
            values.append(user_id)
            query = f"UPDATE civilizations SET {', '.join(set_clauses)} WHERE user_id = ?"
            cursor.execute(query, values)
            conn.commit()
            self.upload_database()
            return True
        except Exception as e:
            logger.error(f"Error updating civilization for {user_id}: {e}")
            return False

    def get_command_cooldown(self, user_id: str, command: str) -> Optional[datetime]:
        try:
            cursor = self.get_connection().cursor()
            cursor.execute('SELECT last_used_at FROM cooldowns WHERE user_id = ? AND command = ?', (user_id, command))
            row = cursor.fetchone()
            return datetime.fromisoformat(row['last_used_at']) if row else None
        except Exception as e:
            logger.error(f"Error getting command cooldown: {e}")
            return None

    def check_cooldown(self, user_id: str, command: str) -> Optional[datetime]:
        return self.get_command_cooldown(user_id, command)

    def set_command_cooldown(self, user_id: str, command: str, timestamp: datetime = None) -> bool:
        try:
            if timestamp is None:
                timestamp = datetime.utcnow()
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('INSERT OR REPLACE INTO cooldowns (user_id, command, last_used_at) VALUES (?, ?, ?)',
                           (user_id, command, timestamp.isoformat()))
            conn.commit()
            self.upload_database()
            return True
        except Exception as e:
            logger.error(f"Error setting command cooldown: {e}")
            return False

    def update_cooldown(self, user_id: str, command: str, timestamp: datetime = None) -> bool:
        return self.set_command_cooldown(user_id, command, timestamp)

    def generate_card_selection(self, user_id: str, tech_level: int) -> bool:
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
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
                {"name": "Military Academy", "type": "bonus", "effect": {"soldier_training_speed": 25}, "description": "+25% soldier training speed"}
            ]
            available_cards = random.sample(card_pool, min(5, len(card_pool)))
            cursor.execute('INSERT OR REPLACE INTO cards (user_id, tech_level, available_cards, status) VALUES (?, ?, ?, ?)',
                           (user_id, tech_level, json.dumps(available_cards), 'pending'))
            conn.commit()
            self.upload_database()
            return True
        except Exception as e:
            logger.error(f"Error generating card selection: {e}")
            return False

    def get_card_selection(self, user_id: str, tech_level: int) -> Optional[Dict]:
        try:
            cursor = self.get_connection().cursor()
            cursor.execute('SELECT * FROM cards WHERE user_id = ? AND tech_level = ? AND status = ?',
                           (user_id, tech_level, 'pending'))
            row = cursor.fetchone()
            if row:
                data = dict(row)
                data['available_cards'] = json.loads(data['available_cards'])
                return data
            return None
        except Exception as e:
            logger.error(f"Error getting card selection: {e}")
            return None

    def select_card(self, user_id: str, tech_level: int, card_name: str) -> Optional[Dict]:
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            selection = self.get_card_selection(user_id, tech_level)
            if not selection:
                return None
            selected = next((c for c in selection['available_cards'] if c['name'].lower() == card_name.lower()), None)
            if not selected:
                return None
            cursor.execute('UPDATE cards SET status = ? WHERE user_id = ? AND tech_level = ?', ('selected', user_id, tech_level))
            conn.commit()
            self.upload_database()
            return selected
        except Exception as e:
            logger.error(f"Error selecting card: {e}")
            return None

    def get_all_civilizations(self) -> List[Dict[str, Any]]:
        try:
            cursor = self.get_connection().cursor()
            cursor.execute('SELECT * FROM civilizations ORDER BY last_active DESC')
            civs = []
            for row in cursor.fetchall():
                civ = dict(row)
                civ['resources'] = json.loads(civ.get('resources', '{}'))
                civ['population'] = json.loads(civ.get('population', '{}'))
                civ['military'] = json.loads(civ.get('military', '{}'))
                civ['territory'] = json.loads(civ.get('territory', '{}'))
                civ['hyper_items'] = json.loads(civ.get('hyper_items', '[]'))
                civ['bonuses'] = json.loads(civ.get('bonuses', '{}'))
                civ['selected_cards'] = json.loads(civ.get('selected_cards', '[]'))
                civs.append(civ)
            return civs
        except Exception as e:
            logger.error(f"Error getting all civilizations: {e}")
            return []

    def create_alliance(self, name: str, leader_id: str, description: str = "") -> bool:
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('INSERT INTO alliances (name, leader_id, members, description) VALUES (?, ?, ?, ?)',
                           (name, leader_id, json.dumps([leader_id]), description))
            conn.commit()
            self.upload_database()
            return True
        except sqlite3.IntegrityError:
            logger.warning(f"Alliance '{name}' already exists")
            return False
        except Exception as e:
            logger.error(f"Error creating alliance: {e}")
            return False

    def log_event(self, user_id: str, event_type: str, title: str, description: str, effects: Dict = None):
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('INSERT INTO events (user_id, event_type, title, description, effects) VALUES (?, ?, ?, ?, ?)',
                           (user_id, event_type, title, description, json.dumps(effects or {})))
            conn.commit()
            self.upload_database()
        except Exception as e:
            logger.error(f"Error logging event: {e}")

    def get_recent_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        try:
            cursor = self.get_connection().cursor()
            cursor.execute('''
                SELECT e.*, c.name as civ_name
                FROM events e
                LEFT JOIN civilizations c ON e.user_id = c.user_id
                ORDER BY e.timestamp DESC LIMIT ?
            ''', (limit,))
            events = []
            for row in cursor.fetchall():
                event = dict(row)
                event['effects'] = json.loads(event['effects'])
                events.append(event)
            return events
        except Exception as e:
            logger.error(f"Error getting recent events: {e}")
            return []

    def create_trade_request(self, sender_id: str, recipient_id: str, offer: Dict, request: Dict) -> bool:
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('INSERT INTO trade_requests (sender_id, recipient_id, offer, request) VALUES (?, ?, ?, ?)',
                           (sender_id, recipient_id, json.dumps(offer), json.dumps(request)))
            conn.commit()
            self.upload_database()
            return True
        except Exception as e:
            logger.error(f"Error creating trade request: {e}")
            return False

    def get_trade_requests(self, user_id: str) -> List[Dict]:
        try:
            cursor = self.get_connection().cursor()
            cursor.execute('''
                SELECT t.*, c.name as sender_name
                FROM trade_requests t
                JOIN civilizations c ON t.sender_id = c.user_id
                WHERE recipient_id = ? AND expires_at > CURRENT_TIMESTAMP
            ''', (user_id,))
            requests = []
            for row in cursor.fetchall():
                req = dict(row)
                req['offer'] = json.loads(req['offer'])
                req['request'] = json.loads(req['request'])
                requests.append(req)
            return requests
        except Exception as e:
            logger.error(f"Error getting trade requests: {e}")
            return []

    def get_trade_request_by_id(self, request_id: int) -> Optional[Dict]:
        try:
            cursor = self.get_connection().cursor()
            cursor.execute('SELECT * FROM trade_requests WHERE id = ? AND expires_at > CURRENT_TIMESTAMP', (request_id,))
            row = cursor.fetchone()
            if row:
                req = dict(row)
                req['offer'] = json.loads(req['offer'])
                req['request'] = json.loads(req['request'])
                return req
            return None
        except Exception as e:
            logger.error(f"Error getting trade request by ID: {e}")
            return None

    def delete_trade_request(self, request_id: int) -> bool:
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('DELETE FROM trade_requests WHERE id = ?', (request_id,))
            conn.commit()
            self.upload_database()
            return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Error deleting trade request: {e}")
            return False

    def create_alliance_invite(self, alliance_id: int, sender_id: str, recipient_id: str) -> bool:
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('INSERT INTO alliance_invitations (alliance_id, sender_id, recipient_id) VALUES (?, ?, ?)',
                           (alliance_id, sender_id, recipient_id))
            conn.commit()
            self.upload_database()
            return True
        except Exception as e:
            logger.error(f"Error creating alliance invite: {e}")
            return False

    def get_alliance_invites(self, user_id: str) -> List[Dict]:
        try:
            cursor = self.get_connection().cursor()
            cursor.execute('''
                SELECT ai.*, a.name as alliance_name
                FROM alliance_invitations ai
                JOIN alliances a ON ai.alliance_id = a.id
                WHERE recipient_id = ? AND expires_at > CURRENT_TIMESTAMP
            ''', (user_id,))
            return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error getting alliance invites: {e}")
            return []

    def get_alliance_invite_by_id(self, invite_id: int) -> Optional[Dict]:
        try:
            cursor = self.get_connection().cursor()
            cursor.execute('''
                SELECT ai.*, a.name as alliance_name
                FROM alliance_invitations ai
                JOIN alliances a ON ai.alliance_id = a.id
                WHERE ai.id = ? AND expires_at > CURRENT_TIMESTAMP
            ''', (invite_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
        except Exception as e:
            logger.error(f"Error getting alliance invite by ID: {e}")
            return None

    def delete_alliance_invite(self, invite_id: int) -> bool:
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('DELETE FROM alliance_invitations WHERE id = ?', (invite_id,))
            conn.commit()
            self.upload_database()
            return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Error deleting alliance invite: {e}")
            return False

    def send_message(self, sender_id: str, recipient_id: str, message: str) -> bool:
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('INSERT INTO messages (sender_id, recipient_id, message) VALUES (?, ?, ?)',
                           (sender_id, recipient_id, message))
            conn.commit()
            self.upload_database()
            return True
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            return False

    def get_messages(self, user_id: str) -> List[Dict]:
        try:
            cursor = self.get_connection().cursor()
            cursor.execute('''
                SELECT m.*, c.name as sender_name
                FROM messages m
                JOIN civilizations c ON m.sender_id = c.user_id
                WHERE recipient_id = ? AND expires_at > CURRENT_TIMESTAMP
                ORDER BY created_at DESC
            ''', (user_id,))
            return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error getting messages: {e}")
            return []

    def delete_message(self, message_id: int) -> bool:
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('DELETE FROM messages WHERE id = ?', (message_id,))
            conn.commit()
            self.upload_database()
            return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Error deleting message: {e}")
            return False

    def get_alliance(self, alliance_id: int) -> Optional[Dict]:
        try:
            cursor = self.get_connection().cursor()
            cursor.execute('SELECT * FROM alliances WHERE id = ?', (alliance_id,))
            row = cursor.fetchone()
            if row:
                alliance = dict(row)
                alliance['members'] = json.loads(alliance['members'])
                alliance['join_requests'] = json.loads(alliance['join_requests'])
                return alliance
            return None
        except Exception as e:
            logger.error(f"Error getting alliance: {e}")
            return None

    def get_alliance_by_name(self, name: str) -> Optional[Dict]:
        try:
            cursor = self.get_connection().cursor()
            cursor.execute('SELECT * FROM alliances WHERE name = ?', (name,))
            row = cursor.fetchone()
            if row:
                alliance = dict(row)
                alliance['members'] = json.loads(alliance['members'])
                alliance['join_requests'] = json.loads(alliance['join_requests'])
                return alliance
            return None
        except Exception as e:
            logger.error(f"Error getting alliance by name: {e}")
            return None

    def add_alliance_member(self, alliance_id: int, user_id: str) -> bool:
        try:
            alliance = self.get_alliance(alliance_id)
            if not alliance:
                return False
            if user_id in alliance['members']:
                return True
            members = alliance['members'] + [user_id]
            join_requests = [u for u in alliance['join_requests'] if u != user_id]
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('UPDATE alliances SET members = ?, join_requests = ? WHERE id = ?',
                           (json.dumps(members), json.dumps(join_requests), alliance_id))
            conn.commit()
            self.upload_database()
            return True
        except Exception as e:
            logger.error(f"Error adding alliance member: {e}")
            return False

    def get_wars(self, user_id: str = None, status: str = 'ongoing') -> List[Dict]:
        try:
            cursor = self.get_connection().cursor()
            if user_id:
                cursor.execute('''
                    SELECT w.*, ac.name as attacker_name, dc.name as defender_name
                    FROM wars w
                    JOIN civilizations ac ON w.attacker_id = ac.user_id
                    JOIN civilizations dc ON w.defender_id = dc.user_id
                    WHERE (w.attacker_id = ? OR w.defender_id = ?) AND w.result = ?
                ''', (user_id, user_id, status))
            else:
                cursor.execute('''
                    SELECT w.*, ac.name as attacker_name, dc.name as defender_name
                    FROM wars w
                    JOIN civilizations ac ON w.attacker_id = ac.user_id
                    JOIN civilizations dc ON w.defender_id = dc.user_id
                    WHERE w.result = ?
                ''', (status,))
            return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error getting wars: {e}")
            return []

    def get_peace_offers(self, user_id: str = None) -> List[Dict]:
        try:
            cursor = self.get_connection().cursor()
            if user_id:
                cursor.execute('''
                    SELECT po.*, oc.name as offerer_name, rc.name as receiver_name
                    FROM peace_offers po
                    JOIN civilizations oc ON po.offerer_id = oc.user_id
                    JOIN civilizations rc ON po.receiver_id = rc.user_id
                    WHERE (po.offerer_id = ? OR po.receiver_id = ?) AND po.status = 'pending'
                ''', (user_id, user_id))
            else:
                cursor.execute('''
                    SELECT po.*, oc.name as offerer_name, rc.name as receiver_name
                    FROM peace_offers po
                    JOIN civilizations oc ON po.offerer_id = oc.user_id
                    JOIN civilizations rc ON po.receiver_id = rc.user_id
                    WHERE po.status = 'pending'
                ''')
            return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error getting peace offers: {e}")
            return []

    def create_peace_offer(self, offerer_id: str, receiver_id: str) -> bool:
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('INSERT INTO peace_offers (offerer_id, receiver_id) VALUES (?, ?)', (offerer_id, receiver_id))
            conn.commit()
            self.upload_database()
            return True
        except Exception as e:
            logger.error(f"Error creating peace offer: {e}")
            return False

    def update_peace_offer(self, offer_id: int, status: str) -> bool:
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('UPDATE peace_offers SET status = ?, responded_at = CURRENT_TIMESTAMP WHERE id = ?',
                           (status, offer_id))
            conn.commit()
            self.upload_database()
            return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Error updating peace offer: {e}")
            return False

    def end_war(self, attacker_id: str, defender_id: str, result: str) -> bool:
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE wars SET result = ?, ended_at = CURRENT_TIMESTAMP
                WHERE ((attacker_id = ? AND defender_id = ?) OR (attacker_id = ? AND defender_id = ?))
                AND result = 'ongoing'
            ''', (result, attacker_id, defender_id, defender_id, attacker_id))
            conn.commit()
            self.upload_database()
            return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Error ending war: {e}")
            return False

    def get_user_statistics(self, user_id: str) -> Dict[str, Any]:
        try:
            civ = self.get_civilization(user_id)
            if not civ:
                return {}
            cursor = self.get_connection().cursor()
            cursor.execute('''
                SELECT COUNT(*) as total_wars,
                       SUM(CASE WHEN result = 'victory' THEN 1 ELSE 0 END) as victories,
                       SUM(CASE WHEN result = 'defeat' THEN 1 ELSE 0 END) as defeats,
                       SUM(CASE WHEN result = 'peace' THEN 1 ELSE 0 END) as peace_treaties
                FROM wars WHERE attacker_id = ? OR defender_id = ?
            ''', (user_id, user_id))
            war_stats = dict(cursor.fetchone()) if cursor.rowcount > 0 else {'total_wars': 0, 'victories': 0, 'defeats': 0, 'peace_treaties': 0}
            cursor.execute('SELECT COUNT(*) FROM events WHERE user_id = ?', (user_id,))
            total_events = cursor.fetchone()[0]
            military_power = civ['military']['soldiers'] * 10 + civ['military']['spies'] * 5 + civ['military']['tech_level'] * 50
            economic_power = sum(civ['resources'].values())
            territorial_power = civ['territory']['land_size']
            total_power = military_power + economic_power + territorial_power
            return {
                'civilization': civ,
                'war_statistics': war_stats,
                'total_events': total_events,
                'power_scores': {'military': military_power, 'economic': economic_power, 'territorial': territorial_power, 'total': total_power}
            }
        except Exception as e:
            logger.error(f"Error getting user statistics: {e}")
            return {}

    def get_leaderboard(self, category: str = 'power', limit: int = 10) -> List[Dict]:
        try:
            cursor = self.get_connection().cursor()
            if category == 'power':
                cursor.execute('SELECT user_id, name, resources, military, territory FROM civilizations')
                civs = []
                for row in cursor.fetchall():
                    civ = dict(row)
                    resources = json.loads(civ['resources'])
                    military = json.loads(civ['military'])
                    territory = json.loads(civ['territory'])
                    mil_pow = military['soldiers'] * 10 + military['spies'] * 5 + military['tech_level'] * 50
                    eco_pow = sum(resources.values())
                    ter_pow = territory['land_size']
                    total = mil_pow + eco_pow + ter_pow
                    civs.append({'user_id': civ['user_id'], 'name': civ['name'], 'score': total})
                return sorted(civs, key=lambda x: x['score'], reverse=True)[:limit]
            elif category == 'gold':
                cursor.execute('SELECT user_id, name, resources FROM civilizations ORDER BY json_extract(resources, "$.gold") DESC LIMIT ?', (limit,))
                return [{'user_id': row['user_id'], 'name': row['name'], 'score': json.loads(row['resources'])['gold']} for row in cursor.fetchall()]
            elif category == 'military':
                cursor.execute('SELECT user_id, name, military FROM civilizations ORDER BY (json_extract(military, "$.soldiers") + json_extract(military, "$.spies")) DESC LIMIT ?', (limit,))
                return [{'user_id': row['user_id'], 'name': row['name'], 'score': json.loads(row['military'])['soldiers'] + json.loads(row['military'])['spies']} for row in cursor.fetchall()]
            elif category == 'territory':
                cursor.execute('SELECT user_id, name, territory FROM civilizations ORDER BY json_extract(territory, "$.land_size") DESC LIMIT ?', (limit,))
                return [{'user_id': row['user_id'], 'name': row['name'], 'score': json.loads(row['territory'])['land_size']} for row in cursor.fetchall()]
            return []
        except Exception as e:
            logger.error(f"Error getting leaderboard: {e}")
            return []

    def cleanup_expired_requests(self):
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('DELETE FROM trade_requests WHERE expires_at <= CURRENT_TIMESTAMP')
            trade_count = cursor.rowcount
            cursor.execute('DELETE FROM alliance_invitations WHERE expires_at <= CURRENT_TIMESTAMP')
            invite_count = cursor.rowcount
            cursor.execute('DELETE FROM messages WHERE expires_at <= CURRENT_TIMESTAMP')
            msg_count = cursor.rowcount
            conn.commit()
            self.upload_database()
            logger.info(f"Cleaned up {trade_count} trades, {invite_count} invites, {msg_count} messages")
            return True
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
            return False

    def backup_database(self, backup_path: str = None) -> bool:
        try:
            import shutil
            if not backup_path:
                backup_path = f"nationbot_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
            shutil.copy2(self.db_path, backup_path)
            if self.dropbox_client:
                dropbox_path = f"/backups/{os.path.basename(backup_path)}"
                with open(backup_path, 'rb') as f:
                    self.dropbox_client.files_upload(f.read(), dropbox_path, mode=dropbox.files.WriteMode('add'))
            return True
        except Exception as e:
            logger.error(f"Error backing up database: {e}")
            return False

    def get_database_info(self) -> Dict[str, Any]:
        try:
            cursor = self.get_connection().cursor()
            info = {}
            tables = ['civilizations', 'wars', 'peace_offers', 'alliances', 'events', 'trade_requests', 'messages', 'cards', 'cooldowns', 'alliance_invitations']
            for t in tables:
                cursor.execute(f'SELECT COUNT(*) FROM {t}')
                info[f'{t}_count'] = cursor.fetchone()[0]
            if os.path.exists(self.db_path):
                info['database_size_mb'] = round(os.path.getsize(self.db_path) / (1024 * 1024), 2)
            cursor.execute("SELECT COUNT(*) FROM civilizations WHERE last_active > datetime('now', '-7 days')")
            info['active_users_week'] = cursor.fetchone()[0]
            return info
        except Exception as e:
            logger.error(f"Error getting database info: {e}")
            return {}

    def close_connections(self):
        if hasattr(self.local, 'connection'):
            self.local.connection.close()
            del self.local.connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
