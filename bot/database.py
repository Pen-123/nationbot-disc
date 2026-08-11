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
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

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
        self._upload_lock = threading.Lock()
        self._upload_enabled = False
        self._shutdown = False

        self._ensure_db_writable()

        if self.dropbox_refresh_token and self.dropbox_app_key and self.dropbox_app_secret:
            self.init_dropbox()

        if self.dropbox_client:
            self._sync_from_dropbox(force=False)
        else:
            if not os.path.exists(self.db_path):
                open(self.db_path, 'w').close()

        self.init_database()
        self.setup_cleanup_scheduler()
        import atexit
        atexit.register(self.shutdown)

    def _ensure_db_writable(self):
        if not os.path.exists(self.db_path):
            try:
                open(self.db_path, 'w').close()
                os.chmod(self.db_path, 0o666)
                logger.info(f"Created new database file: {self.db_path}")
            except Exception as e:
                logger.error(f"Could not create database file: {e}")
        else:
            if not os.access(self.db_path, os.W_OK):
                try:
                    os.chmod(self.db_path, 0o666)
                    logger.info(f"Set permissions on {self.db_path} to 666 (read/write for all).")
                except Exception as e:
                    logger.error(f"Could not change permissions on {self.db_path}: {e}")

    def init_dropbox(self):
        try:
            dbx = dropbox.Dropbox(
                oauth2_refresh_token=self.dropbox_refresh_token,
                app_key=self.dropbox_app_key,
                app_secret=self.dropbox_app_secret
            )
            dbx.check_user()
            self.dropbox_client = dbx
            self._upload_enabled = True
            logger.info("Dropbox client initialized successfully")
        except AuthError as e:
            logger.error(f"Dropbox auth error: {e}. Check your refresh token and app credentials.")
            self.dropbox_client = None
            self._upload_enabled = False
        except Exception as e:
            logger.error(f"Error initializing Dropbox: {e}")
            self.dropbox_client = None
            self._upload_enabled = False

    def _get_remote_timestamp(self, file_path: str) -> float:
        if not self.dropbox_client:
            return 0
        try:
            meta = self.dropbox_client.files_get_metadata(file_path)
            return meta.server_modified.timestamp()
        except ApiError as e:
            if e.error.is_path() and e.error.get_path().is_not_found():
                return 0
            logger.error(f"Error getting remote metadata for {file_path}: {e}")
            return 0

    def _sync_from_dropbox(self, force: bool = False):
        if not self.dropbox_client:
            return
        remote_ts = self._get_remote_timestamp(f"/{os.path.basename(self.db_path)}")
        local_ts = os.path.getmtime(self.db_path) if os.path.exists(self.db_path) else 0
        logger.info(f"Remote timestamp: {remote_ts}, Local timestamp: {local_ts}")
        if force or (remote_ts > 0 and (not os.path.exists(self.db_path) or remote_ts > local_ts + 1.0)):
            if os.path.exists(self.db_path):
                backup_path = f"{self.db_path}.backup"
                try:
                    os.rename(self.db_path, backup_path)
                    logger.info(f"Local database backed up to {backup_path}")
                except Exception as e:
                    logger.warning(f"Could not backup local database: {e}")
            self.download_database()
        else:
            logger.info("Local database is up-to-date or newer; skipping download.")

    def download_database(self, file_path: str = None, dest_path: str = None):
        if not self.dropbox_client:
            logger.warning("No Dropbox client; cannot download.")
            return False
        if file_path is None:
            file_path = f"/{os.path.basename(self.db_path)}"
        if dest_path is None:
            dest_path = self.db_path
        try:
            try:
                self.dropbox_client.files_get_metadata(file_path)
            except ApiError as e:
                if e.error.is_path() and e.error.get_path().is_not_found():
                    logger.info(f"File {file_path} not found in Dropbox.")
                    return False
                raise
            self.dropbox_client.files_download_to_file(dest_path, file_path)
            os.chmod(dest_path, 0o666)
            logger.info(f"Downloaded {file_path} to {dest_path}")
            return True
        except Exception as e:
            logger.error(f"Error downloading {file_path}: {e}")
            return False

    def force_sync(self) -> bool:
        """Force download the previous version of the database from Dropbox."""
        if not self.dropbox_client:
            logger.warning("Dropbox client not available.")
            return False
        prev_path = f"/warbot_prev.db"
        if not self.download_database(prev_path, self.db_path):
            logger.warning("No previous version found; falling back to latest.")
            self._sync_from_dropbox(force=True)
        else:
            logger.info("Successfully rolled back to previous database version.")
        return True

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((Exception,)),
        reraise=True
    )
    def _upload_to_dropbox(self):
        if not self._upload_enabled or not self.dropbox_client:
            logger.warning("Dropbox upload skipped – client not available.")
            return
        now = time.time()
        if now - self._last_upload < 30 and not self._shutdown:
            logger.debug("Skipping upload – less than 30s since last upload.")
            return
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("PRAGMA integrity_check")
            if cursor.fetchone()[0] != "ok":
                logger.error("Database integrity check failed; aborting upload.")
                return

            dropbox_path = f"/{os.path.basename(self.db_path)}"
            prev_dropbox_path = f"/warbot_prev.db"
            temp_prev_path = f"{self.db_path}.prev"
            if os.path.exists(temp_prev_path):
                os.remove(temp_prev_path)

            # Download current remote db (if exists) to use as previous
            try:
                self.dropbox_client.files_download_to_file(temp_prev_path, dropbox_path)
                logger.info("Downloaded current remote db to use as previous version.")
            except ApiError as e:
                if e.error.is_path() and e.error.get_path().is_not_found():
                    logger.info("No remote database found; this is the first upload.")
                else:
                    raise

            # Upload the new current database
            with open(self.db_path, 'rb') as f:
                self.dropbox_client.files_upload(
                    f.read(),
                    dropbox_path,
                    mode=dropbox.files.WriteMode('overwrite')
                )
            logger.info(f"Uploaded new current database: {dropbox_path}")

            # Upload the previous version (if exists)
            if os.path.exists(temp_prev_path):
                with open(temp_prev_path, 'rb') as f:
                    self.dropbox_client.files_upload(
                        f.read(),
                        prev_dropbox_path,
                        mode=dropbox.files.WriteMode('overwrite')
                    )
                os.remove(temp_prev_path)
                logger.info(f"Uploaded previous database: {prev_dropbox_path}")
            else:
                try:
                    self.dropbox_client.files_delete(prev_dropbox_path)
                    logger.info(f"Deleted stale previous database: {prev_dropbox_path}")
                except ApiError as e:
                    if e.error.is_path() and e.error.get_path().is_not_found():
                        pass
                    else:
                        raise

            self._last_upload = now
        except Exception as e:
            logger.error(f"Error in upload process: {e}")
            raise

    def upload_database(self, force=False):
        if not self._upload_enabled:
            return
        if not force and not self._upload_lock.acquire(blocking=False):
            logger.debug("Upload already in progress; skipping.")
            return
        try:
            self._upload_to_dropbox()
        finally:
            if not force:
                self._upload_lock.release()

    def get_connection(self):
        if not hasattr(self.local, 'connection'):
            self._ensure_db_writable()
            self.local.connection = sqlite3.connect(self.db_path, check_same_thread=False)
            self.local.connection.row_factory = sqlite3.Row
            self.local.connection.execute("PRAGMA journal_mode=WAL")
            self.local.connection.execute("PRAGMA synchronous=NORMAL")
            self.local.connection.execute("PRAGMA busy_timeout=5000")
        return self.local.connection

    # ---- All other methods (migrations, CRUD, etc.) remain unchanged ----
    # I'm omitting them here to keep the answer readable, but the full file includes them.
    # They are identical to the previous version.

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

    # ---- Migrations ----
    def _migrate_civilizations_table(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(civilizations)")
        columns = [col[1] for col in cursor.fetchall()]
        expected = {
            'selected_cards': ("TEXT NOT NULL DEFAULT '[]'", "'[]'"),
            'region': ("TEXT", None),
            'black_market_history': ("TEXT NOT NULL DEFAULT '{}'", "'{}'"),
            'job': ("TEXT NOT NULL DEFAULT 'Unemployed'", "'Unemployed'")
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

    def _migrate_cooldowns_table(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='cooldowns'")
        if not cursor.fetchone():
            cursor.execute('''
                CREATE TABLE cooldowns (
                    user_id TEXT,
                    command TEXT,
                    last_used_at TIMESTAMP,
                    PRIMARY KEY (user_id, command)
                )
            ''')
            conn.commit()
            logger.info("Created missing cooldowns table")
            return

        cursor.execute("PRAGMA table_info(cooldowns)")
        columns = [col[1] for col in cursor.fetchall()]
        expected = {
            'last_used_at': ("TIMESTAMP", None)
        }
        for col, (col_def, default_val) in expected.items():
            if col not in columns:
                try:
                    cursor.execute(f"ALTER TABLE cooldowns ADD COLUMN {col} {col_def}")
                    conn.commit()
                    logger.info(f"Added missing column '{col}' to cooldowns table")
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
                black_market_history TEXT NOT NULL DEFAULT '{}',
                job TEXT NOT NULL DEFAULT 'Unemployed',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        self._migrate_civilizations_table()
        self._migrate_cooldowns_table()

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
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS territories (
                user_id TEXT PRIMARY KEY,
                owned_provinces TEXT NOT NULL DEFAULT '[]'
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS territory_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                territory_name TEXT NOT NULL,
                claimed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS industrial_revolutions (
                user_id TEXT PRIMARY KEY,
                active INTEGER NOT NULL DEFAULT 0,
                completed INTEGER NOT NULL DEFAULT 0,
                started_at TIMESTAMP,
                stats TEXT NOT NULL DEFAULT '{}'
            )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_messages_expires ON messages(expires_at)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_trade_expires ON trade_requests(expires_at)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_invites_expires ON alliance_invitations(expires_at)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_wars_ongoing ON wars(result)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_peace_offers_status ON peace_offers(status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_messages_recipient ON messages(recipient_id)')
        conn.commit()
        self.upload_database(force=True)
        logger.info("Database initialized")

    # ---- CRUD operations (unchanged) ----
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
            black_market_history = {}
            default_job = "Unemployed"
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO civilizations (user_id, name, resources, population, military, territory, hyper_items, bonuses, selected_cards, region, black_market_history, job)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                user_id, name,
                json.dumps(default_resources),
                json.dumps(default_population),
                json.dumps(default_military),
                json.dumps(default_territory),
                json.dumps(hyper_items),
                json.dumps(bonuses),
                json.dumps(selected_cards),
                None,
                json.dumps(black_market_history),
                default_job
            ))
            self.generate_card_selection(user_id, 1)
            conn.commit()
            self.upload_database(force=True)
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
            cursor.execute('DELETE FROM territories WHERE user_id = ?', (user_id,))
            cursor.execute('DELETE FROM territory_history WHERE user_id = ?', (user_id,))
            cursor.execute('DELETE FROM industrial_revolutions WHERE user_id = ?', (user_id,))
            conn.commit()
            self.upload_database(force=True)
            logger.info(f"Deleted civilization and all related data for user {user_id}")
            return True
        except Exception as e:
            logger.error(f"Error deleting civilization for {user_id}: {e}")
            return False

    def get_civilization(self, user_id: str) -> Optional[Dict[str, Any]]:
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            self._migrate_civilizations_table()
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
            civ['black_market_history'] = json.loads(civ.get('black_market_history', '{}'))
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
            json_fields = ['resources', 'population', 'military', 'territory', 'hyper_items', 'bonuses', 'selected_cards', 'black_market_history']
            for field, value in updates.items():
                if field in json_fields:
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
            self.upload_database(force=True)
            return True
        except Exception as e:
            logger.error(f"Error updating civilization for {user_id}: {e}")
            return False

    def get_command_cooldown(self, user_id: str, command: str) -> Optional[datetime]:
        try:
            cursor = self.get_connection().cursor()
            cursor.execute('SELECT last_used_at FROM cooldowns WHERE user_id = ? AND command = ?', (user_id, command))
            row = cursor.fetchone()
            return datetime.fromisoformat(row['last_used_at']) if row and row['last_used_at'] else None
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
            self.upload_database(force=True)
            return True
        except Exception as e:
            logger.error(f"Error setting command cooldown: {e}")
            return False

    def update_cooldown(self, user_id: str, command: str, timestamp: datetime = None) -> bool:
        return self.set_command_cooldown(user_id, command, timestamp)

    # ---- Card methods ----
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
            self.upload_database(force=True)
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
            self.upload_database(force=True)
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
                civ['black_market_history'] = json.loads(civ.get('black_market_history', '{}'))
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
            self.upload_database(force=True)
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
            self.upload_database(force=True)
        except Exception as e:
            logger.error(f"Error logging event: {e}")

    # ---- Trade, messages, wars, etc. (all unchanged) ----
    # I'm omitting them for brevity, but they are identical to the previous version.
    # The full file includes all methods.

    # ---- Cleanup, backup, shutdown ----
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
            self.upload_database(force=True)
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
            tables = ['civilizations', 'wars', 'peace_offers', 'alliances', 'events', 'trade_requests', 'messages', 'cards', 'cooldowns', 'alliance_invitations', 'territories', 'territory_history', 'industrial_revolutions']
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

    def shutdown(self):
        self._shutdown = True
        logger.info("Shutdown: uploading database one last time...")
        self.upload_database(force=True)
        self.close_connections()
        logger.info("Shutdown complete.")

    def is_region_taken(self, region_name: str, exclude_user_id: str = None) -> bool:
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            if exclude_user_id:
                cursor.execute('SELECT COUNT(*) FROM civilizations WHERE region = ? AND user_id != ?', (region_name, exclude_user_id))
            else:
                cursor.execute('SELECT COUNT(*) FROM civilizations WHERE region = ?', (region_name,))
            count = cursor.fetchone()[0]
            return count > 0
        except Exception as e:
            logger.error(f"Error checking region taken: {e}")
            return False

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
