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
        self._last_upload = 0  # Throttle uploads
        if self.dropbox_refresh_token and self.dropbox_app_key and self.dropbox_app_secret:
            self.init_dropbox()
        # Only download if local file doesn't exist (avoid overwriting fresh data)
        if not os.path.exists(self.db_path):
            self.download_database()
        self.init_database()
        self.setup_cleanup_scheduler()

    def init_dropbox(self):
        """Initialize Dropbox client with refresh token"""
        try:
            dbx = dropbox.Dropbox(
                oauth2_refresh_token=self.dropbox_refresh_token,
                app_key=self.dropbox_app_key,
                app_secret=self.dropbox_app_secret
            )
            dbx.check_user()  # Verify connection
            self.dropbox_client = dbx
            logger.info("Dropbox client initialized successfully")
        except AuthError as e:
            logger.error(f"Dropbox auth error: {e}")
            self.dropbox_client = None
        except Exception as e:
            logger.error(f"Error initializing Dropbox: {e}")
            self.dropbox_client = None

    def download_database(self):
        """Download the database file from Dropbox if it exists"""
        if not self.dropbox_client:
            logger.warning("No Dropbox client, using local file or creating new")
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
        """Upload the database file to Dropbox (throttled to max once per 30 seconds)"""
        if not self.dropbox_client:
            logger.warning("No Dropbox client, skipping upload")
            return
        # Throttle uploads to avoid excessive API calls and race conditions
        if time.time() - self._last_upload < 30:
            return
        try:
            # Check integrity
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
        """Get thread-local database connection"""
        if not hasattr(self.local, 'connection'):
            self.local.connection = sqlite3.connect(self.db_path, check_same_thread=False)
            self.local.connection.row_factory = sqlite3.Row
        return self.local.connection

    def setup_cleanup_scheduler(self):
        """Schedule daily cleanup of expired requests"""
        def cleanup_task():
            logger.info("Running scheduled cleanup of expired requests...")
            self.cleanup_expired_requests()
            timer = threading.Timer(86400, cleanup_task)
            timer.daemon = True
            timer.start()

        initial_timer = threading.Timer(60, cleanup_task)
        initial_timer.daemon = True
        initial_timer.start()
        logger.info("Scheduled cleanup task initialized")

    def init_database(self):
        """Initialize database tables"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Civilizations table with region support
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
        
        # Cooldowns table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS cooldowns (
                user_id TEXT,
                command TEXT,
                last_used_at TIMESTAMP,
                PRIMARY KEY (user_id, command)
            )
        ''')
        
        # Cards table
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
        
        # Alliances table
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
        
        # Wars table
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
        
        # Peace offers table
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
        
        # Messages table with proper structure
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
        
        # Trade requests table - FIXED: removed double "NOT"
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
        
        # Events table
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
        
        # Global settings table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS global_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        ''')
        
        # Alliance invitation table
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
        
        # Create indexes for faster lookups
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_messages_expires ON messages(expires_at)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_trade_expires ON trade_requests(expires_at)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_invites_expires ON alliance_invitations(expires_at)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_wars_ongoing ON wars(result)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_peace_offers_status ON peace_offers(status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_messages_recipient ON messages(recipient_id)')  # Note: column is 'recipient_id' in schema
        
        conn.commit()
        self.upload_database()
        logger.info("Database initialized successfully")

    def create_civilization(self, user_id: str, name: str, bonus_resources: Dict = None, bonuses: Dict = None, hyper_item: str = None) -> bool:
        """Create a new civilization"""
        try:
            default_resources = {
                "gold": 500,
                "food": 300,
                "stone": 100,
                "wood": 100
            }
            
            if bonus_resources:
                for resource, amount in bonus_resources.items():
                    if resource in default_resources:
                        default_resources[resource] += amount
            
            default_population = {
                "citizens": 100 + bonus_resources.get('population', 0) if bonus_resources else 100,
                "happiness": 50 + bonus_resources.get('happiness', 0) if bonus_resources else 50,
                "hunger": 0,
                "employed": 50
            }
            
            default_military = {
                "soldiers": 10,
                "spies": 2,
                "tech_level": 1
            }
            
            default_territory = {
                "land_size": 1000
            }
            
            hyper_items = [hyper_item] if hyper_item else []
            bonuses = bonuses or {}
            selected_cards = []
            
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO civilizations 
                (user_id, name, resources, population, military, territory, hyper_items, bonuses, selected_cards, region)
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
                None  # Region starts as null
            ))
            
            # Create initial card selection for tech level 1
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
        """Completely delete a civilization and all related data"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Delete from all related tables with correct column mappings
            cursor.execute('DELETE FROM civilizations WHERE user_id = ?', (user_id,))
            cursor.execute('DELETE FROM cooldowns WHERE user_id = ?', (user_id,))
            cursor.execute('DELETE FROM cards WHERE user_id = ?', (user_id,))
            cursor.execute('DELETE FROM events WHERE user_id = ?', (user_id,))
            cursor.execute('DELETE FROM peace_offers WHERE offerer_id = ? OR receiver_id = ?', (user_id, user_id))
            cursor.execute('DELETE FROM messages WHERE sender_id = ? OR recipient_id = ?', (user_id, user_id))
            cursor.execute('DELETE FROM trade_requests WHERE sender_id = ? OR recipient_id = ?', (user_id, user_id))
            cursor.execute('DELETE FROM alliance_invitations WHERE sender_id = ? OR recipient_id = ?', (user_id, user_id))
            
            # Also clean from wars (both as attacker and defender)
            cursor.execute('DELETE FROM wars WHERE attacker_id = ? OR defender_id = ?', (user_id, user_id))
            
            # Handle alliance memberships
            cursor.execute('SELECT id, members FROM alliances')
            alliances = cursor.fetchall()
            for alliance in alliances:
                alliance_id = alliance['id']
                members = json.loads(alliance['members'])
                if user_id in members:
                    members.remove(user_id)
                    cursor.execute('UPDATE alliances SET members = ? WHERE id = ?', 
                                 (json.dumps(members), alliance_id))
            
            conn.commit()
            self.upload_database()
            logger.info(f"Completely deleted civilization for user {user_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error deleting civilization for user {user_id}: {e}")
            return False

   def get_civilization(self, user_id: str) -> Optional[Dict[str, Any]]:
    """Get civilization data for a user"""
    try:
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Ensure selected_cards column exists (simple migration)
        cursor.execute("PRAGMA table_info(civilizations)")
        columns = [col[1] for col in cursor.fetchall()]
        if 'selected_cards' not in columns:
            cursor.execute("ALTER TABLE civilizations ADD COLUMN selected_cards TEXT NOT NULL DEFAULT '[]'")
            conn.commit()
        
        cursor.execute('SELECT * FROM civilizations WHERE user_id = ?', (user_id,))
        row = cursor.fetchone()
        
        if not row:
            return None
        
        civ = dict(row)
        # Safely load JSON fields with defaults
        civ['resources'] = json.loads(civ.get('resources', '{}'))
        civ['population'] = json.loads(civ.get('population', '{}'))
        civ['military'] = json.loads(civ.get('military', '{}'))
        civ['territory'] = json.loads(civ.get('territory', '{}'))
        civ['hyper_items'] = json.loads(civ.get('hyper_items', '[]'))
        civ['bonuses'] = json.loads(civ.get('bonuses', '{}'))
        civ['selected_cards'] = json.loads(civ.get('selected_cards', '[]'))
        
        return civ
        
    except Exception as e:
        logger.error(f"Error getting civilization for user {user_id}: {e}")
        return None

    def update_civilization(self, user_id: str, updates: Dict[str, Any]) -> bool:
        """Update civilization data"""
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
            logger.error(f"Error updating civilization for user {user_id}: {e}")
            return False

    def get_command_cooldown(self, user_id: str, command: str) -> Optional[datetime]:
        """Get the last used time for a command, or None if no cooldown"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT last_used_at FROM cooldowns 
                WHERE user_id = ? AND command = ?
            ''', (user_id, command))
            
            row = cursor.fetchone()
            if row:
                return datetime.fromisoformat(row['last_used_at'])
            return None
            
        except Exception as e:
            logger.error(f"Error getting command cooldown: {e}")
            return None

    def check_cooldown(self, user_id: str, command: str) -> Optional[datetime]:
        """Check if command is on cooldown - returns expiry time if on cooldown, None if available"""
        try:
            last_used = self.get_command_cooldown(user_id, command)
            if last_used:
                # Return the last used time - your utils.py will calculate if it's expired
                return last_used
            return None
            
        except Exception as e:
            logger.error(f"Error checking command cooldown: {e}")
            return None

    def set_command_cooldown(self, user_id: str, command: str, timestamp: datetime = None) -> bool:
        """Set the last used time for a command"""
        try:
            if timestamp is None:
                timestamp = datetime.utcnow()
                
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT OR REPLACE INTO cooldowns (user_id, command, last_used_at)
                VALUES (?, ?, ?)
            ''', (user_id, command, timestamp.isoformat()))
            
            conn.commit()
            self.upload_database()
            return True
            
        except Exception as e:
            logger.error(f"Error setting command cooldown: {e}")
            return False

    def update_cooldown(self, user_id: str, command: str, timestamp: datetime = None) -> bool:
        """Update cooldown - alias for set_command_cooldown for compatibility"""
        return self.set_command_cooldown(user_id, command, timestamp)

    def generate_card_selection(self, user_id: str, tech_level: int) -> bool:
        """Generate 5 random cards for a tech level"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Define card pool (expanded for variety)
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
            
            # Select 5 random cards
            available_cards = random.sample(card_pool, min(5, len(card_pool)))
            
            cursor.execute('''
                INSERT OR REPLACE INTO cards (user_id, tech_level, available_cards, status)
                VALUES (?, ?, ?, ?)
            ''', (user_id, tech_level, json.dumps(available_cards), 'pending'))
            
            conn.commit()
            self.upload_database()
            logger.info(f"Generated card selection for user {user_id} at tech level {tech_level}")
            return True
            
        except Exception as e:
            logger.error(f"Error generating card selection: {e}")
            return False

    def get_card_selection(self, user_id: str, tech_level: int) -> Optional[Dict]:
        """Get available cards for a tech level"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT * FROM cards 
                WHERE user_id = ? AND tech_level = ? AND status = 'pending'
            ''', (user_id, tech_level))
            
            row = cursor.fetchone()
            if row:
                card_data = dict(row)
                card_data['available_cards'] = json.loads(card_data['available_cards'])
                return card_data
            return None
            
        except Exception as e:
            logger.error(f"Error getting card selection for user {user_id}: {e}")
            return None

    def select_card(self, user_id: str, tech_level: int, card_name: str) -> Optional[Dict]:
        """Select a card and mark it as chosen"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            card_selection = self.get_card_selection(user_id, tech_level)
            if not card_selection:
                return None
                
            selected_card = next((card for card in card_selection['available_cards'] if card['name'].lower() == card_name.lower()), None)
            if not selected_card:
                return None
                
            cursor.execute('''
                UPDATE cards SET status = 'selected'
                WHERE user_id = ? AND tech_level = ?
            ''', (user_id, tech_level))
            
            conn.commit()
            self.upload_database()
            logger.info(f"User {user_id} selected card '{card_name}' at tech level {tech_level}")
            return selected_card
            
        except Exception as e:
            logger.error(f"Error selecting card for user {user_id}: {e}")
            return None

    def get_all_civilizations(self) -> List[Dict[str, Any]]:
        """Get all civilizations for leaderboards"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('SELECT * FROM civilizations ORDER BY last_active DESC')
            rows = cursor.fetchall()
            
            civilizations = []
            for row in rows:
                civ = dict(row)
                civ['resources'] = json.loads(civ['resources'])
                civ['population'] = json.loads(civ['population'])
                civ['military'] = json.loads(civ['military'])
                civ['territory'] = json.loads(civ['territory'])
                civ['hyper_items'] = json.loads(civ['hyper_items'])
                civ['bonuses'] = json.loads(civ['bonuses'])
                civ['selected_cards'] = json.loads(civ.get('selected_cards', '[]'))  # Safe fallback
                civilizations.append(civ)
            
            return civilizations
            
        except Exception as e:
            logger.error(f"Error getting all civilizations: {e}")
            return []
        
    def create_alliance(self, name: str, leader_id: str, description: str = "") -> bool:
        """Create a new alliance"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO alliances (name, leader_id, members, description)
                VALUES (?, ?, ?, ?)
            ''', (name, leader_id, json.dumps([leader_id]), description))
            
            conn.commit()
            self.upload_database()
            logger.info(f"Created alliance '{name}' led by {leader_id}")
            return True
            
        except sqlite3.IntegrityError:
            logger.warning(f"Alliance name '{name}' already exists")
            return False
        except Exception as e:
            logger.error(f"Error creating alliance: {e}")
            return False

    def log_event(self, user_id: str, event_type: str, title: str, description: str, effects: Dict = None):
        """Log an event to the database"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO events (user_id, event_type, title, description, effects)
                VALUES (?, ?, ?, ?, ?)
            ''', (user_id, event_type, title, description, json.dumps(effects or {})))
            
            conn.commit()
            self.upload_database()
            logger.debug(f"Logged event: {title} for user {user_id}")
            
        except Exception as e:
            logger.error(f"Error logging event: {e}")

    def get_recent_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get recent events for dashboard"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT e.*, c.name as civ_name 
                FROM events e 
                LEFT JOIN civilizations c ON e.user_id = c.user_id 
                ORDER BY e.timestamp DESC 
                LIMIT ?
            ''', (limit,))
            
            rows = cursor.fetchall()
            events = []
            for row in rows:
                event = dict(row)
                event['effects'] = json.loads(event['effects'])
                events.append(event)
            
            return events
            
        except Exception as e:
            logger.error(f"Error getting recent events: {e}")
            return []

    def create_trade_request(self, sender_id: str, recipient_id: str, offer: Dict, request: Dict) -> bool:
        """Create a new trade request"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO trade_requests (sender_id, recipient_id, offer, request)
                VALUES (?, ?, ?, ?)
            ''', (sender_id, recipient_id, json.dumps(offer), json.dumps(request)))
            conn.commit()
            self.upload_database()
            logger.info(f"Trade request created from {sender_id} to {recipient_id}")
            return True
        except Exception as e:
            logger.error(f"Error creating trade request: {e}")
            return False

    def get_trade_requests(self, user_id: str) -> List[Dict]:
        """Get all active trade requests for a user"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                SELECT t.*, c.name as sender_name 
                FROM trade_requests t
                JOIN civilizations c ON t.sender_id = c.user_id
                WHERE recipient_id = ? AND expires_at > CURRENT_TIMESTAMP
            ''', (user_id,))
            rows = cursor.fetchall()
            requests = []
            for row in rows:
                req = dict(row)
                req['offer'] = json.loads(req['offer'])
                req['request'] = json.loads(req['request'])
                requests.append(req)
            return requests
        except Exception as e:
            logger.error(f"Error getting trade requests: {e}")
            return []

    def get_trade_request_by_id(self, request_id: int) -> Optional[Dict]:
        """Get a specific trade request by ID"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM trade_requests 
                WHERE id = ? AND expires_at > CURRENT_TIMESTAMP
            ''', (request_id,))
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
        """Delete a trade request"""
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
        """Invite a user to an alliance"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO alliance_invitations (alliance_id, sender_id, recipient_id)
                VALUES (?, ?, ?)
            ''', (alliance_id, sender_id, recipient_id))
            conn.commit()
            self.upload_database()
            logger.info(f"Alliance invite created: alliance={alliance_id} to {recipient_id}")
            return True
        except Exception as e:
            logger.error(f"Error creating alliance invite: {e}")
            return False

    def get_alliance_invites(self, user_id: str) -> List[Dict]:
        """Get active alliance invites for a user"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
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
        """Get a specific alliance invite by ID"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
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
        """Delete an alliance invitation"""
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
        """Send a message between users"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO messages (sender_id, recipient_id, message)
                VALUES (?, ?, ?)
            ''', (sender_id, recipient_id, message))
            conn.commit()
            self.upload_database()
            logger.info(f"Message sent from {sender_id} to {recipient_id}")
            return True
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            return False

    def get_messages(self, user_id: str) -> List[Dict]:
        """Get active messages for a user"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
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
        """Delete a message"""
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
        """Get alliance data by ID"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
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
        """Get alliance data by name"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
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
        """Add member to alliance"""
        try:
            alliance = self.get_alliance(alliance_id)
            if not alliance:
                return False
            
            if user_id in alliance['members']:
                return True
            
            members = alliance['members'] + [user_id]
            join_requests = [uid for uid in alliance['join_requests'] if uid != user_id]
            
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE alliances 
                SET members = ?, join_requests = ?
                WHERE id = ?
            ''', (json.dumps(members), json.dumps(join_requests), alliance_id))
            
            conn.commit()
            self.upload_database()
            return True
        except Exception as e:
            logger.error(f"Error adding alliance member: {e}")
            return False

    def get_wars(self, user_id: str = None, status: str = 'ongoing') -> List[Dict]:
        """Get wars involving a user or all wars"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            if user_id:
                cursor.execute('''
                    SELECT w.*, 
                           ac.name as attacker_name, 
                           dc.name as defender_name
                    FROM wars w
                    JOIN civilizations ac ON w.attacker_id = ac.user_id
                    JOIN civilizations dc ON w.defender_id = dc.user_id
                    WHERE (w.attacker_id = ? OR w.defender_id = ?) AND w.result = ?
                ''', (user_id, user_id, status))
            else:
                cursor.execute('''
                    SELECT w.*, 
                           ac.name as attacker_name, 
                           dc.name as defender_name
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
        """Get peace offers for a user or all peace offers"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            if user_id:
                cursor.execute('''
                    SELECT po.*, 
                           oc.name as offerer_name, 
                           rc.name as receiver_name
                    FROM peace_offers po
                    JOIN civilizations oc ON po.offerer_id = oc.user_id
                    JOIN civilizations rc ON po.receiver_id = rc.user_id
                    WHERE (po.offerer_id = ? OR po.receiver_id = ?) AND po.status = 'pending'
                ''', (user_id, user_id))
            else:
                cursor.execute('''
                    SELECT po.*, 
                           oc.name as offerer_name, 
                           rc.name as receiver_name
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
        """Create a peace offer"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO peace_offers (offerer_id, receiver_id)
                VALUES (?, ?)
            ''', (offerer_id, receiver_id))
            conn.commit()
            self.upload_database()
            logger.info(f"Peace offer created from {offerer_id} to {receiver_id}")
            return True
        except Exception as e:
            logger.error(f"Error creating peace offer: {e}")
            return False

    def update_peace_offer(self, offer_id: int, status: str) -> bool:
        """Update peace offer status"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE peace_offers 
                SET status = ?, responded_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (status, offer_id))
            conn.commit()
            self.upload_database()
            return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Error updating peace offer: {e}")
            return False

    def end_war(self, attacker_id: str, defender_id: str, result: str) -> bool:
        """End a war between two civilizations"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE wars 
                SET result = ?, ended_at = CURRENT_TIMESTAMP
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
        """Get comprehensive statistics for a user"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Get basic civilization data
            civ = self.get_civilization(user_id)
            if not civ:
                return {}
            
            # Count wars participated in
            cursor.execute('''
                SELECT 
                    COUNT(*) as total_wars,
                    SUM(CASE WHEN result = 'victory' THEN 1 ELSE 0 END) as victories,
                    SUM(CASE WHEN result = 'defeat' THEN 1 ELSE 0 END) as defeats,
                    SUM(CASE WHEN result = 'peace' THEN 1 ELSE 0 END) as peace_treaties
                FROM wars 
                WHERE attacker_id = ? OR defender_id = ?
            ''', (user_id, user_id))
            
            war_stats_row = cursor.fetchone()
            war_stats = dict(war_stats_row) if war_stats_row else {
                'total_wars': 0, 'victories': 0, 'defeats': 0, 'peace_treaties': 0
            }
            
            # Get recent events
            cursor.execute('''
                SELECT COUNT(*) as total_events
                FROM events 
                WHERE user_id = ?
            ''', (user_id,))
            
            event_count_row = cursor.fetchone()
            event_count = event_count_row[0] if event_count_row else 0
            
            # Calculate power score
            military_power = (civ['military']['soldiers'] * 10 + 
                            civ['military']['spies'] * 5 + 
                            civ['military']['tech_level'] * 50)
            
            economic_power = sum(civ['resources'].values())
            territorial_power = civ['territory']['land_size']
            
            total_power = military_power + economic_power + territorial_power
            
            return {
                'civilization': civ,
                'war_statistics': war_stats,
                'total_events': event_count,
                'power_scores': {
                    'military': military_power,
                    'economic': economic_power,
                    'territorial': territorial_power,
                    'total': total_power
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting user statistics: {e}")
            return {}

    def get_leaderboard(self, category: str = 'power', limit: int = 10) -> List[Dict]:
        """Get leaderboard for different categories"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            if category == 'power':
                cursor.execute('''
                    SELECT user_id, name, resources, military, territory
                    FROM civilizations
                    ORDER BY last_active DESC
                ''')
                
                civs = []
                for row in cursor.fetchall():
                    civ = dict(row)
                    resources = json.loads(civ['resources'])
                    military = json.loads(civ['military'])
                    territory = json.loads(civ['territory'])
                    
                    military_power = (military['soldiers'] * 10 + 
                                    military['spies'] * 5 + 
                                    military['tech_level'] * 50)
                    economic_power = sum(resources.values())
                    territorial_power = territory['land_size']
                    total_power = military_power + economic_power + territorial_power
                    
                    civs.append({
                        'user_id': civ['user_id'],
                        'name': civ['name'],
                        'score': total_power,
                        'military_power': military_power,
                        'economic_power': economic_power,
                        'territorial_power': territorial_power
                    })
                
                return sorted(civs, key=lambda x: x['score'], reverse=True)[:limit]
                
            elif category == 'gold':
                cursor.execute('''
                    SELECT user_id, name, resources
                    FROM civilizations
                    ORDER BY json_extract(resources, '$.gold') DESC
                    LIMIT ?
                ''', (limit,))
                
                civs = []
                for row in cursor.fetchall():
                    civ = dict(row)
                    resources = json.loads(civ['resources'])
                    civs.append({
                        'user_id': civ['user_id'],
                        'name': civ['name'],
                        'score': resources['gold']
                    })
                return civs
                
            elif category == 'military':
                cursor.execute('''
                    SELECT user_id, name, military
                    FROM civilizations
                    ORDER BY (json_extract(military, '$.soldiers') + json_extract(military, '$.spies')) DESC
                    LIMIT ?
                ''', (limit,))
                
                civs = []
                for row in cursor.fetchall():
                    civ = dict(row)
                    military = json.loads(civ['military'])
                    total_units = military['soldiers'] + military['spies']
                    civs.append({
                        'user_id': civ['user_id'],
                        'name': civ['name'],
                        'score': total_units,
                        'soldiers': military['soldiers'],
                        'spies': military['spies']
                    })
                return civs
                
            elif category == 'territory':
                cursor.execute('''
                    SELECT user_id, name, territory
                    FROM civilizations
                    ORDER BY json_extract(territory, '$.land_size') DESC
                    LIMIT ?
                ''', (limit,))
                
                civs = []
                for row in cursor.fetchall():
                    civ = dict(row)
                    territory = json.loads(civ['territory'])
                    civs.append({
                        'user_id': civ['user_id'],
                        'name': civ['name'],
                        'score': territory['land_size']
                    })
                return civs
                
            return []
            
        except Exception as e:
            logger.error(f"Error getting leaderboard: {e}")
            return []

    def cleanup_expired_requests(self):
        """Automatically remove expired requests (runs daily)"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('DELETE FROM trade_requests WHERE expires_at <= CURRENT_TIMESTAMP')
            trade_count = cursor.rowcount
            
            cursor.execute('DELETE FROM alliance_invitations WHERE expires_at <= CURRENT_TIMESTAMP')
            invite_count = cursor.rowcount
            
            cursor.execute('DELETE FROM messages WHERE expires_at <= CURRENT_TIMESTAMP')
            message_count = cursor.rowcount
            
            conn.commit()
            self.upload_database()
            logger.info(f"Cleaned up expired requests: "
                       f"{trade_count} trades, {invite_count} invites, "
                       f"{message_count} messages removed")
            return True
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
            return False

    def backup_database(self, backup_path: str = None) -> bool:
        """Create a backup of the database"""
        try:
            if not backup_path:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                backup_path = f"nationbot_backup_{timestamp}.db"
            
            import shutil
            shutil.copy2(self.db_path, backup_path)
            logger.info(f"Database backed up locally to {backup_path}")
            
            if self.dropbox_client:
                dropbox_path = f"/backups/{os.path.basename(backup_path)}"
                with open(backup_path, 'rb') as f:
                    self.dropbox_client.files_upload(
                        f.read(),
                        dropbox_path,
                        mode=dropbox.files.WriteMode('add')
                    )
                logger.info(f"Database backed up to Dropbox: {dropbox_path}")
            return True
        except Exception as e:
            logger.error(f"Error backing up database: {e}")
            return False

    def get_database_info(self) -> Dict[str, Any]:
        """Get database information and statistics"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            info = {}
            
            # Count records in each table
            tables = [
                'civilizations', 'wars', 'peace_offers', 'alliances', 
                'events', 'trade_requests', 'messages', 'cards', 
                'cooldowns', 'alliance_invitations'
            ]
            
            for table in tables:
                cursor.execute(f'SELECT COUNT(*) FROM {table}')
                info[f'{table}_count'] = cursor.fetchone()[0]
            
            # Get database file size
            import os
            if os.path.exists(self.db_path):
                info['database_size_bytes'] = os.path.getsize(self.db_path)
                info['database_size_mb'] = round(info['database_size_bytes'] / (1024 * 1024), 2)
            
            # Get active users (logged in within last 7 days)
            cursor.execute('''
                SELECT COUNT(*) FROM civilizations 
                WHERE last_active > datetime('now', '-7 days')
            ''')
            info['active_users_week'] = cursor.fetchone()[0]
            
            return info
            
        except Exception as e:
            logger.error(f"Error getting database info: {e}")
            return {}

    def close_connections(self):
        """Close all database connections (for shutdown)"""
        if hasattr(self.local, 'connection'):
            self.local.connection.close()
            del self.local.connection

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
