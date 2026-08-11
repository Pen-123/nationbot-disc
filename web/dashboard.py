"""
Flask dashboard for WarBot.

This file serves the web dashboard and an on-demand /map.png endpoint that
renders the world ownership map using the same regions.geojson and territories
data the bot uses for its .map command.

Paste this file to web/dashboard.py and restart the dashboard server.
"""
from flask import Flask, render_template, jsonify, send_file, make_response
import json
import os
import sys
import logging
import hashlib
from io import BytesIO
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Matplotlib headless backend + geo stack
import matplotlib
matplotlib.use('Agg')
import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

# Add the parent directory to the path so we can import bot modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bot.database import Database
from bot.civilization import CivilizationManager
from bot.utils import format_number, get_civilization_rank, get_happiness_status

load_dotenv()

app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv('FLASK_SECRET_KEY', 'warbot-dashboard-secret-key')

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Globals for DB / civ manager (lazy init)
db = None
civ_manager = None


def get_db_path() -> str:
    return os.getenv("DATABASE_PATH", "warbot.db")


def initialize_services():
    """Lazy initialization of services to improve startup time."""
    global db, civ_manager
    try:
        if db is None:
            db = Database(db_path=get_db_path())
            logger.info("Database connection established")

        if civ_manager is None:
            civ_manager = CivilizationManager(db)
            logger.info("Civilization manager initialized")

        return db, civ_manager
    except Exception as e:
        logger.error(f"Error initializing services: {e}")
        return None, None


# Simple in-memory cache for generated map images to reduce CPU usage
_MAP_CACHE = {"key": None, "buf": None, "ts": None}
CACHE_TTL = int(os.getenv("MAP_CACHE_TTL", "15"))  # seconds


@app.route('/')
def dashboard():
    """Main dashboard page"""
    try:
        initialize_services()

        # Check if services are properly initialized
        if db is None or civ_manager is None:
            return render_template('index.html',
                                   stats=get_empty_stats(),
                                   top_civs=[],
                                   recent_events=[],
                                   alliances=[],
                                   error="Database connection failed")

        # Get statistics
        stats = get_dashboard_stats()

        # Get top civilizations
        top_civs = get_top_civilizations(10)

        # Get recent events
        recent_events = get_recent_events(20)

        # Get alliance information
        alliances = get_alliance_info()

        # Render template (index.html should include an <img src="/map.png">)
        return render_template('index.html',
                               stats=stats,
                               top_civs=top_civs,
                               recent_events=recent_events,
                               alliances=alliances)
    except Exception as e:
        logger.exception(f"Error loading dashboard: {e}")
        return render_template('index.html',
                               stats=get_empty_stats(),
                               top_civs=[],
                               recent_events=[],
                               alliances=[],
                               error="Dashboard temporarily unavailable")


@app.route('/api/stats')
def api_stats():
    """API endpoint for dashboard statistics"""
    try:
        initialize_services()
        if db is None or civ_manager is None:
            return jsonify(get_empty_stats())
        return jsonify(get_dashboard_stats())
    except Exception as e:
        logger.exception(f"Error getting stats: {e}")
        return jsonify(get_empty_stats())


@app.route('/api/civilizations')
def api_civilizations():
    """API endpoint for civilization data"""
    try:
        initialize_services()
        if db is None or civ_manager is None:
            return jsonify([])
        civs = get_top_civilizations(50)
        return jsonify(civs)
    except Exception as e:
        logger.exception(f"Error getting civilizations: {e}")
        return jsonify([])


@app.route('/api/events')
def api_events():
    """API endpoint for recent events"""
    try:
        initialize_services()
        if db is None or civ_manager is None:
            return jsonify([])
        events = get_recent_events(100)
        return jsonify(events)
    except Exception as e:
        logger.exception(f"Error getting events: {e}")
        return jsonify([])


@app.route('/map.png')
def map_png():
    """
    Generate and return the current world map PNG (shows ownership).
    Uses a short-lived in-memory cache keyed by ownership snapshot to avoid
    regenerating on every request.
    """
    try:
        initialize_services()
        if db is None or civ_manager is None:
            fig, ax = plt.subplots(figsize=(10, 6))
            ax.text(0.5, 0.5, "Map unavailable\nServices not ready", ha='center', va='center', fontsize=14)
            ax.set_axis_off()
            buf = BytesIO()
            plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
            plt.close(fig)
            buf.seek(0)
            return send_file(buf, mimetype='image/png')

        geo_path = os.path.join(os.getcwd(), "regions.geojson")
        if not os.path.exists(geo_path):
            fig, ax = plt.subplots(figsize=(10, 6))
            ax.text(0.5, 0.5, "Map data not available\nRun generate_geojson.py", ha='center', va='center', fontsize=14)
            ax.set_axis_off()
            buf = BytesIO()
            plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
            plt.close(fig)
            buf.seek(0)
            return send_file(buf, mimetype='image/png')

        # Read ownership from DB
        conn = db.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT user_id, owned_provinces FROM territories")
        rows = cursor.fetchall()

        ownership = {}
        for row in rows:
            user_id = row[0]
            provinces = json.loads(row[1]) if row[1] else []
            civ = civ_manager.get_civilization(user_id)
            name = civ['name'] if civ else user_id[:6]
            ownership[user_id] = {"provinces": provinces, "name": name}

        # Build deterministic key for cache
        ownership_key = hashlib.md5(json.dumps(ownership, sort_keys=True).encode()).hexdigest()
        now_ts = datetime.utcnow().timestamp()

        # Serve cached buffer if key matches and not expired
        cached = _MAP_CACHE
        if cached["key"] == ownership_key and cached["buf"] is not None and cached["ts"] and (now_ts - cached["ts"] < CACHE_TTL):
            cached["buf"].seek(0)
            response = make_response(send_file(cached["buf"], mimetype='image/png'))
            response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
            return response

        # Load GeoDataFrame
        try:
            gdf = gpd.read_file(geo_path)
            if gdf.crs is None:
                gdf = gdf.set_crs('EPSG:4326', allow_override=True)
            elif gdf.crs != 'EPSG:4326':
                gdf = gdf.to_crs('EPSG:4326')
        except Exception as e:
            logger.exception(f"Failed to read geojson {geo_path}: {e}")
            fig, ax = plt.subplots(figsize=(10, 6))
            ax.text(0.5, 0.5, "Failed to read map data", ha='center', va='center', fontsize=14)
            ax.set_axis_off()
            buf = BytesIO()
            plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
            plt.close(fig)
            buf.seek(0)
            return send_file(buf, mimetype='image/png')

        # Prepare colors and draw map
        colors = plt.cm.tab20.colors
        user_colors = {}
        for i, (user_id, info) in enumerate(ownership.items()):
            user_colors[user_id] = colors[i % len(colors)]

        fig, ax = plt.subplots(figsize=(15, 10))
        for idx, row in gdf.iterrows():
            country_name = row.get('NAME', 'Unknown')
            owner = None
            # Find owner by matching province names (loose matching to mirror .map behavior)
            for user_id, info in ownership.items():
                for province in info["provinces"]:
                    if not province:
                        continue
                    pn = province.lower()
                    cn = str(country_name).lower()
                    if cn == pn or pn in cn or cn in pn:
                        owner = user_id
                        break
                if owner:
                    break

            color = user_colors.get(owner, (0.85, 0.85, 0.85, 1))
            # Plot the single row geometry
            try:
                gpd.GeoDataFrame([row], crs=gdf.crs).plot(ax=ax, facecolor=color, edgecolor='white', linewidth=0.4)
            except Exception:
                # On any plotting error, skip this geometry to keep map generation robust
                continue

        # Legend
        patches = []
        for user_id, color in user_colors.items():
            patches.append(mpatches.Patch(color=color, label=ownership[user_id]["name"]))
        if patches:
            ax.legend(handles=patches, loc='lower left', fontsize=8)

        ax.set_title("World Map of Civilizations", fontsize=14)
        ax.set_axis_off()

        buf = BytesIO()
        plt.savefig(buf, format='png', dpi=150, bbox_inches='tight')
        plt.close(fig)
        buf.seek(0)

        # Cache the image
        cache_buf = BytesIO(buf.getvalue())
        _MAP_CACHE["key"] = ownership_key
        _MAP_CACHE["buf"] = cache_buf
        _MAP_CACHE["ts"] = now_ts

        cache_buf.seek(0)
        response = make_response(send_file(cache_buf, mimetype='image/png'))
        response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
        return response

    except Exception as e:
        logger.exception(f"Failed to generate map.png: {e}")
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.text(0.5, 0.5, "Error generating map", ha='center', va='center', fontsize=14)
        ax.set_axis_off()
        buf = BytesIO()
        plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
        plt.close(fig)
        buf.seek(0)
        return send_file(buf, mimetype='image/png')


@app.route('/api/leaderboard/<category>')
def api_leaderboard(category):
    """API endpoint for specific leaderboards"""
    try:
        valid_categories = ['power', 'population', 'military', 'resources', 'happiness']

        if category not in valid_categories:
            return jsonify({"error": "Invalid category"}), 400

        initialize_services()
        if db is None or civ_manager is None:
            return jsonify([])

        leaderboard = get_leaderboard_by_category(category, 20)
        return jsonify(leaderboard)
    except Exception as e:
        logger.exception(f"Error getting leaderboard for {category}: {e}")
        return jsonify([])


@app.route('/health')
def health_check():
    """Health check endpoint for deployment monitoring"""
    initialize_services()
    db_status = "connected" if db is not None else "disconnected"
    civ_manager_status = "initialized" if civ_manager is not None else "uninitialized"

    return jsonify({
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "service": "warbot-dashboard",
        "database": db_status,
        "civilization_manager": civ_manager_status
    }), 200


def get_dashboard_stats():
    """Get overall dashboard statistics"""
    try:
        if db is None:
            initialize_services()
        civilizations = db.get_all_civilizations()

        if not civilizations:
            logger.info("No civilizations found in database - returning empty stats")
            return get_empty_stats()

        # Calculate totals
        total_population = sum(civ['population']['citizens'] for civ in civilizations)
        total_resources = sum(
            sum(civ['resources'].values()) for civ in civilizations
        )

        # Get active wars
        conn = db.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM wars WHERE result = 'ongoing'")
        result = cursor.fetchone()
        active_wars = result[0] if result else 0

        # Get total alliances
        cursor.execute("SELECT COUNT(*) FROM alliances")
        result = cursor.fetchone()
        total_alliances = result[0] if result else 0

        # Get recent events count (last 24 hours)
        yesterday = datetime.now() - timedelta(days=1)
        cursor.execute("SELECT COUNT(*) FROM events WHERE timestamp > ?", (yesterday,))
        result = cursor.fetchone()
        recent_events = result[0] if result else 0

        # Calculate average happiness
        avg_happiness = sum(civ['population']['happiness'] for civ in civilizations) / len(civilizations)

        # Get ideology distribution
        ideology_count = {}
        for civ in civilizations:
            ideology = civ.get('ideology', 'None')
            if ideology is None:
                ideology = "None"
            ideology_count[ideology] = ideology_count.get(ideology, 0) + 1

        return {
            "total_civilizations": len(civilizations),
            "total_population": total_population,
            "total_resources": total_resources,
            "active_wars": active_wars,
            "total_alliances": total_alliances,
            "recent_events": recent_events,
            "average_happiness": round(avg_happiness, 1),
            "ideology_distribution": ideology_count
        }

    except Exception as e:
        logger.exception(f"Error calculating dashboard stats: {e}")
        return get_empty_stats()


def get_top_civilizations(limit=10):
    """Get top civilizations by power score"""
    try:
        if db is None:
            initialize_services()
        civilizations = db.get_all_civilizations()

        if not civilizations:
            logger.info("No civilizations found for leaderboard")
            return []

        # Calculate power scores and sort
        civ_scores = []
        for civ in civilizations:
            power_score = civ_manager.get_civilization_power(civ['user_id'])
            rank, rank_emoji = get_civilization_rank(power_score)
            happiness_status, happiness_emoji = get_happiness_status(civ['population']['happiness'])

            ideology = civ.get('ideology', 'None')
            if ideology is None:
                ideology = "None"

            civ_scores.append({
                "name": civ['name'],
                "user_id": civ['user_id'],
                "power_score": power_score,
                "rank": rank,
                "rank_emoji": rank_emoji,
                "ideology": ideology,
                "population": civ['population']['citizens'],
                "happiness": civ['population']['happiness'],
                "happiness_status": happiness_status,
                "happiness_emoji": happiness_emoji,
                "resources": civ['resources'],
                "military": civ['military'],
                "territory": civ['territory']['land_size'],
                "last_active": civ['last_active'],
                "hyper_items": len(civ.get('hyper_items', []))
            })

        civ_scores.sort(key=lambda x: x['power_score'], reverse=True)
        return civ_scores[:limit]

    except Exception as e:
        logger.exception(f"Error getting top civilizations: {e}")
        return []


def get_recent_events(limit=20):
    """Get recent events with formatting"""
    try:
        if db is None:
            initialize_services()
        events = db.get_recent_events(limit)

        if not events:
            logger.info("No events found in database")
            return []

        formatted_events = []
        for event in events:
            timestamp = parse_timestamp(event.get('timestamp'))
            time_ago = get_time_ago(timestamp)
            event_icon = get_event_icon(event['event_type'])

            formatted_events.append({
                "title": event['title'],
                "description": event['description'],
                "event_type": event['event_type'],
                "event_icon": event_icon,
                "civilization": event.get('civ_name', 'Global'),
                "timestamp": event['timestamp'],
                "time_ago": time_ago,
                "effects": event['effects']
            })

        return formatted_events

    except Exception as e:
        logger.exception(f"Error getting recent events: {e}")
        return []


def parse_timestamp(value):
    """Parse sqlite/iso timestamps safely."""
    if isinstance(value, datetime):
        return value
    if not value:
        return datetime.now()

    text = str(value).strip()
    if " " in text and "T" not in text:
        text = text.replace(" ", "T", 1)

    try:
        return datetime.fromisoformat(text)
    except ValueError:
        logger.warning(f"Unexpected timestamp format: {value!r}")
        return datetime.now()


def get_alliance_info():
    """Get alliance information"""
    try:
        if db is None:
            initialize_services()
        conn = db.get_connection()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT a.*, COUNT(w.id) as active_wars 
            FROM alliances a 
            LEFT JOIN wars w ON (
                a.members LIKE '%' || w.attacker_id || '%' OR 
                a.members LIKE '%' || w.defender_id || '%'
            ) AND w.result = 'ongoing'
            GROUP BY a.id 
            ORDER BY a.created_at DESC
        ''')

        rows = cursor.fetchall()
        if not rows:
            logger.info("No alliances found in database")
            return []

        alliances = []
        for row in rows:
            alliance = dict(row)
            members = json.loads(alliance['members'])

            member_names = []
            for member_id in members:
                civ = civ_manager.get_civilization(member_id)
                if civ:
                    member_names.append(civ['name'])

            alliances.append({
                "name": alliance['name'],
                "leader_id": alliance['leader_id'],
                "member_count": len(members),
                "member_names": member_names,
                "created_at": alliance['created_at'],
                "active_wars": alliance['active_wars']
            })

        return alliances

    except Exception as e:
        logger.exception(f"Error getting alliance info: {e}")
        return []


def get_leaderboard_by_category(category, limit=20):
    """Get leaderboard for specific category"""
    try:
        if db is None:
            initialize_services()
        civilizations = db.get_all_civilizations()

        if not civilizations:
            logger.info("No civilizations found for leaderboard")
            return []

        leaderboard = []
        for civ in civilizations:
            ideology = civ.get('ideology', 'None')
            if ideology is None:
                ideology = "None"

            entry = {
                "name": civ['name'],
                "user_id": civ['user_id'],
                "ideology": ideology
            }

            if category == 'power':
                entry['value'] = civ_manager.get_civilization_power(civ['user_id'])
                entry['display'] = format_number(entry['value'])
            elif category == 'population':
                entry['value'] = civ['population']['citizens']
                entry['display'] = format_number(entry['value'])
            elif category == 'military':
                entry['value'] = civ['military']['soldiers'] + civ['military']['spies']
                entry['display'] = format_number(entry['value'])
            elif category == 'resources':
                entry['value'] = sum(civ['resources'].values())
                entry['display'] = format_number(entry['value'])
            elif category == 'happiness':
                entry['value'] = civ['population']['happiness']
                entry['display'] = f"{entry['value']}%"

            leaderboard.append(entry)

        leaderboard.sort(key=lambda x: x['value'], reverse=True)
        return leaderboard[:limit]

    except Exception as e:
        logger.exception(f"Error getting {category} leaderboard: {e}")
        return []


def get_time_ago(timestamp):
    """Get human-readable time ago string"""
    now = datetime.now()
    if isinstance(timestamp, str):
        timestamp = datetime.fromisoformat(timestamp)

    diff = now - timestamp

    if diff.days > 0:
        return f"{diff.days} day{'s' if diff.days > 1 else ''} ago"
    elif diff.seconds >= 3600:
        hours = diff.seconds // 3600
        return f"{hours} hour{'s' if hours > 1 else ''} ago"
    elif diff.seconds >= 60:
        minutes = diff.seconds // 60
        return f"{minutes} minute{'s' if minutes > 1 else ''} ago"
    else:
        return "Just now"


def get_event_icon(event_type):
    """Get appropriate icon for event type"""
    event_icons = {
        "war_declaration": "⚔️",
        "victory": "🏆",
        "defeat": "💔",
        "alliance": "🤝",
        "trade": "💰",
        "nuclear_attack": "☢️",
        "random_event": "🎲",
        "global_event": "🌍",
        "store_purchase": "🏪",
        "black_market": "🕴️",
        "diplomacy": "📜",
        "resource_transfer": "📦",
        "obliteration": "💥",
        "siege": "🏰",
        "espionage": "🕵️"
    }

    return event_icons.get(event_type, "📰")


def get_empty_stats():
    """Return empty statistics for when no data is available"""
    return {
        "total_civilizations": 0,
        "total_population": 0,
        "total_resources": 0,
        "active_wars": 0,
        "total_alliances": 0,
        "recent_events": 0,
        "average_happiness": 0,
        "ideology_distribution": {}
    }


# Error handlers
@app.errorhandler(404)
def not_found(error):
    return render_template('index.html',
                           stats=get_empty_stats(),
                           top_civs=[],
                           recent_events=[],
                           alliances=[],
                           error="Page not found"), 404


@app.errorhandler(500)
def internal_error(error):
    return render_template('index.html',
                           stats=get_empty_stats(),
                           top_civs=[],
                           recent_events=[],
                           alliances=[],
                           error="Internal server error"), 500


if __name__ == '__main__':
    # Get port from environment variable or default to 5000
    port = int(os.environ.get('PORT', 5000))
    logger.info(f"Starting server on port {port}")
    app.run(host='0.0.0.0', port=port, debug=False)