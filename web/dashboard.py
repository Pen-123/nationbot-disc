from flask import Flask, render_template, jsonify, send_file, make_response
import json
import os
import sys
import logging
import hashlib
from io import BytesIO
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from pathlib import Path

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

        # locate regions.geojson robustly
        project_root = Path(__file__).resolve().parent.parent
        geo_candidates = [
            project_root / "regions.geojson",
            Path.cwd() / "regions.geojson",
            project_root / "data" / "regions.geojson",
        ]
        geo_path = None
        for p in geo_candidates:
            if p.exists():
                geo_path = str(p)
                break

        if not geo_path:
            fig, ax = plt.subplots(figsize=(10, 6))
            ax.text(0.5, 0.5, "Map data not available\nRun generate_geojson.py", ha='center', va='center', fontsize=14)
            ax.set_axis_off()
            buf = BytesIO()
            plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
            plt.close(fig)
            buf.seek(0)
            return send_file(buf, mimetype='image/png')

        # Read ownership from Firestore using db.get_all_territories()
        territories = db.get_all_territories()  # dict {province_name: {owner_id: ...}}
        ownership = {}
        # We'll build ownership dict: user_id -> {"provinces": list, "name": civ_name}
        for province_name, data in territories.items():
            owner_id = data.get("owner_id")
            if owner_id:
                if owner_id not in ownership:
                    civ = civ_manager.get_civilization(owner_id)
                    name = civ['name'] if civ else owner_id[:6]
                    ownership[owner_id] = {"provinces": [], "name": name}
                ownership[owner_id]["provinces"].append(province_name)

        # Build deterministic key for cache
        ownership_key = hashlib.md5(json.dumps(ownership, sort_keys=True).encode()).hexdigest()
        now_ts = datetime.utcnow().timestamp()

        # Serve cached buffer if key matches and not expired
        cached = _MAP_CACHE
        if cached["key"] == ownership_key and cached["buf"] is not None and cached["ts"] and (now_ts - cached["ts"] < CACHE_TTL):
            try:
                cached["buf"].seek(0)
                response = make_response(send_file(cached["buf"], mimetype='image/png'))
                response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
                return response
            except Exception:
                # If cache is corrupted, ignore and regenerate
                cached["key"] = None
                cached["buf"] = None
                cached["ts"] = None

        # Load GeoDataFrame
        try:
            gdf = gpd.read_file(geo_path)
            # Normalize CRS handling robustly
            try:
                current_crs = gdf.crs.to_string() if gdf.crs is not None else None
            except Exception:
                current_crs = str(gdf.crs) if gdf.crs is not None else None

            if current_crs is None:
                gdf = gdf.set_crs('EPSG:4326', allow_override=True)
            elif '4326' not in current_crs:
                try:
                    gdf = gdf.to_crs('EPSG:4326')
                except Exception:
                    # fallback: try a lenient set_crs
                    gdf = gdf.set_crs('EPSG:4326', allow_override=True)

            # Ensure valid geometries
            if 'geometry' in gdf.columns:
                try:
                    gdf['geometry'] = gdf['geometry'].buffer(0)
                except Exception:
                    # ignore geometry fixes if they fail
                    pass
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
            # skip rows with invalid geometry
            try:
                geom = getattr(row, 'geometry', None)
                if geom is None:
                    continue
            except Exception:
                continue

            country_name = None
            for k in ('NAME', 'ADMIN', 'NAME_LONG', 'name'):
                if k in row and row[k] is not None:
                    country_name = row[k]
                    break
            country_name = country_name or 'Unknown'

            owner = None
            # Find owner by matching province names (loose matching to mirror .map behavior)
            for user_id, info in ownership.items():
                for province in info.get("provinces", []) or []:
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
            try:
                patches.append(mpatches.Patch(color=color, label=ownership[user_id]["name"]))
            except Exception:
                continue
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
    """Get overall dashboard statistics using Firestore."""
    try:
        if db is None:
            initialize_services()
        civilizations = db.get_all_civilizations()

        if not civilizations:
            logger.info("No civilizations found in database - returning empty stats")
            return get_empty_stats()

        # Calculate totals
        total_population = sum(civ['population']['citizens'] for civ in civilizations if 'population' in civ)
        total_resources = sum(
            sum(civ['resources'].values()) for civ in civilizations if 'resources' in civ
        )

        # Get active wars from Firestore
        wars = db.get_wars(status="ongoing")
        active_wars = len(wars)

        # Get total alliances
        alliances_ref = db.client.collection("alliances")
        docs = alliances_ref.stream()
        total_alliances = sum(1 for _ in docs)

        # Get recent events count (last 24 hours)
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        events = db.get_recent_events(limit=1000)  # get many and filter by timestamp
        recent_events = 0
        for ev in events:
            ts = ev.get('timestamp')
            if ts:
                try:
                    if isinstance(ts, str):
                        ts = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                    if ts.tzinfo is None:
                        ts = ts.replace(tzinfo=timezone.utc)
                    if ts >= yesterday:
                        recent_events += 1
                except:
                    pass

        # Calculate average happiness
        happiness_values = [civ['population']['happiness'] for civ in civilizations if 'population' in civ and 'happiness' in civ['population']]
        avg_happiness = sum(happiness_values) / len(happiness_values) if happiness_values else 0

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
                "last_active": civ.get('last_active', ''),
                "hyper_items": len(civ.get('hyper_items', []))
            })

        civ_scores.sort(key=lambda x: x['power_score'], reverse=True)
        return civ_scores[:limit]

    except Exception as e:
        logger.exception(f"Error getting top civilizations: {e}")
        return []


def get_recent_events(limit=20):
    """Get recent events with formatting using Firestore"""
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
                "effects": event.get('effects', {})
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
    """Get alliance information using Firestore"""
    try:
        if db is None:
            initialize_services()
        alliances_ref = db.client.collection("alliances")
        docs = alliances_ref.stream()
        alliances = []

        for doc in docs:
            data = doc.to_dict()
            members = data.get("members", [])
            member_names = []
            for member_id in members:
                civ = civ_manager.get_civilization(member_id)
                if civ:
                    member_names.append(civ['name'])

            # Count active wars for this alliance (any member in war)
            active_wars = 0
            wars = db.get_wars(status="ongoing")
            for war in wars:
                a = war.get("attacker_id")
                d = war.get("defender_id")
                if a in members or d in members:
                    active_wars += 1

            alliances.append({
                "name": data.get('name', 'Unknown'),
                "leader_id": data.get('leader_id', ''),
                "member_count": len(members),
                "member_names": member_names,
                "created_at": data.get('created_at', ''),
                "active_wars": active_wars
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
        try:
            timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        except:
            return "Unknown"

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
