# WarBot - Guilded Civilization Management Bot

## Overview

WarBot is a comprehensive Guilded bot designed for managing virtual civilizations in a multiplayer strategy game environment. Players can create civilizations, manage resources, build military forces, engage in diplomacy, and participate in economic activities. The bot features a rich command system with cooldowns, a web dashboard for statistics tracking, and dynamic events that affect gameplay. It includes unique mechanics like HyperItems for special abilities, ideology systems that provide different bonuses and penalties, and both local and global events that create an engaging strategic experience.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### Bot Framework Architecture
The application is built on the Guilded bot framework using Python's `guilded.py` library with a command-based architecture. The main bot class (`WarBot`) inherits from `commands.Bot` and uses a cog system to organize different command categories (Economy, Military, Diplomacy, Store, HyperItems). This modular approach allows for clean separation of concerns and easy maintenance of different game systems.

### Database Layer
The system uses SQLite as the primary database with a custom `Database` class that provides thread-safe connections using thread-local storage. The database schema includes tables for civilizations (storing user data, resources, military, territory), cooldowns (command rate limiting), alliances (diplomatic relationships), wars (conflict tracking), and events (game history). This design ensures data consistency while supporting concurrent access from multiple users.

### Game Logic Management
The `CivilizationManager` class serves as the core game logic controller, handling civilization creation, resource management, and ideology system calculations. It implements five different ideologies (Fascism, Democracy, Communism, Theocracy, Anarchy) each with unique modifiers that affect various game mechanics. The manager calculates complex interactions between different game systems and applies appropriate bonuses/penalties based on player choices.

### Command System with Cooldowns
All major game commands implement a cooldown system through a decorator pattern (`check_cooldown_decorator`) that prevents spam and maintains game balance. The cooldown system is database-backed and persists across bot restarts. Commands are organized into logical categories (economy, military, diplomacy, store, hyperitems) with each cog handling related functionality and maintaining its own command logic.

### Event System
The `EventManager` implements both local and global events that can affect individual civilizations or all players simultaneously. Events are probability-based and can trigger automatically or through specific game actions. This system adds unpredictability and strategic depth to the game, with events ranging from beneficial (Divine Blessing) to catastrophic (Nuclear Winter).

### Web Dashboard Integration
A Flask-based web dashboard provides real-time statistics and visualization of game data. The dashboard runs in a separate thread and offers views for top civilizations, recent events, alliance information, and global statistics. This allows players and administrators to monitor game state and activity outside of the Guilded interface.

### Resource and Economy System
The game implements a multi-resource economy (gold, wood, stone, food) with various generation methods (gathering, farming, mining, fishing). Resource generation is affected by territory size, ideology bonuses, HyperItem effects, and random events. The system includes resource costs for military training, building purchases, and diplomatic actions.

### Military and Combat Mechanics
Military functionality includes unit training (soldiers, spies), technology research, and combat resolution. The system factors in defensive bonuses, ideology modifiers, and HyperItem effects when calculating battle outcomes. Special military actions like nuclear strikes require rare HyperItems and have significant cooldowns and global consequences.

### Diplomacy and Alliance Framework
Players can form alliances, declare wars, and engage in various diplomatic actions. The system tracks relationship states between civilizations and applies appropriate bonuses/penalties based on diplomatic status. Alliance mechanics include mutual defense pacts and coordinated actions between allied civilizations.

## External Dependencies

### Core Framework Dependencies
- **guilded.py**: Primary bot framework for Guilded platform integration, handling events, commands, and server communication
- **Flask**: Web framework powering the dashboard interface with real-time statistics and game monitoring
- **SQLite3**: Embedded database engine for persistent data storage (no external database server required)

### Python Standard Library Usage
- **asyncio**: Asynchronous programming support for bot operations and event handling
- **threading**: Thread management for web dashboard and database connections
- **logging**: Comprehensive logging system for debugging and monitoring
- **datetime**: Time-based operations for cooldowns, events, and timestamps
- **json**: Data serialization for complex database fields and API responses
- **random**: Probability calculations for events, resource generation, and game mechanics

### Static Assets and Templates
- **Bootstrap 5.3.0**: Frontend CSS framework for responsive dashboard design
- **Font Awesome 6.4.0**: Icon library for enhanced UI elements
- **Custom CSS**: Theme-specific styling for civilization dashboard

### File System Dependencies
- **warbot.log**: Application logging output for debugging and monitoring
- **warbot.db**: SQLite database file for persistent game data storage
- **web/templates/**: HTML templates for dashboard interface
- **web/static/**: Static assets including CSS and potential JavaScript files

### Environment Configuration
- **FLASK_SECRET_KEY**: Environment variable for Flask session security (optional, defaults to hardcoded value)
- **GUILDED_TOKEN**: Bot authentication token for Guilded API access (implied but not explicitly shown in code)

Note: The application is designed to be self-contained with minimal external service dependencies, using SQLite for data persistence and requiring only standard Python libraries plus the specified web frameworks.