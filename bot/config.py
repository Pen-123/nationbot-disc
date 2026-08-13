# bot/config.py
# All game balance parameters in one place.

# ---- ECONOMY GAINS ----
ECONOMY = {
    # .gather
    "gather_base_min": 5,
    "gather_base_max": 20,
    "gather_chance": 0.6,
    "gather_employment_coeff": 0.4,
    "gather_cap": 500000,
    # .work
    "work_gold_per_citizen_min": 2,
    "work_gold_per_citizen_max": 5,
    "work_employment_coeff": 0.4,
    "work_cap": 1500000,
    # .farm
    "farm_base_min": 10,
    "farm_base_max": 40,
    "farm_citizen_divisor": 20,
    "farm_employment_coeff": 0.4,
    "farm_cap": 500000,
    # .mine
    "mine_stone_base_min": 8,
    "mine_stone_base_max": 30,
    "mine_wood_base_min": 5,
    "mine_wood_base_max": 20,
    "mine_employment_coeff": 0.4,
    "mine_stone_cap": 250000,
    "mine_wood_cap": 250000,
    "mine_bonus_gold_chance": 0.2,
    "mine_bonus_gold_min": 3,
    "mine_bonus_gold_max": 15,
    "mine_bonus_gold_cap": 10000,
    # .harvest (buffed)
    "harvest_base_min": 50,
    "harvest_base_max": 120,
    "harvest_citizen_divisor": 10,
    "harvest_happiness_divisor": 4,
    "harvest_employment_coeff": 0.4,
    "harvest_cap": 1500000,
    # .drill (buffed)
    "drill_minerals_min": 20,
    "drill_minerals_max": 80,
    "drill_employment_coeff": 0.4,
    "drill_gold_cap": 2000000,
    "drill_stone_cap": 1000000,
    "drill_bonus_gold_chance": 0.15,
    "drill_bonus_gold_min": 50,
    "drill_bonus_gold_max": 200,
    # .fish
    "fish_food_base_min": 8,
    "fish_food_base_max": 25,
    "fish_treasure_base_min": 10,
    "fish_treasure_base_max": 60,
    "fish_employment_coeff": 0.4,
    "fish_cap": 250000,
    # .tax (nerfed)
    "tax_base_per_citizen": 1,
    "tax_employment_coeff": 0.3,
    "tax_happiness_penalty": -5,
    "tax_fascism_extra_penalty": -8,
    "tax_cap": 300000,
    "tax_population_loss_threshold": 50,
    "tax_population_loss_chance": 0.35,
    "tax_population_loss_min": 10,
    "tax_population_loss_max": 30,
    # .raidcaravan (buffed)
    "raid_gold_min": 150,
    "raid_gold_max": 450,
    "raid_food_min": 80,
    "raid_food_max": 200,
    "raid_wood_min": 40,
    "raid_wood_max": 120,
    "raid_stone_min": 30,
    "raid_stone_max": 100,
    "raid_employment_coeff": 0.6,
    "raid_cap": 2000000,
    "raid_bonus_gold_chance": 0.1,
    "raid_bonus_gold_min": 200,
    "raid_bonus_gold_max": 500,
    "raid_min_soldiers": 5,
    # .labor (buffed)
    "labor_gold_min": 150,
    "labor_gold_max": 450,
    "labor_food_min": 80,
    "labor_food_max": 200,
    "labor_wood_min": 80,
    "labor_wood_max": 200,
    "labor_stone_min": 60,
    "labor_stone_max": 160,
    "labor_employment_coeff": 0.6,
    "labor_cap": 800000,
    "labor_happiness_cost": -10,
    "labor_min_soldiers": 5,
    # .advertise
    "advertise_citizen_min": 120,
    "advertise_citizen_max": 350,
    "advertise_employment_coeff": 0.6,
    "advertise_cap": 5000,
    "advertise_cost": 50,
    # .immigration
    "immigration_citizen_min": 100,
    "immigration_citizen_max": 250,
    "immigration_employment_coeff": 0.4,
    "immigration_cap": 800,
    "immigration_happiness_loss_min": 10,
    "immigration_happiness_loss_max": 22,
    "immigration_riot_chance": 0.30,
    "immigration_riot_happiness_loss_min": 8,
    "immigration_riot_happiness_loss_max": 20,
    "immigration_riot_soldier_loss_min": 2,
    "immigration_riot_soldier_loss_max": 8,
    # .sell
    "sell_common_min": 100,
    "sell_common_max": 250,
    "sell_rare_min": 300,
    "sell_rare_max": 600,
    "sell_legendary_min": 400,
    "sell_legendary_max": 800,
    "sell_cap": 10000,
}

# ---- EXPANSION ----
EXPANSION = {
    "soldier_per_area_small": 595,
    "soldier_per_area_large": 2000,
    "min_soldier_cost": 10,
    "max_soldier_cost": 5000,
    "rapid_multiplier": 2,
    "rapid_min_soldier_cost": 20,
    "rapid_max_soldier_cost": 10000,
    "base_gold_per_province": 300,
    "base_food_per_province": 100,
    "base_wood_per_province": 20,
    "base_stone_per_province": 20,
    "resource_cost_multiplier": 0.75,
}

# ---- MILITARY ----
MILITARY = {
    "soldier_buy_cost": 20,
    "tech_upgrade_cost": 500,
    "train_cost_soldier_gold": 50,
    "train_cost_soldier_food": 10,
    "train_cost_spy_gold": 100,
    "train_cost_spy_food": 5,
    "ship_costs": {
        "frigate": {"gold": 1000, "wood": 200, "stone": 100},
        "destroyer": {"gold": 2000, "wood": 300, "stone": 150},
        "battleship": {"gold": 4000, "wood": 500, "stone": 300},
        "aircraft_carrier": {"gold": 6000, "wood": 800, "stone": 400},
        "submarine": {"gold": 1500, "wood": 100, "stone": 50},
    },
    "plane_costs": {
        "fighter": {"gold": 3000, "wood": 200, "stone": 50},
        "attacker": {"gold": 5000, "wood": 300, "stone": 100},
        "bomber": {"gold": 8000, "wood": 500, "stone": 200},
    },
    "border_cost": {"gold": 1000, "stone": 500, "wood": 300},
}

# ---- COOLDOWNS (in minutes) ----
COOLDOWNS = {
    # Economy
    "gather": 1,
    "work": 1,
    "farm": 1,
    "mine": 1,
    "harvest": 0,
    "drill": 1,
    "fish": 1,
    "tax": 5,
    "lottery": 1,
    "invest": 5,
    "raidcaravan": 5,
    "labor": 5,
    "advertise": 10,
    "immigration": 10,
    "sell": 0,
    "festival": 1,
    "cheer": 1,
    "cheerup": 10,
    "buytech": 0,
    "buysoldiers": 0,
    "burn": 0,
    "buycard": 0,
    # Military
    "train": 2,
    "find": 1,
    "attack": 3,
    "siege": 10,
    "stealthbattle": 4,
    "addborder": 5,
    "removeborder": 2,
    "rectract": 1,
    "retrieve": 1,
    "borderinfo": 1,
    "buildship": 0,
    "buildplane": 0,
    "tech": 0,
    "trainboost": 0,
    # Diplomacy
    "ally": 0,
    "acceptally": 0,
    "rejectally": 0,
    "break": 0,
    "send": 0,
    "trade": 0,
    "accepttrade": 0,
    "rejecttrade": 0,
    "mail": 0,
    "inbox": 0,
    "coalition": 0,
    # Store
    "blackmarket": 0,
    "store": 0,
    "inventory": 0,
    "market": 0,
    # Territory
    "expand": 0,
    "rapidexpansion": 0,
    "territories": 0,
    "map": 0,
    # Countryballs
    "openpacks": 0,
    "evolve": 0,
    "packs": 0,
    "activate": 0,
    "deactivate": 0,
    "synergies": 0,
    # Industrial Revolution
    "industrial_start": 0,
    "industrial_status": 0,
    "industrial_build": 2,
    "industrial_tech": 2,
    "industrial_workers": 2,
    "industrial_cleanup": 2,
    "industrial_railway": 2,
    "industrial_transport": 2,
    "industrial_army": 2,
    "industrial_policy": 2,
    "industrial_import": 2,
    "industrial_export": 2,
    "industrial_steam": 2,
    "industrial_mine": 2,
    "industrial_hospital": 2,
    "industrial_school": 2,
    "industrial_law": 2,
    "industrial_trade": 2,
    "industrial_aid": 2,
    "industrial_suppress": 2,
    "industrial_bribe": 2,
    "industrial_automate": 2,
    "industrial_upgrade": 2,
    "industrial_relief": 2,
    "industrial_expand": 2,
    "industrial_banking": 10,
    "industrial_nationalize": 5,
    "indushelp": 0,
    # ExtraEconomy
    "extrawork": 5,
    "extragamble": 1,
    "extracards": 1,
    "slots": 1,
    "blackjack": 1,
    "job": 1,
    "arrest": 1,
    "rob": 1,
    "code": 0,
    "darkweb": 0,
    "extrastore": 1,
    "extrainventory": 0,
    "setbalance": 0,
    # HyperItems
    "laststand": 60,
    "luckystrike": 60,
    "propaganda": 3,
    "hiremercs": 10,
    "boosttech": 5,
    "mintgold": 10,
    "superharvest": 10,
    "superspy": 10,
    "megainvent": 5,
    "backstab": 180,
    "bomb": 1,
    "nuke": 5,
    "obliterate": 13,
    "sacrifice": 1440,
    # Corporations
    "corporation": 0,
    # Megaprojects
    "megaproject": 0,
    # Policies
    "policy": 0,
    "policieshelp": 0,
}

# ---- CAPS ----
CAPS = {
    "gather": 500000,
    "work": 1500000,
    "farm": 500000,
    "mine_stone": 250000,
    "mine_wood": 250000,
    "harvest": 1500000,
    "drill_gold": 2000000,
    "drill_stone": 1000000,
    "fish": 250000,
    "tax": 300000,
    "raidcaravan": 2000000,
    "labor": 800000,
    "advertise": 5000,
    "immigration": 800,
    "sell": 10000,
}

# ---- IDEOLOGY MODIFIERS ----
IDEOLOGY_MODIFIERS = {
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
    "socialism": {
        "citizen_productivity": 1.15,
        "happiness_boost": 1.10,
        "trade_profit": 0.90
    },
    "terrorism": {
        "guerrilla_effectiveness": 1.40,
        "spy_success": 1.30,
        "diplomacy_success": 0.50,
        "resource_production": 0.80,
        "unrest_multiplier": 1.25
    },
    "capitalism": {
        "trade_profit": 1.20,
        "gold_generation": 1.15,
        "happiness_boost": 0.90
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

# ---- REGION MODIFIERS ----
REGION_MODIFIERS = {
    "Asia": {"food_production": 1.20, "population_capacity": 1.25},
    "Europe": {"tech_research": 1.25, "gold_production": 1.15},
    "Africa": {"mining_efficiency": 1.30, "stone_production": 1.20},
    "North America": {"balanced_production": 1.10, "trade_efficiency": 1.15},
    "South America": {"food_production": 1.25, "wood_production": 1.15},
    "Middle East": {"gold_production": 1.40, "oil_resources": 1.30},
    "Oceania": {"happiness": 1.15, "naval_advantage": 1.20},
    "Antarctica": {"research_speed": 1.25, "unique_discoveries": 1.30}
}

# ---- BLACK MARKET ----
BLACK_MARKET = {
    "entry_cost": 1000,
    "pity_uncommon": 3,
    "pity_rare": 6,
    "pity_legendary": 10,
}

# ---- CARD POOL ----
CARD_POOL = [
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

# ---- TERRITORY FACTOR ----
TERRITORY_FACTOR = {
    "coefficient": 0.8,
    "max": 3.0,
}

# ---- STARTING RESOURCES ----
STARTING_RESOURCES = {
    "gold": 500,
    "food": 300,
    "stone": 100,
    "wood": 100,
    "citizens": 100,
    "happiness": 50,
    "soldiers": 10,
    "spies": 2,
    "tech_level": 1,
    "land_size": 1000,
    "hyper_items": ["Anti-Nuke Shield"],
}

# ---- EASTER EGGS ----
EASTER_EGGS = {
    "ncsw": {
        "bonuses": {"soldier_training_speed": 20, "happiness_boost": -5},
        "hyper_item": "Confederate Battle Flag",
        "message": "⚔️ The spirit of the Confederacy lives on! (+20% soldier training, -5% happiness)"
    },
    "confederate democracy": {
        "bonuses": {"diplomacy_success": 10, "happiness_boost": 10},
        "message": "📜 A unique blend of Southern charm and democratic ideals! (+10% diplomacy, +10% happiness)"
    },
    "uspr": {
        "bonuses": {"resource_production": 15, "trade_profit": -10},
        "hyper_item": "Red Banner",
        "message": "☭ The people's republic rises! (+15% resource production, -10% trade profit)"
    }
}

# ---- INDUSTRIAL REVOLUTION ----
INDUSTRIAL = {
    "banking_max_uses": 5,
}

# ---- EXTRA ECONOMY ----
EXTRACONOMY = {
    "extrawork_base_salary": 50,
    "extrawork_salary_multipliers": {
        "Teller": 100, "Manager": 200, "Executive": 300,
        "Recruit": 150, "Officer": 250, "Captain": 350,
        "Guard": 120, "Supervisor": 220, "Chief": 320,
        "Clerk": 180, "Minister": 280, "President": 500, "Prime Minister": 600,
        "Private": 130, "Sergeant": 230, "Commander": 330
    },
    "job_application_roles": {
        "bank": ["Rejected", "Teller", "Manager", "Executive"],
        "police": ["Rejected", "Recruit", "Officer", "Captain"],
        "security": ["Rejected", "Guard", "Supervisor", "Chief"],
        "government": ["Rejected", "Clerk", "Minister", "President", "Prime Minister"],
        "military": ["Rejected", "Private", "Sergeant", "Commander"]
    },
    "extrastore_prices": {
        "ak": 500,
        "ammo": 100,
        "glock17": 800,
        "crypto_miner": 4000,
    },
    "extrastore_stock": {
        "ak": 5,
        "ammo": 10,
        "glock17": 5,
        "crypto_miner": 2,
    },
    "extrastore_item_names": {
        "ak": "AK-47",
        "ammo": "Ammo Box",
        "glock17": "Glock 17",
        "crypto_miner": "Crypto Miner"
    },
    "darkweb_items": {
        "forged_documents": 5000,
        "stolen_data": 3000,
        "silencer": 1500,
        "explosives": 5000,
        "crypto_miner": 3500,
    },
    "darkweb_scam_chance": 0.5,
    "slots_jackpot_multiplier": 10,
    "slots_triple_multiplier": 2,
    "blackjack_win_multiplier": 1,
    "extracards_win_multiplier": 1,
    "extragamble_win_chance": 0.45,
    "extragamble_jackpot_chance": 0.10,
    "arrest_success_chance": 0.6,
    "arrest_seize_amount": 200,
    "rob_success_chance": 0.5,
    "rob_stolen_min": 100,
    "rob_stolen_max": 300,
    "coding_projects": {
        "virus": {"cost": 250, "duration_seconds": 1500, "reward_min": 250, "reward_max": 763, "risk": 0.25},
        "website": {"cost": 50, "duration_seconds": 600, "reward_min": 50, "reward_max": 150, "risk": 0},
        "messenger": {"cost": 3500, "duration_seconds": 18000, "reward_type": "product", "viral_chance": 0.45}
    },
    "crypto_miner_income": 200,
    "crypto_miner_interval": 3600,
    "product_messenger_base_interval": 10800,
    "product_messenger_viral_interval": 18000,
    "product_messenger_base_payout": 10,
    "product_messenger_viral_payout_min": 1000,
    "product_messenger_viral_payout_max": 5000,
}

# ---- MEGAPROJECTS ----
MEGAPROJECTS = {
    "great_wall": {
        "name": "Great Wall",
        "cost": {"gold": 5000000, "stone": 2000000},
        "effect": {"defense_strength": 50},
        "description": "Permanent +50% defense bonus",
        "tech_required": 5,
    },
    "space_program": {
        "name": "Space Program",
        "cost": {"gold": 10000000, "wood": 500000, "stone": 500000},
        "effect": {"tech_speed": 100},
        "description": "Permanent +100% tech research speed",
        "tech_required": 7,
    },
    "global_bank": {
        "name": "Global Bank",
        "cost": {"gold": 8000000, "food": 1000000},
        "effect": {"tax_bonus": 50},
        "description": "Permanent +50% tax income",
        "tech_required": 6,
    },
    "ai_network": {
        "name": "AI Network",
        "cost": {"gold": 15000000, "wood": 1000000, "stone": 1000000},
        "effect": {"resource_production": 100},
        "description": "Permanent +100% all resource production",
        "tech_required": 8,
    },
    "green_energy": {
        "name": "Green Energy Grid",
        "cost": {"gold": 6000000, "wood": 300000, "stone": 300000},
        "effect": {"happiness_boost": 20},
        "description": "Permanent +20 happiness",
        "tech_required": 4,
    }
}

# ---- POLICIES ----
POLICIES = {
    "military_service": {
        "name": "Military Service",
        "levels": {
            1: {"gold_cost": 50000, "effect": {"soldier_training_speed": 5}, "desc": "+5% soldier training"},
            2: {"gold_cost": 150000, "effect": {"soldier_training_speed": 10}, "desc": "+10% soldier training"},
            3: {"gold_cost": 300000, "effect": {"soldier_training_speed": 20}, "desc": "+20% soldier training"},
        },
        "max_level": 3,
        "base_desc": "Boosts soldier training speed.",
    },
    "agricultural_subsidies": {
        "name": "Agricultural Subsidies",
        "levels": {
            1: {"gold_cost": 40000, "food_cost": 20000, "effect": {"farm_bonus": 10}, "desc": "+10% farm yield"},
            2: {"gold_cost": 120000, "food_cost": 60000, "effect": {"farm_bonus": 20}, "desc": "+20% farm yield"},
            3: {"gold_cost": 250000, "food_cost": 120000, "effect": {"farm_bonus": 35}, "desc": "+35% farm yield"},
        },
        "max_level": 3,
        "base_desc": "Increases food production from farming.",
    },
    "trade_agreements": {
        "name": "Trade Agreements",
        "levels": {
            1: {"gold_cost": 60000, "effect": {"trade_profit": 5}, "desc": "+5% trade profit"},
            2: {"gold_cost": 180000, "effect": {"trade_profit": 12}, "desc": "+12% trade profit"},
            3: {"gold_cost": 400000, "effect": {"trade_profit": 25}, "desc": "+25% trade profit"},
        },
        "max_level": 3,
        "base_desc": "Increases profit from trades.",
    },
    "public_education": {
        "name": "Public Education",
        "levels": {
            1: {"gold_cost": 80000, "effect": {"tech_speed": 5}, "desc": "+5% tech research speed"},
            2: {"gold_cost": 200000, "effect": {"tech_speed": 12}, "desc": "+12% tech research speed"},
            3: {"gold_cost": 500000, "effect": {"tech_speed": 25}, "desc": "+25% tech research speed"},
        },
        "max_level": 3,
        "base_desc": "Accelerates technology research.",
    },
    "environmental_protection": {
        "name": "Environmental Protection",
        "levels": {
            1: {"gold_cost": 70000, "stone_cost": 30000, "effect": {"happiness_boost": 5, "resource_production": -5}, "desc": "+5% happiness, -5% resource production"},
            2: {"gold_cost": 180000, "stone_cost": 80000, "effect": {"happiness_boost": 10, "resource_production": -3}, "desc": "+10% happiness, -3% resource production"},
            3: {"gold_cost": 350000, "stone_cost": 150000, "effect": {"happiness_boost": 15, "resource_production": 0}, "desc": "+15% happiness"},
        },
        "max_level": 3,
        "base_desc": "Boosts happiness but may reduce production initially.",
    },
    "industrial_innovation": {
        "name": "Industrial Innovation",
        "levels": {
            1: {"gold_cost": 90000, "effect": {"resource_production": 5}, "desc": "+5% all resource production"},
            2: {"gold_cost": 220000, "effect": {"resource_production": 12}, "desc": "+12% all resource production"},
            3: {"gold_cost": 500000, "effect": {"resource_production": 25}, "desc": "+25% all resource production"},
        },
        "max_level": 3,
        "base_desc": "Increases all resource production.",
    },
}

# ---- TESTING MODE ----
TESTING_GAIN = 999999999

# ================================================================
#                     VICTORY CONDITIONS
# ================================================================
VICTORY = {
    # Domination: own at least 80% of all provinces and at least 10 territories
    "domination_percentage": 0.80,
    "domination_min_territories": 10,

    # Economic: accumulate 500 million gold and have GDP per citizen ≥ 100,000
    "economic_gold": 500_000_000,
    "economic_gdp_per_citizen": 100_000,

    # Diplomatic: be in at least 3 alliances and have a total alliance membership score ≥ 5
    "diplomatic_alliances": 3,
    "diplomatic_score": 5,

    # Industrial: build at least 3 megaprojects and enact 6 policies
    "industrial_megaprojects": 3,
    "industrial_policies": 6,

    # Conquest: own every province in the world
    "conquest_required": True,

    # United Nations: have a single alliance with at least 5 members
    "united_nations_members": 5,

    # Channel IDs where victory announcements will be sent (set as list of integers)
    "announcement_channels": [],
}
