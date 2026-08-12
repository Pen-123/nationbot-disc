"""
ExtraEconomy cog (gold-in-civ currency, cooldowns, extragamble, .extrawork)

Notes:
- .balance and .profile have been removed as requested.
- Commands replaced/renamed: extrawork, extrastore, extrainventory, extracards, extragamble, etc.
- Currency is stored on the civilization (civ['resources']['gold']) via bot.civ_manager or Database.
- Cooldowns are applied only after successful execution.
"""
from __future__ import annotations

import os
import json
import random
import time
import asyncio
import logging
from threading import Lock
from typing import Dict, Any, Optional, List

import discord
from discord import app_commands
from discord.ext import commands

from bot import config

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

try:
    from bot.database import Database  # type: ignore
except Exception:
    Database = None  # type: ignore


class EconomyManager:
    def __init__(self, storage_dir: str = ".", db: Optional[Any] = None, bot: Optional[commands.Bot] = None):
        self.db = db
        self.bot = bot
        self.storage_dir = storage_dir
        self.lock = Lock()

        os.makedirs(storage_dir, exist_ok=True)
        self.DATA_FALLBACK = os.path.join(storage_dir, "civ_gold_fallback.json")
        if not os.path.exists(self.DATA_FALLBACK):
            with open(self.DATA_FALLBACK, "w") as f:
                json.dump({}, f)
        self._load_fallback()

        # Shop config from config
        self.shop_items = {
            "ak": {"price": config.EXTRACONOMY["extrastore_prices"]["ak"], "stock": config.EXTRACONOMY["extrastore_stock"]["ak"]},
            "ammo": {"price": config.EXTRACONOMY["extrastore_prices"]["ammo"], "stock": config.EXTRACONOMY["extrastore_stock"]["ammo"]},
            "glock17": {"price": config.EXTRACONOMY["extrastore_prices"]["glock17"], "stock": config.EXTRACONOMY["extrastore_stock"]["glock17"]},
            "crypto_miner": {"price": config.EXTRACONOMY["extrastore_prices"]["crypto_miner"], "stock": config.EXTRACONOMY["extrastore_stock"]["crypto_miner"]}
        }

    def _load_fallback(self):
        try:
            with open(self.DATA_FALLBACK, "r") as f:
                self.fallback_gold = json.load(f) or {}
        except Exception:
            logger.exception("Failed to load fallback gold file")
            self.fallback_gold = {}

    def _save_fallback(self):
        try:
            with open(self.DATA_FALLBACK, "w") as f:
                json.dump(self.fallback_gold, f, indent=2)
        except Exception:
            logger.exception("Failed to save fallback gold file")

    # civ lookup / persist helpers
    def _get_civ_via_bot(self, user_id: str) -> Optional[Dict[str, Any]]:
        try:
            if self.bot and hasattr(self.bot, "civ_manager") and self.bot.civ_manager:
                return self.bot.civ_manager.get_civilization(str(user_id))
        except Exception:
            logger.exception("Error calling bot.civ_manager.get_civilization")
        return None

    def _get_civ_via_db(self, user_id: str) -> Optional[Dict[str, Any]]:
        try:
            if self.db and hasattr(self.db, "get_civilization"):
                return self.db.get_civilization(str(user_id))
        except Exception:
            logger.exception("Error calling Database.get_civilization")
        return None

    def _update_civ_via_bot(self, user_id: str, civ: Dict[str, Any]) -> bool:
        try:
            if self.bot and hasattr(self.bot, "civ_manager") and self.bot.civ_manager:
                if hasattr(self.bot.civ_manager, "update_civilization"):
                    return self.bot.civ_manager.update_civilization(str(user_id), civ)
        except Exception:
            logger.exception("Error calling bot.civ_manager.update_civilization")
        return False

    def _update_civ_via_db(self, user_id: str, civ: Dict[str, Any]) -> bool:
        try:
            if self.db and hasattr(self.db, "update_civilization"):
                return self.db.update_civilization(str(user_id), civ)
        except Exception:
            logger.exception("Error calling Database.update_civilization")
        return False

    def _get_civ(self, user_id: str) -> Optional[Dict[str, Any]]:
        civ = self._get_civ_via_bot(user_id)
        if civ:
            return civ
        civ = self._get_civ_via_db(user_id)
        if civ:
            return civ
        return None

    def _persist_civ(self, user_id: str, civ: Dict[str, Any]) -> bool:
        if self._update_civ_via_bot(user_id, civ):
            return True
        if self._update_civ_via_db(user_id, civ):
            return True
        return False

    # gold operations
    def get_gold(self, user_id: str) -> int:
        try:
            civ = self._get_civ(user_id)
            if civ:
                resources = civ.get("resources", {})
                return int(resources.get("gold", 0))
        except Exception:
            logger.exception("get_gold via civ failed")
        return int(self.fallback_gold.get(str(user_id), 0))

    def set_gold(self, user_id: str, amount: int) -> bool:
        user_id = str(user_id)
        try:
            civ = self._get_civ(user_id)
            if civ is not None:
                resources = civ.get("resources", {})
                resources["gold"] = int(amount)
                civ["resources"] = resources
                if self._persist_civ(user_id, civ):
                    return True
            self.fallback_gold[user_id] = int(amount)
            self._save_fallback()
            return True
        except Exception:
            logger.exception("set_gold failed")
            return False

    def add_gold(self, user_id: str, amount: int) -> bool:
        user_id = str(user_id)
        try:
            civ = self._get_civ(user_id)
            if civ is not None:
                resources = civ.get("resources", {})
                resources["gold"] = int(resources.get("gold", 0)) + int(amount)
                civ["resources"] = resources
                if self._persist_civ(user_id, civ):
                    return True
            curr = int(self.fallback_gold.get(user_id, 0))
            self.fallback_gold[user_id] = curr + int(amount)
            self._save_fallback()
            return True
        except Exception:
            logger.exception("add_gold failed")
            return False

    def try_withdraw_gold(self, user_id: str, amount: int) -> bool:
        user_id = str(user_id)
        try:
            civ = self._get_civ(user_id)
            if civ is not None:
                resources = civ.get("resources", {})
                curr = int(resources.get("gold", 0))
                if curr >= int(amount):
                    resources["gold"] = curr - int(amount)
                    civ["resources"] = resources
                    if self._persist_civ(user_id, civ):
                        return True
                    return False
                return False
            curr = int(self.fallback_gold.get(user_id, 0))
            if curr >= int(amount):
                self.fallback_gold[user_id] = curr - int(amount)
                self._save_fallback()
                return True
            return False
        except Exception:
            logger.exception("try_withdraw_gold failed")
            return False

    # inventory/products wrappers (DB-backed)
    def get_inventory(self, user_id: str) -> List[str]:
        try:
            if self.db and hasattr(self.db, "get_inventory"):
                return list(self.db.get_inventory(str(user_id)) or [])
        except Exception:
            logger.debug("db.get_inventory not used")
        return []

    def update_inventory(self, user_id: str, items: List[str]) -> None:
        try:
            if self.db and hasattr(self.db, "update_inventory"):
                return self.db.update_inventory(str(user_id), items)
        except Exception:
            logger.debug("db.update_inventory not used")

    def get_products(self, user_id: str) -> Dict[str, Any]:
        try:
            if self.db and hasattr(self.db, "get_products"):
                return dict(self.db.get_products(str(user_id)) or {})
        except Exception:
            logger.debug("db.get_products not used")
        return {}

    def update_products(self, user_id: str, products: Dict[str, Any]) -> None:
        try:
            if self.db and hasattr(self.db, "update_products"):
                return self.db.update_products(str(user_id), products)
        except Exception:
            logger.debug("db.update_products not used")


class EconomyCog(commands.Cog):
    def __init__(self, bot: commands.Bot, db: Optional[Any] = None, storage_dir: str = "."):
        self.bot = bot
        self.manager = EconomyManager(storage_dir=storage_dir, db=db, bot=bot)
        self.cooldowns: Dict[str, Dict[str, float]] = {}
        self.coding_tasks: Dict[str, tuple] = {}
        self.product_last_pay: Dict[str, Dict[str, float]] = {}
        self._tasks: List[asyncio.Task] = []

    async def cog_load(self):
        self._tasks.append(asyncio.create_task(self._crypto_miner_loop()))
        self._tasks.append(asyncio.create_task(self._product_income_loop()))
        self._tasks.append(asyncio.create_task(self._coding_loop()))
        logger.info("EconomyCog: background tasks started")

    async def cog_unload(self):
        for t in self._tasks:
            try:
                t.cancel()
            except Exception:
                logger.exception("Failed to cancel task")
        self._tasks.clear()
        logger.info("EconomyCog: background tasks cancelled")

    # cooldown helpers (using config)
    def _get_cd_seconds(self, cmd_name: str) -> int:
        return config.COOLDOWNS.get(cmd_name, 0) * 60  # convert minutes to seconds

    def _get_last(self, cmd_name: str, user_id: str) -> float:
        return self.cooldowns.get(cmd_name, {}).get(user_id, 0.0)

    def _set_last(self, cmd_name: str, user_id: str, ts: Optional[float] = None):
        ts = ts or time.time()
        self.cooldowns.setdefault(cmd_name, {})[user_id] = ts

    def _is_on_cooldown(self, cmd_name: str, user_id: str) -> Optional[int]:
        last = self._get_last(cmd_name, user_id)
        if last == 0.0:
            return None
        cd = self._get_cd_seconds(cmd_name)
        if cd == 0:
            return None
        elapsed = time.time() - last
        if elapsed >= cd:
            return None
        return int(cd - elapsed)

    # civ checks
    def _user_has_civ_via_bot(self, user_id: str) -> bool:
        try:
            if hasattr(self.bot, "civ_manager") and self.bot.civ_manager:
                civ = self.bot.civ_manager.get_civilization(str(user_id))
                return civ is not None
        except Exception:
            logger.exception("Error checking civ via bot.civ_manager")
        return False

    def _user_has_civ_via_db(self, user_id: str) -> bool:
        try:
            if self.manager.db and hasattr(self.manager.db, "get_civilization"):
                civ = self.manager.db.get_civilization(str(user_id))
                return civ is not None
        except Exception:
            logger.exception("Error checking civ via Database.get_civilization")
        return False

    def user_has_civ(self, user_id: str) -> bool:
        if self._user_has_civ_via_bot(user_id):
            return True
        if self._user_has_civ_via_db(user_id):
            return True
        return False

    async def require_civ(self, ctx) -> bool:
        uid = str(ctx.author.id)
        if not self.user_has_civ(uid):
            await ctx.send("🚫 You need a civilization to use that command. Create one using your civ commands.")
            return False
        return True

    # background loops (simplified)
    async def _crypto_miner_loop(self):
        try:
            while True:
                await asyncio.sleep(config.EXTRACONOMY["crypto_miner_interval"])
                inv_map = {}
                try:
                    if self.manager.db and hasattr(self.manager.db, "get_all_inventories"):
                        inv_map = self.manager.db.get_all_inventories()
                except Exception:
                    inv_map = {}
                try:
                    for suid, items in list(inv_map.items()):
                        if not isinstance(items, list):
                            continue
                        miner_count = sum(1 for i in items if i == "crypto_miner")
                        if miner_count > 0:
                            self.manager.add_gold(suid, config.EXTRACONOMY["crypto_miner_income"] * miner_count)
                except Exception:
                    logger.exception("crypto miner loop error")
        except asyncio.CancelledError:
            return

    async def _product_income_loop(self):
        try:
            while True:
                await asyncio.sleep(3600)
                now = time.time()
                prod_map = {}
                try:
                    if self.manager.db and hasattr(self.manager.db, "get_all_products"):
                        prod_map = self.manager.db.get_all_products()
                except Exception:
                    prod_map = {}
                try:
                    for suid, prods in list(prod_map.items()):
                        if not isinstance(prods, dict):
                            continue
                        if "messenger" in prods:
                            state = prods["messenger"]
                            last = self.product_last_pay.get(suid, {}).get("messenger", 0)
                            if state == "viral":
                                interval = config.EXTRACONOMY["product_messenger_viral_interval"]
                                if now - last >= interval:
                                    payout = random.randint(config.EXTRACONOMY["product_messenger_viral_payout_min"],
                                                            config.EXTRACONOMY["product_messenger_viral_payout_max"])
                                    self.manager.add_gold(suid, payout)
                                    self.product_last_pay.setdefault(suid, {})["messenger"] = now
                            else:
                                interval = config.EXTRACONOMY["product_messenger_base_interval"]
                                if now - last >= interval:
                                    self.manager.add_gold(suid, config.EXTRACONOMY["product_messenger_base_payout"])
                                    self.product_last_pay.setdefault(suid, {})["messenger"] = now
                except Exception:
                    logger.exception("product income loop error")
        except asyncio.CancelledError:
            return

    async def _coding_loop(self):
        try:
            while True:
                await asyncio.sleep(30)
                now = time.time()
                finished = []
                for suid, task in list(self.coding_tasks.items()):
                    proj, finish_ts = task
                    if now >= finish_ts:
                        finished.append((suid, proj))
                for suid, proj in finished:
                    if proj == "website":
                        payout = random.randint(config.EXTRACONOMY["coding_projects"]["website"]["reward_min"],
                                                config.EXTRACONOMY["coding_projects"]["website"]["reward_max"])
                        self.manager.add_gold(suid, payout)
                    elif proj == "virus":
                        risk = config.EXTRACONOMY["coding_projects"]["virus"]["risk"]
                        if random.random() < risk:
                            logger.debug(f"Virus coder {suid} got caught.")
                        else:
                            payout = random.randint(config.EXTRACONOMY["coding_projects"]["virus"]["reward_min"],
                                                    config.EXTRACONOMY["coding_projects"]["virus"]["reward_max"])
                            self.manager.add_gold(suid, payout)
                    elif proj == "messenger":
                        prods = self.manager.get_products(suid)
                        if random.random() < config.EXTRACONOMY["coding_projects"]["messenger"]["viral_chance"]:
                            prods["messenger"] = "viral"
                        else:
                            prods["messenger"] = "flop"
                        self.manager.update_products(suid, prods)
                    self.coding_tasks.pop(suid, None)
        except asyncio.CancelledError:
            return

    # UI helpers
    def build_store_display(self) -> str:
        lines = ["🛒 Current Store Stock:"]
        for name, data in self.manager.shop_items.items():
            extra = " ⛏️ miner pays hourly" if name == "crypto_miner" else ""
            lines.append(f"- {config.EXTRACONOMY['extrastore_item_names'][name]} ({data['price']} gold) — {data['stock']} in stock{extra}")
        lines.append("\nBuy items with .extrastore buy <item>")
        return "\n".join(lines)

    def build_darkweb_display(self) -> str:
        lines = ["🌑 Dark Web Market (50% scam risk):"]
        for item, price in config.EXTRACONOMY["darkweb_items"].items():
            lines.append(f"- {item} ({price} gold)")
        lines.append("\nUse .darkweb <item> to attempt a purchase.")
        return "\n".join(lines)

    # ---------------- Commands ----------------
    @commands.command()
    async def extrainventory(self, ctx):
        try:
            uid = str(ctx.author.id)
            if not await self.require_civ(ctx):
                return
            inv = self.manager.get_inventory(uid)
            await ctx.send(f"🎒 Inventory: {', '.join(inv) if inv else 'Empty'}")
        except Exception:
            logger.exception("extrainventory command failed")
            await ctx.send("❌ Failed to fetch inventory. No cooldown applied.")

    @commands.command()
    @app_commands.describe(action="Store action", item="Item key")
    @app_commands.choices(
        action=[app_commands.Choice(name="buy", value="buy")],
        item=[
            app_commands.Choice(name="ak", value="ak"),
            app_commands.Choice(name="ammo", value="ammo"),
            app_commands.Choice(name="glock17", value="glock17"),
            app_commands.Choice(name="crypto_miner", value="crypto_miner"),
        ]
    )
    async def extrastore(self, ctx, action: Optional[str] = None, item: Optional[str] = None):
        cmd = "extrastore"
        uid = str(ctx.author.id)
        try:
            if action is None:
                await ctx.send(self.build_store_display())
                return
            action = action.lower()
            if action != "buy":
                await ctx.send("Unknown action. Use `/extrastore` to view or `/extrastore buy <item>` to purchase. No cooldown applied.")
                return
            if item is None:
                await ctx.send("Usage: /extrastore buy <item>. No cooldown applied.")
                return
            if not await self.require_civ(ctx):
                return
            rem = self._is_on_cooldown(cmd, uid)
            if rem:
                await ctx.send(f"⏳ You are on cooldown for {rem}s.")
                return
            key = item.lower()
            prices = {k: v["price"] for k, v in self.manager.shop_items.items()}
            if key not in prices:
                await ctx.send("Item not found. No cooldown applied.")
                return
            price = prices[key]
            if not self.manager.try_withdraw_gold(uid, price):
                await ctx.send("Not enough gold. No cooldown applied.")
                return
            inv = self.manager.get_inventory(uid) or []
            inv.append(key)
            self.manager.update_inventory(uid, inv)
            self._set_last(cmd, uid)
            await ctx.send(f"✅ Purchased {config.EXTRACONOMY['extrastore_item_names'][key]} for {price} gold.")
        except Exception:
            logger.exception("extrastore command failed")
            await ctx.send("❌ Purchase failed. No cooldown applied.")

    @commands.command()
    @app_commands.describe(item="Dark web item key")
    @app_commands.choices(item=[
        app_commands.Choice(name="forged_documents", value="forged_documents"),
        app_commands.Choice(name="stolen_data", value="stolen_data"),
        app_commands.Choice(name="silencer", value="silencer"),
        app_commands.Choice(name="explosives", value="explosives"),
        app_commands.Choice(name="crypto_miner", value="crypto_miner"),
    ])
    async def darkweb(self, ctx, item: Optional[str] = None):
        cmd = "darkweb"
        uid = str(ctx.author.id)
        try:
            if not await self.require_civ(ctx):
                return
            if item is None:
                await ctx.send(self.build_darkweb_display())
                return
            item = item.lower()
            prices = config.EXTRACONOMY["darkweb_items"]
            if item not in prices:
                await ctx.send("Item not available. No cooldown applied.")
                return
            rem = self._is_on_cooldown(cmd, uid)
            if rem:
                await ctx.send(f"⏳ You are on cooldown for {rem}s.")
                return
            price = prices[item]
            if not self.manager.try_withdraw_gold(uid, price):
                await ctx.send("You don't have enough gold. No cooldown applied.")
                return
            if random.random() < config.EXTRACONOMY["darkweb_scam_chance"]:
                inv = self.manager.get_inventory(uid) or []
                inv.append(item)
                self.manager.update_inventory(uid, inv)
                self._set_last(cmd, uid)
                await ctx.send(f"✅ Dark web purchase succeeded: acquired {item}.")
            else:
                self._set_last(cmd, uid)
                await ctx.send(f"💀 Scammed. Lost {price} gold.")
        except Exception:
            logger.exception("darkweb command error")
            await ctx.send("❌ Darkweb purchase failed. No cooldown applied.")

    @commands.command()
    async def slots(self, ctx, amount: Optional[int] = None):
        cmd = "slots"
        uid = str(ctx.author.id)
        try:
            if not await self.require_civ(ctx):
                return
            if amount is None:
                await ctx.send("Usage: .slots <amount>. No cooldown applied.")
                return
            if amount <= 0:
                await ctx.send("Bet must be positive. No cooldown applied.")
                return
            rem = self._is_on_cooldown(cmd, uid)
            if rem:
                await ctx.send(f"⏳ You are on cooldown for {rem}s.")
                return
            if amount > self.manager.get_gold(uid):
                await ctx.send("You don't have enough gold. No cooldown applied.")
                return
            symbols = ["🍒", "🍋", "🔔", "💎", "7️⃣"]
            result = [random.choice(symbols) for _ in range(3)]
            if result == ["7️⃣", "7️⃣", "7️⃣"]:
                win = amount * config.EXTRACONOMY["slots_jackpot_multiplier"]
                self.manager.add_gold(uid, win)
                self._set_last(cmd, uid)
                await ctx.send(f"{' '.join(result)}\n🎉 JACKPOT! You won {win} gold!")
            elif result.count(result[0]) == 3:
                win = amount * config.EXTRACONOMY["slots_triple_multiplier"]
                self.manager.add_gold(uid, win)
                self._set_last(cmd, uid)
                await ctx.send(f"{' '.join(result)}\nNice triple! You won {win} gold!")
            else:
                self.manager.try_withdraw_gold(uid, amount)
                self._set_last(cmd, uid)
                await ctx.send(f"{' '.join(result)}\nNo win. You lost {amount} gold.")
        except Exception:
            logger.exception("slots command error")
            await ctx.send("❌ Slots failed. No cooldown applied.")

    @commands.command()
    async def blackjack(self, ctx, amount: Optional[int] = None):
        cmd = "blackjack"
        uid = str(ctx.author.id)
        try:
            if not await self.require_civ(ctx):
                return
            if amount is None:
                await ctx.send("Usage: .blackjack <amount>. No cooldown applied.")
                return
            if amount <= 0:
                await ctx.send("Bet must be positive. No cooldown applied.")
                return
            rem = self._is_on_cooldown(cmd, uid)
            if rem:
                await ctx.send(f"⏳ You are on cooldown for {rem}s.")
                return
            if amount > self.manager.get_gold(uid):
                await ctx.send("Not enough gold. No cooldown applied.")
                return
            player = [random.randint(2, 11), random.randint(2, 11)]
            dealer = [random.randint(2, 11), random.randint(2, 11)]
            p, d = sum(player), sum(dealer)
            if p > d:
                self.manager.add_gold(uid, amount)
                self._set_last(cmd, uid)
                await ctx.send(f"🃏 You win! {player} ({p}) vs {dealer} ({d}) — +{amount} gold.")
            elif p < d:
                self.manager.try_withdraw_gold(uid, amount)
                self._set_last(cmd, uid)
                await ctx.send(f"🃏 Dealer wins. {player} ({p}) vs {dealer} ({d}) — you lost {amount} gold.")
            else:
                await ctx.send(f"🃏 Tie! {player} ({p}) vs {dealer} ({d}) — no change. No cooldown applied.")
        except Exception:
            logger.exception("blackjack command failed")
            await ctx.send("❌ Blackjack failed. No cooldown applied.")

    @commands.command()
    async def extracards(self, ctx, amount: Optional[int] = None):
        cmd = "extracards"
        uid = str(ctx.author.id)
        try:
            if not await self.require_civ(ctx):
                return
            if amount is None:
                await ctx.send("Usage: .extracards <amount>. No cooldown applied.")
                return
            if amount <= 0:
                await ctx.send("Bet must be positive. No cooldown applied.")
                return
            rem = self._is_on_cooldown(cmd, uid)
            if rem:
                await ctx.send(f"⏳ You are on cooldown for {rem}s.")
                return
            if amount > self.manager.get_gold(uid):
                await ctx.send("Not enough gold. No cooldown applied.")
                return
            you = random.randint(2, 14)
            botc = random.randint(2, 14)
            rank = {11: "J", 12: "Q", 13: "K", 14: "A"}
            y_label = rank.get(you, str(you))
            b_label = rank.get(botc, str(botc))
            if you > botc:
                self.manager.add_gold(uid, amount)
                self._set_last(cmd, uid)
                await ctx.send(f"🂡 You drew {y_label}, bot drew {b_label}. You win +{amount} gold!")
            elif you < botc:
                self.manager.try_withdraw_gold(uid, amount)
                self._set_last(cmd, uid)
                await ctx.send(f"🂱 You drew {y_label}, bot drew {b_label}. You lost {amount} gold.")
            else:
                await ctx.send(f"🂠 Both drew {y_label}. Tie — no change. No cooldown applied.")
        except Exception:
            logger.exception("extracards command failed")
            await ctx.send("❌ Cards failed. No cooldown applied.")

    @commands.command()
    async def extragamble(self, ctx, amount: Optional[int] = None):
        cmd = "extragamble"
        uid = str(ctx.author.id)
        try:
            if not await self.require_civ(ctx):
                return
            if amount is None:
                await ctx.send("Usage: .extragamble <amount>. No cooldown applied.")
                return
            if amount <= 0:
                await ctx.send("Bet must be positive. No cooldown applied.")
                return
            rem = self._is_on_cooldown(cmd, uid)
            if rem:
                await ctx.send(f"⏳ You are on cooldown for {rem}s.")
                return
            if amount > self.manager.get_gold(uid):
                await ctx.send("Not enough gold. No cooldown applied.")
                return
            r = random.random()
            if r < config.EXTRACONOMY["extragamble_win_chance"]:
                # loss
                self.manager.try_withdraw_gold(uid, amount)
                self._set_last(cmd, uid)
                await ctx.send(f"💸 You lost {amount} gold.")
            elif r < config.EXTRACONOMY["extragamble_win_chance"] + config.EXTRACONOMY["extragamble_jackpot_chance"]:
                # jackpot (2x)
                self.manager.add_gold(uid, amount * 2)
                self._set_last(cmd, uid)
                await ctx.send(f"🎊 JACKPOT! You won {amount * 2} gold (2x profit).")
            else:
                # win (1x)
                self.manager.add_gold(uid, amount)
                self._set_last(cmd, uid)
                await ctx.send(f"🎉 You won {amount} gold (1x profit).")
        except Exception:
            logger.exception("extragamble failed")
            await ctx.send("❌ Gambling failed. No cooldown applied.")

    @commands.command()
    async def jobs(self, ctx):
        try:
            roles = config.EXTRACONOMY["job_application_roles"]
            text = ["📋 Available Jobs:"]
            for cat, rs in roles.items():
                text.append(f"- {cat.title()}: {', '.join(rs)}")
            await ctx.send("\n".join(text))
        except Exception:
            logger.exception("jobs failed")
            await ctx.send("❌ Failed to fetch jobs. No cooldown applied.")

    @commands.command()
    @app_commands.describe(job_type="Job category")
    @app_commands.choices(job_type=[
        app_commands.Choice(name="bank", value="bank"),
        app_commands.Choice(name="police", value="police"),
        app_commands.Choice(name="security", value="security"),
        app_commands.Choice(name="government", value="government"),
        app_commands.Choice(name="military", value="military"),
    ])
    async def job(self, ctx, job_type: Optional[str] = None):
        cmd = "job"
        uid = str(ctx.author.id)
        try:
            if not await self.require_civ(ctx):
                return
            if job_type is None:
                await ctx.send("Usage: /job <job_type>. No cooldown applied.")
                return
            rem = self._is_on_cooldown(cmd, uid)
            if rem:
                await ctx.send(f"⏳ You are on cooldown for {rem}s.")
                return
            jt = job_type.lower()
            mapping = config.EXTRACONOMY["job_application_roles"]
            if jt not in mapping:
                await ctx.send("Invalid job type. No cooldown applied.")
                return
            outcome = random.choice(mapping[jt])
            civ = self.manager._get_civ(uid)
            if civ is not None:
                try:
                    civ['job'] = outcome
                    self.manager._persist_civ(uid, civ)
                except Exception:
                    logger.debug("Could not persist job on civ")
            self._set_last(cmd, uid)
            if outcome == "Rejected":
                await ctx.send(f"😢 Application for {jt.title()} was rejected.")
            else:
                await ctx.send(f"🎉 You are now a {outcome} in {jt.title()}.")
        except Exception:
            logger.exception("job failed")
            await ctx.send("❌ Job application failed. No cooldown applied.")

    @commands.command()
    async def extrawork(self, ctx):
        cmd = "extrawork"
        uid = str(ctx.author.id)
        try:
            if not await self.require_civ(ctx):
                return
            rem = self._is_on_cooldown(cmd, uid)
            if rem:
                await ctx.send(f"⏳ You are on cooldown for {rem}s.")
                return
            civ = self.manager._get_civ(uid)
            job_name = "Unemployed"
            if civ is not None:
                job_name = civ.get("job", job_name)
            if job_name == "Unemployed":
                await ctx.send("You need a job to work. Use /job to get one. No cooldown applied.")
                return
            salary_map = config.EXTRACONOMY["extrawork_salary_multipliers"]
            salary = salary_map.get(job_name, config.EXTRACONOMY["extrawork_base_salary"])
            self.manager.add_gold(uid, salary)
            self._set_last(cmd, uid)
            bal = self.manager.get_gold(uid)
            await ctx.send(f"💼 You earned {salary} gold as a {job_name}. Civ gold: {bal}.")
        except Exception:
            logger.exception("extrawork failed")
            await ctx.send("❌ Work failed. No cooldown applied.")

    @commands.command()
    @app_commands.describe(target="Target user")
    async def arrest(self, ctx, target: Optional[discord.Member] = None):
        cmd = "arrest"
        uid = str(ctx.author.id)
        try:
            if not await self.require_civ(ctx):
                return
            if target is None:
                await ctx.send("Usage: /arrest <user>. No cooldown applied.")
                return
            target_id = str(target.id)
            civ = self.manager._get_civ(uid)
            job = civ.get("job", "") if civ else ""
            if job.lower() not in ["recruit", "officer", "captain", "police"]:
                await ctx.send("🚫 Only police can arrest criminals. No cooldown applied.")
                return
            rem = self._is_on_cooldown(cmd, uid)
            if rem:
                await ctx.send(f"⏳ You are on cooldown for {rem}s.")
                return
            if random.random() < config.EXTRACONOMY["arrest_success_chance"]:
                seize = config.EXTRACONOMY["arrest_seize_amount"]
                if self.manager.try_withdraw_gold(target_id, seize):
                    self.manager.add_gold(uid, seize)
                    self._set_last(cmd, uid)
                    await ctx.send(f"🚓 Arrested {target.mention} and seized {seize} gold!")
                else:
                    self._set_last(cmd, uid)
                    await ctx.send(f"🚓 Arrested {target.mention} but they had no funds.")
            else:
                await ctx.send("❌ Arrest failed. No cooldown applied.")
        except Exception:
            logger.exception("arrest failed")
            await ctx.send("❌ Arrest failed due to an error. No cooldown applied.")

    @commands.command()
    @app_commands.describe(target="Target user")
    async def rob(self, ctx, target: Optional[discord.Member] = None):
        cmd = "rob"
        uid = str(ctx.author.id)
        try:
            if not await self.require_civ(ctx):
                return
            if target is None:
                await ctx.send("Usage: /rob <user>. No cooldown applied.")
                return
            target_id = str(target.id)
            civ = self.manager._get_civ(uid)
            job = civ.get("job", "") if civ else ""
            if job.lower() in ["teller", "manager", "executive", "recruit", "officer", "captain",
                               "guard", "supervisor", "chief", "clerk", "minister", "president",
                               "prime minister", "private", "sergeant", "commander"]:
                await ctx.send("🚫 Only criminals can rob others. No cooldown applied.")
                return
            rem = self._is_on_cooldown(cmd, uid)
            if rem:
                await ctx.send(f"⏳ You are on cooldown for {rem}s.")
                return
            if random.random() < config.EXTRACONOMY["rob_success_chance"]:
                stolen = random.randint(config.EXTRACONOMY["rob_stolen_min"], config.EXTRACONOMY["rob_stolen_max"])
                if self.manager.try_withdraw_gold(target_id, stolen):
                    self.manager.add_gold(uid, stolen)
                    self._set_last(cmd, uid)
                    await ctx.send(f"💸 Robbed {target.mention} for {stolen} gold!")
                else:
                    await ctx.send("Target has insufficient funds. No cooldown applied.")
            else:
                await ctx.send("❌ Robbery failed. No cooldown applied.")
        except Exception:
            logger.exception("rob failed")
            await ctx.send("❌ Rob failed due to an error. No cooldown applied.")

    @commands.command()
    @app_commands.describe(project="Coding project")
    @app_commands.choices(project=[
        app_commands.Choice(name="virus", value="virus"),
        app_commands.Choice(name="website", value="website"),
        app_commands.Choice(name="messenger", value="messenger"),
    ])
    async def code(self, ctx, project: Optional[str] = None):
        cmd = "code"
        uid = str(ctx.author.id)
        try:
            if not await self.require_civ(ctx):
                return
            if project is None:
                projects = config.EXTRACONOMY["coding_projects"]
                lines = ["💻 Coding Projects:"]
                for name, data in projects.items():
                    if name == "messenger":
                        lines.append(f"/code {name} — {data['cost']} gold, finishes in ~{data['duration_seconds']//60} min, creates a product")
                    else:
                        lines.append(f"/code {name} — {data['cost']} gold, finishes in ~{data['duration_seconds']//60} min, rewards gold")
                await ctx.send("\n".join(lines))
                return
            rem = self._is_on_cooldown(cmd, uid)
            if rem:
                await ctx.send(f"⏳ You are on cooldown for {rem}s.")
                return
            p = project.lower()
            projects = config.EXTRACONOMY["coding_projects"]
            if p not in projects:
                await ctx.send("Unknown project. No cooldown applied.")
                return
            data = projects[p]
            cost = data["cost"]
            duration = data["duration_seconds"]
            if not self.manager.try_withdraw_gold(uid, cost):
                await ctx.send("Not enough gold. No cooldown applied.")
                return
            self.coding_tasks[str(uid)] = (p, time.time() + duration)
            self._set_last(cmd, uid)
            await ctx.send(f"🛠️ Started coding {p}. It will finish in approx {duration//60} minutes.")
        except Exception:
            logger.exception("code failed")
            await ctx.send("❌ Code command failed. No cooldown applied.")

    @commands.command()
    async def setbalance(self, ctx, amount: Optional[int] = None):
        uid = str(ctx.author.id)
        try:
            allowed_ids = os.getenv("ADMIN_ALLOWED_IDS", "mpGYeq9d,mL2MM1N4").split(",")
            if str(ctx.author.id) not in allowed_ids:
                await ctx.send("❌ You don't have permission to use this command.")
                return
            if amount is None:
                await ctx.send("Usage: .setbalance <amount>. No cooldown applied.")
                return
            if amount < 0:
                await ctx.send("Amount must be non-negative. No cooldown applied.")
                return
            if not await self.require_civ(ctx):
                return
            self.manager.set_gold(uid, int(amount))
            await ctx.send(f"✅ Civ gold set to {amount}.")
        except Exception:
            logger.exception("setbalance failed")
            await ctx.send("❌ Failed to set balance. No cooldown applied.")


async def setup(bot: commands.Bot, db: Optional[Any] = None, storage_dir: str = "."):
    cog = EconomyCog(bot, db=db, storage_dir=storage_dir)
    await bot.add_cog(cog)
    logger.info("EconomyCog registered (ExtraEconomy).")


__all__ = ["EconomyManager", "EconomyCog", "setup"]
