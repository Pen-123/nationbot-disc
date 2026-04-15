import asyncio
import contextlib
import difflib
import logging
import os
import threading

import discord
from dotenv import load_dotenv
from discord.ext import commands

# Ensure existing `@commands.command` decorators are slash-compatible too.
commands.command = commands.hybrid_command

from web.dashboard import app as flask_app
from bot.database import Database
from bot.civilization import CivilizationManager
from bot.commands.basic import BasicCommands
from bot.commands.economy import EconomyCommands
from bot.commands.ExtraEconomy import setup as setup_extra_economy
from bot.commands.military import MilitaryCommands
from bot.commands.diplomacy import DiplomacyCommands
from bot.commands.store import StoreCommands
from bot.commands.hyperitems import HyperItemCommands
from bot.commands.admin import AdminCommands
from bot.events import EventManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('warbot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def get_db_path() -> str:
    return os.getenv("DATABASE_PATH", "warbot.db")

class WarBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.members = True
        intents.guilds = True

        super().__init__(
            command_prefix=commands.when_mentioned_or('.'),
            intents=intents,
            case_insensitive=True
        )
        
        self.db = Database(db_path=get_db_path())
        self.civ_manager = CivilizationManager(self.db)
        self.event_manager = EventManager(self.db)
        self.events_task = None

    async def setup_hook(self):
        try:
            await self.add_cog(BasicCommands(self))

            try:
                await self.add_cog(EconomyCommands(self))
                logger.info("Legacy EconomyCommands cog loaded successfully")
            except Exception as e:
                logger.error(f"Failed to load legacy EconomyCommands cog: {e}")

            try:
                await setup_extra_economy(self, db=self.db, storage_dir="./data")
                logger.info("ExtraEconomy cog loaded successfully")
            except Exception as e:
                logger.error(f"Failed to load ExtraEconomy cog: {e}")

            await self.add_cog(MilitaryCommands(self))
            await self.add_cog(DiplomacyCommands(self))
            await self.add_cog(StoreCommands(self))
            await self.add_cog(HyperItemCommands(self))
            await self.add_cog(AdminCommands(self))
            logger.info("All command cogs loaded successfully")
        except Exception as e:
            logger.error(f"Error loading cogs: {e}")

        if self.events_task is None or self.events_task.done():
            self.events_task = asyncio.create_task(self.event_manager.start_random_events(self))

    async def on_ready(self):
        logger.info(f'{self.user} has connected to Discord!')
        print(f'WarBot is online as {self.user}')

    async def on_message(self, message: discord.Message):
        if message.author == self.user:
            return
        
        # Process commands
        await self.process_commands(message)

    def _get_command_suggestions(self, attempted: str, limit: int = 5):
        """Return closest command names for mistyped prefix commands."""
        if not attempted:
            return []

        attempted = attempted.lower().strip()
        all_names = set()
        for cmd in self.commands:
            all_names.add(cmd.name.lower())
            for alias in getattr(cmd, "aliases", []):
                all_names.add(alias.lower())

        return difflib.get_close_matches(attempted, sorted(all_names), n=limit, cutoff=0.45)

    async def on_command_error(self, ctx, error):
        """Friendly command errors for prefix command usage."""
        if hasattr(ctx.command, "on_error"):
            return

        if isinstance(error, commands.CommandNotFound):
            attempted = (ctx.invoked_with or "").strip()
            suggestions = self._get_command_suggestions(attempted)
            if suggestions:
                suggested_text = "\n".join([f"• `/{name}` or `.{name}`" for name in suggestions])
                await ctx.send(
                    f"❌ Command `.{attempted}` not found.\n"
                    f"Did you mean:\n{suggested_text}\n\n"
                    "Use `/warhelp` (or `.warhelp`) to browse commands."
                )
            else:
                await ctx.send(
                    f"❌ Command `.{attempted}` not found.\n"
                    "Use `/warhelp` (or `.warhelp`) to browse commands."
                )
            return

        if isinstance(error, commands.MissingRequiredArgument):
            await ctx.send(
                f"❌ Missing required argument: `{error.param.name}`.\n"
                "Use `/warhelp` (or `.warhelp`) for usage examples."
            )
            return

        if isinstance(error, commands.BadArgument):
            await ctx.send(
                "❌ Invalid argument type or value.\n"
                "Please check command usage with `/warhelp`."
            )
            return

        if isinstance(error, commands.CheckFailure):
            await ctx.send("❌ You don't have permission to use that command.")
            return

        if isinstance(error, commands.CommandOnCooldown):
            await ctx.send(
                f"⏳ This command is on cooldown. Try again in `{error.retry_after:.1f}s`."
            )
            return

        logger.error(f"Unhandled command error in '{ctx.invoked_with}': {error}", exc_info=True)
        await ctx.send("❌ Something went wrong while running that command. Please try again.")



def start_flask_server():
    """Start the Flask web dashboard in a separate thread"""
    try:
        port = int(os.getenv("PORT", "5000"))
        logger.info(f"Starting Flask dashboard on port {port}")
        flask_app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)
    except Exception as e:
        logger.error(f"Failed to start Flask server: {e}", exc_info=True)


async def run_discord_bot():
    """Start and supervise Discord bot connection."""
    token = os.getenv('DISCORD_BOT_TOKEN')
    if not token:
        logger.warning("DISCORD_BOT_TOKEN is not set. Dashboard will run without the Discord bot.")
        return

    reconnect_delay = 10
    while True:
        bot = WarBot()
        try:
            await bot.start(token)
            logger.warning("Discord bot stopped unexpectedly; restarting shortly.")
        except discord.LoginFailure:
            logger.error("Invalid DISCORD_BOT_TOKEN. Fix the token and redeploy.")
            return
        except discord.PrivilegedIntentsRequired:
            logger.error("Privileged intents are disabled in the Discord Developer Portal for this bot.")
            return
        except Exception as e:
            logger.error(f"Discord bot crashed: {e}", exc_info=True)
        finally:
            if bot.events_task and not bot.events_task.done():
                bot.events_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await bot.events_task
            with contextlib.suppress(Exception):
                await bot.close()
        await asyncio.sleep(reconnect_delay)

async def main():
    """Main function to start the bot"""
    load_dotenv()

    # Start Flask server in background thread
    flask_thread = threading.Thread(target=start_flask_server, daemon=False)
    flask_thread.start()
    logger.info("Startup complete: Flask thread launched")

    await run_discord_bot()

    # Keep process alive in dashboard-only mode.
    if flask_thread.is_alive():
        await asyncio.to_thread(flask_thread.join)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot shutdown requested")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
