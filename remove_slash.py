#!/usr/bin/env python3
import os
import re
import glob

# Define the folder containing your command cogs
COGS_DIR = "bot/commands"

# Patterns to remove
patterns = [
    # Remove @app_commands.describe, @app_commands.choices, @app_commands.autocomplete lines
    (re.compile(r'^\s*@app_commands\.(describe|choices|autocomplete)\([^)]*\)\s*\n', re.MULTILINE), ''),
    # Remove @app_commands.checks or other app_commands decorators
    (re.compile(r'^\s*@app_commands\.[a-zA-Z_]+\([^)]*\)\s*\n', re.MULTILINE), ''),
    # Replace @commands.hybrid_command with @commands.command
    (re.compile(r'@commands\.hybrid_command', re.MULTILINE), '@commands.command'),
    # Remove any lone @app_commands.import (we'll also remove the import later)
]

# Also remove the import line for app_commands if it's not used elsewhere
# We'll handle that separately

def clean_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    # Apply replacements
    for pattern, replacement in patterns:
        content = pattern.sub(replacement, content)

    # Remove 'from discord import app_commands' if it exists and app_commands is not used elsewhere
    # We'll just remove the line if it's a standalone import
    content = re.sub(r'^from discord import app_commands\s*\n', '', content, flags=re.MULTILINE)
    # Remove 'import discord.app_commands' if any
    content = re.sub(r'^import discord\.app_commands\s*\n', '', content, flags=re.MULTILINE)

    # Remove leftover @app_commands that might be on same line as def (rare)
    content = re.sub(r'@app_commands\.[a-zA-Z_]+\([^)]*\)\s+async def', 'async def', content)

    # Write back
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    print(f"Cleaned {filepath}")

def main():
    # Find all .py files in the commands folder (recursively)
    for root, dirs, files in os.walk(COGS_DIR):
        for file in files:
            if file.endswith('.py'):
                clean_file(os.path.join(root, file))

if __name__ == "__main__":
    main()
