"""Slash command system for browser UI (/refresh, /exit, /steps, etc)."""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Callable, Literal

if TYPE_CHECKING:
    from etl.browser.core import BrowserState


@dataclass
class CommandResult:
    """Result of executing a command."""

    action: Literal["continue", "exit", "refresh", "switch_mode", "help"]
    message: str | None = None
    target_mode: str | None = None  # For switch_mode action


@dataclass
class Command:
    """A command that can be executed in the browser."""

    name: str
    description: str
    handler: Callable[["BrowserState"], CommandResult]
    aliases: list[str] = field(default_factory=list)
    group: str = "action"  # "mode" for mode-switch commands, "action" for others


def filter_commands(pattern: str, commands: list[Command]) -> list[Command]:
    """Filter commands by name/alias prefix match.

    Args:
        pattern: The pattern to match (without leading /)
        commands: List of available commands

    Returns:
        Commands that match the pattern, sorted with modes first
    """
    if not pattern:
        # Sort: modes first, then actions (matching visual display order)
        return sorted(commands, key=lambda c: (0 if c.group == "mode" else 1, c.name))

    pattern_lower = pattern.lower()
    matches = []
    for cmd in commands:
        names = [cmd.name] + cmd.aliases
        if any(n.startswith(pattern_lower) for n in names):
            matches.append(cmd)

    # Sort: modes first, then actions (matching visual display order)
    return sorted(matches, key=lambda c: (0 if c.group == "mode" else 1, c.name))


# Default command handlers


def cmd_refresh(state: "BrowserState") -> CommandResult:
    """Handler for /refresh command."""
    return CommandResult(action="refresh", message="Refreshing...")


def cmd_exit(state: "BrowserState") -> CommandResult:
    """Handler for /exit command."""
    return CommandResult(action="exit")


def cmd_help(state: "BrowserState") -> CommandResult:
    """Handler for /help command."""
    return CommandResult(action="help")


# Default commands available in all browsers
DEFAULT_COMMANDS = [
    Command("help", "Show available modes and commands", cmd_help, aliases=["h", "?"]),
    Command("refresh", "Reload cached data", cmd_refresh, aliases=["r"]),
    Command("exit", "Exit browser", cmd_exit, aliases=["quit", "q"]),
]
