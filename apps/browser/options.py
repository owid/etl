"""CLI options support for browser UI.

Provides option extraction from Click commands and state management
for browser options that can be toggled via @ prefix.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import click


@dataclass
class BrowserOption:
    """A single option that can be toggled/set in the browser.

    Attributes:
        name: Python parameter name (e.g., "dry_run")
        flag_name: CLI flag name without -- (e.g., "dry-run")
        is_flag: Whether this is a boolean flag
        default: Default value for the option
        help: Help text describing the option
        value_type: Type of value (for non-flag options)
    """

    name: str
    flag_name: str
    is_flag: bool
    default: Any
    help: str
    value_type: type | None = None


def extract_options_from_click(cmd: click.Command) -> list[BrowserOption]:
    """Extract option metadata from a Click command.

    Args:
        cmd: A Click command to introspect

    Returns:
        List of BrowserOption objects representing the command's options
    """
    options: list[BrowserOption] = []

    for param in cmd.params:
        if not isinstance(param, click.Option):
            continue

        # Get the primary option name (without -- prefix)
        # opts like ['--dry-run'] or ['-f', '--force']
        flag_name = None
        for opt in param.opts:
            if opt.startswith("--"):
                flag_name = opt[2:]  # Remove --
                break

        if flag_name is None:
            # Skip options without a long form
            continue

        # Determine value type for non-flags
        value_type = None
        if not param.is_flag and param.type is not None:
            if isinstance(param.type, click.types.IntParamType):
                value_type = int
            elif isinstance(param.type, click.types.FloatParamType):
                value_type = float
            elif isinstance(param.type, click.types.StringParamType):
                value_type = str

        options.append(
            BrowserOption(
                name=param.name or flag_name.replace("-", "_"),
                flag_name=flag_name,
                is_flag=param.is_flag,
                default=param.default,
                help=param.help or "",
                value_type=value_type,
            )
        )

    return options


@dataclass
class OptionsState:
    """State container for browser options.

    Manages user-set values, auto-detected values, and available options.
    """

    # User-set option values (override auto-detected)
    user_options: dict[str, Any] = field(default_factory=dict)

    # Auto-detected option values (e.g., grapher=True for grapher:// steps)
    auto_options: dict[str, Any] = field(default_factory=dict)

    # Available options for current mode
    available_options: list[BrowserOption] = field(default_factory=list)

    def get_effective(self) -> dict[str, Any]:
        """Get effective options (user values override auto-detected).

        Returns:
            Dictionary of option names to their effective values
        """
        result: dict[str, Any] = {}

        # Start with defaults
        for opt in self.available_options:
            result[opt.name] = opt.default

        # Apply auto-detected values
        result.update(self.auto_options)

        # Apply user-set values (highest priority)
        result.update(self.user_options)

        return result

    def get_option_by_flag(self, flag_name: str) -> BrowserOption | None:
        """Find an option by its flag name.

        Args:
            flag_name: The flag name (with or without -- prefix)

        Returns:
            The matching BrowserOption, or None if not found
        """
        # Normalize: remove -- prefix if present
        if flag_name.startswith("--"):
            flag_name = flag_name[2:]

        for opt in self.available_options:
            if opt.flag_name == flag_name:
                return opt

        return None

    def toggle_flag(self, flag_name: str) -> bool:
        """Toggle a boolean flag option.

        Args:
            flag_name: The flag name to toggle

        Returns:
            True if successfully toggled, False if not a flag or not found
        """
        opt = self.get_option_by_flag(flag_name)
        if opt is None or not opt.is_flag:
            return False

        current = self.user_options.get(opt.name, self.auto_options.get(opt.name, opt.default))
        self.user_options[opt.name] = not current
        return True

    def set_value(self, flag_name: str, value: Any) -> bool:
        """Set a value option.

        Args:
            flag_name: The flag name to set
            value: The value to set

        Returns:
            True if successfully set, False if not found or type mismatch
        """
        opt = self.get_option_by_flag(flag_name)
        if opt is None:
            return False

        # For flags, convert value to bool
        if opt.is_flag:
            if isinstance(value, bool):
                self.user_options[opt.name] = value
            elif isinstance(value, str):
                self.user_options[opt.name] = value.lower() in ("true", "on", "yes", "1")
            else:
                return False
        else:
            # Try to convert to the expected type
            try:
                if opt.value_type is not None:
                    value = opt.value_type(value)
                self.user_options[opt.name] = value
            except (ValueError, TypeError):
                return False

        return True

    def reset(self) -> None:
        """Reset all user-set options to defaults."""
        self.user_options.clear()

    def get_active_flags(self) -> list[str]:
        """Get list of active flag names for status display.

        Returns:
            List of flag names that are currently ON (True)
        """
        active: list[str] = []
        effective = self.get_effective()

        for opt in self.available_options:
            if opt.is_flag and effective.get(opt.name, False):
                active.append(opt.flag_name)

        return active

    def get_status_text(self) -> str:
        """Get status text showing active options.

        Returns:
            String like "@dry-run @workers 4" or empty string if no active options
        """
        parts: list[str] = []
        effective = self.get_effective()

        for opt in self.available_options:
            value = effective.get(opt.name)
            if opt.is_flag:
                # Show active flags
                if value:
                    parts.append(f"@{opt.flag_name}")
            else:
                # Show non-flag options if different from default
                if value != opt.default:
                    parts.append(f"@{opt.flag_name} {value}")

        return " ".join(parts)


def filter_options(pattern: str, options: list[BrowserOption]) -> list[BrowserOption]:
    """Filter options by prefix match on flag name.

    Args:
        pattern: The pattern to match (without @ prefix)
        options: List of available options

    Returns:
        Options that match the pattern
    """
    if not pattern:
        return options

    pattern_lower = pattern.lower()
    return [opt for opt in options if opt.flag_name.lower().startswith(pattern_lower)]


def parse_option_input(text: str) -> tuple[str | None, str | None]:
    """Parse option input like "@dry-run" or "@workers 4".

    Args:
        text: Input text starting with @

    Returns:
        Tuple of (flag_name, value) where value is None for flags
    """
    if not text.startswith("@"):
        return None, None

    text = text[1:].strip()  # Remove @ prefix
    if not text:
        return None, None

    # Split on whitespace for value options
    parts = text.split(None, 1)
    flag_name = parts[0]
    value = parts[1] if len(parts) > 1 else None

    return flag_name, value


@dataclass
class OptionToken:
    """A parsed option token from input."""

    flag_name: str
    value: str | None = None
    start_pos: int = 0  # Position in input string
    end_pos: int = 0


def parse_option_tokens(text: str) -> list[OptionToken]:
    """Parse all @option tokens from input text.

    Supports multiple options like "@dry-run @force @workers 4".

    Args:
        text: Input text potentially containing multiple @options

    Returns:
        List of OptionToken objects
    """
    tokens: list[OptionToken] = []
    i = 0

    while i < len(text):
        # Find next @
        at_pos = text.find("@", i)
        if at_pos == -1:
            break

        # Find the end of this token (next @ or end of string)
        next_at = text.find("@", at_pos + 1)
        if next_at == -1:
            token_text = text[at_pos + 1 :].strip()
            end_pos = len(text)
        else:
            token_text = text[at_pos + 1 : next_at].strip()
            end_pos = next_at

        if token_text:
            # Split into flag_name and optional value
            parts = token_text.split(None, 1)
            flag_name = parts[0]
            value = parts[1] if len(parts) > 1 else None

            tokens.append(
                OptionToken(
                    flag_name=flag_name,
                    value=value,
                    start_pos=at_pos,
                    end_pos=end_pos,
                )
            )

        i = end_pos if next_at == -1 else next_at

    return tokens


def get_current_option_token(text: str, cursor_pos: int) -> tuple[str, int, int]:
    """Get the option token at the cursor position.

    Args:
        text: Input text
        cursor_pos: Cursor position in the text

    Returns:
        Tuple of (partial_flag_name, token_start, token_end)
        Returns empty string if cursor is not in an option token
    """
    # Find the @ that starts the current token
    # Search backwards from cursor
    at_pos = text.rfind("@", 0, cursor_pos + 1)
    if at_pos == -1:
        return "", 0, 0

    # Find the end of this token (next @ or end of string)
    next_at = text.find("@", at_pos + 1)
    if next_at == -1:
        token_end = len(text)
    else:
        token_end = next_at

    # Extract the partial token (from @ to cursor or first space after flag)
    token_text = text[at_pos + 1 : cursor_pos]

    # Get just the flag name part (before any space)
    parts = token_text.split(None, 1)
    partial_flag = parts[0] if parts else ""

    return partial_flag, at_pos, token_end
