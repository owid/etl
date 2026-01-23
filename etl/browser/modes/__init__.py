#
#  modes/__init__.py
#  Browser mode system - protocol and registry for multi-mode browser
#

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Protocol, Tuple

if TYPE_CHECKING:
    from etl.browser.commands import Command
    from etl.browser.core import BrowserState

# Type alias for ranker function
RankerFunc = Callable[[str, List[str]], List[str]]


@dataclass
class ModeConfig:
    """Configuration for a browser mode."""

    name: str  # Mode identifier (e.g., "steps", "snapshots")
    prompt: str  # Prompt text (e.g., "etlr> ")
    item_noun: str  # Singular noun (e.g., "step")
    item_noun_plural: str  # Plural noun (e.g., "steps")
    loading_message: str = "Loading..."
    empty_message: str = "No items found."


@dataclass
class ModeResult:
    """Result from selecting an item in a mode."""

    action: str  # "run", "switch_mode", "exit"
    value: Optional[str] = None  # Selected item or target mode
    is_exact: bool = False  # True if exact item selected, False if pattern


class BrowserMode(Protocol):
    """Protocol defining the interface for browser modes.

    Each mode provides:
    - Configuration (prompt, nouns, messages)
    - Items to browse
    - Optional ranker for match ordering
    - Selection handler
    """

    @property
    def config(self) -> ModeConfig:
        """Mode configuration."""
        ...

    def get_items_loader(self) -> Callable[[], List[str]]:
        """Return a callable that loads items for this mode."""
        ...

    def get_cached_items(self) -> Optional[List[str]]:
        """Return cached items if available, None otherwise."""
        ...

    def get_ranker(self) -> Optional[RankerFunc]:
        """Return optional ranker for match ordering."""
        ...

    def get_commands(self) -> List["Command"]:
        """Return commands available in this mode."""
        ...

    def on_items_loaded(self, items: List[str]) -> None:
        """Called when items finish loading (for caching)."""
        ...

    def on_select(self, item: str, is_exact: bool) -> ModeResult:
        """Handle item selection.

        Args:
            item: Selected item or search pattern
            is_exact: True if specific item selected, False if pattern

        Returns:
            ModeResult indicating what action to take
        """
        ...

    def get_history(self) -> List[str]:
        """Get browsing history for this mode."""
        ...

    def save_history(self, history: List[str]) -> None:
        """Save browsing history for this mode."""
        ...


@dataclass
class ModeRegistry:
    """Registry for browser modes.

    Manages mode registration and provides mode-switching commands.
    """

    _modes: Dict[str, BrowserMode] = field(default_factory=dict)
    _default_mode: Optional[str] = None

    def register(self, mode: BrowserMode, default: bool = False) -> None:
        """Register a browser mode.

        Args:
            mode: The mode to register
            default: If True, this mode becomes the default
        """
        name = mode.config.name
        self._modes[name] = mode
        if default or self._default_mode is None:
            self._default_mode = name

    def get(self, name: str) -> Optional[BrowserMode]:
        """Get a mode by name."""
        return self._modes.get(name)

    def get_default(self) -> Optional[BrowserMode]:
        """Get the default mode."""
        if self._default_mode:
            return self._modes.get(self._default_mode)
        return None

    @property
    def default_mode_name(self) -> Optional[str]:
        """Get the name of the default mode."""
        return self._default_mode

    def list_modes(self) -> List[Tuple[str, ModeConfig]]:
        """List all registered modes with their configs."""
        return [(name, mode.config) for name, mode in self._modes.items()]

    def get_mode_switch_commands(self) -> List["Command"]:
        """Generate commands for switching between modes.

        Creates a /{mode_name} command for each registered mode.
        """
        from etl.browser.commands import Command, CommandResult

        commands: List["Command"] = []

        for name, mode in self._modes.items():
            config = mode.config

            def make_handler(mode_name: str) -> Callable[["BrowserState"], CommandResult]:
                def handler(state: "BrowserState") -> CommandResult:
                    return CommandResult(action="switch_mode", target_mode=mode_name)

                return handler

            cmd = Command(
                name=name,
                description=f"Switch to {config.item_noun_plural} browser",
                handler=make_handler(name),
            )
            commands.append(cmd)

        return commands


# Global registry instance
_registry: Optional[ModeRegistry] = None


def get_registry() -> ModeRegistry:
    """Get the global mode registry, creating it if needed."""
    global _registry
    if _registry is None:
        _registry = ModeRegistry()
    return _registry


def register_mode(mode: BrowserMode, default: bool = False) -> None:
    """Register a mode with the global registry."""
    get_registry().register(mode, default=default)
