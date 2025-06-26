from dataclasses import dataclass
from typing import Callable, ClassVar, List, TypeGuard

from etl.collection.exceptions import DuplicateValuesError, MissingChoiceError
from etl.collection.model.base import MDIMBase, pruned_json


@pruned_json
@dataclass
class DimensionChoice(MDIMBase):
    slug: str
    name: str
    description: str | None = None
    group: str | None = None


@dataclass(frozen=True)
class DimensionPresentationUIType:
    DROPDOWN: ClassVar[str] = "dropdown"
    CHECKBOX: ClassVar[str] = "checkbox"
    RADIO: ClassVar[str] = "radio"
    TEXT_AREA: ClassVar[str] = "text_area"  # Adding a new type automatically works!

    # Compute the list once at class definition time
    ALL: ClassVar[List[str]] = [
        value for key, value in vars().items() if not key.startswith("__") and isinstance(value, str)
    ]

    @classmethod
    def is_valid(cls, value: str) -> TypeGuard[str]:
        return value in cls.ALL


@pruned_json
@dataclass
class DimensionPresentation(MDIMBase):
    type: str
    choice_slug_true: str | None = None

    def __post_init__(self):
        if not DimensionPresentationUIType.is_valid(self.type):
            raise ValueError(f"Invalid type: {self.type}. Accepted values: {DimensionPresentationUIType.ALL}")
        if (self.type == DimensionPresentationUIType.CHECKBOX) and (self.choice_slug_true is None):
            raise ValueError(
                f"`choice_slug_true` slug must be provided for '{DimensionPresentationUIType.CHECKBOX}' type."
            )


@pruned_json
@dataclass
class Dimension(MDIMBase):
    """MDIM/Explorer dimension configuration."""

    slug: str
    name: str
    choices: List[DimensionChoice]
    description: str | None = None
    presentation: DimensionPresentation | None = None

    def __post_init__(self):
        """Validations."""

        # Checks when presentation is checkbox
        if self.ui_type == DimensionPresentationUIType.CHECKBOX:
            assert self.presentation is not None, "Presentation must be provided for 'checkbox' type."

            # Choices must be exactly two
            if (num_choices := len(self.choice_slugs)) != 2:
                raise ValueError(
                    f"Dimension choices for '{DimensionPresentationUIType.CHECKBOX}' must have exactly two choices. Instead, found {num_choices} choices."
                )

            # True slug must be provided, and must be a valid choice
            # assert self.presentation.choice_slug_true is not None
            if self.presentation.choice_slug_true not in self.choice_slugs:
                raise ValueError(f"True slug '{self.presentation.choice_slug_true}' must be one of the choices.")

    @property
    def ui_type(self):
        if self.presentation is None:
            return DimensionPresentationUIType.DROPDOWN
        return self.presentation.type

    @property
    def choice_slugs(self) -> List[str]:
        # if self.choices is not None:
        return [choice.slug for choice in self.choices]

    @property
    def ppt(self):
        return self.presentation

    def sort_choices(self, slug_order: List[str] | Callable):
        """Sort choices based on the given order.

        Args:
        slug_order: List[str] | Callable
            If a list, it must contain all the slugs in the desired order. If a callable, this callable will be applied to the choice slugs to sort them.
        """
        choice_slugs = self.choice_slugs
        if callable(slug_order):
            slug_order_ = slug_order(choice_slugs)
        else:
            slug_order_ = slug_order

        # Make sure all choices are in the given order
        choices_missing = set(choice_slugs) - set(slug_order_)
        if choices_missing:
            raise MissingChoiceError(
                f"All choices for dimension {self.slug} must be in the given order! Missing: {choices_missing}"
            )

        # Create a dictionary to map slugs to their positions for faster sorting
        slug_position = {slug: index for index, slug in enumerate(slug_order_)}

        # Sort based on your desired slug order
        self.choices.sort(key=lambda choice: slug_position.get(choice.slug, float("inf")))

    def validate_choice_names_unique(self):
        """Validate that all choice names are unique."""
        # TODO: Check if (name, group) is unique instead of just name
        names = [choice.name for choice in self.choices]
        if len(names) != len(set(names)):
            raise DuplicateValuesError(f"Dimension choices for '{self.slug}' must have unique names!")

    def validate_choice_slugs_unique(self):
        """Validate that all choice names are unique."""
        slug = [choice.slug for choice in self.choices]
        if len(slug) != len(set(slug)):
            raise DuplicateValuesError(
                f"Dimension choices for '{self.slug}' must have unique names! Review {self.choice_slugs}"
            )
