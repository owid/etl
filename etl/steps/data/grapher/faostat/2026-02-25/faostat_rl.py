"""FAOSTAT grapher step for faostat_rl dataset.

The USSR-breakup entity annotations on this dataset's area indicators are defined in the garden
shared module (USSR_BREAKUP_ENTITY_ANNOTATIONS) and reach this step via metadata inheritance.
"""

from .shared import run  # noqa:F401
