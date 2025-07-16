"""
Utility functions for the OWID MCP server.
"""

import math


def smart_round(value: float | None) -> float | None:
    """Apply smart rounding to reduce context waste while preserving meaningful precision.

    Args:
        value: The numeric value to round, or None

    Returns:
        Rounded value according to smart rounding rules, or None if input is None

    Rounding rules:
    - None values: returned as-is
    - Integers: preserved as integers
    - Very small numbers (< 0.001): rounded to 4 significant digits
    - Small numbers (0.001 - 1): rounded to 3 decimal places
    - Medium numbers (1 - 1000): rounded to 2 decimal places
    - Large numbers (1000 - 10000): rounded to 1 decimal place
    - Very large numbers (>10000): rounded to integers
    """
    if value is None:
        return None

    # Check if it's already an integer (no fractional part)
    if value == int(value):
        return int(value)

    abs_val = abs(value)

    # Very small numbers (< 0.001): round to 4 significant digits
    if abs_val < 0.001:
        if abs_val == 0:
            return 0
        # Find the order of magnitude
        order = math.floor(math.log10(abs_val))
        precision = 3 - order  # 4 significant digits
        rounded = round(value, min(precision, 15))  # Cap at 15 decimal places
        # If rounding results in 0, return the original value with scientific notation
        if rounded == 0:
            return value
        return rounded

    # Small numbers (0.001 - 1): round to 3 decimal places
    elif abs_val < 1:
        return round(value, 3)

    # Medium numbers (1 - 1000): round to 2 decimal places
    elif abs_val < 1000:
        return round(value, 2)

    # Large numbers (1000+): round to 1 decimal place
    elif abs_val < 10000:
        return round(value, 1)

    # Very large numbers: round to nearest integer
    else:
        return round(value)
