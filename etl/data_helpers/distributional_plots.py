"""Distributional plotting utilities with tail fader functionality."""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Optional, Tuple, Union, List, Any
from matplotlib.colors import LinearSegmentedColormap
import warnings


def distributional_plots(
    data: Union[pd.DataFrame, pd.Series, np.ndarray],
    tail_fader: bool = True,
    percentile_low: float = 1.0,
    percentile_high: float = 99.0,
    figsize: Tuple[int, int] = (10, 6),
    alpha: float = 0.7,
    color: str = "blue",
    **kwargs,
) -> plt.Figure:
    """
    Create distributional plots with optional tail fader functionality.

    This function generates kernel density estimation (KDE) plots with the ability to
    fade the tails of the distribution based on specified percentiles.

    Parameters
    ----------
    data : pd.DataFrame, pd.Series, or np.ndarray
        The data to plot. If DataFrame, will plot all numeric columns.
    tail_fader : bool, default True
        Whether to apply tail fading effect.
    percentile_low : float, default 1.0
        Lower percentile threshold for tail fading (0-100).
    percentile_high : float, default 99.0
        Upper percentile threshold for tail fading (0-100).
    figsize : tuple, default (10, 6)
        Figure size as (width, height).
    alpha : float, default 0.7
        Base alpha (transparency) value for the plots.
    color : str, default "blue"
        Base color for the plots.
    **kwargs
        Additional keyword arguments passed to seaborn.kdeplot.

    Returns
    -------
    plt.Figure
        The matplotlib figure object.

    Examples
    --------
    >>> import numpy as np
    >>> data = np.random.normal(0, 1, 1000)
    >>> fig = distributional_plots(data, tail_fader=True, percentile_low=5, percentile_high=95)
    >>> plt.show()
    """
    # Validate percentiles
    if not (0 <= percentile_low < percentile_high <= 100):
        raise ValueError("percentile_low must be < percentile_high, and both must be between 0 and 100")

    # Convert input data to appropriate format
    if isinstance(data, pd.DataFrame):
        # Select only numeric columns
        numeric_data = data.select_dtypes(include=[np.number])
        if numeric_data.empty:
            raise ValueError("No numeric columns found in DataFrame")
        data_to_plot = [numeric_data[col].dropna() for col in numeric_data.columns]
        labels = list(numeric_data.columns)
    elif isinstance(data, pd.Series):
        data_to_plot = [data.dropna()]
        labels = [data.name if data.name else "data"]
    else:
        data_array = np.asarray(data)
        data_to_plot = [data_array[~np.isnan(data_array)]]
        labels = ["data"]

    # Create figure
    fig, ax = plt.subplots(figsize=figsize)

    # Plot each distribution
    for i, (series_data, label) in enumerate(zip(data_to_plot, labels)):
        if len(series_data) < 2:
            warnings.warn(f"Skipping {label}: insufficient data points")
            continue

        if tail_fader:
            _plot_with_tail_fader(
                ax, series_data, label, percentile_low, percentile_high, alpha, color, i, len(data_to_plot), **kwargs
            )
        else:
            # Standard KDE plot without fading
            sns.kdeplot(data=series_data, label=label, alpha=alpha, ax=ax, **kwargs)

    ax.set_xlabel("Value")
    ax.set_ylabel("Density")
    ax.legend()
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    return fig


def distributional_plots_per_row(
    data: pd.DataFrame,
    tail_fader: bool = True,
    percentile_low: float = 1.0,
    percentile_high: float = 99.0,
    figsize: Tuple[int, int] = (12, 8),
    alpha: float = 0.7,
    color: str = "blue",
    **kwargs,
) -> plt.Figure:
    """
    Create distributional plots with one subplot per row (column) of data.

    This function generates separate KDE plots for each numeric column in the DataFrame,
    with optional tail fading functionality.

    Parameters
    ----------
    data : pd.DataFrame
        The DataFrame to plot. Each numeric column will get its own subplot.
    tail_fader : bool, default True
        Whether to apply tail fading effect.
    percentile_low : float, default 1.0
        Lower percentile threshold for tail fading (0-100).
    percentile_high : float, default 99.0
        Upper percentile threshold for tail fading (0-100).
    figsize : tuple, default (12, 8)
        Figure size as (width, height).
    alpha : float, default 0.7
        Base alpha (transparency) value for the plots.
    color : str, default "blue"
        Base color for the plots.
    **kwargs
        Additional keyword arguments passed to seaborn.kdeplot.

    Returns
    -------
    plt.Figure
        The matplotlib figure object.

    Examples
    --------
    >>> import pandas as pd
    >>> import numpy as np
    >>> data = pd.DataFrame({
    ...     'var1': np.random.normal(0, 1, 1000),
    ...     'var2': np.random.exponential(1, 1000),
    ...     'var3': np.random.gamma(2, 2, 1000)
    ... })
    >>> fig = distributional_plots_per_row(data, tail_fader=True)
    >>> plt.show()
    """
    if not isinstance(data, pd.DataFrame):
        raise ValueError("data must be a pandas DataFrame")

    # Validate percentiles
    if not (0 <= percentile_low < percentile_high <= 100):
        raise ValueError("percentile_low must be < percentile_high, and both must be between 0 and 100")

    # Select only numeric columns
    numeric_data = data.select_dtypes(include=[np.number])
    if numeric_data.empty:
        raise ValueError("No numeric columns found in DataFrame")

    n_cols = len(numeric_data.columns)
    n_rows = (n_cols + 1) // 2  # Two columns of subplots

    # Create figure with subplots
    fig, axes = plt.subplots(n_rows, 2, figsize=figsize)
    if n_rows == 1:
        axes = axes.reshape(1, -1)

    # Flatten axes for easy iteration
    axes_flat = axes.flatten()

    # Plot each column
    for i, col in enumerate(numeric_data.columns):
        ax = axes_flat[i]
        series_data = numeric_data[col].dropna()

        if len(series_data) < 2:
            ax.text(0.5, 0.5, f"Insufficient data\nfor {col}", ha="center", va="center", transform=ax.transAxes)
            ax.set_title(col)
            continue

        if tail_fader:
            _plot_with_tail_fader(ax, series_data, col, percentile_low, percentile_high, alpha, color, 0, 1, **kwargs)
        else:
            # Standard KDE plot without fading
            sns.kdeplot(data=series_data, alpha=alpha, ax=ax, color=color, **kwargs)

        ax.set_title(col)
        ax.set_xlabel("Value")
        ax.set_ylabel("Density")
        ax.grid(True, alpha=0.3)

    # Hide unused subplots
    for i in range(n_cols, len(axes_flat)):
        axes_flat[i].set_visible(False)

    plt.tight_layout()
    return fig


def _plot_with_tail_fader(
    ax: plt.Axes,
    data: np.ndarray,
    label: str,
    percentile_low: float,
    percentile_high: float,
    alpha: float,
    base_color: str,
    color_idx: int,
    total_colors: int,
    **kwargs,
) -> None:
    """
    Plot a KDE with tail fading effect.

    This internal function creates a KDE plot where the tails beyond the specified
    percentiles fade smoothly to transparent.

    Parameters
    ----------
    ax : plt.Axes
        The axes to plot on.
    data : np.ndarray
        The data to plot.
    label : str
        Label for the plot.
    percentile_low : float
        Lower percentile threshold for fading.
    percentile_high : float
        Upper percentile threshold for fading.
    alpha : float
        Base alpha value.
    base_color : str
        Base color for the plot.
    color_idx : int
        Index for color variation when plotting multiple series.
    total_colors : int
        Total number of series being plotted.
    **kwargs
        Additional keyword arguments for kdeplot.
    """
    # Calculate percentile values from the original data
    p_low = np.percentile(data, percentile_low)
    p_high = np.percentile(data, percentile_high)

    # Get color for this series
    if total_colors > 1:
        colors = plt.cm.tab10(np.linspace(0, 1, total_colors))
        color = colors[color_idx]
    else:
        color = base_color

    # Create KDE plot to get the curve data
    kde_plot = sns.kdeplot(data=data, ax=ax, alpha=0, **kwargs)  # Invisible plot to get data

    # Extract the line data from the plot
    line = ax.lines[-1]
    x_kde = line.get_xdata()
    y_kde = line.get_ydata()

    # Remove the invisible line
    line.remove()

    # Create segments with different alpha values
    n_points = len(x_kde)
    alphas = np.ones(n_points) * alpha

    # Calculate fade factors for the tails
    for i in range(n_points):
        x_val = x_kde[i]
        if x_val < p_low:
            # Left tail fading
            fade_distance = (p_low - x_val) / (p_low - x_kde.min()) if p_low != x_kde.min() else 1
            fade_factor = np.exp(-3 * fade_distance)  # Exponential fade
            alphas[i] = alpha * fade_factor
        elif x_val > p_high:
            # Right tail fading
            fade_distance = (x_val - p_high) / (x_kde.max() - p_high) if x_kde.max() != p_high else 1
            fade_factor = np.exp(-3 * fade_distance)  # Exponential fade
            alphas[i] = alpha * fade_factor

    # Plot the KDE with varying alpha
    # We'll use a collection of line segments with different alphas
    from matplotlib.collections import LineCollection

    # Create line segments
    points = np.array([x_kde, y_kde]).T.reshape(-1, 1, 2)
    segments = np.concatenate([points[:-1], points[1:]], axis=1)

    # Create line collection with varying alpha
    lc = LineCollection(segments, colors=color, alpha=alphas[:-1], linewidths=kwargs.get("linewidth", 1.5))
    ax.add_collection(lc)

    # Add area under curve with fading
    # Split into segments and fill each with appropriate alpha
    segment_size = max(1, n_points // 50)  # Adjust for performance
    for i in range(0, n_points - segment_size, segment_size):
        end_idx = min(i + segment_size, n_points)
        x_segment = x_kde[i:end_idx]
        y_segment = y_kde[i:end_idx]
        alpha_segment = np.mean(alphas[i:end_idx]) * 0.3  # Lighter for fill

        ax.fill_between(x_segment, y_segment, alpha=alpha_segment, color=color)

    # Add a legend entry with a regular line
    ax.plot([], [], color=color, alpha=alpha, linewidth=kwargs.get("linewidth", 1.5), label=label)


def run() -> None:
    """
    Example usage of the distributional plotting functions.

    This function demonstrates how to use both distributional_plots and
    distributional_plots_per_row with various configurations.
    """
    # Generate sample data
    np.random.seed(42)
    n_samples = 1000

    # Create sample datasets with different distributions
    data = pd.DataFrame(
        {
            "normal": np.random.normal(0, 1, n_samples),
            "exponential": np.random.exponential(2, n_samples),
            "gamma": np.random.gamma(2, 2, n_samples),
            "beta": np.random.beta(2, 5, n_samples) * 10,  # Scale for visibility
            "uniform": np.random.uniform(-3, 3, n_samples),
        }
    )

    # Add some outliers to make tail fading more visible
    data.loc[data.index[:20], "normal"] = np.random.normal(0, 1, 20) + 5  # Right outliers
    data.loc[data.index[20:40], "normal"] = np.random.normal(0, 1, 20) - 5  # Left outliers

    print("Demonstrating distributional plotting functions...")
    print(f"Data shape: {data.shape}")
    print(f"Data columns: {list(data.columns)}")

    # Example 1: Single combined plot with tail fader
    print("\n1. Creating combined distributional plot with tail fader...")
    fig1 = distributional_plots(data, tail_fader=True, percentile_low=5, percentile_high=95, figsize=(12, 6))
    fig1.suptitle("Combined Distributions with Tail Fader (5th-95th percentiles)")
    plt.show()

    # Example 2: Single combined plot without tail fader
    print("\n2. Creating combined distributional plot without tail fader...")
    fig2 = distributional_plots(data, tail_fader=False, figsize=(12, 6))
    fig2.suptitle("Combined Distributions without Tail Fader")
    plt.show()

    # Example 3: Per-row plots with tail fader
    print("\n3. Creating per-row distributional plots with tail fader...")
    fig3 = distributional_plots_per_row(data, tail_fader=True, percentile_low=1, percentile_high=99, figsize=(14, 10))
    fig3.suptitle("Individual Distribution Plots with Tail Fader (1st-99th percentiles)")
    plt.show()

    # Example 4: Per-row plots without tail fader
    print("\n4. Creating per-row distributional plots without tail fader...")
    fig4 = distributional_plots_per_row(data, tail_fader=False, figsize=(14, 10))
    fig4.suptitle("Individual Distribution Plots without Tail Fader")
    plt.show()

    # Example 5: Single series with extreme tail fading
    print("\n5. Demonstrating extreme tail fading on single series...")
    single_series = data["normal"]
    fig5 = distributional_plots(
        single_series, tail_fader=True, percentile_low=10, percentile_high=90, figsize=(10, 6), color="red"
    )
    fig5.suptitle("Single Series with Extreme Tail Fader (10th-90th percentiles)")
    plt.show()

    print("\nDemonstration complete!")


if __name__ == "__main__":
    run()
