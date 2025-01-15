def prepare_summary(
    df_top_10_total_views, producers_selected, total_views, average_daily_views, min_date, max_date
) -> str:
    """Prepare summary at the end of the app."""
    # Prepare the total number of views.
    total_views_str = f"{total_views:9,}"
    # Prepare the average daily views.
    average_views_str = f"{round(average_daily_views):9,}"
    # Prepare a summary of the top 10 charts to be copy-pasted.
    if len(producers_selected) == 0:
        producers_selected_str = "all producers"
    elif len(producers_selected) == 1:
        producers_selected_str = producers_selected[0]
    else:
        producers_selected_str = ", ".join(producers_selected[:-1]) + " and " + producers_selected[-1]
    # NOTE: I tried .to_string() and .to_markdown() and couldn't find a way to keep a meaningful format.
    df_summary_str = ""
    for _, row in df_top_10_total_views.sort_values("renders", ascending=False).iterrows():
        df_summary_str += f"{row['renders']:9,}" + " - " + row["grapher"] + "\n"

    # Define the content to copy.
    summary = f"""\
Analytics of charts using data by {producers_selected_str} between {min_date} and {max_date}:
- Total number of chart views: {total_views_str}
- Average daily chart views: {average_views_str}
- Views of top performing charts:
{df_summary_str}

    """
    return summary
