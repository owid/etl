"""What the model is told about how much a chart is read."""

import datetime as dt

from apps.chart_critic.critic import readership_line


def test_readership_is_per_day_over_a_year_for_an_established_chart():
    line = readership_line(400, dt.datetime(2024, 5, 8, 11, 29))
    assert line == "Readership: about 1 view a day, first published 2024-05-08."


def test_a_new_chart_is_judged_over_the_days_it_has_been_live():
    """300 views in its first ten days is 30 a day, not the 0.8 a flat year would make it."""
    published = dt.date.today() - dt.timedelta(days=10)
    assert readership_line(300, published) == f"Readership: about 30 views a day, first published {published}."


def test_a_chart_nobody_has_opened_yet_says_so():
    published = dt.date.today() - dt.timedelta(days=1)
    assert readership_line(None, published) == f"Readership: no recorded views, first published {published}."


def test_readership_without_a_publish_date():
    assert readership_line(36500, None) == "Readership: about 100 views a day."
