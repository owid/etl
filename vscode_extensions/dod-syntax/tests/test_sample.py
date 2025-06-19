# Sample Python file to test DOD highlighting

# This should be highlighted (raw string)
description1 = r"The [Gini index](#dod:gini) measures inequality on a scale from 0 to 100."

# This should ALSO be highlighted (regular string)
description2 = "The [Gini index](#dod:gini) measures inequality on a scale from 0 to 100."

# This should be highlighted (triple-quoted raw string)
long_description = r"""
The [Gini coefficient](#dod:gini) is a measure of statistical dispersion 
intended to represent the income inequality within a nation or any other group of people.
"""

# This should ALSO be highlighted (regular triple-quoted string)
regular_long = """
The [Gini coefficient](#dod:gini) is a measure of statistical dispersion 
intended to represent the income inequality within a nation or any other group of people.
"""

# This should be highlighted (single-quoted raw string)
description3 = r'The [Poverty headcount](#dod:poverty_headcount) shows the percentage of people living below the poverty line.'

# This should ALSO be highlighted (single-quoted regular string)
description4 = 'The [Poverty headcount](#dod:poverty_headcount) shows the percentage of people living below the poverty line.'

# This should NOT be highlighted (not in a string)
# description5 = [Invalid DOD](#dod:invalid)  # This would be invalid Python syntax