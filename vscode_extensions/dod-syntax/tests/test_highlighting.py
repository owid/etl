# Simple test file for DOD highlighting

# These should be highlighted (in strings):
desc1 = "This has a [test DOD](#dod:test) reference."
desc2 = 'Another [DOD reference](#dod:another) in single quotes.'
desc3 = """
Multiline string with [DOD](#dod:multiline) reference.
"""

# Copying exact format from migration file:
ADDITIONAL_DESCRIPTIONS = {
    "test": {
        "title": "Test",
        "description": "The total number of [immigrants](#dod:immigrant) (people moving into a given country) minus the number of [emigrants](#dod:emigrant) (people moving out of the country).",
    },
}

# This should NOT be highlighted (not in a string):
# invalid = [not in string](#dod:invalid)