import re

import pandas as pd


def extract_data_papers_with_code(html_content):
    # Define the regex pattern to match the table information
    pattern = r'\{"x": "(.*?)", "y": (.*?), "name": "(.*?)", "nameShort": "(.*?)", "nameDetails": "(.*?)", "paperSlug": "(.*?)", "usesAdditionalData": (.*?)\}'

    # Find all matches of the pattern
    matches = re.findall(pattern, html_content)

    # Process the matches
    data = []
    for match in matches:
        x = match[0]
        y = match[1]
        name = match[2]
        uses_additional_data = match[6]

        # Append the extracted information to the data list
        data.append({"date": x, "performance": y, "name": name, "additional_data": uses_additional_data})

    # Create the DataFrame from the data list
    df = pd.DataFrame(data)

    return df
