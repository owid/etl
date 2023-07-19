import xml.etree.ElementTree as ET

import pandas as pd
import requests


def import_US_cpi_API():
    """
    Imports the US Consumer Price Index (CPI) data from the World Bank API.

    Returns:
        DataFrame: A DataFrame containing the extracted CPI data with columns 'year' and 'fp_cpi_totl'.
                  Returns None if the API request fails.
    """

    YEARS = 23
    # API endpoint URL
    url = f"https://api.worldbank.org/v2/country/us/indicator/FP.CPI.TOTL?mrnev={YEARS}"

    try:
        # Send a GET request to the API endpoint
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for unsuccessful requests

        # Parse the XML response
        root = ET.fromstring(response.content)

        # Initialize lists to store the extracted data
        data = []

        # Iterate over each 'wb:data' element
        for data_elem in root.findall(".//{http://www.worldbank.org}data"):
            # Extract the desired data from the 'wb:data' element
            date = int(data_elem.find(".//{http://www.worldbank.org}date").text)
            value = float(data_elem.find(".//{http://www.worldbank.org}value").text)

            # Append the extracted data to the list
            data.append({"year": date, "fp_cpi_totl": value})

        # Create a DataFrame from the extracted data
        df = pd.DataFrame(data)

        return df

    except requests.exceptions.RequestException as e:
        # Request was not successful
        print(f"Request failed: {str(e)}")
        return None
