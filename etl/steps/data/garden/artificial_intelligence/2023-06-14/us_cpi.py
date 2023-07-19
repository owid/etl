import xml.etree.ElementTree as ET

import pandas as pd
import requests
from structlog import get_logger

log = get_logger()

# change number of years to go back if neccessary (used up to 2000 in 2023)

YEARS = 23


def import_US_cpi_API(years_back=YEARS):
    """
    Imports the US Consumer Price Index (CPI) data from the World Bank API.

    Returns:
        DataFrame: A DataFrame containing the extracted CPI data with columns 'year' and 'fp_cpi_totl'.
                  Returns None if the API request fails.
    """

    # API endpoint URL
    url = f"https://api.worldbank.org/v2/country/us/indicator/FP.CPI.TOTL?mrnev={years_back}"

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
            date_elem = data_elem.find(".//{http://www.worldbank.org}date")
            value_elem = data_elem.find(".//{http://www.worldbank.org}value")

            date = date_elem.text if date_elem is not None else None
            value = value_elem.text if value_elem is not None else None

            if date_elem is not None and value_elem is not None:
                # Append the extracted data to the list
                data.append({"year": date, "fp_cpi_totl": value})
            else:
                log.info("Year and CPI not found!")
                return
        # Create a DataFrame from the extracted data
        df = pd.DataFrame(data)
        df["year"] = df["year"].astype(int)
        df["fp_cpi_totl"] = df["fp_cpi_totl"].astype(float)

        return df

    except requests.exceptions.RequestException as e:
        # Request was not successful
        print(f"Request failed: {str(e)}")
        return None
