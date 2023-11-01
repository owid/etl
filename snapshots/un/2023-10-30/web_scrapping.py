"""
This code scrapes the UN website to get a list of all member states and the year they joined.
Run this code first to get the list of countries and their admission year.
By Edouard Mathieu
"""

import re
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

# Define path of this file
PARENT_DIR = Path(__file__).parent.absolute()

# Fetch the web page
url = "https://www.un.org/en/about-us/member-states"
page = requests.get(url, headers={"User-Agent": "Mozilla/8.0"})
soup = BeautifulSoup(page.content, "html.parser")

# Find the divs containing the country data
divs = soup.find_all(class_="country")

# Extract the country name and admission year
country = [div.find("h2").text for div in divs]
admission = [re.search(r"\d{4}$", div.find(class_="text-muted").text).group() for div in divs]  # type: ignore

# Check the length of the lists to make sure they are the same
if len(country) != len(admission):
    raise ValueError("Length mismatch between country and admission lists")

# Create a DataFrame
df = pd.DataFrame({"country": country, "admission": admission})

# Save to csv
df.to_csv(f"{PARENT_DIR}/un_members.csv", index=False)
