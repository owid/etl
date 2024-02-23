"""Script to create a snapshot of dataset."""

import os
import tempfile
import time
from pathlib import Path

import click
import pandas as pd
from owid.datautils.io import df_to_file
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from structlog import get_logger
from tqdm import tqdm

from etl.snapshot import Snapshot

# Initialize logger.
log = get_logger()


# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

COUNTRIES = {
    "ALB": "Albania",
    "AND": "Andorra",
    "AUT": "Austria",
    "BLR": "Belarus",
    "BEL": "Belgium",
    "BIH": "Bosnia and Herzegovina",
    "BGR": "Bulgaria",
    "HRV": "Croatia",
    "CZE": "Czech Republic",
    "DNK": "Denmark",
    "EST": "Estonia",
    "FRO": "Faroe Islands",
    "FIN": "Finland",
    "FRA": "France",
    "DEU": "Germany",
    "GIB": "Gibraltar",
    "GRC": "Greece",
    "GGY": "Guernsey",
    "HUN": "Hungary",
    "ISL": "Iceland",
    "IRL": "Ireland",
    "IMN": "Isle of Man",
    "ITA": "Italy",
    "JEY": "Jersey",
    "XKO": "Kosovo under UNSCR 1244",
    "LVA": "Latvia",
    "LIE": "Liechtenstein",
    "LTU": "Lithuania",
    "LUX": "Luxembourg",
    "MLT": "Malta",
    "MDA": "Moldova",
    "MCO": "Monaco",
    "MNE": "Montenegro",
    "NLD": "Netherlands",
    "MKD": "North Macedonia",
    "NOR": "Norway",
    "POL": "Poland",
    "PRT": "Portugal",
    "ROU": "Romania",
    "RUS": "Russia",
    "SMR": "San Marino",
    "SRB": "Serbia",
    "SVK": "Slovakia",
    "SVN": "Slovenia",
    "ESP": "Spain",
    "SJM": "Svalbard and Jan Mayen",
    "SWE": "Sweden",
    "CHE": "Switzerland",
    "UKR": "Ukraine",
    "GBR": "United Kingdom",
    "VAT": "Vatican City",
    "ALA": "Åland",
    "DZA": "Algeria",
    "AGO": "Angola",
    "BEN": "Benin",
    "BWA": "Botswana",
    "IOT": "British Indian Ocean Territory",
    "BFA": "Burkina Faso",
    "BDI": "Burundi",
    "CMR": "Cameroon",
    "CPV": "Cape Verde",
    "CAF": "Central African Republic",
    "TCD": "Chad",
    "COM": "Comoros",
    "CIV": "Côte d'Ivoire",
    "COD": "Democratic Republic of the Congo",
    "DJI": "Djibouti",
    "EGY": "Egypt",
    "GNQ": "Equatorial Guinea",
    "ERI": "Eritrea",
    "ETH": "Ethiopia",
    "ATF": "French Southern Territories",
    "GAB": "Gabon",
    "GMB": "Gambia",
    "GHA": "Ghana",
    "GIN": "Guinea",
    "GNB": "Guinea-Bissau",
    "KEN": "Kenya",
    "LSO": "Lesotho",
    "LBR": "Liberia",
    "LBY": "Libya",
    "MDG": "Madagascar",
    "MWI": "Malawi",
    "MLI": "Mali",
    "MRT": "Mauritania",
    "MUS": "Mauritius",
    "MYT": "Mayotte",
    "MAR": "Morocco",
    "MOZ": "Mozambique",
    "NAM": "Namibia",
    "NER": "Niger",
    "NGA": "Nigeria",
    "COG": "Republic of Congo",
    "REU": "Reunion",
    "RWA": "Rwanda",
    "SHN": "Saint Helena",
    "SEN": "Senegal",
    "SYC": "Seychelles",
    "SLE": "Sierra Leone",
    "SOM": "Somalia",
    "ZAF": "South Africa",
    "SSD": "South Sudan",
    "SDN": "Sudan",
    "SWZ": "Swaziland",
    "STP": "São Tomé and Príncipe",
    "TZA": "Tanzania",
    "TGO": "Togo",
    "TUN": "Tunisia",
    "UGA": "Uganda",
    "ESH": "Western Sahara",
    "ZMB": "Zambia",
    "ZWE": "Zimbabwe",
    "AIA": "Anguilla",
    "ATG": "Antigua and Barbuda",
    "ARG": "Argentina",
    "ABW": "Aruba",
    "BHS": "Bahamas",
    "BRB": "Barbados",
    "BLZ": "Belize",
    "BMU": "Bermuda",
    "BOL": "Bolivia",
    "BES": "Bonaire, Sint Eustatius and Saba",
    "BVT": "Bouvet Island",
    "BRA": "Brazil",
    "VGB": "British Virgin Islands",
    "CAN": "Canada",
    "CYM": "Cayman Islands",
    "CHL": "Chile",
    "COL": "Colombia",
    "CRI": "Costa Rica",
    "CUB": "Cuba",
    "CUW": "Curaçao",
    "DMA": "Dominica",
    "DOM": "Dominican Republic",
    "ECU": "Ecuador",
    "SLV": "El Salvador",
    "FLK": "Falkland Islands",
    "GUF": "French Guiana",
    "GRL": "Greenland",
    "GRD": "Grenada",
    "GLP": "Guadeloupe",
    "GTM": "Guatemala",
    "GUY": "Guyana",
    "HTI": "Haiti",
    "HND": "Honduras",
    "JAM": "Jamaica",
    "MTQ": "Martinique",
    "MEX": "Mexico",
    "MSR": "Montserrat",
    "NIC": "Nicaragua",
    "PAN": "Panama",
    "PRY": "Paraguay",
    "PER": "Peru",
    "PRI": "Puerto Rico",
    "KNA": "Saint Kitts and Nevis",
    "LCA": "Saint Lucia",
    "SPM": "Saint Pierre and Miquelon",
    "VCT": "Saint Vincent and the Grenadines",
    "BLM": "Saint-Barthélemy",
    "MAF": "Saint-Martin",
    "SXM": "Sint Maarten",
    "SGS": "South Georgia and the South Sandwich Islands",
    "SUR": "Suriname",
    "TTO": "Trinidad and Tobago",
    "TCA": "Turks and Caicos Islands",
    "USA": "United States",
    "URY": "Uruguay",
    "VEN": "Venezuela",
    "VIR": "Virgin Islands, U.S.",
    "AFG": "Afghanistan",
    "XAD": "Akrotiri and Dhekelia",
    "ARM": "Armenia",
    "AZE": "Azerbaijan",
    "BHR": "Bahrain",
    "BGD": "Bangladesh",
    "BTN": "Bhutan",
    "BRN": "Brunei",
    "KHM": "Cambodia",
    "XCA": "Caspian Sea",
    "CHN": "China",
    "CYP": "Cyprus",
    "GEO": "Georgia",
    "HKG": "Hong Kong",
    "IND": "India",
    "IDN": "Indonesia",
    "IRN": "Iran",
    "IRQ": "Iraq",
    "ISR": "Israel",
    "JPN": "Japan",
    "JOR": "Jordan",
    "KAZ": "Kazakhstan",
    "KWT": "Kuwait",
    "KGZ": "Kyrgyzstan",
    "LAO": "Laos",
    "LBN": "Lebanon",
    "MAC": "Macao",
    "MYS": "Malaysia",
    "MDV": "Maldives",
    "MNG": "Mongolia",
    "MMR": "Myanmar",
    "NPL": "Nepal",
    "PRK": "North Korea",
    "XNC": "Northern Cyprus",
    "OMN": "Oman",
    "PAK": "Pakistan",
    "PSE": "Palestina",
    "PHL": "Philippines",
    "QAT": "Qatar",
    "SAU": "Saudi Arabia",
    "SGP": "Singapore",
    "KOR": "South Korea",
    "LKA": "Sri Lanka",
    "SYR": "Syria",
    "TWN": "Taiwan",
    "TJK": "Tajikistan",
    "THA": "Thailand",
    "TLS": "Timor-Leste",
    "TUR": "Turkey",
    "TKM": "Turkmenistan",
    "ARE": "United Arab Emirates",
    "UZB": "Uzbekistan",
    "VNM": "Vietnam",
    "YEM": "Yemen",
    "ASM": "American Samoa",
    "AUS": "Australia",
    "CXR": "Christmas Island",
    "CCK": "Cocos Islands",
    "COK": "Cook Islands",
    "FJI": "Fiji",
    "PYF": "French Polynesia",
    "GUM": "Guam",
    "HMD": "Heard Island and McDonald Islands",
    "KIR": "Kiribati",
    "MHL": "Marshall Islands",
    "FSM": "Micronesia",
    "NRU": "Nauru",
    "NCL": "New Caledonia",
    "NZL": "New Zealand",
    "NIU": "Niue",
    "NFK": "Norfolk Island",
    "MNP": "Northern Mariana Islands",
    "PLW": "Palau",
    "PNG": "Papua New Guinea",
    "PCN": "Pitcairn Islands",
    "WSM": "Samoa",
    "SLB": "Solomon Islands",
    "TKL": "Tokelau",
    "TON": "Tonga",
    "TUV": "Tuvalu",
    "UMI": "United States Minor Outlying Islands",
    "VUT": "Vanuatu",
    "WLF": "Wallis and Futuna",
    "UN_OCE": "Oceania",
    "UN_ASI": "Asia",
    "UN_EUR": "Europe",
    "UN_AME": "Americas",
    "UN_AFR": "Africa",
}


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"climate/{SNAPSHOT_VERSION}/weekly_wildfires.csv")
    all_dfs = []

    years = range(2003, 2024)
    for year in tqdm(years, desc="Processing years"):
        for country, country_name in tqdm(COUNTRIES.items(), desc=f"Processing regions for year {year}"):
            with tempfile.TemporaryDirectory() as download_path:
                log.info(f"Processing {country_name} for year {year}.")
                chrome_options = Options()
                chrome_options.add_argument("--headless")
                chrome_options.add_argument("--window-size=1920,1080")
                chrome_options.add_argument(
                    "--disable-gpu"
                )  # Sometimes recommended for headless mode to avoid unnecessary use of GPU
                chrome_options.add_argument(
                    "--no-sandbox"
                )  # Bypass OS security model; be cautious with this in production environments
                chrome_options.add_argument("--disable-dev-shm-usage")  # Overcome limited resource problems

                # Explicitly allow downloads in headless mode
                chrome_options.add_experimental_option(
                    "prefs",
                    {
                        "download.default_directory": download_path,
                        "download.prompt_for_download": False,
                        "download.directory_upgrade": True,
                        "safebrowsing.enabled": True,
                        "profile.default_content_settings.popups": 0,
                        "profile.content_settings.exceptions.automatic_downloads.*.setting": 1,
                    },
                )

                # Start the browser with the custom options
                driver = webdriver.Chrome(options=chrome_options)
                wait = WebDriverWait(driver, 50)  # Adjust wait time to a reasonable value
                link = f"https://gwis.jrc.ec.europa.eu/apps/gwis.statistics/seasonaltrend/{country}/{year}/CO2"
                driver.get(link)
                all_dfs = []
                try:
                    chart_containers = wait.until(
                        EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.col-lg-6.col-12"))
                    )
                    # Include only the first, third, and fifth containers (area burnt, CO2 emissions and number of areas)
                    containers_to_include = [0, 2, 4]
                    chart_containers = [chart_containers[i] for i in containers_to_include]
                    for index, container in tqdm(
                        enumerate(chart_containers),
                        total=len(chart_containers),
                        desc="Processing chart containers",
                    ):
                        # Scroll the container into view
                        driver.execute_script(
                            "arguments[0].scrollIntoView({behavior: 'smooth', block: 'nearest'});", container
                        )
                        time.sleep(5)  # Give time for any lazy-loaded elements to load

                        try:
                            # Remove potential overlays by setting their CSS to 'none'
                            # Attempt to interact with the container
                            main_export = container.find_element(
                                By.CSS_SELECTOR, ".amcharts-amexport-label.amcharts-amexport-label-level-0"
                            )
                            main_export.click()

                            data_link_specific_selector = (
                                '.amcharts-amexport-item-blank[aria-label="Data [Click, tap or press ENTER to open]"]'
                            )
                            data_link = container.find_element(By.CSS_SELECTOR, data_link_specific_selector)
                            data_link.click()

                            csv_option = container.find_element(
                                By.CSS_SELECTOR,
                                "li.amcharts-amexport-item.amcharts-amexport-item-level-2.amcharts-amexport-item-csv",
                            )

                            csv_option.click()

                        except NoSuchElementException:
                            log.info(f"Necessary element was not found in container {index+1}.")
                        except Exception as e:
                            log.info(f"An error occurred while processing container {index+1}: {e}")

                except TimeoutException:
                    log.info("Failed to locate containers within the timeout period.")
                finally:
                    # Now, load the files. This part assumes there's only one file and its name ends with '.csv'
                    downloaded_files = [f for f in os.listdir(download_path) if f.endswith(".csv")]
                    for file in downloaded_files:
                        file_path = os.path.join(download_path, file)
                        df = pd.read_csv(file_path)
                        cols_to_keep = ["Day", f"Year {year}"]
                        df = df[cols_to_keep]
                        df["year"] = year
                        df["country"] = country_name
                        column_name = ""
                        parts = file.split("_")
                        for i, part in enumerate(parts):
                            if part.isupper():
                                column_name = "_".join(parts[:i])
                        df["indicator"] = column_name
                        df = df.rename(columns={f"Year {year}": "value", "Day": "day"})
                        all_dfs.append(df)

    all_dfs = pd.concat(all_dfs)
    df_to_file(all_dfs, file_path=snap.path)
    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
