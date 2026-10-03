"""Script to create a snapshot of dataset.
The data is taken from the 'Rinderpest and its eradication report', found here: https://www.woah.org/en/document/rinderpest-and-its-eradication/
"""

from pathlib import Path

import click
import pandas as pd

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


df = pd.DataFrame(
    [
        {
            "country": "Afghanistan",
            "last_recorded_rinderpest": "1995",
            "listing_date": "2008; dossier of evidence to the OIE",
        },
        {"country": "Albania", "last_recorded_rinderpest": "1934", "listing_date": "2000; baseline historical list"},
        {
            "country": "Algeria",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2000; baseline historical list",
        },
        {
            "country": "Andorra",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2000; baseline historical list",
        },
        {
            "country": "Angola",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2000; baseline historical list",
        },
        {
            "country": "Antigua and Barbados",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2011; additional historical list",
        },
        {
            "country": "Argentina",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2000; baseline historical list",
        },
        {"country": "Armenia", "last_recorded_rinderpest": "1928", "listing_date": "2009; additional historical list"},
        {
            "country": "Australia",
            "last_recorded_rinderpest": "1923 (imported)",
            "listing_date": "2000; baseline historical list",
        },
        {"country": "Austria", "last_recorded_rinderpest": "1881", "listing_date": "2000; baseline historical list"},
        {
            "country": "Azerbaijan",
            "last_recorded_rinderpest": "1928",
            "listing_date": "2011; additional historical list",
        },
        {
            "country": "Bahamas",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2011; additional historical list",
        },
        {"country": "Bahrain", "last_recorded_rinderpest": "1980", "listing_date": "2009; additional historical list"},
        {
            "country": "Bangladesh",
            "last_recorded_rinderpest": "1958",
            "listing_date": "2010; additional historical list",
        },
        {
            "country": "Barbados",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2001; additional historical list",
        },
        {
            "country": "Belarus",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2008; additional historical list",
        },
        {
            "country": "Belgium",
            "last_recorded_rinderpest": "1920 (imported)",
            "listing_date": "2000; baseline historical list",
        },
        {
            "country": "Belize",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2009; additional historical list",
        },
        {
            "country": "Benin",
            "last_recorded_rinderpest": "1987",
            "listing_date": "2005; dossier of evidence to the OIE",
        },
        {
            "country": "Bhutan",
            "last_recorded_rinderpest": "1971",
            "listing_date": "2005; dossier of evidence to the OIE",
        },
        {
            "country": "Bolivia",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2000; baseline historical list",
        },
        {
            "country": "Bosnia and Herzegovina",
            "last_recorded_rinderpest": "1883",
            "listing_date": "2000; baseline historical list",
        },
        {"country": "Botswana", "last_recorded_rinderpest": "1899", "listing_date": "2000; baseline historical list"},
        {
            "country": "Brazil",
            "last_recorded_rinderpest": "1921 (imported)",
            "listing_date": "2000; baseline historical list",
        },
        {
            "country": "Brunei",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2009; additional historical list",
        },
        {"country": "Bulgaria", "last_recorded_rinderpest": "1913", "listing_date": "2000; baseline historical list"},
        {
            "country": "Burkina Faso",
            "last_recorded_rinderpest": "1988",
            "listing_date": "2006; dossier of evidence to the OIE",
        },
        {"country": "Burundi", "last_recorded_rinderpest": "1934", "listing_date": "2006; additional historical list"},
        {"country": "Cambodia", "last_recorded_rinderpest": "1964", "listing_date": "2010; additional historical list"},
        {
            "country": "Cameroon",
            "last_recorded_rinderpest": "1986",
            "listing_date": "2010; dossier of evidence to the OIE",
        },
        {
            "country": "Canada",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2000; baseline historical list",
        },
        {
            "country": "Cabo Verde",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2009; additional historical list",
        },
        {
            "country": "Central African Republic",
            "last_recorded_rinderpest": "1984",
            "listing_date": "2010; dossier of evidence to the OIE",
        },
        {"country": "Chad", "last_recorded_rinderpest": "1983", "listing_date": "2010; dossier of evidence to the OIE"},
        {
            "country": "Chile",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2000; baseline historical list",
        },
        {
            "country": "China",
            "last_recorded_rinderpest": "1956",
            "listing_date": "2008; additional historical list (25-year rule)",
        },
        {
            "country": "Chinese Taipei",
            "last_recorded_rinderpest": "1949",
            "listing_date": "2000; baseline historical list",
        },
        {
            "country": "Colombia",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2000; baseline historical list",
        },
        {
            "country": "Comoros",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2011; additional historical list",
        },
        {
            "country": "Congo",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2006; additional historic list",
        },
        {
            "country": "Cook Islands",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2009; additional historical list",
        },
        {
            "country": "Costa Rica",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2000; baseline historical list",
        },
        {
            "country": "Côte d’Ivoire",
            "last_recorded_rinderpest": "1986",
            "listing_date": "2007; dossier of evidence to the OIE",
        },
        {"country": "Croatia", "last_recorded_rinderpest": "1883", "listing_date": "2000; baseline historical list"},
        {
            "country": "Cuba",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2000; baseline historical list",
        },
        {
            "country": "Cyprus",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2000; baseline historical list",
        },
        {"country": "Czechia", "last_recorded_rinderpest": "1881", "listing_date": "2000; baseline historical list"},
        {
            "country": "Democratic Republic of the Congo",
            "last_recorded_rinderpest": "1961",
            "listing_date": "2006; additional historical list (ten-year rule)",
        },
        {"country": "Denmark", "last_recorded_rinderpest": "1782", "listing_date": "2000; baseline historical list"},
        {
            "country": "Djibouti",
            "last_recorded_rinderpest": "1985",
            "listing_date": "2010; dossier of evidence to the OIE",
        },
        {
            "country": "Dominica",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2010; additional historical list",
        },
        {
            "country": "Dominican Republic",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2009; additional historical list",
        },
        {
            "country": "Ecuador",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2000; baseline historical list",
        },
        {
            "country": "Egypt",
            "last_recorded_rinderpest": "1986",
            "listing_date": "2006; dossier of evidence to the OIE",
        },
        {
            "country": "El Salvador",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2000; baseline historical list",
        },
        {
            "country": "Equatorial Guinea",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2008; additional historical list",
        },
        {
            "country": "Eritrea",
            "last_recorded_rinderpest": "1995",
            "listing_date": "2005; dossier of evidence to the OIE",
        },
        {
            "country": "Estonia",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2000; baseline historical list",
        },
        {
            "country": "Ethiopia",
            "last_recorded_rinderpest": "1995",
            "listing_date": "2008; dossier of evidence to the OIE",
        },
        {
            "country": "Fiji",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2009; additional historical list",
        },
        {"country": "Finland", "last_recorded_rinderpest": "1877", "listing_date": "2000; baseline historical list"},
        {"country": "France", "last_recorded_rinderpest": "1870", "listing_date": "2000; baseline historical list"},
        {
            "country": "Gabon",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2008; additional historical list",
        },
        {
            "country": "Gambia",
            "last_recorded_rinderpest": "1965",
            "listing_date": "2011; dossier of evidence to the OIE",
        },
        {"country": "Georgia", "last_recorded_rinderpest": "1989", "listing_date": "2010; additional historical list"},
        {"country": "Germany", "last_recorded_rinderpest": "1870", "listing_date": "2000; baseline historical list"},
        {
            "country": "Ghana",
            "last_recorded_rinderpest": "1988",
            "listing_date": "2007; dossier of evidence to the OIE",
        },
        {"country": "Greece", "last_recorded_rinderpest": "1926", "listing_date": "2000; baseline historical list"},
        {
            "country": "Grenada",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2011; additional historical list",
        },
        {
            "country": "Guatemala",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2000; baseline historical list",
        },
        {
            "country": "Guinea",
            "last_recorded_rinderpest": "1967",
            "listing_date": "2006; dossier of evidence to the OIE",
        },
        {
            "country": "Guinea-Bissau",
            "last_recorded_rinderpest": "1967",
            "listing_date": "2006; additional historical list",
        },
        {
            "country": "Guyana",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2000; baseline historical list",
        },
        {
            "country": "Haiti",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2000; baseline historical list",
        },
        {
            "country": "Honduras",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2000; baseline historical list",
        },
        {"country": "Hungary", "last_recorded_rinderpest": "1881", "listing_date": "2000; baseline historical list"},
        {
            "country": "Iceland",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2000; baseline historical list",
        },
        {
            "country": "India",
            "last_recorded_rinderpest": "1995",
            "listing_date": "2006; dossier of evidence to the OIE",
        },
        {"country": "Indonesia", "last_recorded_rinderpest": "1907", "listing_date": "2000; Baseline Historic List"},
        {
            "country": "Iran (Islamic Republic of)",
            "last_recorded_rinderpest": "1994",
            "listing_date": "2008; dossier of evidence to the OIE",
        },
        {"country": "Iraq", "last_recorded_rinderpest": "1996", "listing_date": "2009; dossier of evidence to the OIE"},
        {"country": "Ireland", "last_recorded_rinderpest": "1866", "listing_date": "2000; baseline historical list"},
        {
            "country": "Israel",
            "last_recorded_rinderpest": "1983",
            "listing_date": "2010; dossier of evidence to the OIE",
        },
        {
            "country": "Italy",
            "last_recorded_rinderpest": "1947 (imported)",
            "listing_date": "2000; baseline historical list",
        },
        {
            "country": "Jamaica",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2000; baseline historical list",
        },
        {"country": "Japan", "last_recorded_rinderpest": "1924", "listing_date": "2000; baseline historical list"},
        {"country": "Jordan", "last_recorded_rinderpest": "1972", "listing_date": "2008; additional historical list"},
        {
            "country": "Kazakhstan",
            "last_recorded_rinderpest": "1927",
            "listing_date": "2011; dossier of evidence to the OIE/ historical",
        },
        {
            "country": "Kenya",
            "last_recorded_rinderpest": "2001",
            "listing_date": "2009; dossier of evidence to the OIE",
        },
        {
            "country": "Kiribati",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2011; additional historical list",
        },
        {
            "country": "Korea (Democratic People’s Republic of)",
            "last_recorded_rinderpest": "1948",
            "listing_date": "2009; additional historical list",
        },
        {
            "country": "Korea (Republic of)",
            "last_recorded_rinderpest": "1931",
            "listing_date": "2000; baseline historical list",
        },
        {
            "country": "Kosovo (a)",
            "last_recorded_rinderpest": "1890s",
            "listing_date": "2011; additional historical list",
        },
        {
            "country": "Kuwait",
            "last_recorded_rinderpest": "1985",
            "listing_date": "2010; dossier of evidence to the OIE",
        },
        {
            "country": "Kyrgyzstan",
            "last_recorded_rinderpest": "1928",
            "listing_date": "2011; additional historical list",
        },
        {
            "country": "Lao People's Democratic Republic",
            "last_recorded_rinderpest": "1966",
            "listing_date": "2011; additional historical list",
        },
        {"country": "Latvia", "last_recorded_rinderpest": "1921", "listing_date": "2000; baseline historical list"},
        {"country": "Lebanon", "last_recorded_rinderpest": "1982", "listing_date": "2008; additional historical list"},
        {"country": "Lesotho", "last_recorded_rinderpest": "1896", "listing_date": "2000; baseline historical list"},
        {
            "country": "Liberia",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2011; additional historical list",
        },
        {"country": "Libya", "last_recorded_rinderpest": "1963", "listing_date": "2009; additional historical list"},
        {
            "country": "Liechtenstein",
            "last_recorded_rinderpest": "19th century",
            "listing_date": "2009; additional historical list",
        },
        {
            "country": "Lithuania",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2000; baseline historical list",
        },
        {
            "country": "Luxembourg",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2000; baseline historical list",
        },
        {
            "country": "Madagascar",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2000; baseline historical list",
        },
        {
            "country": "Malawi",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2003; historical ten-year rule",
        },
        {"country": "Malaysia", "last_recorded_rinderpest": "1935", "listing_date": "2000; baseline historical list"},
        {
            "country": "Maldives",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2010; additional historical list",
        },
        {"country": "Mali", "last_recorded_rinderpest": "1986", "listing_date": "2006; dossier of evidence to the OIE"},
        {
            "country": "Malta",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2000; baseline historical list",
        },
        {
            "country": "Marshall Islands",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2009; additional historical list",
        },
        {
            "country": "Mauritania",
            "last_recorded_rinderpest": "1986",
            "listing_date": "2007; dossier of evidence to the OIE",
        },
        {
            "country": "Mauritius",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2000; baseline historical list",
        },
        {
            "country": "Mexico",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2000; baseline historical list",
        },
        {
            "country": "Micronesia",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2011; additional historical list",
        },
        {
            "country": "Moldova",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2000; baseline historical list",
        },
        {
            "country": "Mongolia",
            "last_recorded_rinderpest": "1992",
            "listing_date": "2005; dossier of evidence to the OIE",
        },
        {
            "country": "Montenegro",
            "last_recorded_rinderpest": "1883",
            "listing_date": "2009; additional historical list",
        },
        {
            "country": "Morocco",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2000; baseline historic list",
        },
        {
            "country": "Mozambique",
            "last_recorded_rinderpest": "1896",
            "listing_date": "2007; additional historical list",
        },
        {
            "country": "Myanmar",
            "last_recorded_rinderpest": "1957",
            "listing_date": "2006; dossier of evidence to the OIE/historical",
        },
        {"country": "Namibia", "last_recorded_rinderpest": "1905", "listing_date": "2000; baseline historical list"},
        {
            "country": "Nauru",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2009; additional historical list",
        },
        {
            "country": "Nepal",
            "last_recorded_rinderpest": "1990",
            "listing_date": "2002; dossier of evidence to the OIE",
        },
        {
            "country": "Netherlands",
            "last_recorded_rinderpest": "1869",
            "listing_date": "2000; baseline historical list",
        },
        {
            "country": "New Caledonia",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2000; baseline historical list",
        },
        {
            "country": "New Zealand",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2000; baseline historical list",
        },
        {
            "country": "Nicaragua",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2009; additional historical list",
        },
        {
            "country": "Niger",
            "last_recorded_rinderpest": "1986",
            "listing_date": "2010; dossier of evidence to the OIE",
        },
        {
            "country": "Nigeria",
            "last_recorded_rinderpest": "1987",
            "listing_date": "2010; dossier of evidence to the OIE",
        },
        {
            "country": "Niue",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2009; additional historical list",
        },
        {
            "country": "North Macedonia",
            "last_recorded_rinderpest": "1883",
            "listing_date": "2000; baseline historical list",
        },
        {
            "country": "Norway",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2000; baseline historical list",
        },
        {"country": "Oman", "last_recorded_rinderpest": "1995", "listing_date": "2009; dossier of evidence to the OIE"},
        {
            "country": "Pakistan",
            "last_recorded_rinderpest": "2000",
            "listing_date": "2007; dossier of evidence to the OIE",
        },
        {
            "country": "Palau",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2009; additional historical list",
        },
        {
            "country": "Palestinian Autonomous Territories",
            "last_recorded_rinderpest": "1983",
            "listing_date": "2010; additional historical list",
        },
        {
            "country": "Panama",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2000; baseline historical list",
        },
        {
            "country": "Papua New Guinea",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2009; additional historical list",
        },
        {
            "country": "Paraguay",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2000; baseline historical list",
        },
        {
            "country": "Peru",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2000; baseline historical list",
        },
        {
            "country": "Philippines",
            "last_recorded_rinderpest": "1955",
            "listing_date": "2000; baseline historical list",
        },
        {"country": "Poland", "last_recorded_rinderpest": "1921", "listing_date": "2000; baseline historical list"},
        {
            "country": "Portugal",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2000; baseline historical list",
        },
        {
            "country": "Qatar",
            "last_recorded_rinderpest": "1987",
            "listing_date": "2010: dossier of evidence to the OIE",
        },
        {"country": "Romania", "last_recorded_rinderpest": "1886", "listing_date": "2000; baseline historical list"},
        {
            "country": "Russian Federation",
            "last_recorded_rinderpest": "1998",
            "listing_date": "2010; dossier of evidence to the OIE",
        },
        {"country": "Rwanda", "last_recorded_rinderpest": "1932", "listing_date": "2006; additional historical list"},
        {
            "country": "Saint Kitts and Nevis",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2011; additional historical list",
        },
        {
            "country": "Saint Lucia",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2011; additional historical list",
        },
        {
            "country": "Saint Vincent and the Grenadines",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2009; additional historical list",
        },
        {
            "country": "San Marino",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2009; additional historical list",
        },
        {
            "country": "Samoa",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2009; additional historic list",
        },
        {
            "country": "São Tomé and Príncipe",
            "last_recorded_rinderpest": "1950s (imported)",
            "listing_date": "2011; additional historical list",
        },
        {
            "country": "Saudi Arabia",
            "last_recorded_rinderpest": "1999",
            "listing_date": "2011; dossier of evidence to the OIE",
        },
        {
            "country": "Senegal",
            "last_recorded_rinderpest": "1979",
            "listing_date": "2005; dossier of evidence to the OIE",
        },
        {"country": "Serbia", "last_recorded_rinderpest": "1883", "listing_date": "2008; additional historical list"},
        {
            "country": "Seychelles",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2009; additional historical list",
        },
        {
            "country": "Sierra Leone",
            "last_recorded_rinderpest": "1958",
            "listing_date": "2011; additional historical list",
        },
        {"country": "Singapore", "last_recorded_rinderpest": "1930", "listing_date": "2000; baseline historical list"},
        {"country": "Slovakia", "last_recorded_rinderpest": "1881", "listing_date": "2000; baseline historical list"},
        {"country": "Slovenia", "last_recorded_rinderpest": "1883", "listing_date": "2000; baseline historical list"},
        {
            "country": "Solomon Islands",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2009; additional historical list",
        },
        {
            "country": "Somalia",
            "last_recorded_rinderpest": "1993",
            "listing_date": "2010; dossier of evidence to the OIE",
        },
        {
            "country": "South Africa",
            "last_recorded_rinderpest": "1904",
            "listing_date": "2000; baseline historical list",
        },
        {
            "country": "Spain",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2000; baseline historical list",
        },
        {
            "country": "Sri Lanka",
            "last_recorded_rinderpest": "1994",
            "listing_date": "2011; dossier of evidence to the OIE",
        },
        {
            "country": "Sudan",
            "last_recorded_rinderpest": "1998",
            "listing_date": "2008; dossier of evidence to the OIE",
        },
        {
            "country": "Suriname",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2009; additional historical list",
        },
        {"country": "Swaziland", "last_recorded_rinderpest": "1898", "listing_date": "2000; baseline historical list"},
        {"country": "Sweden", "last_recorded_rinderpest": "1700", "listing_date": "2000; baseline historical list"},
        {
            "country": "Switzerland",
            "last_recorded_rinderpest": "1871",
            "listing_date": "2000; baseline historical list",
        },
        {
            "country": "Syrian Arab Republic",
            "last_recorded_rinderpest": "1983",
            "listing_date": "2010; dossier of evidence to the OIE",
        },
        {
            "country": "Tajikistan",
            "last_recorded_rinderpest": "1949",
            "listing_date": "2008; dossier of evidence to the OIE",
        },
        {
            "country": "Tanzania (United Republic of)",
            "last_recorded_rinderpest": "1997",
            "listing_date": "2007; dossier of evidence to the OIE",
        },
        {"country": "Thailand", "last_recorded_rinderpest": "1956", "listing_date": "2004; dossier to the OIE"},
        {
            "country": "Timor Leste",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2009; additional historical list",
        },
        {
            "country": "Tonga",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2010; additional historical list",
        },
        {"country": "Togo", "last_recorded_rinderpest": "1986", "listing_date": "2005; dossier of evidence to the OIE"},
        {
            "country": "Trinidad and Tobago",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2000; baseline historical list",
        },
        {
            "country": "Tunisia",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2000; baseline historical list",
        },
        {
            "country": "Turkey",
            "last_recorded_rinderpest": "1996",
            "listing_date": "2005; dossier of evidence to the OIE",
        },
        {
            "country": "Turkmenistan",
            "last_recorded_rinderpest": "1954",
            "listing_date": "2011; dossier of evidence to the OIE",
        },
        {
            "country": "Tuvalu",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2010; additional historical list",
        },
        {
            "country": "Uganda",
            "last_recorded_rinderpest": "1994",
            "listing_date": "2008; dossier of evidence to the OIE",
        },
        {
            "country": "Ukraine",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2000; baseline historical list",
        },
        {
            "country": "United Arab Emirates",
            "last_recorded_rinderpest": "1995",
            "listing_date": "2011; dossier of evidence to the OIE",
        },
        {
            "country": "United Kingdom",
            "last_recorded_rinderpest": "1900",
            "listing_date": "2000; baseline historical list",
        },
        {
            "country": "United States of America",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2000; baseline historical list",
        },
        {
            "country": "Uruguay",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2000; baseline historical list",
        },
        {
            "country": "Uzbekistan",
            "last_recorded_rinderpest": "1928",
            "listing_date": "2008; dossier of evidence to the OIE",
        },
        {
            "country": "Vanuatu",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2000; baseline historical list",
        },
        {
            "country": "Venezuela",
            "last_recorded_rinderpest": "Never reported",
            "listing_date": "2000; baseline historical list",
        },
        {"country": "Viet Nam", "last_recorded_rinderpest": "1977", "listing_date": "2000; baseline historical list"},
        {
            "country": "Yemen",
            "last_recorded_rinderpest": "1995",
            "listing_date": "2010; dossier of evidence to the OIE",
        },
        {"country": "Zambia", "last_recorded_rinderpest": "1896", "listing_date": "2006; additional historical list"},
        {"country": "Zimbabwe", "last_recorded_rinderpest": "1898", "listing_date": "2000; baseline historical list"},
    ]
)


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def run(upload: bool) -> None:
    # Initialize a new snapshot.
    snap = Snapshot(f"health/{SNAPSHOT_VERSION}/rinderpest.csv")

    # Save snapshot.
    snap.create_snapshot(upload=upload, data=df)


if __name__ == "__main__":
    run()
