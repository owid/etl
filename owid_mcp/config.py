"""Configuration for OWID MCP Server."""

import os
import httpx

# Base URLs
DATASETTE_BASE = os.getenv("OWID_DATASETTE_BASE", "https://datasette-public.owid.io/owid.json")
OWID_API_BASE = os.getenv("OWID_API_BASE", "https://api.ourworldindata.org/v1/indicators")
GRAPHER_BASE = os.getenv("GRAPHER_BASE", "https://ourworldindata.org/grapher")

# HTTP configuration
HTTP_TIMEOUT = httpx.Timeout(10.0)

# SQL tool configuration
MAX_ROWS_DEFAULT = 1000
MAX_ROWS_HARD = 5000

# Common entities list for instructions
COMMON_ENTITIES = """
Abkhazia
Afghanistan
Africa
Akrotiri and Dhekelia
Aland Islands
Albania
Algeria
American Samoa
Andorra
Angola
Anguilla
Antarctica
Antigua and Barbuda
Argentina
Armenia
Aruba
Asia
Australia
Austria
Austria-Hungary
Azerbaijan
Bahamas
Bahrain
Bangladesh
Barbados
Belarus
Belgium
Belize
Benin
Bermuda
Bhutan
Bolivia
Bonaire Sint Eustatius and Saba
Bosnia and Herzegovina
Bosnia and Herz.
Botswana
Bouvet Island
Brazil
British Indian Ocean Territory
British Virgin Islands
Brunei
Bulgaria
Burkina Faso
Burundi
Cambodia
Cameroon
Canada
Cape Verde
Cayman Islands
Central African Republic
Chad
Channel Islands
Chile
China
Christmas Island
Cocos Islands
Colombia
Comoros
Congo
Cook Islands
Costa Rica
Cote d'Ivoire
Croatia
Cuba
Curacao
Cyprus
Czechia
Czechoslovakia
Democratic Republic of Congo
DR Congo
Denmark
Djibouti
Dominica
Dominican Republic
East Germany
East Timor
Ecuador
Egypt
El Salvador
Equatorial Guinea
Eritrea
Ethiopia (former)
Estonia
Eswatini
Ethiopia
Europe
European Union (27)
Faroe Islands
Falkland Islands
Fiji
Finland
France
French Guiana
French Polynesia
French Southern Territories
Gabon
Gambia
Gaza Strip
Georgia
Germany
Ghana
Gibraltar
Greece
Greenland
Grenada
Guadeloupe
Guam
Guatemala
Guernsey
Guinea
Guinea-Bissau
Guyana
Haiti
Heard Island and McDonald Islands
Honduras
Hong Kong
Hungary
Iceland
India
Indonesia
Iran
Iraq
Ireland
Isle of Man
Israel
Italy
Jamaica
Japan
Jersey
Jordan
Kazakhstan
Kenya
Kiribati
Kosovo
Kuwait
Kyrgyzstan
Laos
Latvia
Lebanon
Lesotho
Liberia
Libya
Liechtenstein
Lithuania
Luxembourg
Macao
Madagascar
Malawi
Malaysia
Maldives
Mali
Malta
Marshall Islands
Martinique
Mauritania
Mauritius
Mayotte
Melanesia
Mexico
Micronesia (country)
Moldova
Monaco
Mongolia
Montenegro
Montserrat
Morocco
Mozambique
Myanmar
Nagorno-Karabakh
Namibia
Nauru
Nepal
Netherlands
Netherlands Antilles
New Caledonia
New Zealand
Nicaragua
Niger
Nigeria
Niue
Norfolk Island
North America
North Korea
North Macedonia
Northern Cyprus
Northern Mariana Islands
Norway
Oceania
Oman
Pakistan
Palau
Palestine
Panama
Papua New Guinea
Paraguay
Peru
Philippines
Pitcairn
Poland
Polynesia
Portugal
Puerto Rico
Qatar
Democratic Republic of Vietnam
Republic of Vietnam
Reunion
Romania
Russia
Rwanda
Saint Barthelemy
Saint Helena
Saint Kitts and Nevis
Saint Lucia
Saint Martin (French part)
Saint Pierre and Miquelon
Saint Vincent and the Grenadines
Samoa
San Marino
Sao Tome and Principe
Saudi Arabia
Senegal
Serbia
Serbia and Montenegro
Serbia excluding Kosovo
Seychelles
Sierra Leone
Singapore
Sint Maarten (Dutch part)
Slovakia
Slovenia
Solomon Islands
Somalia
Somaliland
South Africa
South America
South Georgia and the South Sandwich Islands
South Korea
South Ossetia
South Sudan
Spain
Sri Lanka
Sudan
Sudan (former)
Suriname
Svalbard and Jan Mayen
Sweden
Switzerland
Syria
Taiwan
Tajikistan
Tanzania
Thailand
Togo
Tokelau
Tonga
Transnistria
Trinidad and Tobago
Tunisia
Turkey
Turkmenistan
Turks and Caicos Islands
Turks and Caicos
Tuvalu
USSR
Uganda
Ukraine
United Arab Emirates
United Kingdom
Korea (former)
United States
United States Minor Outlying Islands
United States Virgin Islands
Uruguay
Uzbekistan
Vanuatu
Vatican
Venezuela
Vietnam
Wallis and Futuna
West Germany
Western Sahara
World
Yemen
Yemen Arab Republic
Yemen People's Republic
Yugoslavia
Zambia
Zanzibar
Zimbabwe
Grand Duchy of Tuscany
Kingdom of the Two Sicilies
Duchy of Modena and Reggio
Kingdom of Sardinia
Duchy of Parma and Piacenza
Grand Duchy of Baden
Kingdom of Bavaria
Kingdom of Saxony
Kingdom of Wurttemberg
Great Colombia
Federal Republic of Central America
"""