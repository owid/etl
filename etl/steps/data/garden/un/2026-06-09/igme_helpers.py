# Helper function with hardcoded "expected" changes to remove from the rapid change check, based on visual inspection of the data and research. This is to avoid flagging these changes as errors when they are actually expected due to known events such as wars, famines, natural disasters, etc.


def check_expected_changes(changes, country: str):
    """
    Removed "expected" changes from the rapid change check, as these are not necessarily errors. These are based on visual inspection of the data and research.
    """
    if country == "Afghanistan":
        # 1971-1972: likely Drought and famine
        changes = changes[~((changes["year"] >= 1971) & (changes["year"] <= 1972))]
        # 1979–1989: Soviet-Afghan War
        # https://en.wikipedia.org/wiki/Soviet%E2%80%93Afghan_War
        changes = changes[~((changes["year"] >= 1979) & (changes["year"] <= 1989))]
        # 1993–1994: Afghan Civil War (post-Soviet factional fighting)
        # https://en.wikipedia.org/wiki/Afghan_Civil_War_(1992%E2%80%931996)
        changes = changes[~((changes["year"] >= 1993) & (changes["year"] <= 1994))]
        # 1998–1999: Taliban offensives + Takhar earthquake
        # https://en.wikipedia.org/wiki/1998_Afghanistan_earthquake
        changes = changes[~((changes["year"] >= 1998) & (changes["year"] <= 1999))]
        # 2006–2022: War in Afghanistan (NATO escalation to Taliban takeover)
        # https://en.wikipedia.org/wiki/War_in_Afghanistan_(2001%E2%80%932021)
        changes = changes[~((changes["year"] >= 2006) & (changes["year"] <= 2022))]

    elif country == "Albania":
        # 1997–1998: Collapse of pyramid schemes → civil unrest
        # https://en.wikipedia.org/wiki/1997_rebellion_in_Albania
        changes = changes[~((changes["year"] >= 1997) & (changes["year"] <= 1998))]

    elif country == "Algeria":
        # 1994–1999: Algerian Civil War ("Black Decade")
        # https://en.wikipedia.org/wiki/Algerian_Civil_War
        changes = changes[~((changes["year"] >= 1994) & (changes["year"] <= 1999))]

    elif country == "Angola":
        # 1993–1998: Angolan Civil War (post-1992 election collapse of ceasefire)
        # https://en.wikipedia.org/wiki/Angolan_Civil_War
        changes = changes[~((changes["year"] >= 1993) & (changes["year"] <= 1998))]

    elif country == "Armenia":
        # 1988–1989: Spitak earthquake (~25,000 deaths)
        # https://en.wikipedia.org/wiki/1988_Spitak_earthquake
        changes = changes[~((changes["year"] >= 1988) & (changes["year"] <= 1989))]
        # 1992–1993: First Nagorno-Karabakh War
        # https://en.wikipedia.org/wiki/First_Nagorno-Karabakh_War
        changes = changes[~((changes["year"] >= 1992) & (changes["year"] <= 1993))]
        # 2022: Second Nagorno-Karabakh War / Azerbaijani offensive
        # https://en.wikipedia.org/wiki/2022_Nagorno-Karabakh_clashes
        changes = changes[~(changes["year"] == 2022)]

    elif country == "Azerbaijan":
        # 1991–1996: First Nagorno-Karabakh War and aftermath
        # https://en.wikipedia.org/wiki/First_Nagorno-Karabakh_War
        changes = changes[~((changes["year"] >= 1991) & (changes["year"] <= 1996))]
        # 2020–2024: Second Nagorno-Karabakh War and 2023 offensive
        # https://en.wikipedia.org/wiki/2020_Nagorno-Karabakh_war
        changes = changes[~((changes["year"] >= 2020) & (changes["year"] <= 2024))]

    elif country == "Bahamas":
        # 2019–2020: Hurricane Dorian (Category 5, 74+ deaths)
        # https://en.wikipedia.org/wiki/Hurricane_Dorian
        changes = changes[~((changes["year"] >= 2019) & (changes["year"] <= 2020))]

    elif country == "Bangladesh":
        # 1970–1972: 1970 Bhola cyclone + Bangladesh Liberation War
        # https://en.wikipedia.org/wiki/1970_Bhola_cyclone
        changes = changes[~((changes["year"] >= 1970) & (changes["year"] <= 1972))]
        # 1974–1976: Bangladesh famine
        # https://en.wikipedia.org/wiki/Bangladesh_famine_of_1974
        changes = changes[~((changes["year"] >= 1974) & (changes["year"] <= 1976))]
        # 1991–1992: 1991 Bangladesh cyclone (~138,000 deaths)
        # https://en.wikipedia.org/wiki/1991_Bangladesh_cyclone
        changes = changes[~((changes["year"] >= 1991) & (changes["year"] <= 1992))]

    elif country == "Bosnia and Herzegovina":
        # 1992–1996: Bosnian War
        # https://en.wikipedia.org/wiki/Bosnian_War
        changes = changes[~((changes["year"] >= 1992) & (changes["year"] <= 1996))]

    elif country == "Burundi":
        # 1972–1973: Burundian genocide against Hutu
        # https://en.wikipedia.org/wiki/1972_Burundian_genocide
        changes = changes[~((changes["year"] >= 1972) & (changes["year"] <= 1973))]
        # 1993–2002: Burundian Civil War
        # https://en.wikipedia.org/wiki/Burundian_Civil_War
        changes = changes[~((changes["year"] >= 1993) & (changes["year"] <= 2002))]

    elif country == "Cambodia":
        # 1979–1980: Vietnamese invasion / post-Khmer Rouge humanitarian crisis
        # https://en.wikipedia.org/wiki/Cambodian%E2%80%93Vietnamese_War
        changes = changes[~((changes["year"] >= 1979) & (changes["year"] <= 1980))]

    elif country == "Central African Republic":
        # 2009–2023: CAR Civil War
        # https://en.wikipedia.org/wiki/Central_African_Republic_Civil_War_(2012%E2%80%93present)
        changes = changes[~((changes["year"] >= 2009) & (changes["year"] <= 2023))]

    elif country == "Colombia":
        # 1985–1986: Nevado del Ruiz eruption / Armero disaster (~23,000 deaths)
        # https://en.wikipedia.org/wiki/1985_Nevado_del_Ruiz_eruption
        changes = changes[~((changes["year"] >= 1985) & (changes["year"] <= 1986))]

    elif country == "Congo":
        # 1997–1999: Republic of Congo Civil War
        # https://en.wikipedia.org/wiki/Republic_of_the_Congo_Civil_War_(1997%E2%80%931999)
        changes = changes[~((changes["year"] >= 1997) & (changes["year"] <= 1999))]
        # 2019–2020: Instability / Ebola epidemic spillover
        # https://en.wikipedia.org/wiki/Kivu_Ebola_epidemic
        changes = changes[~((changes["year"] >= 2019) & (changes["year"] <= 2020))]

    elif country == "Croatia":
        # 1991–1992: Croatian War of Independence
        # https://en.wikipedia.org/wiki/Croatian_War_of_Independence
        changes = changes[~((changes["year"] >= 1991) & (changes["year"] <= 1992))]
        # 2022–2024: COVID-19 excess mortality
        # https://en.wikipedia.org/wiki/COVID-19_pandemic_in_Croatia
        changes = changes[~((changes["year"] >= 2022) & (changes["year"] <= 2024))]

    elif country == "Democratic Republic of Congo":
        # 1996–2005: First and Second Congo Wars
        # https://en.wikipedia.org/wiki/Second_Congo_War
        changes = changes[~((changes["year"] >= 1996) & (changes["year"] <= 2005))]

    elif country == "Djibouti":
        # 1991–1993: Djiboutian Civil War
        # https://en.wikipedia.org/wiki/Djiboutian_Civil_War
        changes = changes[~((changes["year"] >= 1991) & (changes["year"] <= 1993))]

    elif country == "Dominica":
        # 1979–1980: Hurricane David (Category 5, 56 deaths)
        # https://en.wikipedia.org/wiki/Hurricane_David
        changes = changes[~((changes["year"] >= 1979) & (changes["year"] <= 1980))]
        # 2017–2018: Hurricane Maria (Category 5, 31+ deaths, near-total destruction)
        # https://en.wikipedia.org/wiki/Hurricane_Maria
        changes = changes[~((changes["year"] >= 2017) & (changes["year"] <= 2018))]

    elif country == "East Timor":
        # 1999–2000: East Timor independence crisis / Indonesian military violence
        # https://en.wikipedia.org/wiki/1999_East_Timorese_crisis
        changes = changes[~((changes["year"] >= 1999) & (changes["year"] <= 2000))]
        # 2014–2015: Political instability
        # https://en.wikipedia.org/wiki/East_Timor
        changes = changes[~((changes["year"] >= 2014) & (changes["year"] <= 2015))]

    elif country == "El Salvador":
        # 1980–1981: Salvadoran Civil War onset / El Mozote massacre
        # https://en.wikipedia.org/wiki/Salvadoran_Civil_War
        changes = changes[~((changes["year"] >= 1980) & (changes["year"] <= 1981))]
        # 1991–1992: Salvadoran Civil War final phase
        # https://en.wikipedia.org/wiki/Salvadoran_Civil_War
        changes = changes[~((changes["year"] >= 1991) & (changes["year"] <= 1992))]
        # 1998–1999: Hurricane Mitch
        # https://en.wikipedia.org/wiki/Hurricane_Mitch
        changes = changes[~((changes["year"] >= 1998) & (changes["year"] <= 1999))]
        # 2001–2002: El Salvador earthquakes (~1,000 deaths)
        # https://en.wikipedia.org/wiki/2001_El_Salvador_earthquakes
        changes = changes[~((changes["year"] >= 2001) & (changes["year"] <= 2002))]

    elif country == "Eritrea":
        # 1983–1986: Eritrean War of Independence
        # https://en.wikipedia.org/wiki/Eritrean_War_of_Independence
        changes = changes[~((changes["year"] >= 1983) & (changes["year"] <= 1986))]
        # 1998–2001: Eritrean–Ethiopian War
        # https://en.wikipedia.org/wiki/Eritrean%E2%80%93Ethiopian_War
        changes = changes[~((changes["year"] >= 1998) & (changes["year"] <= 2001))]

    elif country == "Ethiopia":
        # 1983–1987: Ethiopian famine (~1 million deaths) + civil war
        # https://en.wikipedia.org/wiki/1983%E2%80%931985_famine_in_Ethiopia
        changes = changes[~((changes["year"] >= 1983) & (changes["year"] <= 1987))]
        # 1999–2000: Eritrean–Ethiopian War
        # https://en.wikipedia.org/wiki/Eritrean%E2%80%93Ethiopian_War
        changes = changes[~((changes["year"] >= 1999) & (changes["year"] <= 2000))]
        # 2021–2023: Tigray War
        # https://en.wikipedia.org/wiki/Tigray_War
        changes = changes[~((changes["year"] >= 2021) & (changes["year"] <= 2023))]

    elif country == "Georgia":
        # 1992–1994: Georgian Civil War / Abkhazia and South Ossetia conflicts
        # https://en.wikipedia.org/wiki/Georgian_Civil_War
        changes = changes[~((changes["year"] >= 1992) & (changes["year"] <= 1994))]
        # 2008–2009: Russo-Georgian War
        # https://en.wikipedia.org/wiki/Russo-Georgian_War
        changes = changes[~((changes["year"] >= 2008) & (changes["year"] <= 2009))]

    elif country == "Guatemala":
        # 1976–1977: Guatemala earthquake (~23,000 deaths)
        # https://en.wikipedia.org/wiki/1976_Guatemala_earthquake
        changes = changes[~((changes["year"] >= 1976) & (changes["year"] <= 1977))]
        # 1982–1983: Guatemalan genocide (Ríos Montt massacres)
        # https://en.wikipedia.org/wiki/Guatemalan_genocide
        changes = changes[~((changes["year"] >= 1982) & (changes["year"] <= 1983))]

    elif country == "Haiti":
        # 2010–2011: Haiti earthquake (~100,000–300,000 deaths)
        # https://en.wikipedia.org/wiki/2010_Haiti_earthquake
        changes = changes[~((changes["year"] >= 2010) & (changes["year"] <= 2011))]

    elif country == "Honduras":
        # 1974–1975: Hurricane Fifi (~8,000 deaths)
        # https://en.wikipedia.org/wiki/Hurricane_Fifi
        changes = changes[~((changes["year"] >= 1974) & (changes["year"] <= 1975))]
        # 1998–1999: Hurricane Mitch (~7,000 deaths in Honduras)
        # https://en.wikipedia.org/wiki/Hurricane_Mitch
        changes = changes[~((changes["year"] >= 1998) & (changes["year"] <= 1999))]

    elif country == "Indonesia":
        # 1965: Indonesian mass killings (500,000–1,000,000 deaths)
        # https://en.wikipedia.org/wiki/Indonesian_mass_killings_of_1965%E2%80%9366
        changes = changes[~(changes["year"] == 1965)]
        # 2004–2005: Indian Ocean tsunami (~170,000 deaths in Indonesia)
        # https://en.wikipedia.org/wiki/2004_Indian_Ocean_earthquake_and_tsunami
        changes = changes[~((changes["year"] >= 2004) & (changes["year"] <= 2005))]

    elif country == "Iran":
        # 1991: Manjil–Rudbar earthquake 1990 (~40,000 deaths; data lagged)
        # https://en.wikipedia.org/wiki/1990_Manjil%E2%80%93Rudbar_earthquake
        changes = changes[~(changes["year"] == 1991)]
        # 2003–2004: Bam earthquake (~26,000 deaths)
        # https://en.wikipedia.org/wiki/2003_Bam_earthquake
        changes = changes[~((changes["year"] >= 2003) & (changes["year"] <= 2004))]

    elif country == "Iraq":
        # 1988–1989: Anfal genocide against Kurds
        # https://en.wikipedia.org/wiki/Anfal_genocide
        changes = changes[~((changes["year"] >= 1988) & (changes["year"] <= 1989))]
        # 1991–1992: Gulf War and aftermath
        # https://en.wikipedia.org/wiki/Gulf_War
        changes = changes[~((changes["year"] >= 1991) & (changes["year"] <= 1992))]
        # 2003–2009: Iraq War + sectarian civil war
        # https://en.wikipedia.org/wiki/Iraq_War
        changes = changes[~((changes["year"] >= 2003) & (changes["year"] <= 2009))]
        # 2016–2018: War against ISIS / Battle of Mosul
        # https://en.wikipedia.org/wiki/Battle_of_Mosul_(2016%E2%80%932017)
        changes = changes[~((changes["year"] >= 2016) & (changes["year"] <= 2018))]

    elif country == "Japan":
        # 1995: Great Hanshin (Kobe) earthquake (~6,434 deaths)
        # https://en.wikipedia.org/wiki/Great_Hanshin_earthquake
        changes = changes[~(changes["year"] == 1995)]
        # 2011–2012: Tōhoku earthquake and tsunami (~19,000 deaths)
        # https://en.wikipedia.org/wiki/2011_T%C5%8Dhoku_earthquake_and_tsunami
        changes = changes[~((changes["year"] >= 2011) & (changes["year"] <= 2012))]

    elif country == "Kuwait":
        # 1991–1992: Gulf War / Iraqi occupation and liberation
        # https://en.wikipedia.org/wiki/Gulf_War
        changes = changes[~((changes["year"] >= 1991) & (changes["year"] <= 1992))]

    elif country == "Lebanon":
        # 1976–1991: Lebanese Civil War
        # https://en.wikipedia.org/wiki/Lebanese_Civil_War
        changes = changes[~((changes["year"] >= 1976) & (changes["year"] <= 1991))]
        # 2006–2007: Israel–Hezbollah War
        # https://en.wikipedia.org/wiki/2006_Lebanon_War
        changes = changes[~((changes["year"] >= 2006) & (changes["year"] <= 2007))]
        # 2024: Israeli strikes / displacement crisis
        # https://en.wikipedia.org/wiki/2024_Israel%E2%80%93Lebanon_conflict
        changes = changes[~(changes["year"] == 2024)]

    elif country == "Liberia":
        # 1991–1997: First Liberian Civil War
        # https://en.wikipedia.org/wiki/First_Liberian_Civil_War
        changes = changes[~((changes["year"] >= 1991) & (changes["year"] <= 1997))]
        # 2003–2004: Second Liberian Civil War
        # https://en.wikipedia.org/wiki/Second_Liberian_Civil_War
        changes = changes[~((changes["year"] >= 2003) & (changes["year"] <= 2004))]

    elif country == "Libya":
        # 2011–2024: First and Second Libyan Civil Wars
        # https://en.wikipedia.org/wiki/Second_Libyan_Civil_War
        changes = changes[~((changes["year"] >= 2011) & (changes["year"] <= 2024))]

    elif country == "Malawi":
        # 2001–2002: Malawi food crisis / famine
        # https://en.wikipedia.org/wiki/2002_Malawi_food_crisis
        changes = changes[~((changes["year"] >= 2001) & (changes["year"] <= 2002))]

    elif country == "Maldives":
        # 2004–2005: Indian Ocean tsunami (82 deaths)
        # https://en.wikipedia.org/wiki/Effect_of_the_2004_Indian_Ocean_earthquake_on_the_Maldives
        changes = changes[~((changes["year"] >= 2004) & (changes["year"] <= 2005))]

    elif country == "Myanmar":
        # 2008–2009: Cyclone Nargis (~138,000 deaths)
        # https://en.wikipedia.org/wiki/Cyclone_Nargis
        changes = changes[~((changes["year"] >= 2008) & (changes["year"] <= 2009))]
        # 2021–2022: Military coup and crackdown
        # https://en.wikipedia.org/wiki/2021_Myanmar_coup_d%27%C3%A9tat
        changes = changes[~((changes["year"] >= 2021) & (changes["year"] <= 2022))]

    elif country == "Nepal":
        # 2002: Maoist insurgency escalation
        # https://en.wikipedia.org/wiki/Communist_Party_of_Nepal_(Maoist)
        changes = changes[~(changes["year"] == 2002)]
        # 2015–2016: Nepal earthquake (~9,000 deaths)
        # https://en.wikipedia.org/wiki/2015_Nepal_earthquake
        changes = changes[~((changes["year"] >= 2015) & (changes["year"] <= 2016))]

    elif country == "Nicaragua":
        # 1972: Managua earthquake (~10,000 deaths)
        # https://en.wikipedia.org/wiki/1972_Managua_earthquake
        changes = changes[~(changes["year"] == 1972)]
        # 1998–1999: Hurricane Mitch (~3,500 deaths)
        # https://en.wikipedia.org/wiki/Hurricane_Mitch
        changes = changes[~((changes["year"] >= 1998) & (changes["year"] <= 1999))]

    elif country == "Nigeria":
        # 1968–1971: Nigerian Civil War / Biafra War and famine
        # https://en.wikipedia.org/wiki/Nigerian_Civil_War
        changes = changes[~((changes["year"] >= 1968) & (changes["year"] <= 1971))]

    elif country == "North Korea":
        # 1952–1954: Korean War and immediate aftermath
        # https://en.wikipedia.org/wiki/Korean_War
        changes = changes[~((changes["year"] >= 1952) & (changes["year"] <= 1954))]
        # 1995–2003: North Korean famine ("Arduous March")
        # https://en.wikipedia.org/wiki/North_Korean_famine
        changes = changes[~((changes["year"] >= 1995) & (changes["year"] <= 2003))]

    elif country == "Pakistan":
        # 2005–2006: Kashmir earthquake (~73,000 deaths)
        # https://en.wikipedia.org/wiki/2005_Kashmir_earthquake
        changes = changes[~((changes["year"] >= 2005) & (changes["year"] <= 2006))]

    elif country == "Palestine":
        # 2000–2002: Second Intifada
        # https://en.wikipedia.org/wiki/Second_Intifada
        changes = changes[~((changes["year"] >= 2000) & (changes["year"] <= 2002))]
        # 2005–2006: Post-disengagement violence / Hamas–Fatah conflict
        # https://en.wikipedia.org/wiki/Fatah%E2%80%93Hamas_conflict
        changes = changes[~((changes["year"] >= 2005) & (changes["year"] <= 2006))]
        # 2008–2010: Gaza War (Operation Cast Lead)
        # https://en.wikipedia.org/wiki/Gaza_War_(2008%E2%80%932009)
        changes = changes[~((changes["year"] >= 2008) & (changes["year"] <= 2010))]
        # 2014–2015: 2014 Gaza War (Operation Protective Edge)
        # https://en.wikipedia.org/wiki/2014_Gaza_War
        changes = changes[~((changes["year"] >= 2014) & (changes["year"] <= 2015))]
        # 2023: 2023–present Gaza war
        # https://en.wikipedia.org/wiki/2023_Israel%E2%80%93Hamas_war
        changes = changes[~(changes["year"] == 2023)]

    elif country == "Peru":
        # 1970–1971: Ancash earthquake (~66,000 deaths)
        # https://en.wikipedia.org/wiki/1970_Ancash_earthquake
        changes = changes[~((changes["year"] >= 1970) & (changes["year"] <= 1971))]

    elif country == "Rwanda":
        # 1994–1995: Rwandan genocide (~800,000 deaths)
        # https://en.wikipedia.org/wiki/Rwandan_genocide
        changes = changes[~((changes["year"] >= 1994) & (changes["year"] <= 1995))]
        # 1997–1999: First Congo War spillover / Rwandan insurgency in DRC
        # https://en.wikipedia.org/wiki/First_Congo_War
        changes = changes[~((changes["year"] >= 1997) & (changes["year"] <= 1999))]

    elif country == "Samoa":
        # 2009–2010: Samoa earthquake and tsunami (~189 deaths)
        # https://en.wikipedia.org/wiki/2009_Samoa_earthquake
        changes = changes[~((changes["year"] >= 2009) & (changes["year"] <= 2010))]
        # 2019–2020: Measles outbreak (83 deaths, 2.8% of population infected)
        # https://en.wikipedia.org/wiki/2019_Samoa_measles_outbreak
        changes = changes[~((changes["year"] >= 2019) & (changes["year"] <= 2020))]

    elif country == "Sierra Leone":
        # 1994–2000: Sierra Leone Civil War
        # https://en.wikipedia.org/wiki/Sierra_Leone_Civil_War
        changes = changes[~((changes["year"] >= 1994) & (changes["year"] <= 2000))]

    elif country == "Somalia":
        # 1991–1994: Somali Civil War onset + 1992 famine
        # https://en.wikipedia.org/wiki/Somali_Civil_War
        changes = changes[~((changes["year"] >= 1991) & (changes["year"] <= 1994))]
        # 2010–2023: Ongoing civil war, Al-Shabaab conflict, 2011 famine
        # https://en.wikipedia.org/wiki/Somali_Civil_War_(2009%E2%80%93present)
        changes = changes[~((changes["year"] >= 2010) & (changes["year"] <= 2023))]

    elif country == "South Sudan":
        # 1987–2003: Second Sudanese Civil War (South Sudan component)
        # https://en.wikipedia.org/wiki/Second_Sudanese_Civil_War
        changes = changes[~((changes["year"] >= 1987) & (changes["year"] <= 2003))]
        # 2014–2021: South Sudanese Civil War
        # https://en.wikipedia.org/wiki/South_Sudanese_Civil_War
        changes = changes[~((changes["year"] >= 2014) & (changes["year"] <= 2021))]

    elif country == "Sri Lanka":
        # 1985–2002: Sri Lankan Civil War (various phases)
        # https://en.wikipedia.org/wiki/Sri_Lankan_Civil_War
        changes = changes[~((changes["year"] >= 1985) & (changes["year"] <= 2002))]
        # 2004–2005: Indian Ocean tsunami (~35,000 deaths in Sri Lanka)
        # https://en.wikipedia.org/wiki/2004_Indian_Ocean_earthquake_and_tsunami
        changes = changes[~((changes["year"] >= 2004) & (changes["year"] <= 2005))]
        # 2006–2009: Final phase of Sri Lankan Civil War (Eelam War IV)
        # https://en.wikipedia.org/wiki/Eelam_War_IV
        changes = changes[~((changes["year"] >= 2006) & (changes["year"] <= 2009))]

    elif country == "Sudan":
        # 1983–2008: Second Sudanese Civil War + Darfur conflict
        # https://en.wikipedia.org/wiki/War_in_Darfur
        changes = changes[~((changes["year"] >= 1983) & (changes["year"] <= 2008))]
        # 2023: Sudanese Civil War
        # https://en.wikipedia.org/wiki/2023_Sudanese_civil_war
        changes = changes[~(changes["year"] == 2023)]

    elif country == "Syria":
        # 1982–1983: Hama massacre
        # https://en.wikipedia.org/wiki/Hama_massacre
        changes = changes[~((changes["year"] >= 1982) & (changes["year"] <= 1983))]
        # 2011–2024: Syrian Civil War
        # https://en.wikipedia.org/wiki/Syrian_civil_war
        changes = changes[~((changes["year"] >= 2011) & (changes["year"] <= 2024))]

    elif country == "Tajikistan":
        # 1992–1997: Tajikistani Civil War
        # https://en.wikipedia.org/wiki/Tajikistani_Civil_War
        changes = changes[~((changes["year"] >= 1992) & (changes["year"] <= 1997))]

    elif country == "Turkey":
        # 1995: PKK conflict escalation
        # https://en.wikipedia.org/wiki/Kurdish%E2%80%93Turkish_conflict_(1978%E2%80%93present)
        changes = changes[~(changes["year"] == 1995)]
        # 1999–2000: İzmit earthquake (~17,000 deaths)
        # https://en.wikipedia.org/wiki/1999_%C4%B0zmit_earthquake
        changes = changes[~((changes["year"] >= 1999) & (changes["year"] <= 2000))]
        # 2011: Van earthquakes (~600 deaths)
        # https://en.wikipedia.org/wiki/2011_Van_earthquakes
        changes = changes[~(changes["year"] == 2011)]
        # 2023–2024: Kahramanmaraş earthquakes (~50,000 deaths)
        # https://en.wikipedia.org/wiki/2023_Turkey%E2%80%93Syria_earthquakes
        changes = changes[~((changes["year"] >= 2023) & (changes["year"] <= 2024))]

    elif country == "Ukraine":
        # 2022–2024: Russian invasion of Ukraine
        # https://en.wikipedia.org/wiki/Russian_invasion_of_Ukraine
        changes = changes[~((changes["year"] >= 2022) & (changes["year"] <= 2024))]

    elif country == "Venezuela":
        # 2015–2016: Venezuelan economic crisis (healthcare collapse, rising infant/maternal mortality)
        # https://en.wikipedia.org/wiki/Crisis_in_Venezuela
        changes = changes[~((changes["year"] >= 2015) & (changes["year"] <= 2016))]

    elif country == "Vietnam":
        # 1975: Fall of Saigon / end of Vietnam War
        # https://en.wikipedia.org/wiki/Fall_of_Saigon
        changes = changes[~(changes["year"] == 1975)]

    elif country == "Yemen":
        # 2015–2022: Yemeni Civil War
        # https://en.wikipedia.org/wiki/Yemeni_Civil_War_(2014%E2%80%93present)
        changes = changes[~((changes["year"] >= 2015) & (changes["year"] <= 2022))]

    return changes
