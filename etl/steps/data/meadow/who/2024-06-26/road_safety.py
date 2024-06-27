"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# As reference
ALL_COLUMNS = [
    "Country name",
    # reported fatalities
    "Reported fatalities",
    "Year reported fatalities",
    "Reported fatalities gender distribution  (% Males)",
    "Reported fatalities gender distribution  (%Females)",
    "Reported fatalities user distribution  (% powered light vehicles)",
    "Reported fatalities user distribution  (% powered 2/ wheelers)",
    "Reported fatalities user distribution  (% pedestrian)",
    "Reported fatalities user distribution  (% cyclist)",
    "Reported fatalities user distribution  (% other)",
    "WHO-estimated road traffic fatalities ",
    "Lower bound WHO-estimated road traffic fatalities",
    "Upper bound WHO-estimated road traffic fatalities",
    "Year WHO-estimated road traffic fatalities",
    "WHO-estimated rate per 100 000 population",
    # National laws to post-crash care
    "National law on universal access to emergency care \u202f",
    "National law guaranteeing free-of-charge access to rehabilitative care for all injured",
    "National law guaranteeing free-of-charge access to psychological services to road crash victims and their families ",
    "National good Samaritan law",
    "National emergency care access number",
    # paved kilometers
    "Total paved kilometres (year)",
    "Year total paved kilometres",
    # registered vehicles
    "Total registered vehicles ",
    "Total registered vehicles rate per 100 000 pop",
    "Cars and 4-wheeled light vehicles ",
    "Powered 2- and 3-wheelers ",
    "Heavy trucks ",
    "Buses ",
    "Other ",
    "Year total registered vehicles",
    # legislation on road safety
    "Legislation on periodic vehicle technical inspection",
    "Presence of high-quality safety standards for used-vehicle imports/exports 2",
    "National law on front and side impact protection",
    "National law on seat belt and seat belt anchorages",
    "National law on electronic stability control",
    "National law on pedestrian protection",
    "National law on motorcycle anti-lock braking systems",
    "Government vehicle procurement practices include safety prerequisites",
    "Presence of strategies to promote alternatives to individuals' use of powered vehicles",
    "National legislation mandating third-party liability insurance for powered vehicles",
    "National law on driving time and rest periods for professional drivers",
    # road safety national strategy
    "National road safety strategy",
    "Funding to implement strategy",
    "Presence of national lead agency to implement  national road safety action plan",
    "Presence of agencies that  coordinate pre‐hospital and emergency medical services",
    # speed limits
    "Legislation setting appropriate urban speed limits for passenger cars and motorcycles",
    "National law setting a speed limit  ",
    "Maximum urban speed limit ",
    "Maximum rural speed limit ",
    "Maximum motorway speed limit ",
    # drunk driving
    "Legislation on drink driving",
    "National law on drink-driving ",
    "BAC limit – general population",
    "BAC limit – young or novice drivers ",
    "Legislation on drug driving",
    # distracted driving
    "Legislation on distracted driving (mobile phones) while driving",
    "Ban on mobile phone use (Hand held)",
    "Ban on mobile phone use (Hand free)",
    # motorcycle helmet law/ use
    "Legislation requiring adult motorcycle riders to wear a helmet properly fastened that meets appropriate standards",
    "National motorcycle helmet law ",
    "Helmet wearing rate (%Driver)",
    "Helmet wearing rate (% Passenger)",
    "Minimum age/height children are allowed as passengers on motorcycles nationally",
    # seat belt law/ use
    "Legislation on the use of seat belts for all motor vehicle occupants (UNVTI 8a)",
    "National seat-belt law ",
    "Legislation applies to front and rear seat occupants ",
    "Seat-belt wearing rate  (% Drivers)",
    "Seat-belt wearing rate (% front seat occupants)",
    "Seat-belt wearing rate  (% rear seat occupants)",
    # child restraint system
    "Legislation requiring the use of child safety restraint systems that meet appropriate standards",
    "National child restraints use law",
    "Children seated in front seat ",
    "Age or height specified for children requiring child restraint",
    "Child restraint standard referred to and/or specified ",
    # estimated fatalities (previous years)
    "2010 WHO-estimated road traffic fatalities (update)",
    "2010 Lower bound WHO-estimated road traffic fatalities (update)",
    "2010 Upper bound WHO-estimated road traffic fatalities (update)",
    "2010 WHO-estimated rate per 100 000 population (update)",
    "2016 WHO-estimated road traffic fatalities (update)",
    "2016 Lower bound WHO-estimated road traffic fatalities (update)",
    "2016 Upper bound WHO-estimated road traffic fatalities (update)",
    "2016 WHO-estimated rate per 100 000 population (update)",
]

# Columns to drop
COLS_TO_DROP = [
    "WHO status",
    "ISO_3 country name",
    "Population",
    "Income group",
    "WHO Region",
    "GRSSR participation 2009",
    "GRSSR participation 2013",
    "GRSSR participation 2015",
    "GRSSR participation 2018",
    "GRSSR participation 2013.1",
    "Year WHO-estimated road traffic fatalities (Update)",
    "Year WHO-estimated road traffic fatalities (Update).1",
    # option for not validated - this is used for visualization
    "emergency care access legislation not validated",
    "Access to rehab care legislation not validated",
    "Psychological care access legislation not validated",
    "Good Samaritan law not validated",
    "Front and side impact protection legislation not validated",
    "VTI legislation not validated",
    "Drug and diving legislation not validated",
    "Drink an drive legislation not validated",
    "Distracted driving legislation not validated",
    "Helmet legislation not validated",
    "Seat belt legislation not validated",
    "CRS legislation not validated",
    "Legislation on road inspection/assessment not validated",
    "Unnamed: 87",
    "Speed legislation not validated",
    "On driving time legislation  not validated",
    "Insurance legislation not validated",
    # Evolution of variables - this is used for visualization
    "Evolution of population",
    "Evolution of income group",
    "Evolution of reported fatalities",
    "Evolution of WHO-estimated fatalities",
    "Evolution of WHO-estimated fatality rate",
    "Evolution of presence of systematic approaches to assess/audit new roads",
    "Evolution of investments to upgrade high risk locations ",
    "Evolution of total registered vehicles",
    "Evolution of cars and 4-wheeled light vehicles ",
    "Evolution of powered 2- and 3-wheelers ",
    "Evolution of heavy trucks ",
    "Evolution of buses ",
    "Evolution of other ",
    "Evolution of national emergency care access number",
    "Evolution of strategies to promote alternatives to individuals' use of powered vehicles",
    "Evolution of National road safety strategy",
    "Evolution of fatality reduction target",
    "Evolution of non fatal reduction target",
    "Evolution of national lead agency to implement  national road safety action plan",
    "Evolution Legislation setting appropriate urban speed limits for passenger cars and motorcycles",
    "Evolution of national law setting a speed limit  ",
    "Evolution of maximum urban speed limit ",
    "Evolution of maximum rural speed limit ",
    "Evolution of maximum motorway speed limit ",
    "Evolution of whether local authorities can modify limits ",
    "Evolution of available types of enforcement",
    "Evolution of legislation on drink driving",
    "Evolution of national law on drink-driving ",
    "Evolution of BAC limit – general population",
    "Evolution of BAC limit – young or novice drivers ",
    "Evolution of Random breath testing carried out ",
    "Evolution of Testing carried out in case of fatal crash ",
    "Evolution of Legislation on drug driving",
    "Evolution of Legislation on distracted driving (mobile phones) while driving",
    "Evolution on ban on mobile phone use",
    "Evolution of Legislation requiring adult motorcycle riders to wear a helmet properly fastened that meets appropriate standards",
    "Evolution of National motorcycle helmet law ",
    "Evolution of presence of targets to reduce driving after drinking nationally ",
    "Evolution of legislation applies to drivers and passengers ",
    "Evolution of legislation applies to all road types",
    "Evolution of legislation applies to all engine types",
    "Evolution of legislation refers to and/or specifies helmet standard",
    "Evolution of Helmet wearing rate",
    "Evolution of minimum age/height children are allowed as passengers on motorcycles nationally",
    "Evolution of legislation on the use of seat belts for all motor vehicle occupants",
    "Evolution of national seat-belt law ",
    "Evolution of legislation applies to front and rear seat occupants ",
    "Evolution of seat-belt wearing rate",
    "Evolution of legislation requiring the use of child safety restraint systems that meet appropriate standards",
    "Evolution of national child restraints use law",
    "Evolution of children seated in front seat ",
    "Evolution of age or height specified for children requiring child restraint",
    "Evolution of child restraint standard referred to and/or specified ",
    "Evolution in Civil Registration and Vital Statistics qualification",
    "Item not collected before",
    "Evolution in adhesion UN conventions",
    # Speed limits
    "Local authorities can modify limits ",
    "Presence of targets to reduce speeds nationally",
    "Year by targets to reduce speeds nationally",
    # Motorcycle helmet law
    "Legislation requires helmet fastening",
    "Legislation applies to drivers and passengers ",
    "Legislation applies to all road types",
    "Legislation applies to all engine types",
    "Legislation refers to and/or specifies helmet standard",
    # Reduction targets
    "Year by National target for time between serious crash and initial provision of professional emergency care (year)",
    "Presence of targets to reduce driving after drinking nationally (year)",
    "Year by targets to reduce driving after drinking nationally ",
    "Year by fatality reduction target ",
    "Year by target to increase seat belt use (year)",
    "Year by non fatal reduction target",
    "Year by target to increase helmet use",
    "Year by targets to increase child safety restraint use",
    "National target for time between serious crash and initial provision of professional emergency care",
    "Fatality reduction target",
    "Non fatal reduction target (year)",
    "Presence of targets to reduce distracted driving nationally ",
    "Year by targets to reduce distracted driving nationally (year)",
    "Presence of targets to increase helmet use",
    "Presence of targets to increase seat belt use",
    "Presence of targets to increase child safety restraint use",
    # road quality
    "Presence of technical standards for new roads that take account of all road-user safety, or align with relevant UN Conventions and regulate compliance with them ",
    "On roads technical standards country adheres to corresponding UN or equivalent international safety regulation",
    "Presence of systematic approaches to assess/audit new roads ",
    "National law requiring a formal road safety inspection/assessment",
    "Target for roads to meet technical safety standards for all users",
    "Year by target for roads to meet technical safety standards for all users (year)",
    "Investments to upgrade high risk locations ",
    # Statistics level of country
    "Civil Registration and Vital Statistics level",
    "Evolution of frequency and distribution of journeys by modal type",
    "Speeding violations and speeding‐related injuries and fatalities",
    "Driving under the influence of alcohol or drugs  and related road traffic‐related fatalities and injuries",
    "Seat belt  and child-restraint systems use",
    "Powered 2- and 3- wheeler helmet use",
    "Mobile phone use while driving",
    # misc (e.g. for vizualisation)
    "Alcohol consumption prohibited in country",
    "Random breath testing carried out ",
    "Testing carried out in case of fatal crash ",
    "Available types of enforcement",
    # legislation standards (used for visualization)
    "On VTI country adheres to corresponding UN or equivalent international safety regulation",
    "On VTI corresponding EU regulation mandatory for country.",
    "On front and side impact protection country adheres to corresponding UN or equivalent international safety regulation",
    "On front and side impact protection corresponding EU regulation mandatory for country.",
    "Country adheres to corresponding UN or equivalent international safety regulation",
    "Corresponding EU regulation mandatory for country.",
    "Helmet standard adheres to corresponding UN or equivalent international safety regulation",
    "Helmet standard corresponding EU regulation mandatory for country.",
    "On pedestrian protection country adheres to corresponding UN or equivalent international safety regulation",
    "On pedestrian protection corresponding EU regulation mandatory for country.",
    "On ESC country adheres to corresponding UN or equivalent international safety regulation",
    "On ESC corresponding EU regulation mandatory for country.",
    "On pedestrian protection country adheres to corresponding UN or equivalent international safety regulation",
    "On pedestrian protection corresponding EU regulation mandatory for country.",
    "On ABS country adheres to corresponding UN or equivalent international safety regulation",
    "On ABS corresponding EU regulation mandatory for country.",
    "On driving time legislation country adheres to corresponding UN or equivalent international safety regulation",
    "Adhesion to at least one of the 7 UN core vehicle regulations",
    "Adherence to one or more of the 7 UN road safety conventions",
    "CRS standard adheres to corresponding UN or equivalent international safety regulation",
    "CRS standard corresponding EU regulation mandatory for country.",
]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("road_safety.xlsx")

    # Load data from snapshot.
    tb = snap.read(header=1)

    tb = tb.drop(columns=COLS_TO_DROP)

    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    # remove additional spaces ‐
    tb.columns = tb.columns.str.strip()
    tb.columns = [col.replace("-", " ") for col in tb.columns]
    tb.columns = [col.replace("‐", " ") for col in tb.columns]
    tb.columns = [" ".join(col.split()) for col in tb.columns]

    tb = tb.rename(columns={"Country name": "country"})

    tb = tb.format(["country"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
