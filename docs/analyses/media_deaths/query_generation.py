"""
This file generates the queries used in the media_deaths_analysis notebook. It includes the following functions:

function create_queries()
    This function includes all the keywords for each cause of death and creates
    a dictionary with keywords, combinations, and terms to exclude.

function create_query_str(query_dict, proximity=1000)
    This function takes the query dictionary for each cause of death (from create_queries)
    and creates a query string, including both the keywords and combinations of keywords.

function create_queries_by_cause(dict_queries)
    This function takes the dictionary of all causes of death and all query terms
    and creates a string query (by calling create_query_str) for each cause of death.

function create_full_queries()
    This function creates the full queries for all causes of death.
    The output is a dictionary of the form {cause_of_death: query_string}

function create_single_keyword_queries()
    This function creates queries that take all articles with even
    a single mention into account for all causes of death.
    The output is a dictionary of the form {cause_of_death: single_keyword_query_string}

The full queries can also be found in the methodology document here:
    https://docs.owid.io/projects/etl/analyses/media_deaths/methodology/#queries-for-each-cause-of-death
"""


def create_full_queries():
    queries = create_queries()
    queries = create_queries_by_cause(queries)
    return queries


def create_single_keyword_queries():
    queries = create_queries()
    single_keyword_queries = {}
    for cause, query_dict in queries.items():
        query_str = ""
        for term in query_dict["single_terms"][:-1]:
            query_str += f'"{term}" OR '
        query_str += f'"{query_dict["single_terms"][-1]}"'
        single_keyword_queries[cause] = query_str
    return single_keyword_queries


def create_query_str(query_dict, proximity=1000):
    """
    Create a query string from a dictionary of queries.
    If proximity is set, it will determine how far apart the keywords in the combinations can be.
    The default is 1000 words for an entire article.
    """
    comb_ls = query_dict["combinations"]
    query_str = ""
    for el in comb_ls[:-1]:
        query_str += f'"{el}"~{proximity} OR '
    query_str += f'"{comb_ls[-1]}"~{proximity}'

    query_str = f"({query_str}) AND ("

    for term in query_dict["single_terms"][:-1]:
        query_str += f'"{term}" OR '
    query_str += f'"{query_dict["single_terms"][-1]}")'

    if query_dict["exclude_terms"]:
        ex_terms = '" OR "'.join(query_dict["exclude_terms"])
        query_str += f' NOT ("{ex_terms}")'

    return query_str


def create_queries_by_cause(dict_queries):
    string_queries = {}
    for term, query_dict in dict_queries.items():
        string_queries[term] = create_query_str(query_dict)
    return string_queries


def create_queries():
    """Create queries for each cause of death.
    Each query should be a dictionary with the following format:
    {"single_terms": [list of single terms],
     "combinations": [list of combinations of terms],
     "exclude_terms": [list of terms to exclude]}"""

    # Define queries for each cause of death
    query_heart_dict = {
        "single_terms": [
            "heart disease",
            "heart attack",
            "cardiac arrest",
            "infarct",
            "coronary artery disease",  # 8
            "arrhythmia",  # 13
            "heart failure",
            "pericarditis",  # 3
            "endocarditis",  # 2
            "cardiomyopathy",  # 6
            "high blood pressure",
            "hypertension",
            "heart infection",
            "cardiology",
            "cardiologist",
        ],
        "combinations": [
            "heart disease heart",
            "heart attack heart",
            "heart cardiac",
            "cardiac cardiac",
            "heart infarct artery",
            "heart coronary artery",
            "cardiac coronary artery",
            "heart arrhythmia",
            "arrhythmia cardiac",
            "heart failure heart",
            "heart failure cardiac",
            "heart pericarditis",
            "heart endocarditis",
            "heart cardiomyopathy",
            "heart hypotension",
            "cardiac hypertension",
            "heart infection heart",
            "heart cardiology",
            "heart cardiologist",
            "heart disease blood pressure",
            "heart attack blood pressure",
        ],
        "exclude_terms": [],
    }
    query_cancer_dict = {
        "single_terms": [
            "cancer",
            "tumor",
            "tumour",
            "carcinoma",
            "sarcoma",
            "leukemia",
            "lymphoma",
            "melanoma",
            "oncology",
            "oncologist",
            "chemotherapy",
            "radiation therapy",
            "immunotherapy",
            "targeted therapy",
            "biopsy",
            "oncogene",
            "carcinogenesis",
            "metastasis",
            "remission",
            "carcinogenic",
            "carcinogen",
        ],
        "combinations": [
            "cancer cancer",
            "cancer tumor",
            "cancer tumour",
            "cancer carcinoma",
            "cancer sarcoma",
            "cancer leukemia",
            "cancer lymphoma",
            "cancer melanoma",
            "cancer oncology",
            "cancer chemotherapy",
            "cancer radiation therapy",
            "cancer immunotherapy",
            "cancer targeted therapy",
            "cancer biopsy",
            "cancer remission",
            "cancer carcinogen",
        ],
        "exclude_terms": [],
    }

    query_accidents_dict = {
        "single_terms": [
            "road accident",
            "car crash",
            "vehicle accident",
            "traffic collision",
            "car accident",
            "motorcycle accident",
            "hit and run",
            "plane crash",
            "train accident",
            "boat accident",
            "plane accident",
            "industrial accident",
            "workplace accident",
            "electrocution",
            "burn injury",
            "drowning",
            "drowned",
            "house fire",
            "blaze",
            "burns",
            "burning",
            "burned",
        ],
        "combinations": [
            "accident accident ",
            "crash accident ",
            "crash crash",
            "crash collision",
            "collision collision",
            "car accident car",
            "motorcycle accident motorcycle",
            "hit and run accident",
            "plane crash plane",
            "plane accident plane",
            "train accident train",
            "boat accident boat",
            "electrocution electric",
            "burn burn",
            "drowning drown",
            "fire fire died",
            "fire fire dead",
            "fire fire killed",
            "fire fire burn injury",
        ],
        "exclude_terms": [],
    }

    query_stroke_dict = {
        "single_terms": [
            "stroke",
            "brain attack",
            "transient ischemic attack",
            "brain hemorrhage",
            "embolism",
            "ischemia",
            "brain injury",
            "neurology",
        ],
        "combinations": [
            "stroke stroke",
            "stroke cerebrovascular",
            "stroke brain attack",
            "stroke transient ischemic attack",
            "stroke cerebral",
            "stroke hemorrhage",
            "stroke neurology",
            "stroke neurologist",
            "thrombosis brain",
            "thrombosis stroke",
            "embolism brain",
            "embolism stroke",
            "brain ischemia",
            "stroke ischemia",
        ],
        "exclude_terms": ["golf", "golfing"],
    }

    query_respiratory_dict = {
        "single_terms": [
            "chronic obstructive pulmonary disease",
            "COPD",
            "C.O.P.D.",
            "chronic bronchitis",
            "emphysema",
            "asthma",
            "respiratory failure",
            "lung disease",
            "pulmonary disease",
            "respiratory illness",
            "respiratory disease",
            "respiratory tract infection",
        ],
        "combinations": [
            "COPD COPD",
            "COPD lung",
            "COPD respiratory",
            "COPD pulmonary",
            "COPD chronic bronchitis",
            "COPD emphysema",
            "COPD asthma",
            "C.O.P.D. C.O.P.D.",
            "C.O.P.D. lung",
            "C.O.P.D. respiratory",
            "C.O.P.D. pulmonary",
            "C.O.P.D. chronic bronchitis",
            "C.O.P.D. emphysema",
            "C.O.P.D. asthma",
            "chronic bronchitis bronchitis",
            "chronic bronchitis lung",
            "chronic bronchitis respiratory",
            "chronic bronchitis pulmonary",
            "chronic bronchitis emphysema",
            "chronic bronchitis asthma",
            "emphysema emphysema",
            "emphysema lung",
            "emphysema respiratory",
            "emphysema pulmonary",
            "emphysema asthma",
            "asthma asthma",
            "asthma lung",
            "asthma respiratory",
            "asthma pulmonary",
            "respiratory respiratory",
            "respiratory lung",
            "respiratory pulmonary",
            "pulmonary pulmonary",
            "pulmonary lung",
            "lung lung",
        ],
        "exclude_terms": [],
    }

    query_alzheimers_dict = {
        "single_terms": [
            "Alzheimer",
            "Alzheimers",
            "Alzheimer’s",
            "Alzheimer's",
            "dementia",
        ],
        "combinations": [
            "Alzheimer Alzheimer",
            "Alzheimer’s Alzheimer’s",
            "Alzheimer's Alzheimer's",
            "Alzheimers Alzheimers",
            "Alzheimers dementia",
            "Alzheimer’s dementia",
            "Alzheimer's dementia",
            "dementia dementia",
            "Alzheimer dementia",
        ],
        "exclude_terms": [],
    }

    query_diabetes_dict = {
        "single_terms": ["diabetes", "insulin", "hyperglycemia", "diabetic"],
        "combinations": [
            "diabetes diabetes",
            "hyperglycemia hyperglycemia",
            "diabetic diabetic",
            "diabetes insulin",
            "diabetes diabetic",
            "insulin diabetic",
        ],
        "exclude_terms": [],
    }

    query_kidney_dict = {
        "single_terms": [
            "kidney disease",
            "renal disease",
            "renal failure",
            "kidney failure",
            "dialysis",
            "nephropathy",
            "nephrology",
            "nephrologist",
        ],
        "combinations": [
            "kidney disease kidney",
            "kidney renal failure",
            "renal renal",
            "kidney chronic kidney disease",
            "kidney dialysis",
            "renal dialysis",
            "kidney nephrology",
            "kidney nephrologist",
        ],
        "exclude_terms": [],
    }

    query_liver_dict = {
        "single_terms": [
            "liver disease",
            "cirrhosis",
            "chronic liver disease",
            "hepatitis",
            "liver failure",
            "fatty liver",
            "steatohepatitis",
            "hepatology",
            "hepatologist",
            "liver transplant",
        ],
        "combinations": [
            "liver disease liver",
            "liver cirrhosis",
            "liver chronic liver disease",
            "liver hepatitis",
            "liver liver failure",
            "liver fatty liver",
            "liver steatohepatitis",
            "liver hepatology",
            "liver hepatologist",
            "liver transplant",
            "hepatitis hepatitis",
        ],
        "exclude_terms": [],
    }

    query_covid_dict = {
        "single_terms": ["COVID-19", "coronavirus", "SARS-CoV-2"],
        "combinations": [
            "COVID-19 COVID-19",
            "coronavirus coronavirus",
            "SARS-CoV-2 SARS-CoV-2",
            "COVID-19 coronavirus",
            "COVID-19 SARS-CoV-2",
            "coronavirus SARS-CoV-2",
        ],
        "exclude_terms": [],
    }

    query_suicide_dict = {
        "single_terms": ["suicide", "self-harm", "self-inflicted injury", "suicidal"],
        "combinations": [
            "suicide suicide",
            "suicide self-harm",
            "suicide kill",
            "suicide suicidal",
            "suicide self-inflicted",
            "self-harm kill",
            "self-harm suicidal",
            "self-harm injury",
            "suicidal suicidal",
            "suicide depression",
            "suicide depressed",
            "depressed suicidal",
        ],
        "exclude_terms": [],
    }

    query_influenza_dict = {
        "single_terms": [
            "influenza",
            "flu",
            "respiratory infection",
            "pneumonia",
            "lung infection",
            "bronchopneumonia",
        ],
        "combinations": [
            "influenza flu",
            "influenza lung",
            "influenza influenza",
            "influenza respiratory",
            "influenza pneumonia",
            "flu flu",
            "flu lung",
            "flu respiratory",
            "flu pneumonia",
            "respiratory infection respiratory",
            "respiratory infection pneumonia",
            "respiratory infection lung",
            "pneumonia pneumonia",
            "pneumonia lung",
        ],
        "exclude_terms": [],
    }

    query_drug_dict = {
        "single_terms": [
            "drug use",
            "overdose",
            "drug-related",
            "substance use disorder",
            "substance abuse",
            "addiction",
            "opioid",
            "heroin",
            "fentanyl",
            "morphine",
            "cocaine",
            "drug abuse",
            "illicit drug",
        ],
        "combinations": [
            "drug overdose",
            "drug substance",
            "drug addiction",
            "drug opioid",
            "drug heroin",
            "drug fentanyl",
            "drug morphine",
            "drug cocaine",
            "drug drug use",
            "drug drug illicit",
            "overdose overdose",
            "overdose substance",
            "overdose addiction",
            "overdose opioid",
            "overdose heroin",
            "overdose fentanyl",
            "overdose morphine",
            "overdose cocaine",
            "substance addiction",
            "substance opioid",
            "substance heroin",
            "substance fentanyl",
            "substance morphine",
            "substance cocaine",
            "addiction addiction",
            "addiction opioid",
            "addiction heroin",
            "addiction fentanyl",
            "addiction morphine",
            "addiction cocaine",
            "opioid opioid",
            "opioid heroin",
            "opioid fentanyl",
            "opioid morphine",
            "opioid cocaine",
            "heroin heroin",
            "heroin fentanyl",
            "heroin morphine",
            "heroin cocaine",
            "fentanyl fentanyl",
            "fentanyl morphine",
            "fentanyl cocaine",
            "morphine morphine",
            "morphine cocaine",
            "cocaine cocaine",
        ],
        "exclude_terms": [],
    }
    query_homicide_dict = {
        "single_terms": [
            "homicide",
            "assault",
            "shooting",
            "murder",
            "manslaughter",
            "violent crime",
            "domestic violence",
            "gang violence",
            "knife attack",
            "stabbing",
            "lynching",
            "execution",
            "killer",
            "killing",
        ],
        "combinations": [
            # all terms combined with homicide
            "homicide homicide",
            "homicide assault",
            "homicide shooting",
            "homicide murder",
            "homicide manslaughter",
            "homicide crime",
            "homicide violent",
            "homicide violence",
            "homicide knife attack",
            "homicide stabbing",
            "homicide kill",
            # all terms combined with assault
            # "assault assault",
            "assault shooting",
            "assault murder",
            "assault manslaughter",
            "assault crime",
            "assault violent",
            "assault violence",
            "assault knife attack",
            "assault stabbing",
            "assault lynching",
            "assault execution",
            "assault kill",
            # all terms combined with shooting
            "shooting shooting",
            "shooting murder",
            "shooting manslaughter",
            "shooting crime",
            "shooting violence",
            "shooting violent",
            "shooting knife attack",
            "shooting stabbing",
            "shooting kill",
            # all terms combined with murder
            "murder murder",
            "murder manslaughter",
            "murder crime",
            "murder violence",
            "murder violent",
            "murder knife attack",
            "murder stabbing",
            "murder lynching",
            "murder execution",
            "murder kill",
            "crime crime",
            "crime violence",
            "crime violent",
            "crime knife attack",
            "crime stabbing",
            "crime kill",
            "violence knife attack",
            "violence stabbing",
            "violence kill",
            "stabbing stabbing",
            "stabbing knife attack",
            "kill killer",
            "kill killing",
        ],
        "exclude_terms": [],  # ["war", "military", "drone"],
    }

    query_terrorism_dict = {
        "single_terms": [
            "terrorism",
            "terrorist",
            "extremism",
            "radicalization",
            "suicide bomb",
            "car bomb",
            "hostage",
            "terror attack",
            "terror plot",
            "terror cell",
            "terror network",
        ],
        "combinations": [
            "terrorist terror",
            "terrorist terrorism",
            "terrorist terrorist",
            "terrorism terror",
            "terrorism terrorist",
            "terrorism terrorism",
            "extremist violence",
            "extremist kill",
            "extremist terror",
            "extremist terrorist",
            "extremist terrorism",
            "extremism violence",
            "extremism kill",
            "extremism terror",
            "extremism terrorist",
            "extremism terrorism",
            "radicalization violence",
            "radicalization kill",
            "radicalization terrorism",
            "radicalization terrorist",
            "terrorist violent",
            "terrorist violence",
            "terrorist kill",
            "terrorist bomb",
            "terrorist hostage",
            "terrorism violent",
            "terrorism violence",
            "terrorism kill",
            "terrorism bomb",
            "terrorism hostage",
        ],
        "exclude_terms": [],
    }

    query_war_dict = {
        "single_terms": [
            "war",
            "warfare",
            "armed conflict",
            "military operation",
            "combat",
            "invasion",
            "occupation",
            "insurgency",
            "guerrilla warfare",
            "airstrike",
            "bombing",
            "siege",
            "troops",
            "military intervention",
            "peacekeeping",
            "ceasefire",
            "truce",
            "hostilities",
            "genocide",
        ],
        "combinations": [
            "war war",
            "warfare war",
            "armed conflict war",
            "military war",
            "combat war",
            "invasion war",
            "occupation war",
            "insurgency war",
            "guerrilla war",
            "airstrike war",
            "bombing war",
            "siege war",
            "troops war",
            "ceasefire war",
            "truce war",
            "hostilities war",
            "genocide war",
            "military military",
            "armed conflict military",
            "combat military",
            "invasion military",
            "occupation military",
            "insurgency military",
            "guerrilla warfare military",
            "airstrike military",
            "bombing military",
            "siege military",
            "troops military",
            "military intervention military",
            "peacekeeping military",
            "ceasefire military",
            "truce military",
            "hostilities military",
            "genocide military",
        ],
        "exclude_terms": ["trade war"],
    }

    query_hiv_dict = {
        "single_terms": [
            "HIV",
            "AIDS",
            "HIV/AIDS",
            "human immunodeficiency virus",
            "acquired immunodeficiency syndrome",
            "antiretroviral therapy",
            "HIV infection",
            "HIV positive",
            "HIV test",
            "HIV treatment",
        ],
        "combinations": [
            "HIV HIV",
            "HIV AIDS",
            "HIV human immunodeficiency virus",
            "HIV acquired immunodeficiency syndrome",
            "HIV antiretroviral therapy",
            "HIV treatment",
            "HIV medication",
            "HIV infection",
            "AIDS AIDS",
            "AIDS human immunodeficiency virus",
            "AIDS acquired immunodeficiency syndrome",
            "AIDS antiretroviral therapy",
            "AIDS treatment",
            "AIDS medication",
            "AIDS infection",
        ],
        "exclude_terms": [],
    }
    query_malaria_dict = {
        "single_terms": [
            "malaria",
            "mosquito-borne disease",
            "antimalarial",
            "chloroquine",
        ],
        "combinations": [
            "malaria malaria",
            "plasmodium plasmodium",
            "mosquito mosquito",
            "malaria fever",
            "malaria parasite",
            "malaria treatment",
            "malaria prevention",
            "malaria mosquito",
            "malaria antimalarial",
            "malaria drugs",
            "malaria chloroquine",
        ],
        "exclude_terms": [],
    }
    query_tb_dict = {
        "single_terms": [
            "tuberculosis",
            "TB",
            "tuberculin skin test",
            "BCG vaccine",
            "antitubercular therapy",
        ],
        "combinations": [
            "tuberculosis tuberculosis",
            "tuberculosis TB",
            "tuberculosis tuberculin",
            "tuberculosis BCG vaccine",
            "tuberculosis antibiotics",
            "tuberculosis antibacterial",
            "tuberculosis treatment",
            "tuberculosis prevention",
            "tuberculosis diagnosis",
        ],
        "exclude_terms": [],
    }
    query_diarrhea_dict = {
        "single_terms": [
            "diarrheal diseases",
            "diarrhea",
            "gastroenteritis",
            "dehydration",
            "rotavirus",
            "cholera",
            "shigellosis",
            "salmonellosis",
            "E. coli infection",
        ],
        "combinations": [
            "diarrheal diarrhea",
            "diarrhea gastroenteritis",
            "diarrhea dehydration",
            "diarrhea rotavirus",
            "diarrhea cholera",
            "diarrhea diarrhea",
            "gastroenteritis gastroenteritis",
            "rotavirus rotavirus",
            "cholera cholera",
        ],
        "exclude_terms": [],
    }

    all_queries = {
        "heart disease": query_heart_dict,
        "cancer": query_cancer_dict,
        "accidents": query_accidents_dict,
        "stroke": query_stroke_dict,
        "respiratory": query_respiratory_dict,
        "alzheimers": query_alzheimers_dict,
        "diabetes": query_diabetes_dict,
        "kidney": query_kidney_dict,
        "liver": query_liver_dict,
        "covid": query_covid_dict,
        "suicide": query_suicide_dict,
        "influenza": query_influenza_dict,
        "drug overdose": query_drug_dict,
        "homicide": query_homicide_dict,
        "terrorism": query_terrorism_dict,
        "war": query_war_dict,
        "hiv": query_hiv_dict,
        "malaria": query_malaria_dict,
        "tb": query_tb_dict,
        "diarrhea": query_diarrhea_dict,
    }

    return all_queries
