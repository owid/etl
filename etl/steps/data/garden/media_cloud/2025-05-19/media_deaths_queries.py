def create_full_queries():
    queries = create_queries()
    queries = create_string_queries(queries)
    return queries


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
            "myocardial infarction",
            "coronary artery disease",
            "arrhythmia",
            "heart failure",
            "congestive heart failure",
            "valvular heart disease",
            "pericarditis",
            "endocarditis",
            "cardiomyopathy",
        ],
        "combinations": [
            "heart disease heart",
            "heart attack heart",
            "heart cardiac",
            "heart disease heart attack",
            # "heart infarction", 0 results
            # "heart coronary artery", 8 results
            # "heart arrhythmia", 5 results
            "heart disease heart failure",
            # "heart endocarditis", 2 results
            # "heart cardiomyopathy", 4 results
            # additional terms: cardiac arrest, cardiac without heart
        ],
        "exclude_terms": [],
    }
    query_cancer_dict = {
        "single_terms": [
            "cancer",
            "malignant neoplasm",
            "tumor",
            "tumour",
            "carcinoma",
            "sarcoma",
            "leukemia",
            "lymphoma",
            "melanoma",
            "mass lesion",
            "oncology",
            "oncology",
            "chemotherapy",
            "radiation therapy",
            "immunotherapy",
            "targeted therapy",
            "biopsy",
            "oncogene",
            "carcinogenesis",
            "metastasis",
            "remission",
            "cargonenic",
            "carcinogen",
        ],
        "combinations": [
            "cancer cancer",
            "cancer malignant neoplasm",
            "cancer tumor",
            "cancer tumour",
            "cancer carcinoma",
            "cancer sarcoma",
            "cancer leukemia",
            "cancer lymphoma",
            "cancer melanoma",
            "cancer mass lesion",
            "cancer oncology",
            "cancer chemotherapy",
            "cancer radiation therapy",
            "cancer immunotherapy",
            "cancer targeted therapy",
            "cancer biopsy",
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
            "airplane accident",
            "industrial accident",
            "workplace accident",
            "falling object",
            "electrocution",
            "burn injury",
            "drowning",
        ],
        "combinations": [
            "accident accident",
            "crash accident",
            "crash collision",
            "traffic collision ",
            "car accident car",
            "motorcycle accident motorcycle",
            "hit and run hit",
            "plane crash plane",
            "train accident train",
            "boat accident boat",
            "workplace accident workplace",
            "falling object",
            "electrocution electric",
            "burn burn",
            "drowning drown",
        ],
        "exclude_terms": [],
    }

    query_stroke_dict = {
        "single_terms": [
            "stroke",
            "cerebrovascular disease",
            "brain attack",
            "transient ischemic attack",
            "cerebral infarction",
            "brain hemorrhage",
            "subarachnoid hemorrhage",
            "intracerebral hemorrhage",
            "cerebral thrombosis",
            "cerebral embolism",
            "brain ischemia",
            "brain injury",
            "T.I.A.",
        ],
        "combinations": [
            "stroke stroke",
            "stroke cerebrovascular",
            "stroke brain attack",
            "stroke transient ischemic attack",
            "stroke cerebral infarction",
            "stroke brain hemorrhage",
            "cerebral thrombosis brain",
            "cerebral thrombosis stroke",
            "cerebral embolism brain",
            "cerebral embolism stroke",
            "brain ischemia brain",
            "brain ischemia stroke",
            "T.I.A. stroke",
            "T.I.A. brain",
        ],
        "exclude_terms": [],
    }

    query_respiratory_dict = {
        "single_terms": [
            "chronic obstructive pulmonary disease",
            "COPD",
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
            "chronic obstructive pulmonary disease COPD",
            "COPD chronic bronchitis",
            "COPD COPD" "COPD lung",
            "COPD respiratory failure",
            "COPD pulmonary disease",
            "COPD respiratory illness",
            "COPD respiratory disease",
            "COPD respiratory tract infection",
            "lung emphysema",
            "asthma lung",
            "lung disease pulmonary disease",
            "respiratory illness respiratory disease",
        ],
        "exclude_terms": [],
    }

    query_alzheimers_dict = {
        "single_terms": ["Alzheimer", "dementia", "Alzheimer's disease"],
        "combinations": ["Alzheimer Alzheimer", "dementia dementia", "Alzheimer's disease Alzheimer's"],
        "exclude_terms": [],
    }

    query_diabetes_dict = {
        "single_terms": ["diabetes", "insulin", "hyperglycemia", "diabetic"],
        "combinations": [
            "diabetes diabetes",
            "insulin insulin",
            "hyperglycemia hyperglycemia",
            "diabetic diabetic",
            "diabetes insulin",
            "diabetes hyperglycemia",
            "diabetes diabetic",
            "insulin hyperglycemia",
            "insulin diabetic",
            "hyperglycemia diabetic",
        ],
        "exclude_terms": [],
    }

    query_kidney_dict = {
        "single_terms": ["kidney disease", "renal failure", "chronic kidney disease", "nephropathy", "dialysis"],
        "combinations": [
            "kidney disease kidney",
            "kidney renal failure",
            "kidney chronic kidney disease",
            "kidney nephropathy",
            "kidney dialysis",
        ],
        "exclude_terms": [],
    }

    query_liver_dict = {
        "single_terms": ["liver disease", "cirrhosis", "chronic liver disease", "hepatitis", "liver failure"],
        "combinations": [
            "liver disease liver",
            "liver cirrhosis",
            "liver chronic liver disease",
            "liver hepatitis",
            "liver liver failure",
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
            "influenza lung" "influenza influenza",
            "flu flu" "flu lung" "influenza pneumonia",
            "flu pneumonia",
            "respiratory infection pneumonia",
            "flu respiratory" "flu infection" "influenza infection" "lung infection pneumonia",
            "bronchopneumonia lung",
            "bronchopneumonia flu" "bronchopneumonia influenza",
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
    query_homocide_dict = {
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
            "homicide violen*",
            "homicide knife attack",
            "homicide stabbing",
            "homicide kill",
            # all terms combined with assault
            "assault assault",
            "assault shooting",
            "assault murder",
            "assault manslaughter",
            "assault crime",
            "assault violen*",
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
            "shooting violen*",
            "shooting knife attack",
            "shooting stabbing",
            "shooting kill",
            # all terms combined with murder
            "murder murder",
            "murder manslaughter",
            "murder crime",
            "murder violen*",
            "murder knife attack",
            "murder stabbing",
            "murder lynching",
            "murder execution",
            "murder kill",
            "crime crime",
            "crime violen*",
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
        "excluded_terms": [],
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
            "terror* terror",
            "extremism violen*",
            "extremism kill",
            "extremism terror*",
            "radicalization violen*",
            "radicalization kill",
            "radicalization terror*" "terror* violen*",
            "terror* kill",
            "terror* bomb",
            "terror* hostage",
        ],
        "excluded_terms": [],
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
        "excluded_terms": ["trade war"],
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
        "homicide": query_homocide_dict,
        "terrorism": query_terrorism_dict,
        "war": query_war_dict,
    }

    return all_queries


def create_query_str(query_dict, add_single_terms=False, proximity=1000):
    """
    Create a query string from a dictionary of queries.
    If add_single_terms is True, single terms will be added to the query.
    If proximity is set, it will be used for phrase queries.
    """
    comb_ls = query_dict["combinations"]
    query_str = ""
    for el in comb_ls[:-1]:
        query_str += f'"{el}"~{proximity} OR '
    query_str += f'"{comb_ls[-1]}"~{proximity}'

    if add_single_terms:
        for term in query_dict["single_terms"]:
            query_str += f' OR "{term}"'
    return query_str


def create_string_queries(dict_queries):
    string_queries = {}
    for term, query_dict in dict_queries:
        string_queries[term] = create_query_str(query_dict)
