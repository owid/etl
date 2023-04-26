"""Utils (mostly mappings)

These are manually created, based on:

- "Guide to using the data": https://cms.wellcome.org/sites/default/files/2021-10/wgmdata-mental-health-data-dictionary-user-guide.docx (provides mapping of answer IDs to answer labels, only for MH* fields)
- "Full questionnaire":
    - WGM mental health 2020: https://cms.wellcome.org/sites/default/files/2021-11/WGM_Full_Questionnaire.pdf (based on order of answers, we manually map answer IDs to answer labels. Ideally WGM would provide a dictionary)
    - Need to complement with https://wellcome.org/reports/wellcome-global-monitor-covid-19/2020 (for the W* questions)

"""


MAPPING_GENDER_VALUES = {
    1: "male",
    2: "female",
}
MAPPING_AGE_VALUES = {
    1: "15-29",
    2: "30-49",
    3: "50-64",
    4: "65+",
    99: "Don't know/Refused",
}
# Rename fields
MAPPING_COLUMN_NAMES = {
    "countrynew": "country",
    "wgt": "weight_intra_country",
    "projwt": "weight_inter_country",
    "year_wave": "year",
    "gender": "gender",
    "age_var2": "age_group",
}
# Extracted from
# - https://cms.wellcome.org/sites/default/files/2021-11/WGM_Full_Questionnaire.pdf (full questionaire)
# - https://cms.wellcome.org/sites/default/files/2021-10/wgm-mentalhealthmodule-crossnational-tabs.xlsx (data report, xlsx)
MAPPING_QUESTION_VALUES = {
    "mh1": {
        "title": "Importance of mental health for well-being",
        "answers": {
            "1": "More important",
            "2": "As important",
            "3": "Less important",
            "99": "Don't know/Refused",
        },
    },
    "mh2a": {
        "title": "How much science can explain how the human body works",
        "answers": {
            "1": "A lot",
            "2": "Some",
            "3": "Not much",
            "4": "Not at all",
            "99": "Don't know/Refused",
        },
    },
    "mh2b": {
        "title": "How much science can explain how feelings and emotions work",
        "answers": {
            "1": "A lot",
            "2": "Some",
            "3": "Not much",
            "4": "Not at all",
            "99": "Don't know/Refused",
        },
    },
    "mh3a": {
        "title": "How much science helps to treat cancer",
        "answers": {
            "1": "A lot",
            "2": "Some",
            "3": "Not much",
            "4": "Not at all",
            "99": "Don't know/Refused",
        },
    },
    "mh3b": {
        "title": "How much science helps to treat anxiety or depression",
        "answers": {
            "1": "A lot",
            "2": "Some",
            "3": "Not much",
            "4": "Not at all",
            "99": "Don't know/Refused",
        },
    },
    "mh3c": {
        "title": "How much science helps to treat infectious diseases",
        "answers": {
            "1": "A lot",
            "2": "Some",
            "3": "Not much",
            "4": "Not at all",
            "99": "Don't know/Refused",
        },
    },
    "mh3d": {
        "title": "How much science helps to treat obesity",
        "answers": {
            "1": "A lot",
            "2": "Some",
            "3": "Not much",
            "4": "Not at all",
            "99": "Don't know/Refused",
        },
    },
    "mh4a": {
        "title": "Important for national government to fund research on cancer",
        "answers": {
            "1": "Extremely important",
            "2": "Somewhat important",
            "3": "Not too important",
            "4": "Not important at all",
            "99": "Don't know/Refused",
        },
    },
    "mh4b": {
        "title": "Important for national government to fund research on anxiety/depression",
        "answers": {
            "1": "Extremely important",
            "2": "Somewhat important",
            "3": "Not too important",
            "4": "Not important at all",
            "99": "Don't know/Refused",
        },
    },
    "mh5": {
        "title": "Someone local comfortable speaking about anxiety/depression with someone they know",
        "answers": {
            "1": "Very comfortable",
            "2": "Somewhat comfortable",
            "3": "Not at all comfortable",
            "99": "Don't know/Refused",
        },
    },
    "mh6": {
        "title": "Friends/family have been anxious/depressed",
        "answers": {
            "1": "Yes",
            "2": "No",
            "99": "Don't know/Refused",
        },
    },
    "mh7a": {
        "title": "Have been anxious/depressed",
        "answers": {
            "1": "Yes",
            "2": "No",
            "99": "Don't know/Refused",
        },
    },
    "mh7b": {
        "title": "Age when first felt anxious/depressed",
        "answers": {},
    },
    "mh7b_2": {
        "title": "Age range when first felt anxious/depressed",
        "answers": {
            "1": "Ages <13",
            "2": "Ages 13-19",
            "3": "Ages 20-29",
            "4": "Ages 30-39",
            "5": "Ages ≥40",
            "99": "Don't know/Refused",
        },
    },
    "mh7c": {
        "title": "Have felt anxious/depressed more than once",
        "answers": {
            "1": "Yes",
            "2": "No",
            "99": "Don't know/Refused",
        },
    },
    "mh8a": {
        "title": "Talked to mental health professional when anxious/depressed",
        "answers": {
            "1": "Yes",
            "2": "No",
            "99": "Don't know/Refused",
        },
    },
    "mh8b": {
        "title": "Engaged in religious/spiritual activities when anxious/depressed",
        "answers": {
            "1": "Yes",
            "2": "No",
            "99": "Don't know/Refused",
        },
    },
    "mh8c": {
        "title": "Talked to friends or family when anxious/depressed",
        "answers": {
            "1": "Yes",
            "2": "No",
            "99": "Don't know/Refused",
        },
    },
    "mh8d": {
        "title": "Took prescribed medication when anxious/depressed",
        "answers": {
            "1": "Yes",
            "2": "No",
            "99": "Don't know/Refused",
        },
    },
    "mh8e": {
        "title": "Improved healthy lifestyle behaviors when anxious/depressed",
        "answers": {
            "1": "Yes",
            "2": "No",
            "99": "Don't know/Refused",
        },
    },
    "mh8f": {
        "title": "Made a change to work situation when anxious/depressed",
        "answers": {
            "1": "Yes",
            "2": "No",
            "99": "Don't know/Refused",
        },
    },
    "mh8g": {
        "title": "Made a change to personal relationships when anxious/depressed",
        "answers": {
            "1": "Yes",
            "2": "No",
            "99": "Don't know/Refused",
        },
    },
    "mh8h": {
        "title": "Spent time in nature/the outdoors when anxious/depressed",
        "answers": {
            "1": "Yes",
            "2": "No",
            "99": "Don't know/Refused",
        },
    },
    "mh9a": {
        "title": "How helpful was talking to mental health professional when anxious/depressed?",
        "answers": {
            "1": "Very helpful",
            "2": "Somewhat helpful",
            "3": "Not helpful",
            "99": "Don't know/Refused",
        },
    },
    "mh9b": {
        "title": "How helpful was engaging in religious or spiritual activities when anxious/depressed?",
        "answers": {
            "1": "Very helpful",
            "2": "Somewhat helpful",
            "3": "Not helpful",
            "99": "Don't know/Refused",
        },
    },
    "mh9c": {
        "title": "How helpful was talking to friends or family when anxious/depressed?",
        "answers": {
            "1": "Very helpful",
            "2": "Somewhat helpful",
            "3": "Not helpful",
            "99": "Don't know/Refused",
        },
    },
    "mh9d": {
        "title": "How helpful was taking prescribed medication when anxious/depressed?",
        "answers": {
            "1": "Very helpful",
            "2": "Somewhat helpful",
            "3": "Not helpful",
            "99": "Don't know/Refused",
        },
    },
    "mh9e": {
        "title": "How helpful was improving healthy lifestyle behaviors when anxious/depressed?",
        "answers": {
            "1": "Very helpful",
            "2": "Somewhat helpful",
            "3": "Not helpful",
            "99": "Don't know/Refused",
        },
    },
    "mh9f": {
        "title": "How helpful was making a change to work situation when anxious/depressed?",
        "answers": {
            "1": "Very helpful",
            "2": "Somewhat helpful",
            "3": "Not helpful",
            "99": "Don't know/Refused",
        },
    },
    "mh9g": {
        "title": "How helpful was making a change to personal relationships when anxious/depressed?",
        "answers": {
            "1": "Very helpful",
            "2": "Somewhat helpful",
            "3": "Not helpful",
            "99": "Don't know/Refused",
        },
    },
    "mh9h": {
        "title": "How helpful was spending time in nature/the outdoors when anxious/depressed?",
        "answers": {
            "1": "Very helpful",
            "2": "Somewhat helpful",
            "3": "Not helpful",
            "99": "Don't know/Refused",
        },
    },
    "w1": {
        "title": "How much do you know about science?",
        "answers": {
            "1": "A lot",
            "2": "Some",
            "3": "Not much",
            "4": "Nothing at all",
            "99": "Don't know/Refused",
        },
    },
    "w2": {
        "title": "Understanding of words 'science' and 'scientist'",
        "answers": {
            "1": "All of it",
            "2": "Some of it",
            "3": "Not much of it",
            "4": "None of it",
            "99": "Don't know/Refused",
        },
    },
    "w3": {
        "title": "Highest level of education where last learnt about science",
        "answers": {
            "0": "None",
            "1": "Primary",
            "2": "Secondary and post-secondary",
            "3": "University",
        },
    },
    "w4": {
        "title": "Confidence in hospitals and health clinics in your country",
        "answers": {
            "1": "A lot",
            "2": "Some",
            "3": "Not much",
            "4": "None at all",
            "99": "Don't know/Refused",
        },
    },
    "w5a": {
        "title": "Trust in people in your neighbourhood",
        "answers": {
            "1": "A lot",
            "2": "Some",
            "3": "Not much",
            "4": "None at all",
            "99": "Don't know/Refused",
        },
    },
    "w5b": {
        "title": "Trust in the national government of your country",
        "answers": {
            "1": "A lot",
            "2": "Some",
            "3": "Not much",
            "4": "Not at all",
            "99": "Don't know/Refused",
        },
    },
    "w5c": {
        "title": "Trust in scientists in your country",
        "answers": {
            "1": "A lot",
            "2": "Some",
            "3": "Not much",
            "4": "Not at all",
            "99": "Don't know/Refused",
        },
    },
    "w5d": {
        "title": "Trust in journalists in your country",
        "answers": {
            "1": "A lot",
            "2": "Some",
            "3": "Not much",
            "4": "Not at all",
            "99": "Don't know/Refused",
        },
    },
    "w5e": {
        "title": "Trust in doctors and nurses in your country",
        "answers": {
            "1": "A lot",
            "2": "Some",
            "3": "Not much",
            "4": "Not at all",
            "99": "Don't know/Refused",
        },
    },
    "w5f": {
        "title": "Trust in people working for NGOs/charitable organizations in your country",
        "answers": {
            "1": "A lot",
            "2": "Some",
            "3": "Not much",
            "4": "Not at all",
            "99": "Don't know/Refused",
        },
    },
    "w5g": {
        "title": "Trust in traditional healers in your country",
        "answers": {
            "1": "A lot",
            "2": "Some",
            "3": "Not much",
            "4": "Not at all",
            "99": "Don't know/Refused",
        },
    },
    "w6": {
        "title": "Trust in science",
        "answers": {
            "1": "A lot",
            "2": "Some",
            "3": "Not much",
            "4": "Not at all",
            "99": "Don't know/Refused",
        },
    },
    "w7a": {
        "title": "Trust scientists to find out accurate information about the world",
        "answers": {
            "1": "A lot",
            "2": "Some",
            "3": "Not much",
            "4": "Not at all",
            "99": "Don't know/Refused",
        },
    },
    "w7b": {
        "title": "Trust scientists to do their work with the intention of benefiting the public",
        "answers": {
            "1": "A lot",
            "2": "Some",
            "3": "Not much",
            "4": "Not at all",
            "99": "Don't know/Refused",
        },
    },
    "w7c": {
        "title": "Trust leaders in the national government to value opinions and expertise of scientists",
        "answers": {
            "1": "A lot",
            "2": "Some",
            "3": "Not much",
            "4": "Not at all",
            "99": "Don't know/Refused",
        },
    },
    "w8": {
        "title": "Work from scientists in your country benefits most, some, or very few people",
        "answers": {
            "1": "Most",
            "2": "Some",
            "3": "Very few",
            "99": "Don't know/Refused",
        },
    },
    "w9": {
        "title": "Work from scientists in your country has benefited people like you",
        "answers": {
            "1": "A lot",
            "2": "A little",
            "3": "Not at all",
            "99": "Don't know/Refused",
        },
    },
    "w10": {
        "title": "Science and technology will increase/decrease the number of jobs in your local area in the next 5 years",
        "answers": {
            "1": "Increase",
            "2": "Decrease",
            "3": "Neither/No effect",
            "99": "Don't know/Refused",
        },
    },
    "w11a": {
        "title": "Impact of developments in science in your personal health",
        "answers": {
            "1": "Mostly positive impact",
            "2": "Mostly negative impact",
            "3": "No impact at all",
            "4": "Both positive and negative impact",
            "99": "Don't know/Refused",
        },
    },
    "w11b": {
        "title": "Impact of developments in science in the quality of the environment in your local area/city you live",
        "answers": {
            "1": "Mostly positive impact",
            "2": "Mostly negative impact",
            "3": "No impact at all",
            "4": "Both positive and negative impact",
            "99": "Don't know/Refused",
        },
    },
    "w13": {
        "title": "Heard about climate change / global warming before",
        "answers": {
            "1": "Yes",
            "2": "No",
            "99": "Don't know/Refused",
        },
    },
    "w14": {
        "title": "Understanding of climate change / global warming issue",
        "answers": {
            "1": "Very well",
            "2": "Fairly well",
            "3": "Not very well",
            "4": "Not at all",
            "99": "Don't know/Refused",
        },
    },
    "w15": {
        "title": "Climate change / global warming is a threat in your country right now",
        "answers": {
            "1": "Major threat",
            "2": "Minor threat",
            "3": "Not a threat",
            "4": "Climate change / global warming is not happening",
            "99": "Don't know/Refused",
        },
    },
    "w15_1a": {
        "title": "Based decisions about coronavirus on scientific advice from the National government",
        "answers": {
            "1": "A lot",
            "2": "Some",
            "3": "Not much",
            "4": "Not at all",
            "99": "Don't know/Refused",
        },
    },
    "w15_1b": {
        "title": "Based decisions about coronavirus on scientific advice from friends and family",
        "answers": {
            "1": "A lot",
            "2": "Some",
            "3": "Not much",
            "4": "Not at all",
            "99": "Don't know/Refused",
        },
    },
    "w15_1c": {
        "title": "Based decisions about coronavirus on scientific advice from the WHO",
        "answers": {
            "1": "A lot",
            "2": "Some",
            "3": "Not much",
            "4": "Not at all",
            "99": "Don't know/Refused",
        },
    },
    "w15_1d": {
        "title": "Based decisions about coronavirus on scientific advice from doctors and nurses in your country",
        "answers": {
            "1": "A lot",
            "2": "Some",
            "3": "Not much",
            "4": "Not at all",
            "99": "Don't know/Refused",
        },
    },
    "w15_1e": {
        "title": "Based decisions about coronavirus on scientific advice from religious leaders",
        "answers": {
            "1": "A lot",
            "2": "Some",
            "3": "Not much",
            "4": "Not at all",
            "99": "Don't know/Refused",
        },
    },
    "w15_2a": {
        "title": "After the COVID crisis ends, your government should spend money to help other countries prevent and cure diseases wherever they occur",
        "answers": {
            "1": "Strongly agree",
            "2": "Somewhat agree",
            "3": "Somewhat disagree",
            "4": "Strongly disagree",
            "99": "Don't know/Refused",
        },
    },
    "w15_2b": {
        "title": "After the COVID crisis ends, your government should spend money on preventing and curing diseases ONLY if they pose a risk to people in your country",
        "answers": {
            "1": "Strongly agree",
            "2": "Somewhat agree",
            "3": "Somewhat disagree",
            "4": "Strongly disagree",
            "99": "Don't know/Refused",
        },
    },
    "w27": {
        "title": "Used social media in past 30 days",
        "answers": {
            "1": "Yes",
            "2": "No",
            "99": "Don't know/Refused",
        },
    },
    "w28": {
        "title": "How often use social media",
        "answers": {
            "1": "Several times an hour",
            "2": "Almost every hour",
            "3": "Several times a day",
            "4": "Once a day",
            "5": "A few days a week",
            "6": "Less than few days a week",
            "99": "Don't know/Refused",
        },
    },
    "w29": {
        "title": "How often see information about health on social media",
        "answers": {
            "1": "All of the time",
            "2": "Most of the time",
            "3": "Some of the time",
            "4": "Never",
            "99": "Don't know/Refused",
        },
    },
    # "wp21757": {
    #     "title": "Life affected by coronavirus",
    #     "answers": {
    #         "1": "A lot",
    #         "2": "Some",
    #         "3": "Not much",
    #         "4": "Not at all",
    #         "99": "Don't know/Refused",
    #     },
    # },
    # "wp21758": {
    #     "title": "Due to the coronavirus situation: Temporarily stopped working at your job / business",
    #     "answers": {
    #         "1": "Yes",
    #         "2": "No",
    #         "3": "Does not apply / no job",
    #         "99": "Don't know/Refused",
    #     },
    # },
    # "wp21759": {
    #     "title": "Due to the coronavirus situation: Lost your job / business",
    #     "answers": {
    #         "1": "Yes",
    #         "2": "No",
    #         "3": "Does not apply / no job",
    #         "99": "Don't know/Refused",
    #     },
    # },
    # "wp21760": {
    #     "title": "Due to the coronavirus situation: Worked fewer hours at your job / business",
    #     "answers": {
    #         "1": "Yes",
    #         "2": "No",
    #         "3": "Does not apply / no job",
    #         "99": "Don't know/Refused",
    #     },
    # },
    # "wp21761": {
    #     "title": "Due to the coronavirus situation: Received LESS money than usual from your employer / business",
    #     "answers": {
    #         "1": "Yes",
    #         "2": "No",
    #         "3": "Does not apply / no job",
    #         "99": "Don't know/Refused",
    #     },
    # },
    # "wp21768": {
    #     "title": "If vaccines to prevent coronavirus was available at no cost, would you agree to be vaccinated?",
    #     "answers": {
    #         "1": "Yes, would agree",
    #         "2": "No, would not agree",
    #         "99": "Don't know/Refused",
    #     },
    # },
}
