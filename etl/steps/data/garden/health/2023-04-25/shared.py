"""Utils (mostly mappings).

The mappings are created from the metadata dictionary provided by the source (a tab in the excel sheet)
"""


# Gender ID mappings
MAPPING_GENDER_VALUES = {
    1: "male",
    2: "female",
}

# Rename fields
MAPPING_COLUMN_NAMES = {
    "country": "country",
    "wgt": "weight_intra_country",
    "projwt": "weight_inter_country",
    "year_calendar": "year",
    "gender": "gender",
}
MAPPING_QUESTION_VALUES = {
    "q1": {
        "title": "How much do you know about science?",
        "answers": {
            "1": "A lot",
            "2": "Some",
            "3": "Not much",
            "4": "Nothing at all",
            "99": "Don't know/Refused",
        },
    },
    "q2": {
        "title": "Understanding of words 'science' and 'scientist'",
        "answers": {
            "1": "All of it",
            "2": "Some of it",
            "3": "Not much of it",
            "4": "Nothing at all",
            "99": "Don't know/Refused",
        },
    },
    "q3": {
        "title": "Do you think studying diseases is a part of science?",
        "answers": {
            "1": "Yes",
            "2": "No",
            "99": "Don't know/Refused",
        },
    },
    "q4": {
        "title": "Do you think writing poetry is a part of science?",
        "answers": {
            "1": "Yes",
            "2": "No",
            "99": "Don't know/Refused",
        },
    },
    "q5a": {
        "title": "Have you ever learned about science at Primary School?",
        "answers": {
            "1": "Yes",
            "2": "No",
            "97": "Never attended this type of school",
            "99": "Don't know/Refused",
        },
    },
    "q5b": {
        "title": "Have you ever learned about science at Secondary School?",
        "answers": {
            "1": "Yes",
            "2": "No",
            "97": "Never attended this type of school",
            "99": "Don't know/Refused",
        },
    },
    "q5c": {
        "title": "Have you ever learned about science at College/University?",
        "answers": {
            "1": "Yes",
            "2": "No",
            "97": "Never attended this type of school",
            "99": "Don't know/Refused",
        },
    },
    "q6": {
        "title": "Have you tried to get any information about science in the past 30 days?",
        "answers": {
            "1": "Yes",
            "2": "No",
            "99": "Don't know/Refused",
        },
    },
    "q7": {
        "title": "Have you tried to get any information about medicine, disease, or health in the past 30 days?",
        "answers": {
            "1": "Yes",
            "2": "No",
            "99": "Don't know/Refused",
        },
    },
    "q8": {
        "title": "Would you like to know more about science?",
        "answers": {
            "1": "Yes",
            "2": "No",
            "99": "Don't know/Refused",
        },
    },
    "q9": {
        "title": "Would you like to know more about medicine, disease, or health?",
        "answers": {
            "1": "Yes",
            "2": "No",
            "99": "Don't know/Refused",
        },
    },
    "q10a": {
        "title": "In (country), do you have confidence in Non-governmental organizations or Non-profit organizations. ",
        "answers": {
            "1": "Yes",
            "2": "No",
            "99": "Don't know/Refused",
        },
    },
    "q10b": {
        "title": "In (country), do you have confidence in each of the following, or not? How about Hospitals and Health Clinics.",
        "answers": {
            "1": "Yes",
            "2": "No",
            "99": "Don't know/Refused",
        },
    },
    "q11a": {
        "title": "Trust in people in your neighbourhood",
        "answers": {
            "1": "A lot",
            "2": "Some",
            "3": "Not much",
            "4": "Not at all",
            "99": "Don't know/Refused",
        },
    },
    "q11b": {
        "title": "Trust in the national government of your country",
        "answers": {
            "1": "A lot",
            "2": "Some",
            "3": "Not much",
            "4": "Not at all",
            "99": "Don't know/Refused",
        },
    },
    "q11c": {
        "title": "Trust in scientists in your country",
        "answers": {
            "1": "A lot",
            "2": "Some",
            "3": "Not much",
            "4": "Not at all",
            "99": "Don't know/Refused",
        },
    },
    "q11d": {
        "title": "Trust in journalists in your country",
        "answers": {
            "1": "A lot",
            "2": "Some",
            "3": "Not much",
            "4": "Not at all",
            "99": "Don't know/Refused",
        },
    },
    "q11e": {
        "title": "Trust in doctors and nurses in your country",
        "answers": {
            "1": "A lot",
            "2": "Some",
            "3": "Not much",
            "4": "Not at all",
            "99": "Don't know/Refused",
        },
    },
    "q11f": {
        "title": "Trust in people working for NGOs/charitable organizations in your country",
        "answers": {
            "1": "A lot",
            "2": "Some",
            "3": "Not much",
            "4": "Not at all",
            "99": "Don't know/Refused",
        },
    },
    "q11g": {
        "title": "Trust in traditional healers in your country",
        "answers": {
            "1": "A lot",
            "2": "Some",
            "3": "Not much",
            "4": "Not at all",
            "99": "Don't know/Refused",
        },
    },
    "q12": {
        "title": "Trust in science",
        "answers": {
            "1": "A lot",
            "2": "Some",
            "3": "Not much",
            "4": "Not at all",
            "99": "Don't know/Refused",
        },
    },
    "q13": {
        "title": "Trust scientists to find out accurate information about the world",
        "answers": {
            "1": "A lot",
            "2": "Some",
            "3": "Not much",
            "4": "Not at all",
            "99": "Don't know/Refused",
        },
    },
    "q14a": {
        "title": "Trust scientists in colleges/universities to do their work with the intention of benefiting the public",
        "answers": {
            "1": "A lot",
            "2": "Some",
            "3": "Not much",
            "4": "Not at all",
            "99": "Don't know/Refused",
        },
    },
    "q14b": {
        "title": "Trust scientists in colleges/universities to be open and honest about who is paying for their work",
        "answers": {
            "1": "A lot",
            "2": "Some",
            "3": "Not much",
            "4": "Not at all",
            "99": "Don't know/Refused",
        },
    },
    "q15a": {
        "title": "Trust scientists in companies to do their work with the intention of benefiting the public",
        "answers": {
            "1": "A lot",
            "2": "Some",
            "3": "Not much",
            "4": "Not at all",
            "99": "Don't know/Refused",
        },
    },
    "q15b": {
        "title": "Trust scientists in companies to be open and honest about who is paying for their work",
        "answers": {
            "1": "A lot",
            "2": "Some",
            "3": "Not much",
            "4": "Not at all",
            "99": "Don't know/Refused",
        },
    },
    "q16": {
        "title": "Work from scientists in your country benefits most, some, or very few people",
        "answers": {
            "1": "Most",
            "2": "Some",
            "3": "Very few",
            "99": "Don't know/Refused",
        },
    },
    "q17": {
        "title": "Work from scientists in your country has benefited people like you",
        "answers": {
            "1": "Yes",
            "2": "No",
            "99": "Don't know/Refused",
        },
    },
    "q18": {
        "title": "Overall, do you think that science and technology will help improve life for the next generation?",
        "answers": {
            "1": "Yes",
            "2": "No",
            "99": "Don't know/Refused",
        },
    },
    "q19": {
        "title": "Science and technology will increase/decrease the number of jobs in your local area in the next 5 years",
        "answers": {
            "1": "Increase",
            "2": "Decrease",
            "3": "Neither/Have no effect",
            "99": "Don't know/Refused",
        },
    },
    "q20": {
        "title": "Which of the following people do you trust MOST to give you medical or health advice?",
        "answers": {
            "1": "Your family and friends",
            "2": "A religious leader",
            "3": "A doctor or nurse",
            "4": "A famous person",
            "5": "Traditional healer (or equivalent)",
            "97": "None of these/Someone else",
            "99": "Don't know/Refused",
        },
    },
    "q21": {
        "title": "Trust medical and health advice from the government in your country",
        "answers": {
            "1": "A lot",
            "2": "Some",
            "3": "Not much",
            "4": "Not at all",
            "99": "Don't know/Refused",
        },
    },
    "q22": {
        "title": "Trust medical and health advice from medical workers in your country",
        "answers": {
            "1": "A lot",
            "2": "Some",
            "3": "Not much",
            "4": "Not at all",
            "99": "Don't know/Refused",
        },
    },
    "q23": {
        "title": "Before today, had you ever heard of a vaccine?",
        "answers": {
            "1": "Yes",
            "2": "No",
            "99": "Don't know/Refused",
        },
    },
    "q24": {
        "title": "Vaccines are important for children to have",
        "answers": {
            "1": "Strongly agree",
            "2": "Somewhat agree",
            "3": "Neither agree nor disagree",
            "4": "Somewhat disagree",
            "5": "Strongly disagree",
            "99": "Don't know/Refused",
        },
    },
    "q25": {
        "title": "Vaccines are safe",
        "answers": {
            "1": "Strongly agree",
            "2": "Somewhat agree",
            "3": "Neither agree nor disagree",
            "4": "Somewhat disagree",
            "5": "Strongly disagree",
            "99": "Don't know/Refused",
        },
    },
    "q26": {
        "title": "Vaccines are effective",
        "answers": {
            "1": "Strongly agree",
            "2": "Somewhat agree",
            "3": "Neither agree nor disagree",
            "4": "Somewhat disagree",
            "5": "Strongly disagree",
            "99": "Don't know/Refused",
        },
    },
    "q27": {
        "title": "You have children",
        "answers": {
            "1": "Yes",
            "2": "No",
            "3": "Yes but no longer living",
            "5": "Don't know/Refused",
        },
    },
    "q28": {
        "title": "Any of your children has received a vaccine that was supposed to prevent them from getting childhood diseases",
        "answers": {
            "1": "Yes",
            "2": "No",
            "99": "Don't know/Refused",
        },
    },
    "d1": {
        "title": "Your religion",
        "answers": {
            "1": "Named a specific religion",
            "2": "Secular/Non-religious",
            "99": "Don't know/Refused",
        },
    },
    "q29": {
        "title": "Has science ever disagreed with the teachings of your religion?",
        "answers": {
            "1": "Yes",
            "2": "No",
            "99": "Don't know/Refused",
        },
    },
    "q30": {
        "title": "When science disagrees with the teachings of your religion, what do you believe?",
        "answers": {
            "1": "Science",
            "2": "The teachings of your religion",
            "97": "(It depends)",
            "99": "Don't know/Refused",
        },
    },
}
