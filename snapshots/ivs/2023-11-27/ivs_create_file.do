/*
COMMANDS TO EXTRACT TRUST QUESTIONS FROM THE INTEGRATED VALUES SURVEY

This code collapses microdata from the Integrated Values Survey (World Values Survey + European Values Survey) and generates a csv file.

NOTE: For now I am extracting data on trust, but this will be expanded in 2024 to include other variables.

INSTRUCTIONS

	1.	Follow the instructions to construct the IVS file from WVS and EVS microdata here: https://www.worldvaluessurvey.org/WVSEVStrend.jsp
			The files required are the WVS and EVS trend files, and the merge syntax file (in our case in Stata). Keep these files in the same folder.
	2.	Run the EVS_WVS_Merge_Syntax_stata 4.do file in Stata. This will generate the IVS main dataset.
	3.	Run this do-file in Stata. It will generate the file ivs.csv
	4.	Add snapshot. Currently the command is
 			python snapshots/ivs/{date}/integrated_values_survey.py --path-to-file snapshots/ivs/{date}/ivs.csv
	5.	Delete csv file
	6.	Run etl wvs_trust

*/


use Integrated_values_surveys_1981-2021, clear

*List of confidence questions
ds E069*
global confidence_questions `r(varlist)'

* List of other trust questions (family, neighborhood, other religion, other nationality, churches)
global other_trust_questions D001_B G007_18_B G007_35_B G007_36_B

* Join confidence and other trust questions
global additional_questions $confidence_questions $other_trust_questions

* List of questions to work with
* NOTE: A168 is not available in IVS
global questions A165 A168 G007_33_B G007_34_B $additional_questions

 * Keep wave ID, country, weight and the list of questions
keep S002VS S003 S017 $questions

* Replace wave ID with last year of survey
replace S002VS=1984 if S002VS==1
replace S002VS=1993 if S002VS==2
replace S002VS=1998 if S002VS==3
replace S002VS=2004 if S002VS==4
replace S002VS=2010 if S002VS==5
replace S002VS=2014 if S002VS==6
replace S002VS=2022 if S002VS==7

rename S002VS year
rename S003 country

* Preserve dataset in these conditions
preserve

* List the labels of the selected questions
// label list $questions

/*
A165 is the question "most people can be trusted"
*/

* Keep only "most people can be trusted" (1), "Need to be very careful" (2), "Don't know" (-1)
keep if A165 >= -1

*Generate trust variable, with 1 if it's option 1
gen trust = 0
replace trust = 1 if A165 == 1

* Make dataset of the mean trust (which ends up being the % of people saying "most people can be trusted") by wave and country (CHECK WEIGHTS)
collapse (mean) trust, by (year country)
tempfile trust_file
save "`trust_file'"

restore
preserve

/*
G007_34 is the question about trusting people you meet fot the first time
*/

keep if G007_34 >= -1

gen trust_first = 0
replace trust_first = 1 if G007_34 == 1 | G007_34 == 2

gen trust_first_not_very_much = 0
replace trust_first_not_very_much = 1 if G007_34 == 3

gen trust_first_not_at_all = 0
replace trust_first_not_at_all = 1 if G007_34 == 4

collapse (mean) trust_first trust_first_not_very_much trust_first_not_at_all, by (year country)
tempfile trust_first_file
save "`trust_first_file'"

restore
preserve

/*
G007_33 is the question about trusting people you know personally
*/

keep if G007_33 >= -1

gen trust_personally = 0
replace trust_personally = 1 if G007_33 == 1 | G007_33 == 2

gen trust_personally_not_very_much = 0
replace trust_personally_not_very_much = 1 if G007_33 == 3

gen trust_personally_not_at_all = 0
replace trust_personally_not_at_all = 1 if G007_33 == 4

collapse (mean) trust_personally trust_personally_not_very_much trust_personally_not_at_all, by (year country)
tempfile trust_personally_file
save "`trust_personally_file'"

restore
preserve

/*
A168 is the question "do you think most people try to take advantage of you"
*/

keep if A168 >= -1

gen take_advantage = 0
replace take_advantage = 1 if A168 == 1

collapse (mean) take_advantage, by (year country)
tempfile take_advantage_file
save "`take_advantage_file'"

restore
preserve

/*
Processing of multiple additional trust and confidence questions
*/

foreach var in $additional_questions {
	keep if `var' >= -1

	gen confidence_`var' = 0
	replace confidence_`var' = 1 if `var' == 1 | `var' == 2

	collapse (mean) confidence_`var', by (year country)
	tempfile confidence_`var'_file
	save "`confidence_`var'_file'"

	restore
	preserve
}



* Combine all the saved datasets
use "`trust_file'", clear

merge 1:1 year country using "`trust_first_file'", nogenerate // keep(master match)
merge 1:1 year country using "`trust_personally_file'", nogenerate // keep(master match)
merge 1:1 year country using "`take_advantage_file'", nogenerate // keep(master match)

foreach var in $additional_questions {
	merge 1:1 year country using "`confidence_`var'_file'", nogenerate // keep(master match)
}

* Get a list of variables excluding country and year
ds country year, not

* Multiply variables by 100 to get percentages
foreach var of varlist `r(varlist)' {
	replace `var' = `var'*100
}

* Export as csv
export delimited using "ivs.csv", datafmt replace
