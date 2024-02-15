/*
COMMANDS TO EXTRACT QUESTIONS FROM THE INTEGRATED VALUES SURVEY

This code collapses microdata from the Integrated Values Survey (World Values Survey + European Values Survey) and generates a csv file.

INSTRUCTIONS

	1.	Follow the instructions to construct the IVS file from WVS and EVS microdata here: https://www.worldvaluessurvey.org/WVSEVStrend.jsp
			The files required are the WVS and EVS trend files, and the merge syntax file (in our case in Stata). Keep these files in the same folder.
	2.	Run the EVS_WVS_Merge_Syntax_stata 4.do file in Stata. This will generate the IVS main dataset.
	3.	Run this do-file in Stata. It will generate the file ivs.csv
	4.	Add snapshot. Currently the command is
 			python snapshots/ivs/{date}/integrated_values_survey.py --path-to-file snapshots/ivs/{date}/ivs.csv
	5.	Delete csv file
	6.	Run `etl integrated_values_survey`

*/


use Integrated_values_surveys_1981-2021, clear

*List of confidence questions
ds E069*
global confidence_questions `r(varlist)'

* List of other trust questions (family, neighborhood, other religion, other nationality, churches)
global other_trust_questions D001_B G007_18_B G007_35_B G007_36_B

* Join confidence and other trust questions
global additional_questions $confidence_questions $other_trust_questions

* List of questions about things important in life
global important_in_life_questions A001 A002 A003 A004 A005 A006

* list of questions about politics
global politics_questions E023 E025 E026 E027 E028

* protecting the environment vs. economic growth
global environment_vs_econ_questions B008

* income equality
global income_equality_questions E035

* List of questions to work with
* NOTE: A168 is not available in IVS
global questions A165 A168 G007_33_B G007_34_B $additional_questions $important_in_life_questions $politics_questions $environment_vs_econ_questions $income_equality_questions

 * Keep wave ID, country, weight and the list of questions
keep S002VS S002EVS S003 S017 $questions

* Replace wave ID with last year of survey
replace S002VS=1984 if S002VS==1
replace S002VS=1993 if S002VS==2
replace S002VS=1998 if S002VS==3
replace S002VS=2004 if S002VS==4
replace S002VS=2010 if S002VS==5
replace S002VS=2014 if S002VS==6
replace S002VS=2022 if S002VS==7

* There are several S002VS missing (only in EVS), so they are replaced according to the year of survey of EVS
replace S002VS = 1984 if S002VS==. & S002EVS==1
replace S002VS = 1993 if S002VS==. & S002EVS==2
replace S002VS = 2001 if S002VS==. & S002EVS==3
replace S002VS = 2010 if S002VS==. & S002EVS==4
replace S002VS = 2021 if S002VS==. & S002EVS==5

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
collapse (mean) trust [w=S017], by (year country)
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

collapse (mean) trust_first trust_first_not_very_much trust_first_not_at_all [w=S017], by (year country)
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

collapse (mean) trust_personally trust_personally_not_very_much trust_personally_not_at_all [w=S017], by (year country)
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

collapse (mean) take_advantage [w=S017], by (year country)
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

	collapse (mean) confidence_`var' [w=S017], by (year country)
	tempfile confidence_`var'_file
	save "`confidence_`var'_file'"

	restore
	preserve
}

* Processing "important in life" questions
/*
           1 Very important
           2 Rather important
           3 Not very important
           4 Not at all important
          .a Don't know
          .b No answer
          .c Not applicable
          .d Not asked in survey
          .e Missing: other
*/


foreach var in $important_in_life_questions {
	keep if `var' >= 1

	gen important_in_life_`var' = 0
	replace important_in_life_`var' = 1 if `var' == 1 | `var' == 2
	
	gen not_important_in_life_`var' = 0
	replace not_important_in_life_`var' = 1 if `var' == 3 | `var' == 4
	
	gen very_important_in_life_`var' = 0
	replace very_important_in_life_`var' = 1 if `var' == 1
	
	gen rather_important_in_life_`var' = 0
	replace rather_important_in_life_`var' = 1 if `var' == 2
	
	gen not_very_important_in_life_`var' = 0
	replace not_very_important_in_life_`var' = 1 if `var' == 3
	
	gen notatall_important_in_life_`var' = 0
	replace notatall_important_in_life_`var' = 1 if `var' == 4
	
	gen dont_know_important_in_life_`var' = 0
	replace dont_know_important_in_life_`var' = 1 if `var' == .a
	
	gen missing_important_in_life_`var' = 0
	replace missing_important_in_life_`var' = 1 if `var' == .b | `var' == .c | `var' == .d | `var' == .e

	collapse (mean) important_in_life_`var' not_important_in_life_`var' very_important_in_life_`var' rather_important_in_life_`var' not_very_important_in_life_`var' notatall_important_in_life_`var' dont_know_important_in_life_`var' missing_important_in_life_`var' [w=S017], by (year country)
	tempfile important_in_life_`var'_file
	save "`important_in_life_`var'_file'"

	restore
	preserve
}

* Processing "politics" questions

* E023 (interest in politics) has a different structure
/*
           1 Very interested
           2 Somewhat interested
           3 Not very interested
           4 Not at all interested
          .a Don't know
          .b No answer
          .c Not applicable
          .d Not asked in survey
          .e Missing: other

*/
keep if E023 >= 1

gen interested_politics = 0
replace interested_politics = 1 if E023 == 1 | E023 == 2

gen not_interested_politics = 0
replace not_interested_politics = 1 if E023 == 3 | E023 == 4

gen very_interested_politics = 0
replace very_interested_politics = 1 if E023 == 1

gen somewhat_interested_politics = 0
replace somewhat_interested_politics = 1 if E023 == 2

gen not_very_interested_politics = 0
replace not_very_interested_politics = 1 if E023 == 3

gen not_at_all_interested_politics = 0
replace not_at_all_interested_politics = 1 if E023 == 4

gen dont_know_interested_politics = 0
replace dont_know_interested_politics = 1 if E023 == .a

gen missing_interested_politics = 0
replace missing_interested_politics = 1 if E023 == .b | E023 == .c | E023 == .d | E023 == .e

collapse (mean) interested_politics not_interested_politics very_interested_politics somewhat_interested_politics not_very_interested_politics not_at_all_interested_politics dont_know_interested_politics missing_interested_politics [w=S017], by (year country)
tempfile interest_politics_file
save "`interest_politics_file'"

restore
preserve

* For political action questions
/*
           1 Have done
           2 Might do
           3 Would never do
          .a Don't know
          .b No answer
          .c Not applicable
          .d Not asked in survey
          .e Missing: other

*/

global interest_politics E023
global rest_politics_questions : list global(politics_questions) - global(interest_politics)

foreach var of varlist $rest_politics_questions {
	
	keep if `var' >= 1

	gen have_done_political_action_`var' = 0
	replace have_done_political_action_`var' = 1 if `var' == 1
	
	gen might_do_political_action_`var' = 0
	replace might_do_political_action_`var' = 1 if `var' == 2
	
	gen never_political_action_`var' = 0
	replace never_political_action_`var' = 1 if `var' == 3
	
	gen dont_know_political_action_`var' = 0
	replace dont_know_political_action_`var' = 1 if `var' == .a
	
	gen missing_political_action_`var' = 0
	replace missing_political_action_`var' = 1 if `var' == .b | `var' == .c | `var' == .d | `var' == .e

	collapse (mean) have_done_political_action_`var' might_do_political_action_`var' never_political_action_`var' dont_know_political_action_`var' missing_political_action_`var' [w=S017], by (year country)
	tempfile politics_`var'_file
	save "`politics_`var'_file'"
	
	restore
	preserve
}

* Processing environment vs. economic growth questions
/*
           1 Protecting environment
           2 Economy growth and creating jobs
           3 Other answer
          .a Don't know
          .b No answer
          .c Not applicable
          .d Not asked in survey
          .e Missing: other

*/

* Keep only answers
keep if B008 >= 1

*Generate variables
gen environment_env_ec = 0
replace environment_env_ec = 1 if B008 == 1

gen economy_env_ec = 0
replace economy_env_ec = 1 if B008 == 2

gen other_answer_env_ec = 0
replace other_answer_env_ec = 1 if B008 == 3

gen dont_know_env_ec = 0
replace dont_know_env_ec = 1 if B008 == .a

gen missing_env_ec = 0
replace missing_env_ec = 1 if B008 == .b | B008 == .c | B008 == .d | B008 == .e

* Make dataset of the mean trust (which ends up being the % of people saying "most people can be trusted") by wave and country (CHECK WEIGHTS)
collapse (mean) environment_env_ec economy_env_ec other_answer_env_ec dont_know_env_ec missing_env_ec [w=S017], by (year country)
tempfile environment_vs_econ_file
save "`environment_vs_econ_file'"

restore
preserve

* Processing income equality questions
/*
           1 Incomes should be made more equal
           2 2
           3 3
           4 4
           5 5
           6 6
           7 7
           8 8
           9 9
          10 We need larger income differences as incentives
          .a Don't know
          .b No answer
          .c Not applicable
          .d Not asked in survey
          .e Missing: other

*/

* Keep only answers
keep if E035 >= 1

*Generate variables
gen equality_eq_ineq = 0
replace equality_eq_ineq = 1 if E035 <= 4

gen neutral_eq_ineq = 0
replace neutral_eq_ineq = 1 if E035 == 5

gen inequality_eq_ineq = 0
replace inequality_eq_ineq = 1 if E035 >= 6 & E035 <=10

gen dont_know_eq_ineq = 0
replace dont_know_eq_ineq = 1 if E035 == .a

gen missing_eq_ineq = 0
replace missing_eq_ineq = 1 if E035 == .b | E035 == .c | E035 == .d | E035 == .e

gen avg_score_eq_ineq = E035

* Make dataset of the mean trust (which ends up being the % of people saying "most people can be trusted") by wave and country (CHECK WEIGHTS)
collapse (mean) equality_eq_ineq neutral_eq_ineq inequality_eq_ineq dont_know_eq_ineq missing_eq_ineq avg_score_eq_ineq [w=S017], by (year country)
tempfile income_equality_file
save "`income_equality_file'"

restore
preserve



* Combine all the saved datasets
use "`trust_file'", clear

merge 1:1 year country using "`trust_first_file'", nogenerate // keep(master match)
merge 1:1 year country using "`trust_personally_file'", nogenerate // keep(master match)
merge 1:1 year country using "`take_advantage_file'", nogenerate // keep(master match)

foreach var in $additional_questions {
	merge 1:1 year country using "`confidence_`var'_file'", nogenerate // keep(master match)
}

foreach var in $important_in_life_questions {
	merge 1:1 year country using "`important_in_life_`var'_file'", nogenerate // keep(master match)
}

merge 1:1 year country using "`interest_politics_file'", nogenerate // keep(master match)

foreach var in $rest_politics_questions {
	merge 1:1 year country using "`politics_`var'_file'", nogenerate // keep(master match)
}

merge 1:1 year country using "`environment_vs_econ_file'", nogenerate // keep(master match)

merge 1:1 year country using "`income_equality_file'", nogenerate // keep(master match)


* Get a list of variables excluding country and year (and avg_score_eq_ineq to not multiply it by 100)
ds country year avg_score_eq_ineq, not

* Multiply variables by 100 to get percentages
foreach var of varlist `r(varlist)' {
	replace `var' = `var'*100
}

* Export as csv
export delimited using "ivs.csv", datafmt replace
