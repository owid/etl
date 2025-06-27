/*
COMMANDS TO EXTRACT QUESTIONS FROM THE INTEGRATED VALUES SURVEY

This code collapses microdata from the Integrated Values Survey (World Values Survey + European Values Survey) and generates a csv file.

INSTRUCTIONS

	1.	Follow the instructions to construct the IVS file from WVS and EVS microdata here: https://www.worldvaluessurvey.org/WVSEVStrend.jsp
			The files required are the WVS and EVS trend files, and the merge syntax file (in our case in Stata). Keep these files in the same folder.
	2.	In the EVS_WVS_Merge_Syntax_stata.do file, fill in your local path for the global macros WVS_TF, EVS_TF and IVS. Run the code. This will generate the IVS main dataset (it takes some time).
	3.	Run _this_ do-file in Stata. It will generate the file ivs.csv
	4.	Add snapshot. Currently the command is
 			python snapshots/ivs/{date}/integrated_values_survey.py --path-to-file snapshots/ivs/{date}/ivs.csv
	5.	Delete csv file
	6.	Run `etl integrated_values_survey`

*/


use Integrated_values_surveys_1981-2022, clear

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

* Schwartz questions
global schwartz_questions A189 A190 A191 A192 A193 A194 A195 A196 A197 A198

* Work vs. leisure
global work_leisure_questions C008

* Work
global work_questions C039 C041

* Most serious problem
global most_serious_problem_questions E238

* Justifiable
global justifiable_questions F114A F114B F114C F114D F115 F116 F117 F118 F119 F120 F121 F122 F123 F132 F135A F144_01 F144_02 F199 E290

* Worries
global worries_questions H006_01 H006_02 H006_03 H006_04 H006_05

* Happiness
global happiness_questions A008

* Neighbors
global neighbors_questions A124_02 A124_03 A124_06 A124_07 A124_08 A124_09 A124_12 A124_17 A124_42 A124_43

* Homosexuals as parents
global homosexuals_parents_questions D081

* Democracy questions
global democracy_satisfied E111_01
global democracy_very_good_very_bad E115 E116 E117
global democracy_essential_char E224 E225 E226 E227 E228 E229 E233 E233A E233B
global democracy_importance E235
global democracy_democraticness E236
global democracy_elections_makes_diff E266

* List of questions to work with
* NOTE: A168 is not available in IVS
global questions A165 A168 G007_33_B G007_34_B $additional_questions $important_in_life_questions $politics_questions $environment_vs_econ_questions $income_equality_questions $schwartz_questions $work_leisure_questions $work_questions $most_serious_problem_questions $justifiable_questions $worries_questions $happiness_questions $neighbors_questions $homosexuals_parents_questions $democracy_satisfied $democracy_very_good_very_bad $democracy_essential_char $democracy_importance $democracy_democraticness $democracy_elections_makes_diff

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

* There are several S002VS missing (only in EVS), so they are replaced according to the WVS-EVS waves
replace S002VS = 1984 if S002VS==. & S002EVS==1
replace S002VS = 1993 if S002VS==. & S002EVS==2
replace S002VS = 2004 if S002VS==. & S002EVS==3
replace S002VS = 2010 if S002VS==. & S002EVS==4
replace S002VS = 2022 if S002VS==. & S002EVS==5

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
keep if A165 >= 1
keep if A165 != .c
keep if A165 != .d
keep if A165 != .e

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

keep if G007_34 >= 1

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

keep if G007_33 >= 1

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

keep if A168 >= 1
keep if A168 != .c
keep if A168 != .d
keep if A168 != .e

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
	keep if `var' >= 1
	keep if `var' != .c
	keep if `var' != .d
	keep if `var' != .e

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
	keep if `var' != .c
	keep if `var' != .d
	keep if `var' != .e

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

	gen no_answer_important_in_life_`var' = 0
	replace no_answer_important_in_life_`var' = 1 if `var' == .b

	collapse (mean) important_in_life_`var' not_important_in_life_`var' very_important_in_life_`var' rather_important_in_life_`var' not_very_important_in_life_`var' notatall_important_in_life_`var' dont_know_important_in_life_`var' no_answer_important_in_life_`var' [w=S017], by (year country)
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
keep if E023 != .c
keep if E023 != .d
keep if E023 != .e

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

gen no_answer_interested_politics = 0
replace no_answer_interested_politics = 1 if E023 == .b

collapse (mean) interested_politics not_interested_politics very_interested_politics somewhat_interested_politics not_very_interested_politics not_at_all_interested_politics dont_know_interested_politics no_answer_interested_politics [w=S017], by (year country)
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
	keep if `var' != .c
	keep if `var' != .d
	keep if `var' != .e

	gen have_done_political_action_`var' = 0
	replace have_done_political_action_`var' = 1 if `var' == 1

	gen might_do_political_action_`var' = 0
	replace might_do_political_action_`var' = 1 if `var' == 2

	gen never_political_action_`var' = 0
	replace never_political_action_`var' = 1 if `var' == 3

	gen dont_know_political_action_`var' = 0
	replace dont_know_political_action_`var' = 1 if `var' == .a

	gen no_answer_political_action_`var' = 0
	replace no_answer_political_action_`var' = 1 if `var' == .b

	collapse (mean) have_done_political_action_`var' might_do_political_action_`var' never_political_action_`var' dont_know_political_action_`var' no_answer_political_action_`var' [w=S017], by (year country)
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
keep if B008 != .c
keep if B008 != .d
keep if B008 != .e

*Generate variables
gen environment_env_ec = 0
replace environment_env_ec = 1 if B008 == 1

gen economy_env_ec = 0
replace economy_env_ec = 1 if B008 == 2

gen other_answer_env_ec = 0
replace other_answer_env_ec = 1 if B008 == 3

gen dont_know_env_ec = 0
replace dont_know_env_ec = 1 if B008 == .a

gen no_answer_env_ec = 0
replace no_answer_env_ec = 1 if B008 == .b

* Make dataset of the mean trust (which ends up being the % of people saying "most people can be trusted") by wave and country (CHECK WEIGHTS)
collapse (mean) environment_env_ec economy_env_ec other_answer_env_ec dont_know_env_ec no_answer_env_ec [w=S017], by (year country)
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
keep if E035 != .c
keep if E035 != .d
keep if E035 != .e

*Generate variables
gen equality_eq_ineq = 0
replace equality_eq_ineq = 1 if E035 <= 4

gen neutral_eq_ineq = 0
replace neutral_eq_ineq = 1 if E035 == 5

gen inequality_eq_ineq = 0
replace inequality_eq_ineq = 1 if E035 >= 6 & E035 <=10

gen dont_know_eq_ineq = 0
replace dont_know_eq_ineq = 1 if E035 == .a

gen no_answer_eq_ineq = 0
replace no_answer_eq_ineq = 1 if E035 == .b

gen avg_score_eq_ineq = E035

* Make dataset of the mean trust (which ends up being the % of people saying "most people can be trusted") by wave and country (CHECK WEIGHTS)
collapse (mean) equality_eq_ineq neutral_eq_ineq inequality_eq_ineq dont_know_eq_ineq no_answer_eq_ineq avg_score_eq_ineq [w=S017], by (year country)
tempfile income_equality_file
save "`income_equality_file'"

restore
preserve

* Processing "Schwartz" questions
/*
           1 Not at all like me
           2 Not like me
           3 A little like me
           4 Somewhat like me
           5 Like me
           6 Very much like me
          .a Don't know
          .b No answer
          .c Not applicable
          .d Not asked in survey
          .e Missing: other

*/


foreach var in $schwartz_questions {
	keep if `var' >= 1
	keep if `var' != .c
	keep if `var' != .d
	keep if `var' != .e

	gen like_me_agg_`var' = 0
	replace like_me_agg_`var' = 1 if `var' == 4 | `var' == 5 | `var' == 6

	gen not_like_me_agg_`var' = 0
	replace not_like_me_agg_`var' = 1 if `var' == 1 | `var' == 2

	gen very_much_like_me_`var' = 0
	replace very_much_like_me_`var' = 1 if `var' == 6

	gen like_me_`var' = 0
	replace like_me_`var' = 1 if `var' == 5

	gen somewhat_like_me_`var' = 0
	replace somewhat_like_me_`var' = 1 if `var' == 4

	gen a_little_like_me_`var' = 0
	replace a_little_like_me_`var' = 1 if `var' == 3

	gen not_like_me_`var' = 0
	replace not_like_me_`var' = 1 if `var' == 2

	gen not_at_all_like_me_`var' = 0
	replace not_at_all_like_me_`var' = 1 if `var' == 1

	gen dont_know_`var' = 0
	replace dont_know_`var' = 1 if `var' == .a

	gen no_answer_`var' = 0
	replace no_answer_`var' = 1 if `var' == .b

	collapse (mean) like_me_agg_`var' not_like_me_agg_`var' very_much_like_me_`var' like_me_`var' somewhat_like_me_`var' a_little_like_me_`var' not_like_me_`var' not_at_all_like_me_`var' dont_know_`var' no_answer_`var' [w=S017], by (year country)
	tempfile schwartz_`var'_file
	save "`schwartz_`var'_file'"

	restore
	preserve
}

* Processing work vs. leisure question
/*
           1 It's leisure that makes life worth living, not work
           2 2
           3 3
           4 4
           5 Work is what makes life worth living, not leisure
          .a Don't know
          .b No answer
          .c Not applicable
          .d Not asked in survey
          .e Missing: other
*/

* Keep only answers
keep if C008 >= 1
keep if C008 != .c
keep if C008 != .d
keep if C008 != .e

*Generate variables
gen leisure_lei_vs_wk = 0
replace leisure_lei_vs_wk = 1 if C008 <= 2

gen neutral_lei_vs_wk = 0
replace neutral_lei_vs_wk = 1 if C008 == 3

gen work_lei_vs_wk = 0
replace work_lei_vs_wk = 1 if C008 == 4 | C008 == 5

gen lei_vs_wk_1 = 0
replace lei_vs_wk_1  = 1 if C008 == 1

gen lei_vs_wk_2 = 0
replace lei_vs_wk_2  = 1 if C008 == 2

gen lei_vs_wk_4 = 0
replace lei_vs_wk_4  = 1 if C008 == 4

gen lei_vs_wk_5 = 0
replace lei_vs_wk_5  = 1 if C008 == 5

gen dont_know_lei_vs_wk = 0
replace dont_know_lei_vs_wk = 1 if C008 == .a

gen no_answer_lei_vs_wk = 0
replace no_answer_lei_vs_wk = 1 if C008 == .b

gen avg_score_lei_vs_wk = C008

* Make dataset of the mean trust (which ends up being the % of people saying "most people can be trusted") by wave and country (CHECK WEIGHTS)
collapse (mean) leisure_lei_vs_wk neutral_lei_vs_wk work_lei_vs_wk lei_vs_wk_1 lei_vs_wk_2 lei_vs_wk_4 lei_vs_wk_5 dont_know_lei_vs_wk no_answer_lei_vs_wk avg_score_lei_vs_wk [w=S017], by (year country)
tempfile leisure_vs_work_file
save "`leisure_vs_work_file'"

restore
preserve

* Processing work questions
/*
           1 Strongly agree
           2 Agree
           3 Neither agree nor disagree
           4 Disagree
           5 Strongly disagree
          .a Don't know
          .b No answer
          .c Not applicable
          .d Not asked in survey
          .e Missing: other

*/


foreach var in $work_questions {
	keep if `var' >= 1
	keep if `var' != .c
	keep if `var' != .d
	keep if `var' != .e

	gen agree_agg_`var' = 0
	replace agree_agg_`var' = 1 if `var' == 1 | `var' == 2

	gen disagree_agg_`var' = 0
	replace disagree_agg_`var' = 1 if `var' == 4 | `var' == 5

	gen strongly_agree_`var' = 0
	replace strongly_agree_`var' = 1 if `var' == 1

	gen agree_`var' = 0
	replace agree_`var' = 1 if `var' == 2

	gen neither_`var' = 0
	replace neither_`var' = 1 if `var' == 3

	gen disagree_`var' = 0
	replace disagree_`var' = 1 if `var' == 4

	gen strongly_disagree_`var' = 0
	replace strongly_disagree_`var' = 1 if `var' == 5

	gen dont_know_`var' = 0
	replace dont_know_`var' = 1 if `var' == .a

	gen no_answer_`var' = 0
	replace no_answer_`var' = 1 if `var' == .b

	collapse (mean) agree_agg_`var' disagree_agg_`var' strongly_agree_`var' agree_`var' neither_`var' disagree_`var' strongly_disagree_`var' dont_know_`var' no_answer_`var' [w=S017], by (year country)
	tempfile work_`var'_file
	save "`work_`var'_file'"

	restore
	preserve
}

* Processing most serious problem of the world
/*
           1 People living in poverty and need
           2 Discrimination against girls and women
           3 Poor sanitation and infectious diseases
           4 Inadequate education
           5 Environmental pollution
          .a Don't know
          .b No answer
          .c Not applicable
          .d Not asked in survey
          .e Missing: other

*/

* Keep only answers
keep if E238 >= 1
keep if E238 != .c
keep if E238 != .d
keep if E238 != .e

*Generate variables
gen poverty_most_serious = 0
replace poverty_most_serious = 1 if E238 == 1

gen women_discr_most_serious = 0
replace women_discr_most_serious = 1 if E238 == 2

gen sanitation_most_serious = 0
replace sanitation_most_serious = 1 if E238 == 3

gen education_most_serious = 0
replace education_most_serious  = 1 if E238 == 4

gen pollution_most_serious = 0
replace pollution_most_serious  = 1 if E238 == 5

gen dont_know_most_serious = 0
replace dont_know_most_serious = 1 if E238 == .a

gen no_answer_most_serious = 0
replace no_answer_most_serious = 1 if E238 == .b

* Make dataset of the mean trust (which ends up being the % of people saying "most people can be trusted") by wave and country (CHECK WEIGHTS)
collapse (mean) poverty_most_serious women_discr_most_serious sanitation_most_serious education_most_serious pollution_most_serious dont_know_most_serious no_answer_most_serious [w=S017], by (year country)
tempfile most_serious_file
save "`most_serious_file'"

restore
preserve

* Processing justifiable questions
/*
           1 Never justifiable
           2 2
           3 3
           4 4
           5 5
           6 6
           7 7
           8 8
           9 9
          10 Always justifiable
          .a Don't know
          .b No answer
          .c Not applicable
          .d Not asked in survey
          .e Missing: other


*/


foreach var in $justifiable_questions {
	keep if `var' >= 1
	keep if `var' != .c
	keep if `var' != .d
	keep if `var' != .e

	gen never_just_agg_`var' = 0
	replace never_just_agg_`var' = 1 if `var' <= 4

	gen always_just_agg_`var' = 0
	replace always_just_agg_`var' = 1 if `var' >= 6 & `var' <= 10

	gen never_just_`var' = 0
	replace never_just_`var' = 1 if `var' == 1

	gen always_just_`var' = 0
	replace always_just_`var' = 1 if `var' == 10

	gen neutral_`var' = 0
	replace neutral_`var' = 1 if `var' == 5

	gen dont_know_`var' = 0
	replace dont_know_`var' = 1 if `var' == .a

	gen no_answer_`var' = 0
	replace no_answer_`var' = 1 if `var' == .b

	gen avg_score_`var' = `var'

	collapse (mean) never_just_agg_`var' always_just_agg_`var' never_just_`var' always_just_`var' neutral_`var' dont_know_`var' no_answer_`var' avg_score_`var' [w=S017], by (year country)
	tempfile justifiable_`var'_file
	save "`justifiable_`var'_file'"

	restore
	preserve
}

* Processing worries questions
/*
           1 Very much
           2 A great deal
           3 Not much
           4 Not at all
          .a Don't know
          .b No answer
          .c Not applicable
          .d Not asked in survey
          .e Missing: other

*/


foreach var in $worries_questions {
	keep if `var' >= 1
	keep if `var' != .c
	keep if `var' != .d
	keep if `var' != .e

	gen worry_`var' = 0
	replace worry_`var' = 1 if `var' <= 2

	gen not_worry_`var' = 0
	replace not_worry_`var' = 1 if `var' == 3 | `var' == 4

	gen very_much_`var' = 0
	replace very_much_`var' = 1 if `var' == 1

	gen a_great_deal_`var' = 0
	replace a_great_deal_`var' = 1 if `var' == 2

	gen not_much_`var' = 0
	replace not_much_`var' = 1 if `var' == 3

	gen not_at_all_`var' = 0
	replace not_at_all_`var' = 1 if `var' == 4

	gen dont_know_`var' = 0
	replace dont_know_`var' = 1 if `var' == .a

	gen no_answer_`var' = 0
	replace no_answer_`var' = 1 if `var' == .b

	gen avg_score_`var' = `var'

	collapse (mean) worry_`var' not_worry_`var' very_much_`var' a_great_deal_`var' not_much_`var' not_at_all_`var' dont_know_`var' no_answer_`var' avg_score_`var' [w=S017], by (year country)
	tempfile worries_`var'_file
	save "`worries_`var'_file'"

	restore
	preserve
}

* Processing happiness questions
/*
           1 Very happy
           2 Quite happy
           3 Not very happy
           4 Not at all happy
          .a Don't know
          .b No answer
          .c Not applicable
          .d Not asked in survey
          .e Missing: other
*/

* Keep only answers
keep if A008 >= 1
keep if A008 != .c
keep if A008 != .d
keep if A008 != .e

*Generate variables
gen happy = 0
replace happy = 1 if A008 == 1 | A008 == 2

gen not_happy = 0
replace not_happy = 1 if A008 == 3 | A008 == 4

gen very_happy = 0
replace very_happy = 1 if A008 == 1

gen quite_happy = 0
replace quite_happy = 1 if A008 == 2

gen not_very_happy = 0
replace not_very_happy = 1 if A008 == 3

gen not_at_all_happy = 0
replace not_at_all_happy = 1 if A008 == 4

gen dont_know_happy = 0
replace dont_know_happy = 1 if A008 == .a

gen no_answer_happy = 0
replace no_answer_happy = 1 if A008 == .b

* Make dataset of the mean trust (which ends up being the % of people saying "most people can be trusted") by wave and country (CHECK WEIGHTS)
collapse (mean) happy not_happy very_happy quite_happy not_very_happy not_at_all_happy dont_know_happy no_answer_happy [w=S017], by (year country)
tempfile happiness_file
save "`happiness_file'"

restore
preserve

* Processing neighbors questions
/*
           0 Not mentioned
           1 Mentioned
          .a Don't know
          .b No answer
          .c Not applicable
          .d Not asked in survey
          .e Missing: other


*/


foreach var in $neighbors_questions {
	keep if `var' >= 0
	keep if `var' != .c
	keep if `var' != .d
	keep if `var' != .e

	gen notmentioned_neighbors_`var' = 0
	replace notmentioned_neighbors_`var' = 1 if `var' == 0

	gen mentioned_neighbors_`var' = 0
	replace mentioned_neighbors_`var' = 1 if `var' == 1

	gen dont_know_neighbors_`var' = 0
	replace dont_know_neighbors_`var' = 1 if `var' == .a

	gen no_answer_neighbors_`var' = 0
	replace no_answer_neighbors_`var' = 1 if `var' == .b

	collapse (mean) notmentioned_neighbors_`var' mentioned_neighbors_`var' dont_know_neighbors_`var' no_answer_neighbors_`var' [w=S017], by (year country)
	tempfile neighbors_`var'_file
	save "`neighbors_`var'_file'"

	restore
	preserve
}

* Processing homosexuals as parents question
/*
           1 Agree strongly
           2 Agree
           3 Neither agree nor disagree
           4 Disagree
           5 Disagree strongly
          .a Don't know
          .b No answer
          .c Not applicable
          .d Not asked in survey
          .e Missing: other

*/

keep if D081 >= 1
keep if D081 != .c
keep if D081 != .d
keep if D081 != .e

gen agree_agg_homosx_prnts = 0
replace agree_agg_homosx_prnts = 1 if D081 == 1 | D081 == 2

gen disagree_agg_homosx_prnts = 0
replace disagree_agg_homosx_prnts = 1 if D081 == 4 | D081 == 5

gen strongly_agree_homosx_prnts = 0
replace strongly_agree_homosx_prnts = 1 if D081 == 1

gen agree_homosx_prnts = 0
replace agree_homosx_prnts = 1 if D081 == 2

gen neither_homosx_prnts = 0
replace neither_homosx_prnts = 1 if D081 == 3

gen disagree_homosx_prnts = 0
replace disagree_homosx_prnts = 1 if D081 == 4

gen strongly_disagree_homosx_prnts = 0
replace strongly_disagree_homosx_prnts = 1 if D081 == 5

gen dont_know_homosx_prnts = 0
replace dont_know_homosx_prnts = 1 if D081 == .a

gen no_answer_homosx_prnts = 0
replace no_answer_homosx_prnts = 1 if D081 == .b

collapse (mean) agree_agg_homosx_prnts disagree_agg_homosx_prnts strongly_agree_homosx_prnts agree_homosx_prnts neither_homosx_prnts disagree_homosx_prnts strongly_disagree_homosx_prnts dont_know_homosx_prnts no_answer_homosx_prnts [w=S017], by (year country)
tempfile homosexual_parents_file
save "`homosexual_parents_file'"

restore
preserve

* Processing satisfaction with democracy
/*
           1 Not satisfied at all
           2 2
           3 3
           4 4
           5 5
           6 6
           7 7
           8 8
           9 9
          10 Completely satisfied
          .a Don't know
          .b No answer
          .c Not applicable
          .d Not asked in survey
          .e Missing: other
*/

keep if $democracy_satisfied >= 1
keep if $democracy_satisfied != .c
keep if $democracy_satisfied != .d
keep if $democracy_satisfied != .e


gen not_satisfied_democracy = 0
replace not_satisfied_democracy = 1 if $democracy_satisfied <= 4

gen satisfied_democracy = 0
replace satisfied_democracy = 1 if $democracy_satisfied >= 6 & $democracy_satisfied <= 10

gen not_satisfied_at_all_democracy = 0
replace not_satisfied_at_all_democracy = 1 if $democracy_satisfied == 1

gen satisfied_completely_democracy = 0
replace satisfied_completely_democracy = 1 if $democracy_satisfied == 10

gen neutral_satisfied_democracy = 0
replace neutral_satisfied_democracy = 1 if $democracy_satisfied == 5

gen dont_know_satisfied_democracy = 0
replace dont_know_satisfied_democracy = 1 if $democracy_satisfied == .a

gen no_answer_satisfied_democracy = 0
replace no_answer_satisfied_democracy = 1 if $democracy_satisfied == .b

gen avg_score_satisfied_democracy = $democracy_satisfied

collapse (mean) not_satisfied_democracy satisfied_democracy not_satisfied_at_all_democracy satisfied_completely_democracy neutral_satisfied_democracy dont_know_satisfied_democracy no_answer_satisfied_democracy avg_score_satisfied_democracy  [w=S017], by (year country)
tempfile democracy_satisfied_file
save "`democracy_satisfied_file'"

restore
preserve

* Processing questions about political systems
/*
           1 Very good
           2 Fairly good
           3 Fairly bad
           4 Very bad
          .a Don't know
          .b No answer
          .c Not applicable
          .d Not asked in survey
          .e Missing: other
*/

foreach var in $democracy_very_good_very_bad {
	keep if `var' >= 1
	keep if `var' != .c
	keep if `var' != .d
	keep if `var' != .e

	gen good_`var' = 0
	replace good_`var' = 1 if `var' == 1 | `var' == 2

	gen bad_`var' = 0
	replace bad_`var' = 1 if `var' == 3 | `var' == 4

	gen very_good_`var' = 0
	replace very_good_`var' = 1 if `var' == 1

	gen fairly_good_`var' = 0
	replace fairly_good_`var' = 1 if `var' == 2

	gen fairly_bad_`var' = 0
	replace fairly_bad_`var' = 1 if `var' == 3

	gen very_bad_`var' = 0
	replace very_bad_`var' = 1 if `var' == 4

	gen dont_know_`var' = 0
	replace dont_know_`var' = 1 if `var' == .a

	gen no_answer_`var' = 0
	replace no_answer_`var' = 1 if `var' == .b

	gen avg_score_`var' = `var'

	collapse (mean) good_`var' bad_`var' very_good_`var' fairly_good_`var' fairly_bad_`var' very_bad_`var' dont_know_`var' no_answer_`var' avg_score_`var' [w=S017], by (year country)
	tempfile political_systems_`var'_file
	save "`political_systems_`var'_file'"

	restore
	preserve
}


* Processing "x essential characteristic of democracy"
/*
           0 It is against democracy (spontaneous)
           1 Not an essential characteristic of democracy
           2 2
           3 3
           4 4
           5 5
           6 6
           7 7
           8 8
           9 9
          10 An essential characteristic of democracy
          .a Don't know
          .b No answer
          .c Not applicable
          .d Not asked in survey
          .e Missing: other
*/

foreach var in $democracy_essential_char {
	keep if `var' >= 0
	keep if `var' != .c
	keep if `var' != .d
	keep if `var' != .e

	gen not_essential_dem_agg_`var' = 0
	replace not_essential_dem_agg_`var' = 1 if `var' <= 4

	gen essential_dem_agg_`var' = 0
	replace essential_dem_agg_`var' = 1 if `var' >= 6 & `var' <= 10

	gen not_essential_dem_`var' = 0
	replace not_essential_dem_`var' = 1 if `var' == 1 | `var' == 0 //Note that I am including the spontaneous "it is against democracy" answer

	gen essential_dem_`var' = 0
	replace essential_dem_`var' = 1 if `var' == 10

	gen neutral_essential_dem_`var' = 0
	replace neutral_essential_dem_`var' = 1 if `var' == 5

	gen dont_know_`var' = 0
	replace dont_know_`var' = 1 if `var' == .a

	gen no_answer_`var' = 0
	replace no_answer_`var' = 1 if `var' == .b

	gen avg_score_`var' = `var'

	collapse (mean) not_essential_dem_agg_`var' essential_dem_agg_`var' not_essential_dem_`var' essential_dem_`var' neutral_essential_dem_`var' dont_know_`var' no_answer_`var' avg_score_`var' [w=S017], by (year country)
	tempfile democracy_essential_`var'_file
	save "`democracy_essential_`var'_file'"

	restore
	preserve
}

* Processing importance of democracy question
/*
           1 Not at all important
           2 2
           3 3
           4 4
           5 5
           6 6
           7 7
           8 8
           9 9
          10 Absolutely important
          .a Don't know
          .b No answer
          .c Not applicable
          .d Not asked in survey
          .e Missing: other
*/

keep if $democracy_importance >= 1
keep if $democracy_importance != .c
keep if $democracy_importance != .d
keep if $democracy_importance != .e


gen not_important_democracy = 0
replace not_important_democracy = 1 if $democracy_importance <= 4

gen important_democracy = 0
replace important_democracy = 1 if $democracy_importance >= 6 & $democracy_importance <= 10

gen not_at_all_important_democracy = 0
replace not_at_all_important_democracy = 1 if $democracy_importance == 1

gen absolutely_important_democracy = 0
replace absolutely_important_democracy = 1 if $democracy_importance == 10

gen neutral_important_democracy = 0
replace neutral_important_democracy = 1 if $democracy_importance == 5

gen dont_know_important_democracy = 0
replace dont_know_important_democracy = 1 if $democracy_importance == .a

gen no_answer_important_democracy = 0
replace no_answer_important_democracy = 1 if $democracy_importance == .b

gen avg_score_important_democracy = $democracy_importance

collapse (mean) not_important_democracy important_democracy not_at_all_important_democracy absolutely_important_democracy neutral_important_democracy dont_know_important_democracy no_answer_important_democracy avg_score_important_democracy [w=S017], by (year country)
tempfile democracy_important_file
save "`democracy_important_file'"

restore
preserve

* Processing democraticness in own country question
/*
           1 Not at all democratic
           2 2
           3 3
           4 4
           5 5
           6 6
           7 7
           8 8
           9 9
          10 Completely democratic
          .a Don't know
          .b No answer
          .c Not applicable
          .d Not asked in survey
          .e Missing: other
*/

keep if $democracy_democraticness >= 1
keep if $democracy_democraticness != .c
keep if $democracy_democraticness != .d
keep if $democracy_democraticness != .e


gen not_democratic = 0
replace not_democratic = 1 if $democracy_democraticness <= 4

gen democratic = 0
replace democratic = 1 if $democracy_democraticness >= 6 & $democracy_democraticness <= 10

gen not_at_all_democratic = 0
replace not_at_all_democratic = 1 if $democracy_democraticness == 1

gen completely_democratic = 0
replace completely_democratic = 1 if $democracy_democraticness == 10

gen neutral_democratic = 0
replace neutral_democratic = 1 if $democracy_democraticness == 5

gen dont_know_democratic = 0
replace dont_know_democratic = 1 if $democracy_democraticness == .a

gen no_answer_democratic = 0
replace no_answer_democratic = 1 if $democracy_democraticness == .b

gen avg_score_democratic = $democracy_democraticness

collapse (mean) not_democratic democratic not_at_all_democratic completely_democratic neutral_democratic dont_know_democratic no_answer_democratic avg_score_democratic [w=S017], by (year country)
tempfile democratic_file
save "`democratic_file'"

restore
preserve

* Processing "honest elections makes a difference in democracy" questions
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

foreach var in $democracy_elections_makes_diff {
	keep if `var' >= 1
	keep if `var' != .c
	keep if `var' != .d
	keep if `var' != .e

	gen important_`var' = 0
	replace important_`var' = 1 if `var' == 1 | `var' == 2

	gen not_important_`var' = 0
	replace not_important_`var' = 1 if `var' == 3 | `var' == 4

	gen very_important_`var' = 0
	replace very_important_`var' = 1 if `var' == 1

	gen rather_important_`var' = 0
	replace rather_important_`var' = 1 if `var' == 2

	gen not_very_important_`var' = 0
	replace not_very_important_`var' = 1 if `var' == 3

	gen not_at_all_important_`var' = 0
	replace not_at_all_important_`var' = 1 if `var' == 4

	gen dont_know_`var' = 0
	replace dont_know_`var' = 1 if `var' == .a

	gen no_answer_`var' = 0
	replace no_answer_`var' = 1 if `var' == .b

	gen avg_score_`var' = `var'

	collapse (mean) important_`var' not_important_`var' very_important_`var' rather_important_`var' not_very_important_`var' not_at_all_important_`var' dont_know_`var' no_answer_`var' avg_score_`var' [w=S017], by (year country)
	tempfile elections_difference_`var'_file
	save "`elections_difference_`var'_file'"

	restore
	preserve
}


* Combine all the saved datasets
use "`trust_file'", clear

qui merge 1:1 year country using "`trust_first_file'", nogenerate // keep(master match)
qui merge 1:1 year country using "`trust_personally_file'", nogenerate // keep(master match)
qui merge 1:1 year country using "`take_advantage_file'", nogenerate // keep(master match)

foreach var in $additional_questions {
	qui merge 1:1 year country using "`confidence_`var'_file'", nogenerate // keep(master match)
}

foreach var in $important_in_life_questions {
	qui merge 1:1 year country using "`important_in_life_`var'_file'", nogenerate // keep(master match)
}

qui merge 1:1 year country using "`interest_politics_file'", nogenerate // keep(master match)

foreach var in $rest_politics_questions {
	qui merge 1:1 year country using "`politics_`var'_file'", nogenerate // keep(master match)
}

qui merge 1:1 year country using "`environment_vs_econ_file'", nogenerate // keep(master match)

qui merge 1:1 year country using "`income_equality_file'", nogenerate // keep(master match)

foreach var in $schwartz_questions {
	qui merge 1:1 year country using "`schwartz_`var'_file'", nogenerate // keep(master match)
}

qui merge 1:1 year country using "`leisure_vs_work_file'", nogenerate // keep(master match)

foreach var in $work_questions {
	qui merge 1:1 year country using "`work_`var'_file'", nogenerate // keep(master match)
}

qui merge 1:1 year country using "`most_serious_file'", nogenerate // keep(master match)

foreach var in $justifiable_questions {
	qui merge 1:1 year country using "`justifiable_`var'_file'", nogenerate // keep(master match)
}

foreach var in $worries_questions {
	qui merge 1:1 year country using "`worries_`var'_file'", nogenerate // keep(master match)
}

foreach var in $neighbors_questions {
	qui merge 1:1 year country using "`neighbors_`var'_file'", nogenerate // keep(master match)
}

qui merge 1:1 year country using "`happiness_file'", nogenerate // keep(master match)
qui merge 1:1 year country using "`homosexual_parents_file'", nogenerate // keep(master match)
qui merge 1:1 year country using "`democracy_satisfied_file'", nogenerate // keep(master match)

foreach var in $democracy_very_good_very_bad {
	qui merge 1:1 year country using "`political_systems_`var'_file'", nogenerate // keep(master match)
}

foreach var in $democracy_essential_char {
	qui merge 1:1 year country using "`democracy_essential_`var'_file'", nogenerate // keep(master match)
}

qui merge 1:1 year country using "`democracy_important_file'", nogenerate // keep(master match)

qui merge 1:1 year country using "`democratic_file'", nogenerate // keep(master match)

qui merge 1:1 year country using "`elections_difference_`var'_file'", nogenerate // keep(master match)


* Get a list of variables excluding country and year (and avg_score_eq_ineq to not multiply it by 100)
ds country year avg_score*, not

* Multiply variables by 100 to get percentages
foreach var of varlist `r(varlist)' {
	replace `var' = `var'*100
}

* Export as csv
export delimited using "ivs.csv", datafmt replace
