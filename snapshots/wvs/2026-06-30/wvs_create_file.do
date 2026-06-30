/*
COMMANDS TO EXTRACT QUESTIONS FROM THE WORLD VALUES SURVEY (TIME SERIES)

This code collapses microdata from the World Values Survey Time-Series (1981-2022) and generates a csv file
with country x year response shares. It mirrors ivs_create_file.do, but the WVS Time-Series codes missing
values as NEGATIVE numbers (not Stata extended-missing), so the recode idiom differs:

    * IVS:  keep if var >= 1   (+ drop .c/.d/.e);  dont_know = .a ; no_answer = .b
    * WVS:  keep if var >= -2 & var < .            (drops -5 Missing, -4 Not asked, -3 Not applicable,
                                                     and system-missing; keeps substantive + DK + NA)
            dont_know = (var == -1) ; no_answer = (var == -2)
            avg_score = var if var >= 0            (exclude DK/NA from the mean)

INSTRUCTIONS

    1.  Download the WVS Time-Series 1981-2022 Stata file (WVS_Time_Series_1981-2022_stata_v5_0.dta) from
        https://www.worldvaluessurvey.org/WVSDocumentationWVL.jsp and keep it in the same folder as this .do.
    2.  Run this do-file in Stata. It will generate the file wvs.csv
    3.  Add snapshot. Currently the command is
            etls wvs/{date}/world_values_survey --path-to-file snapshots/wvs/{date}/wvs.csv
    4.  Delete the csv file.
*/

use "WVS_Time_Series_1981-2022_stata_v5_0", clear

* Seed questions (all WVS-only, i.e. not available in the Integrated Values Surveys)
* D066_01: Problem if women have more income than husband (5-point agree)
global women_income_question D066_01
* E069_64: Confidence: Elections (4-point)
global confidence_elections_question E069_64
* F114E: Justifiable: Terrorism as a political, ideological or religious mean (10-point)
global terrorism_question F114E

* List of questions to work with
global questions $women_income_question $confidence_elections_question $terrorism_question

* Keep wave ID, country, weight and the list of questions
keep S002VS S003 S017 $questions

* Replace wave ID with the last year of each WVS wave (for comparability across waves, mirroring IVS)
replace S002VS=1984 if S002VS==1
replace S002VS=1993 if S002VS==2
replace S002VS=1998 if S002VS==3
replace S002VS=2004 if S002VS==4
replace S002VS=2010 if S002VS==5
replace S002VS=2014 if S002VS==6
replace S002VS=2022 if S002VS==7

rename S002VS year
rename S003 country

preserve

* Processing "problem if women have more income than husband" (D066_01)
/*
           1 Strongly agree
           2 Agree
           3 Neither agree nor disagree
           4 Disagree
           5 Strongly disagree
          -1 Don't know
          -2 No answer
          -3 Not applicable / -4 Not asked / -5 Missing (excluded)
*/
keep if D066_01 >= -2 & D066_01 < .

gen agree_agg_women_income = 0
replace agree_agg_women_income = 1 if D066_01 == 1 | D066_01 == 2

gen disagree_agg_women_income = 0
replace disagree_agg_women_income = 1 if D066_01 == 4 | D066_01 == 5

gen strongly_agree_women_income = 0
replace strongly_agree_women_income = 1 if D066_01 == 1

gen agree_women_income = 0
replace agree_women_income = 1 if D066_01 == 2

gen neither_women_income = 0
replace neither_women_income = 1 if D066_01 == 3

gen disagree_women_income = 0
replace disagree_women_income = 1 if D066_01 == 4

gen strongly_disagree_women_income = 0
replace strongly_disagree_women_income = 1 if D066_01 == 5

gen dont_know_women_income = 0
replace dont_know_women_income = 1 if D066_01 == -1

gen no_answer_women_income = 0
replace no_answer_women_income = 1 if D066_01 == -2

collapse (mean) agree_agg_women_income disagree_agg_women_income strongly_agree_women_income agree_women_income neither_women_income disagree_women_income strongly_disagree_women_income dont_know_women_income no_answer_women_income [w=S017], by (year country)
tempfile women_income_file
save "`women_income_file'"

restore
preserve

* Processing "confidence: elections" (E069_64)
/*
           1 A great deal
           2 Quite a lot
           3 Not very much
           4 None at all
          -1 Don't know
          -2 No answer
*/
keep if E069_64 >= -2 & E069_64 < .

gen confidence_elections = 0
replace confidence_elections = 1 if E069_64 == 1 | E069_64 == 2

gen great_deal_elections = 0
replace great_deal_elections = 1 if E069_64 == 1

gen quite_a_lot_elections = 0
replace quite_a_lot_elections = 1 if E069_64 == 2

gen not_very_much_elections = 0
replace not_very_much_elections = 1 if E069_64 == 3

gen none_at_all_elections = 0
replace none_at_all_elections = 1 if E069_64 == 4

gen dont_know_elections = 0
replace dont_know_elections = 1 if E069_64 == -1

gen no_answer_elections = 0
replace no_answer_elections = 1 if E069_64 == -2

collapse (mean) confidence_elections great_deal_elections quite_a_lot_elections not_very_much_elections none_at_all_elections dont_know_elections no_answer_elections [w=S017], by (year country)
tempfile confidence_elections_file
save "`confidence_elections_file'"

restore
preserve

* Processing "justifiable: terrorism as a political, ideological or religious mean" (F114E)
/*
           1 Never justifiable
           ...
          10 Always justifiable
          -1 Don't know
          -2 No answer
*/
keep if F114E >= -2 & F114E < .

gen never_just_agg_terrorism = 0
replace never_just_agg_terrorism = 1 if F114E >= 1 & F114E <= 4

gen always_just_agg_terrorism = 0
replace always_just_agg_terrorism = 1 if F114E >= 7 & F114E <= 10

gen never_just_terrorism = 0
replace never_just_terrorism = 1 if F114E == 1

gen always_just_terrorism = 0
replace always_just_terrorism = 1 if F114E == 10

gen neutral_terrorism = 0
replace neutral_terrorism = 1 if F114E == 5 | F114E == 6

gen dont_know_terrorism = 0
replace dont_know_terrorism = 1 if F114E == -1

gen no_answer_terrorism = 0
replace no_answer_terrorism = 1 if F114E == -2

* Average score on the native 1-10 scale, excluding DK/NA (negative codes)
gen avg_score_terrorism = F114E if F114E >= 0

collapse (mean) never_just_agg_terrorism always_just_agg_terrorism never_just_terrorism always_just_terrorism neutral_terrorism dont_know_terrorism no_answer_terrorism avg_score_terrorism [w=S017], by (year country)
tempfile terrorism_file
save "`terrorism_file'"

restore

* Combine all the saved datasets
use "`women_income_file'", clear
qui merge 1:1 year country using "`confidence_elections_file'", nogenerate
qui merge 1:1 year country using "`terrorism_file'", nogenerate

* Get a list of variables excluding country and year (and all avg_score_* columns, which stay on their native scales and must not be multiplied by 100)
ds country year avg_score*, not

* Multiply variables by 100 to get percentages
foreach var of varlist `r(varlist)' {
    replace `var' = `var'*100
}

* Export as csv
export delimited using "wvs.csv", datafmt replace
