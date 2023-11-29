/*
COMMANDS TO EXTRACT TRUST QUESTIONS FROM THE EUROPEAN SOCIAL SURVEY
This code collapses microdata on trust from the European Social Survey and generates a csv file.

NOTE: For now I will keep only rounds >=9, because to make the other work I need to merge the datasets with several other files

INSTRUCTIONS

	1. In the ESS Data Portal, register and download all the survey files from the Data Wizard, https://ess-search.nsd.no/CDW/RoundCountry.
	2. Extract the dta file from the zip file.
	3. Run this do-file in Stata. If it fails, check the name of the dta file in the first line of the code.
	4. The output is given in Stata's output window. Copy and paste it into a csv file, called `ess_trust.csv`.
	5. Add snapshot. The command is:
 		python snapshots/ess/{version}/ess_trust.py --path-to-file snapshots/ess/{version}/ess_trust.csv
	6. Delete csv file
	7. Run `etl ess_trust`

*/

use ESS-Data-Wizard-subset-2023-08-01, clear

global trust_questions ppltrst trstep trstlgl trstplc trstplt trstprl trstprt trstun gvimpc19 trstsci

* There is an additional question, if
* Confident that justice always prevails over injustice
global additional_questions jstprev

* Create anweight for rounds before 9
replace anweight = pspwght * pweight if essround < 9

* Create year variable to identify rounds
gen year = 0

replace year = 2002 if essround == 1
replace year = 2004 if essround == 2
replace year = 2006 if essround == 3
replace year = 2008 if essround == 4
replace year = 2010 if essround == 5
replace year = 2012 if essround == 6
replace year = 2014 if essround == 7
replace year = 2016 if essround == 8
replace year = 2018 if essround == 9
replace year = 2020 if essround == 10

* For now I will keep only rounds >=9, because to make the other work I need to merge the datasets with several other files
keep if essround >= 9

* Combine country and year
egen country_year = concat(cntry year)

* Create the macro surveys, which is the total set of country-years available
quietly levelsof country_year, local(surveys) clean

* Define first survey listed
local first_survey : word 1 of `surveys'

* Join positive trust answers
gen trust = 0
replace trust = 1 if ppltrst >=6

* Define survey
svyset psu [pweight = anweight], strata(stratum)

* Here results are survey-adjusted results and the results are printed in a comma-separated text
foreach survey in `surveys' {

	* If we are in the first country of the list print the title
	if "`survey'" == "`first_survey'" {
		di "survey,ppltrst,trstep,trstlgl,trstplc,trstplt,trstprl,trstprt,trstun,gvimpc19,trstsci"
	}

	* Running this for each trust question
	foreach question in $trust_questions {

		qui sum `question' if country_year == "`survey'"
		local n_`question' = r(N)

		if `n_`question'' > 0 {

			* Calculate values for each survey
			qui svy: proportion `question' if country_year == "`survey'"

			* Save matrix of estimates
			matrix b = e(b)

			* Save trust variable as the sum of scores >= 6
			local `question': di %3.2f ((b[1,7] + b[1,8] + b[1,9] + b[1,10] + b[1,11]) * 100)

		}

		else {

			local `question' = .
		}

	}


	* Print results by survey
	di "`survey',`ppltrst',`trstep',`trstlgl',`trstplc',`trstplt',`trstprl',`trstprt',`trstun',`gvimpc19',`trstsci'"
}




