
* Define survey years (update when needed)
local survey_years 2022 2019 2017 2014 2011 2009

**2022**

local year 2022

use AB7_ENG_Release_Version6, clear

rename COUNTRY country
gen year = `year'

* Keep only "most people can be trusted" (1), "Need to be very careful" (2)
keep if Q103 >= 1 & Q103 <= 2

gen trust = 0
replace trust = 1 if Q103 == 1

collapse (mean) trust [w=WT], by (country year)

tempfile trust_`year'
save "`trust_`year''"

**2019**

local year 2019

use ABV_Release_Data, clear

// rename COUNTRY country
gen year = `year'

* Keep only "most people can be trusted" (1), "Need to be very careful" (2)
keep if Q103 >= 1 & Q103 <= 2

gen trust = 0
replace trust = 1 if Q103 == 1

collapse (mean) trust [w=wt], by (country year)

tempfile trust_`year'
save "`trust_`year''"

**2017**

local year 2017

use ABIV_English, clear

// rename COUNTRY country
gen year = `year'

* Keep only "most people can be trusted" (1), "Need to be very careful" (2)
keep if q103 >= 1 & q103 <= 2

gen trust = 0
replace trust = 1 if q103 == 1

collapse (mean) trust [w=wt], by (country year)

tempfile trust_`year'
save "`trust_`year''"

**2014**

local year 2014

use ABIII_English, clear

// rename COUNTRY country
gen year = `year'

* Keep only "most people can be trusted" (1), "Need to be very careful" (2)
keep if q103 >= 1 & q103 <= 2

gen trust = 0
replace trust = 1 if q103 == 1

collapse (mean) trust [w=wt], by (country year)

tempfile trust_`year'
save "`trust_`year''"

**2011**

local year 2011

use ABII_English, clear

// rename COUNTRY country
gen year = `year'

* Keep only "most people can be trusted" (1), "Need to be very careful" (2)
keep if q103 >= 1 & q103 <= 2

gen trust = 0
replace trust = 1 if q103 == 1

collapse (mean) trust [w=wt], by (country year)

tempfile trust_`year'
save "`trust_`year''"

**2009**

local year 2009

use ABI_English, clear

// rename COUNTRY country
gen year = `year'

* Keep only "most people can be trusted" (1), "Need to be very careful" (2)
keep if q204 >= 1 & q204 <= 2

gen trust = 0
replace trust = 1 if q204 == 1

collapse (mean) trust, by (country year)

tempfile trust_`year'
save "`trust_`year''"

* Combine all the saved datasets

local first_year : word 1 of `survey_years'
local survey_years: list survey_years- first_year

use "`trust_`first_year''", clear

foreach year in `survey_years' {
	qui merge 1:1 year country using "`trust_`year''", nogenerate // keep(master match)
}

* Get a list of variables excluding country and year
ds country year, not

* Multiply variables by 100 to get percentages
foreach var of varlist `r(varlist)' {
	replace `var' = `var'*100
}

* Some countries are exported as numbers. I clean them here

* NOTE: I can't replace these values, but not export them. I will change them in the ETL instead.
// label define country 17 "Saudi Arabia", modify
// label define country 22 "Yemen", modify
// label define country 2 "Palestine", modify
// label define country 3 "Algeria", modify
// label define country 4 "Morocco", modify
// label define country 6 "Lebanon", modify

* Export as csv
export delimited using "arab_barometer_trust.csv", datafmt replace
