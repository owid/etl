
* Define survey years (update when needed)
local survey_years 1996 1997 1998 2000 2001 2002 2003 2004 2005 2006 2007 2008 2009 2010 2011 2013 2015 2016 2017 2018 2020

**2020**

local year 2020

use Latinobarometro_2020_Eng_Stata_v1_0, clear

rename idenpa country
gen year = `year'

* Keep only "most people can be trusted" (1), "Need to be very careful" (2)
keep if p9stgbs >= 1

gen trust = 0
replace trust = 1 if p9stgbs == 1

collapse (mean) trust [w=wt], by (country year)

tempfile trust_`year'
save "`trust_`year''"

**2018**

local year 2018

use Latinobarometro_2018_Esp_Stata_v20190303, clear

rename IDENPA country
gen year = `year'

* Keep only "most people can be trusted" (1), "Need to be very careful" (2)
keep if P11STGBS >= 1

gen trust = 0
replace trust = 1 if P11STGBS == 1

collapse (mean) trust [w=WT], by (country year)

tempfile trust_`year'
save "`trust_`year''"

**2017**

local year 2017

use Latinobarometro2017Eng_v20180117, clear

rename idenpa country
gen year = `year'

* Keep only "most people can be trusted" (1), "Need to be very careful" (2)
keep if P13STGBS >= 1

gen trust = 0
replace trust = 1 if P13STGBS == 1

collapse (mean) trust [w=wt], by (country year)

tempfile trust_`year'
save "`trust_`year''"

**2016**

local year 2016

use Latinobarometro2016Eng_v20170205, clear

rename idenpa country
gen year = `year'

* Keep only "most people can be trusted" (1), "Need to be very careful" (2)
keep if P12STGBS >= 1

gen trust = 0
replace trust = 1 if P12STGBS == 1

collapse (mean) trust [w=wt], by (country year)

tempfile trust_`year'
save "`trust_`year''"

**2015**

local year 2015

use Latinobarometro_2015_Eng, clear

rename idenpa country
gen year = `year'

* Keep only "most people can be trusted" (1), "Need to be very careful" (2)
keep if P15STGBS >= 1

gen trust = 0
replace trust = 1 if P15STGBS == 1

collapse (mean) trust [w=wt], by (country year)

tempfile trust_`year'
save "`trust_`year''"

**2013**

local year 2013

use Latinobarometro2013Eng, clear

rename idenpa country
gen year = `year'

* Keep only "most people can be trusted" (1), "Need to be very careful" (2)
keep if P29STGBS >= 1

gen trust = 0
replace trust = 1 if P29STGBS == 1

collapse (mean) trust [w=wt], by (country year)

tempfile trust_`year'
save "`trust_`year''"


**2011**

local year 2011

use Latinobarometro_2011_eng, clear

rename idenpa country
gen year = `year'

* Keep only "most people can be trusted" (1), "Need to be very careful" (2)
keep if P25ST >= 1

gen trust = 0
replace trust = 1 if P25ST == 1

collapse (mean) trust [w=wt], by (country year)

tempfile trust_`year'
save "`trust_`year''"

**2010**

local year 2010

use Latinobarometro_2010_datos_eng_v2014_06_27, clear

rename idenpa country
gen year = `year'

* Keep only "most people can be trusted" (1), "Need to be very careful" (2)
keep if P55ST >= 1

gen trust = 0
replace trust = 1 if P55ST == 1

collapse (mean) trust [w=wt], by (country year)

tempfile trust_`year'
save "`trust_`year''"

**2009**

local year 2009

use Latinobarometro_2009_datos_eng_v2014_06_27, clear

rename idenpa country
gen year = `year'

* Keep only "most people can be trusted" (1), "Need to be very careful" (2)
keep if p58st >= 1

gen trust = 0
replace trust = 1 if p58st == 1

collapse (mean) trust [w=wt], by (country year)

tempfile trust_`year'
save "`trust_`year''"

**2008**

local year 2008

use Latinobarometro_2008_datos_eng_v2014_06_27, clear

rename idenpa country
gen year = `year'

* Keep only "most people can be trusted" (1), "Need to be very careful" (2)
keep if p21wvsst >= 1

gen trust = 0
replace trust = 1 if p21wvsst == 1

collapse (mean) trust [w=wt], by (country year)

tempfile trust_`year'
save "`trust_`year''"

**2007**

local year 2007

use Latinobarometro_2007_datos_eng_v2014_06_27, clear

rename idenpa country
gen year = `year'

* Keep only "most people can be trusted" (1), "Need to be very careful" (2)
keep if p23st >= 1

gen trust = 0
replace trust = 1 if p23st == 1

collapse (mean) trust [w=wt], by (country year)

tempfile trust_`year'
save "`trust_`year''"

**2006**

local year 2006

use Latinobarometro_2006_datos_eng_v2014_06_27, clear

rename idenpa country
gen year = `year'

* Keep only "most people can be trusted" (1), "Need to be very careful" (2)
keep if p45st >= 1

gen trust = 0
replace trust = 1 if p45st == 1

collapse (mean) trust [w=wt], by (country year)

tempfile trust_`year'
save "`trust_`year''"

**2005**

local year 2005

use Latinobarometro_2005_datos_eng_v2014_06_27, clear

rename idenpa country
gen year = `year'

* Keep only "most people can be trusted" (1), "Need to be very careful" (2)
keep if p14st >= 1

gen trust = 0
replace trust = 1 if p14st == 1

collapse (mean) trust [w=wt], by (country year)

tempfile trust_`year'
save "`trust_`year''"

**2004**

local year 2004

use Latinobarometro_2004_datos_eng_v2014_06_27, clear

rename idenpa country
gen year = `year'

* Keep only "most people can be trusted" (1), "Need to be very careful" (2)
keep if p43st >= 1

gen trust = 0
replace trust = 1 if p43st == 1

collapse (mean) trust [w=wt], by (country year)

tempfile trust_`year'
save "`trust_`year''"

**2003**

local year 2003

use Latinobarometro_2003_datos_eng_v2014_06_27, clear

rename idenpa country
gen year = `year'

* Keep only "most people can be trusted" (1), "Need to be very careful" (2)
keep if p20st >= 1

gen trust = 0
replace trust = 1 if p20st == 1

collapse (mean) trust [w=wt], by (country year)

tempfile trust_`year'
save "`trust_`year''"

**2002**

local year 2002

use Latinobarometro_2002_datos_eng_v2014_06_27, clear

rename idenpa country
gen year = `year'

* Keep only "most people can be trusted" (1), "Need to be very careful" (2)
keep if p29st >= 1

gen trust = 0
replace trust = 1 if p29st == 1

collapse (mean) trust [w=wt], by (country year)

tempfile trust_`year'
save "`trust_`year''"

**2001**

local year 2001

use Latinobarometro_2001_datos_english_v2014_06_27, clear

rename idenpa country
gen year = `year'

* Keep only "most people can be trusted" (1), "Need to be very careful" (2)
keep if p42st >= 1

gen trust = 0
replace trust = 1 if p42st == 1

collapse (mean) trust [w=wt], by (country year)

tempfile trust_`year'
save "`trust_`year''"

**2000**

local year 2000

use Latinobarometro_2000_datos_eng_v2014_06_27, clear

rename idenpa country
gen year = `year'

* Keep only "most people can be trusted" (1), "Need to be very careful" (2)
keep if P17ST >= 1

gen trust = 0
replace trust = 1 if P17ST == 1

collapse (mean) trust [w=wt], by (country year)

tempfile trust_`year'
save "`trust_`year''"

**1998**

local year 1998

use Latinobarometro_1998_datos_english_v2014_06_27, clear

rename idenpa country
gen year = `year'

* Keep only "most people can be trusted" (1), "Need to be very careful" (2)
keep if sp20 >= 1

gen trust = 0
replace trust = 1 if sp20 == 1

collapse (mean) trust [w=pondera], by (country year)

tempfile trust_`year'
save "`trust_`year''"

**1997**

local year 1997

use Latinobarometro_1997_datos_english_v2014_06_27, clear

rename idenpa country
gen year = `year'

* Keep only "most people can be trusted" (1), "Need to be very careful" (2)
keep if sp21 >= 1

gen trust = 0
replace trust = 1 if sp21 == 1

collapse (mean) trust [w=wt], by (country year)

tempfile trust_`year'
save "`trust_`year''"

**1996**

local year 1996

use Latinobarometro_1996_datos_english_v2014_06_27, clear

rename pais country
gen year = `year'

* Keep only "most people can be trusted" (1), "Need to be very careful" (2)
keep if p12 >= 1

gen trust = 0
replace trust = 1 if p12 == 1

collapse (mean) trust [w=wt], by (country year)

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

* Export as csv
export delimited using "latinobarometro_trust.csv", datafmt replace
