/*
wid_population.do — extract adult and total population from WID.world.

This is the demographic companion to the main WID extraction (see
snapshots/wid/<version>/wid_indices.do): WID's population counts (npopul) for
ages 992 (adults, 20+) and 999 (all ages), population unit i (individuals),
for every country and year.

Why unit i, when the income series the main extraction pulls are 992j
(equal-split adults)? Because the population dimension only exists for
distributed series. WID's codes dictionary: "Differences in population units
only matter for distributed series. For aggregate series (or prices, exchange
rates, and population series), we use the letter 'i' (for individuals) by
default." Confirmed against the API: asking for population(i j) on npopul
returns "1 population category" and yields only npopul992i / npopul999i, while
the same request on aptinc returns both aptinc992i and aptinc992j. The `j` in
aptinc992j says how income is split among adults; it does not change how many
adults there are, so aptinc992j / npopul992i is the correct pairing.

These counts are the common demographic yardstick used by the derived
poverty_inequality steps that compare WID with PIP: WID's income series are
per adult while PIP's are per capita, so converting between the two bases and
weighting countries in global decompositions requires WID's own adult and
total population (using another source's population would leak cross-source
demographic disagreements into the comparison).

Unlike the main extraction (which takes hours), this is a single fast API
call — a few minutes at most.

To update:
1. Run this file in a local Stata installation with the `wid` package
   (ssc install wid), with the working directory set to this folder:
       /Applications/StataNow/StataSE.app/Contents/MacOS/stata-se -b do wid_population.do
   Use the full path: several Stata versions can sit side by side under
   /Applications, and a bare `stata-se` may resolve to an older one whose
   licence has expired ("Your license has expired" means the wrong install,
   not an unusable Stata). `-b` writes the output to wid_population.log rather
   than the terminal, so read that for errors.
   It writes wid_population_992_999_i.csv into this directory.
2. Create the snapshot:
       etls wid/<version>/wid_population
3. Delete the leftover CSV and log:
       rm snapshots/wid/<version>/wid_population_992_999_i.csv snapshots/wid/<version>/wid_population.log
*/

clear all
set more off

* npopul, ages 992 (adults, 20+) and 999 (all ages), population unit i
* (individuals), all countries, all years.
wid, indicators(npopul) areas(_all) ages(992 999) population(i) clear

* Raw passthrough: export the response exactly as the wid command returns it
* (country / variable / percentile / year / value). The reshape to one column
* per variable and the descriptive column names happen in the meadow step.
export delimited using "wid_population_992_999_i.csv", replace delim(",")
