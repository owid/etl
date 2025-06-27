use Integrated_values_surveys_1981-2022, clear

global democracy_questions E111_01 E115 E116 E117 E224 E225 E226 E227 E228 E229 E233 E233A E233B E235 E236 E266

global democracy_satisfied E111_01

global democracy_very_good_bery_bad E115 E116 E117

global democracy_essential_char E224 E225 E226 E227 E228 E229 E233 E233A E233B

global democracy_importance E235

global democracy_democraticness E236

global democracy_elections_makes_diff E266

foreach var in $democracy_questions {
	di "`var'"
	tab `var'
	label list `var'
}
