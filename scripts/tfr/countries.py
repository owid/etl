"""The country registry.

Every country carries two independent attributes:

* ``tier`` — what the national number is built from. This is the quality ladder, and it says
  nothing about whether we could recompute it.
* ``recalculated`` — the validation level. True means the figure was rebuilt from counted births
  and women and checked against what the office publishes; False means the office's own rate was
  copied straight from the source. This says how far we could verify the number, not how good the
  number is: an incomplete registry we can decompose is still an incomplete registry.

``loader`` is None for countries with no national figure to plot.
"""

import os
import sys
import warnings

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
warnings.filterwarnings("ignore")

from argentina import argentina_tfr  # noqa: E402
from australia import australia_tfr  # noqa: E402
from canada import canada_tfr  # noqa: E402
from chile import chile_tfr  # noqa: E402
from china import china_tfr  # noqa: E402
from czechia import czechia_tfr  # noqa: E402
from ecuador import ecuador_tfr  # noqa: E402
from colombia import dane_female_pop, dane_registered_tfr  # noqa: E402
from france import france_tfr  # noqa: E402
from guatemala import guatemala_tfr  # noqa: E402
from hungary import hungary_tfr  # noqa: E402
from india import india  # noqa: E402
from peru import peru_tfr  # noqa: E402
from sri_lanka import sri_lanka_tfr  # noqa: E402
from sweden import sweden_tfr  # noqa: E402
from taiwan import taiwan_tfr  # noqa: E402
from philippines import philippines  # noqa: E402
from poland import poland_tfr  # noqa: E402
from kazakhstan import kazakhstan_tfr  # noqa: E402
from korea import korea_tfr  # noqa: E402
from malaysia import malaysia_tfr  # noqa: E402
from myanmar import myanmar_tfr  # noqa: E402
from netherlands import netherlands_tfr  # noqa: E402
from romania import romania_tfr  # noqa: E402
from published import (  # noqa: E402
    afghanistan,
    algeria,
    angola,
    azerbaijan,
    bangladesh,
    benin,
    bolivia,
    burkina_faso,
    burundi,
    cambodia,
    cameroon,
    chad,
    cote_divoire,
    cuba,
    dominican_republic,
    drc,
    ethiopia,
    ghana,
    guinea,
    haiti,
    honduras,
    indonesia,
    iraq,
    jordan,
    kenya,
    madagascar,
    malawi,
    mali,
    morocco,
    mozambique,
    nepal,
    niger,
    nigeria,
    north_korea,
    pakistan,
    papua_new_guinea,
    russia,
    rwanda,
    saudi_arabia,
    senegal,
    somalia,
    south_sudan,
    sudan,
    syria,
    tajikistan,
    tanzania,
    tunisia,
    ukraine,
    uzbekistan,
    venezuela,
    vietnam,
    yemen,
    zambia,
    zimbabwe,
)
from sources import egypt, england_wales, germany, japan, mexico, thailand, united_states  # noqa: E402
from south_africa import south_africa_tfr  # noqa: E402
from spain import spain_tfr  # noqa: E402
from turkey import turkey_tfr  # noqa: E402
from uganda import uganda_tfr  # noqa: E402

START = 2000

# tier key -> (label, color). Colors are ColorBrewer Dark2, which is qualitative — the ordering
# carries no meaning, it just keeps the six categories apart at small sizes and in both themes.
TIERS = {
    "complete": ("Complete registration", "#1B9E77"),
    "incomplete": ("Incomplete registration", "#D95F02"),
    "sample": ("Sample registration", "#7570B3"),
    "survey": ("Survey or census", "#E7298A"),
    "projection": ("Projection only", "#E6AB02"),
    "none": ("No official figure", "#666666"),
}


def colombia():
    pop = dane_female_pop()
    v = dane_registered_tfr(pop)[["year", "value"]].sort_values("year")
    span = range(int(v.year.min()), int(v.year.max()) + 1)
    return v.set_index("year").reindex(span).rename_axis("year").reset_index()


def brazil():
    import brazil as br

    return br.tfr()[["year", "value"]]


def italy():
    return pd.read_csv(os.path.join(HERE, "data", "italy_tfr.csv"))


def france():
    return france_tfr()


def C(name, src, loader, wpp_name, tier, recalculated, found, method, caveats, url=""):
    """found — what the office publishes. method — what we did with it. caveats — what to
    watch out for. Written plainly, because this is the documentation for later."""
    return dict(name=name, src=src, loader=loader, wpp_name=wpp_name, tier=tier,
                recalculated=recalculated, found=found, method=method, caveats=caveats, url=url)


COUNTRIES = [
    C("India", "Registrar General — Sample Registration System", india, "India", "sample", False,
      "The SRS Statistical Report 2024 publishes a total fertility rate for India and the bigger states, "
      "to one decimal place. Annexure table 15 gives it annually for 2019-24. There is also a chart of "
      "the 1985-2024 trend, but it is a picture with no numbers behind it.",
      "We read the India row of annexure table 15 straight out of the report's PDF. No arithmetic of our "
      "own — the number is exactly what the Registrar General states.",
      "The SRS is a sample, not full civil registration: roughly 8 million people are enumerated "
      "continuously and the rates are scaled up. India's civil registration now records almost all "
      "births, but it does not tabulate the mother's age consistently across states, so there is no "
      "registration-based national figure to use instead. One decimal place also means the series looks "
      "flatter than it is.",
      "https://censusindia.gov.in/nada/index.php/catalog/47152"),
    C("Colombia", "DANE — Estadísticas Vitales", colombia, "Colombia", "complete", True,
      "DANE publishes registered births by five-year age group of mother every year, and national "
      "population by single year of age and sex.",
      "We divided registered births in each age group by the female population of that group and summed "
      "across groups. Our figures match DANE's own published rates to within rounding.",
      "DANE also publishes a projection-based fertility rate of about 1.68 for 2024 — far above what its "
      "own registry shows. The 2025 figure is provisional and will revise up slightly, as 2024 did. "
      "The denominators are DANE's population projections, which are themselves modeled.",
      "https://www.dane.gov.co/index.php/estadisticas-por-tema/salud/nacimientos-y-defunciones"),
    C("Mexico", "INEGI — registered births, over CONAPO population", mexico, "Mexico", "complete", True,
      "INEGI's OLAP cube gives registered births by year of occurrence and mother's age group, for every "
      "registration year separately. CONAPO publishes mid-year population by sex and single age.",
      "We summed births for each year of occurrence across all registration years, then divided by "
      "CONAPO's female population and summed across age groups.",
      "We stop at 2022. Later years of occurrence are still filling up with late registrations — only "
      "about 993,000 of 2024's births had been registered by the time of this extract, against roughly "
      "1.67 million expected — so plotting them would show a collapse that is not real. CONAPO's own "
      "model-based figure is well above the registry.",
      "https://www.inegi.org.mx/programas/natalidad/"),
    C("Ukraine", "Держстат — Population 1990-2021", ukraine, "Ukraine", "complete", False,
      "Held out to 2021: the fertility rate for every year from 1990, with the age-specific rates behind "
      "it, in one downloadable workbook.",
      "We read the total column. Summing the age-specific rates and multiplying by the band width "
      "reproduces it — 1.159 against the published 1.16 for 2021.",
      "The series stops at 2021 not because publication is running late but because Держстат cannot "
      "compute a population to divide by. Its own orders trace the problem: a method for estimating "
      "population from mobile network records was approved in July 2023 and took effect in January 2024, "
      "then in October 2025 it formed an interagency working group to find additional sources, and was "
      "still changing that group's membership in December 2025. Nothing on births, fertility or population "
      "by age has appeared for any year after 2021, and its public data bank returns no observations at all "
      "for the births dataflow — the structure is defined, including a mother's age dimension, but nothing "
      "is loaded. The monthly regional releases stop dead after January 2022. Territory matters too: the "
      "fertility figures exclude Crimea and Sevastopol from 2015, and from that year they drop the whole of "
      "Donetsk and Luhansk rather than only the occupied parts — a wider exclusion than the 2014 footnote, "
      "inside the same publication.",
      "https://stat.gov.ua/uk/datasets/narodzhuvanist"),
    C("Morocco", "HCP — censuses and demographic surveys", morocco, "Morocco", "survey", False,
      "HCP publishes one long-run fertility series behind its indicator page, as a spreadsheet: 2.47 for "
      "2004, 2.19 for 2010, 2.21 for 2014 and 2.00 for 2024.",
      "We read the whole-country column of that series.",
      "The series mixes instruments. The 2004, 2014 and 2024 points come from censuses, which ask about "
      "births in the twelve months before enumeration; 2010 comes from a household survey. Fertility is only "
      "asked on the census long form, given to about 30% of households and then weighted up — a sampling "
      "extrapolation, not a demographic correction, and HCP publishes no raw-versus-corrected pair. The 2024 "
      "census figure is 1.97 exactly; the series rounds it to 2.00, and we keep the series so the line comes "
      "from one source. HCP has not published the age-specific rates behind the 2024 figure, so it cannot be "
      "rebuilt from components — only a regional report quotes the national number at all, and no national "
      "fertility volume exists yet. Birth registration is essentially complete at 99.5%, but it is reported "
      "as a coverage indicator and is not what the rate is built on. Watch out for a separate figure of 2.38 "
      "sometimes attached to 2014: that is a health ministry survey covering roughly 2015-17, not the census.",
      "https://www.hcp.ma/Naissances-et-fecondite_r554.html"),
    C("Uzbekistan", "Statistics Agency — registered births over its own population estimate", uzbekistan,
      "Uzbekistan", "complete", False,
      "The agency publishes a fertility rate annually from 2010, with urban and rural splits, through an "
      "open endpoint that needs no key. Its own metadata says the births come from the justice ministry's "
      "civil registry and the denominator is its estimate of the average number of women in each age group.",
      "We read the republic-wide row. No arithmetic of our own.",
      "Uzbekistan is the one country here whose fertility rose rather than fell — from 2.42 in 2017 to 3.445 "
      "in 2023 — and the rise is in the agency's own registration-based numbers, not just in outside "
      "estimates, and shows in both the urban and rural series. It has since eased back to 3.20 in 2025, so "
      "the peak may already have passed on the agency's own account. Rural fertility runs 0.3 to 0.5 above "
      "urban throughout. Recent years are open to revision. The agency publishes no births by age of mother "
      "and no female population on the standard five-year grid — its population brackets are administrative "
      "ones that do not split 40-49 and have no clean 15-19 — so there is nothing to recalculate from. Those "
      "tables do exist in the printed demographic yearbook, but the server hosting it times out.",
      "https://stat.uz/en/official-statistics/demography"),
    C("Saudi Arabia", "GASTAT — Population Estimates", saudi_arabia, "Saudi Arabia", "complete", False,
      "GASTAT publishes a fertility rate annually from 2011, in three parallel series: Saudis, non-Saudis, "
      "and everyone resident in the kingdom. For 2024 those are 2.7, 0.8 and 2.0.",
      "We took the whole-population series, which is the one comparable with the UN's figures. No "
      "arithmetic of our own.",
      "Getting the nationality split wrong here would be worse than any other country in this dataset: "
      "non-Saudis are 44% of the population and their fertility is a third of the Saudi figure, because the "
      "non-Saudi resident population is overwhelmingly working-age and often without families in the "
      "country. GASTAT resolves the trap for us by publishing all three. Its own methodology describes the "
      "fertility figures as assumptions inside a cohort-component projection rolled forward from the 2022 "
      "census, with the components taken from the national administrative records — so this is registration "
      "data inside a model rather than a straight count. GASTAT publishes no age-specific rates and no "
      "births by age of mother, so nothing can be rebuilt: the nearest thing, average births by mother\'s "
      "age, counts children a woman has ever had rather than births in the year, and summing that would be "
      "meaningless. One translation trap: GASTAT\'s English text repeatedly says these rates are per 1,000 "
      "women, but the numbers are children per woman.",
      "https://www.stats.gov.sa/en/statistics-tabs/-/categories/gastat-statistics/119025"),
    C("Ghana", "GSS — Demographic and Health Survey", ghana, "Ghana", "survey", False,
      "GSS runs the survey itself and publishes a fertility rate from each round: 4.4 for 2003, 4.0 for "
      "2008 and 3.9 for 2022. Its censuses give different figures again, and its 2021 census counts are "
      "published as raw tables through an open database.",
      "We read the survey rounds. Summing the 2022 report's own age-specific rates reproduces its published "
      "3.9 exactly.",
      "Ghana is the clearest example in this dataset of how much the correction matters. For the 2010 census "
      "GSS publishes both figures and names the method: 3.28 as reported, 4.57 after a relational Gompertz "
      "adjustment, and it says plainly that comparing the census against the survey before it "
      "\"clearly indicates miss-reporting of births\". For 2000 the pair is 3.99 and 5.66. For the 2021 "
      "census it has published neither — the fertility volume has still not appeared five years on, even "
      "though the underlying counts have been in its public database since 2023. Those counts imply 3.10 "
      "unadjusted, which by GSS\'s own pattern would land about a child higher once corrected, so we do not "
      "use it and neither should anyone else without that caveat. Its own survey puts birth registration at "
      "75% of under-fives, and GSS says outright that census and survey estimation are used because vital "
      "registration is unreliable. The site renders its publication list only in a browser, but an "
      "unauthenticated file API behind it lists everything; its statistics database was unreachable from "
      "here even though it answers elsewhere.",
      "https://statsghana.gov.gh/"),
    C("Madagascar", "INSTAT — Enquête Démographique et de Santé", madagascar, "Madagascar", "survey",
      False,
      "INSTAT publishes a fertility rate from each survey round — 5.2 for 2003-04, 4.8 for 2008-09 and 4.3 "
      "for 2021 — and its 2018 census gives 4.3 as well, with the births and women behind it printed as "
      "counts.",
      "We read the survey rounds. We also checked the census: dividing its own counts and summing gives "
      "4.290 against the published 4.3, and adding the 12-14 and 50-54 groups gives 4.82 against the "
      "published 4.8 for the wider age range.",
      "Madagascar is the one country here where the office computed a correction and then refused it. Its "
      "census volume applies the Brass method, finds every age group under-reporting recent births, and "
      "reports that the correction would raise fertility from 4.3 to 4.7 — then argues the gap looks more "
      "like a real recent decline than omitted births, concludes \"there will be no adjustment of the "
      "data\", and publishes the lower figure. Everywhere else that computes a correction applies it. Civil "
      "registration is not used and INSTAT says why: in a country where the system is not functional, the "
      "census is the only source for the population\'s age structure. Registration is better than in much of "
      "the region — 74% of under-fives registered, 57% holding a certificate — but still not usable for "
      "this. Two operational notes: INSTAT\'s homepage is currently compromised with injected spam links, "
      "though the report pages and PDFs are clean, and its data dashboard is down.",
      "https://www.instat.mg/autres/rgph-3"),
    C("Mali", "INSTAT — Enquête Démographique et de Santé", mali, "Mali", "survey", False,
      "INSTAT publishes a fertility rate from each of seven survey rounds since 1987, the latest 6.0 for "
      "2023-24. Its 2022 census gives 6.1, and publishes the births and women behind it as counts.",
      "We read the survey rounds. We also checked the census: dividing its own adjusted counts reproduces "
      "every one of its published age-specific rates and gives 6.09 against the published 6.1.",
      "Mali applied the largest correction in this dataset by far. Its census found the raw count of births "
      "in the previous twelve months unusable — sex ratios of up to 148 boys per 100 girls, and only about "
      "70% as many declared births as there were children under one in the same count — and states plainly "
      "that the data \"are of poor quality and require adjustment\". It names the method, the Trussell "
      "variant of the Brass technique, and says which alternatives it tested and rejected. The effect is "
      "close to a doubling: 494,742 declared births become 930,503 adjusted ones, about 88% more. INSTAT "
      "does not print the two totals side by side, so that comparison is ours from its own tables. Civil "
      "registration is not used: INSTAT says between 40 and 60% of births go unregistered, even though 83% "
      "of people eventually hold a certificate. Its statistical database at pxweb.instat.ml accepts "
      "connections then drops them without serving a certificate at all.",
      "https://www.instat-mali.org/fr/publications/enquete-demographique-et-de-sante-eds"),
    C("Malaysia", "DOSM — age-specific fertility rates over its population estimates", malaysia_tfr,
      "Malaysia", "complete", False,
      "DOSM publishes age-specific fertility rates and its own total, annually from 1958, plus population "
      "by five-year age group and sex — all as parquet from an open store with no key. It computes the "
      "rates from the age of mother recorded at birth registration, but does not release those counts.",
      "We took DOSM's published total. The age-band comparison multiplies each rate by the female "
      "population, which is how DOSM built the rate in the first place; that implies 453,911 births to "
      "mothers aged 15-49 in 2023 against the 455,761 registered in total, and reproduces the published "
      "1.73.",
      "Malaysia is the mirror image of the Japan trap, and getting it backwards would bias everything: "
      "DOSM's rate counts all residents including non-citizens, on both sides of the division, where "
      "Japan's counts nationals only. Non-citizen women are about a tenth of some childbearing age groups — "
      "131,000 of the 1.3 million aged 25-29 in 2023. A birth is counted if it is registered in Malaysia, so "
      "a foreign resident who registers a child at home instead drops out. The 2024 rates are rounded to "
      "whole numbers where earlier years carry two decimals, which reads as a year not yet final, though "
      "DOSM does not flag it. Population for 2011-19 is still on the 2010 census basis and DOSM says it will "
      "be revised; 2024 onward are projections. The old DOSM site is a shell now — the open data store is "
      "the working source.",
      "https://open.dosm.gov.my/data-catalogue/fertility"),
    C("Mozambique", "INE — Inquérito Demográfico e de Saúde", mozambique, "Mozambique", "survey", False,
      "INE publishes a fertility rate for each survey round, and its 2022-23 report sets all four side by "
      "side: 5.2 in 1997, 5.5 in 2003, 5.9 in 2011 and 4.9 in 2022-23. The 2017 census publishes the raw "
      "material — births in the twelve months before enumeration by mother's age, and women by age — but no "
      "fertility rate computed from it.",
      "We read the trend table. Dividing the census's own counts gives 4.18 for 2017, which sits below both "
      "the survey before it and the survey after, so we do not use it.",
      "That INE publishes no fertility rate from its own census is itself the finding. The 4.18 the census "
      "counts imply is the familiar signature of a twelve-month recall question undercounting births, but "
      "unlike Tanzania, Uganda or Angola, INE names no correction anywhere in the results volume — and the "
      "one folder that might hold an adjusted figure renders empty without JavaScript, so we cannot rule out "
      "that a published version exists. Mozambican civil registration cannot be used at all, and is getting "
      "worse rather than better: the share of under-fives registered fell from 48% in 2011 to 31% in "
      "2022-23. INE's site serves an incomplete certificate chain, and its catalogue prepends a stray byte "
      "to every download, which breaks PDF and spreadsheet readers until it is stripped.",
      "https://www.ine.gov.mz/"),
    C("Poland", "GUS — births and population by single year of age", poland_tfr, "Poland",
      "complete", True,
      "GUS publishes live births by single year of the mother's age, population by single year of age and "
      "sex twice a year, and its own fertility rate — all through one open API with no key.",
      "We divided the births at each single age by the women of that age at 30 June and summed. That is the "
      "population GUS says it divides by, and the result reproduces its own published rate to three "
      "decimals: 1.1576 against 1.158 for 2023, 1.0987 against 1.099 for 2024, 1.0675 against 1.068 for "
      "2025.",
      "Poland's rate is now among the lowest anywhere, and still falling. The series starts in 2013 because "
      "that is where the mid-year population by single age begins; births reach back to 2002. GUS counts "
      "everyone registered or actually resident for three months or more, which as a matter of law takes in "
      "Ukrainian nationals given temporary registration after 2022 — so part of the recent fall is the "
      "denominator growing. That affects GUS's own figure exactly as much as ours, and GUS does not quantify "
      "it. Nothing is flagged provisional, even 2025. One trap: the summary table in the printed yearbook "
      "reports births in thousands while the API returns raw counts.",
      "https://bdl.stat.gov.pl/"),
    C("Peru", "INEI — registered births over its population estimates", peru_tfr, "Peru", "complete",
      True,
      "INEI publishes registered births by age group of mother as spreadsheets, and female population by "
      "age group. It also publishes two fertility rates of its own, neither of them from the registry: 1.8 "
      "for 2023 from its continuous household survey, and 2.2 as the assumption inside its population "
      "projection for 2020-25.",
      "We divided the births by the female population in each age group and summed, giving 1.84 for 2022, "
      "1.69 for 2023 and 1.51 for 2024.",
      "Peru follows the Colombia and Mexico pattern: the registry shows a much steeper fall than the "
      "official figure. Our 1.69 for 2023 sits 0.5 below the projection assumption, which is a 2019 vintage "
      "predating both the pandemic and the decline since. INEI\'s own text already expects a real fall, "
      "attributing it to women postponing or forgoing motherhood rather than to a data problem. Births are "
      "counted by the year they were registered, not the year they occurred, though Peru\'s deadline is 60 "
      "to 90 days so the two are close. Coverage is high and INEI states it: 97 to 99% of registrations come "
      "through the electronic birth certificate, and late registration was under 3% in 2023 — but it rose "
      "again in 2024, so that year is the least final. The 2020 collapse in registrations was lockdown "
      "closing registry offices, which INEI says outright, so it is not a fertility signal; our series "
      "starts after it. Each annual annex sits at its own unrelated file id, with no pattern to follow.",
      "https://www.gob.pe/institucion/inei/informes-publicaciones"),
    C("Philippines", "PSA — OpenSTAT registered live births", philippines, "Philippines", "incomplete", True,
      "PSA's OpenSTAT database gives registered live births by age group of mother, one table per year, "
      "and a projected population by five-year age group and sex.",
      "We divided registered births by the projected female population in each age group and summed. "
      "The population table is published in thousands, which we scaled up.",
      "Registration covers roughly 90% of births and is tabulated by year of registration, so this "
      "understates the real rate — our 1.49 for 2024 against PSA's own survey-based figure of about "
      "1.9. Only two years, because OpenSTAT publishes a separate table per year.",
      "https://openstat.psa.gov.ph/"),
    C("Egypt", "CAPMAS — Annual Bulletin of Births and Deaths", egypt, "Egypt", "complete", False,
      "Each annual bulletin has a table 13 giving age-specific fertility rates, the female population of "
      "each age group, and CAPMAS's own total fertility rate. Table 12 separately gives registered live "
      "births by age of mother, as counts.",
      "We summed the age-specific rates and multiplied by the five-year band width, which reproduces "
      "CAPMAS's own published total exactly. The age-band comparison uses the registered births from table "
      "12 rather than the rates, because those are the only figures the registry itself produces.",
      "CAPMAS's published rates are not its registered births divided by its own population. The bulletin "
      "says so plainly: they are estimated with the fertility module of the Population Analysis "
      "Spreadsheets, a US Census Bureau tool that takes the crude birth rate and the female population and "
      "imposes a model age pattern. Dividing the registry's own counts instead gives 2.38 for 2024 against "
      "the published 2.41 — close in total, but with a very different shape, 143 births per thousand women "
      "aged 25-29 where CAPMAS publishes 164. That means Egypt's line is closer in kind to a UN estimate "
      "than to a pure count. The fertility table only appears from the 2019 edition onward and the counts "
      "by age of mother only from 2021, so the series is short; CAPMAS also revised its 2015 figure from "
      "3.7 down to 3.3 between editions without explanation. One band is labeled 40-45 where it means "
      "40-44. CAPMAS's site is a JavaScript application, but the catalog behind it answers plain requests "
      "and every publication file downloads directly.",
      "https://www.capmas.gov.eg/"),
    C("Brazil", "IBGE — Estatísticas do Registro Civil", brazil, "Brazil", "complete", True,
      "IBGE's SIDRA database gives registered births by mother's age group, and population projections "
      "by sex and single age. Both have a public API.",
      "We divided births by the female population in each age group and summed. Births come from SIDRA "
      "table 197 for 2000-02 and table 2612 for 2003 onward.",
      "The 2000-02 points come from the older table and sit too low, because birth registration coverage "
      "was still improving — the step up in 2003 is coverage, not fertility. The 2024 figure is "
      "provisional. IBGE's own published fertility rate comes from projections, not the registry.",
      "https://sidra.ibge.gov.br/pesquisa/registro-civil/tabelas"),
    C("England and Wales", "ONS — birth registrations over the mid-year population", england_wales,
      "United Kingdom", "complete", False,
      "ONS table 10 gives live births and age-specific fertility rates by age group of mother, back to 1938. "
      "ONS also publishes mid-year population by single year of age and sex for the United Kingdom and each "
      "of its nations, 2011 to 2024, in one spreadsheet.",
      "We summed the age-specific rates and multiplied by the band width. The age-band comparison divides "
      "the births by ONS's own mid-year female population, which is what ONS itself divides by.",
      "This is England and Wales, about 89% of UK births, while the UN figures are UK-wide. We measured how "
      "much that matters: ONS's own reference table gives both, and England and Wales sits a steady 0.02 "
      "above the UK — 1.55 against 1.53 in 2021, 1.58 against 1.56 in 2020, 1.65 against 1.63 in 2019. That "
      "is far smaller than the gaps this page is about, so the shorter, more current England and Wales "
      "series is used rather than the UK one, which ONS stopped publishing after 2021. Building a UK figure "
      "ourselves would mean stitching in Scotland and Northern Ireland; Scotland's office publishes what is "
      "needed, but Northern Ireland's refuses scripted downloads outright, so it would not be reproducible. "
      "The 2025 figure is provisional and ONS calculates it against population projections rather than "
      "estimates, so the age-band comparison for that year falls back to the population its own rate "
      "implies. Table 10 publishes rates on both a 15-44 and a 15-49 base; the 15-49 column is empty for "
      "most recent years, so the 15-44 one is used throughout.",
      "https://www.ons.gov.uk/peoplepopulationandcommunity/birthsdeathsandmarriages/livebirths"),
    C("Germany", "Destatis — Geburtenstatistik over the average resident population", germany, "Germany",
      "complete", False,
      "Destatis table 12612-0008 gives live births per 1,000 women for every single year of age 15-49, from "
      "1972 to 2025. Table 12612-0005 gives the births themselves by age of mother and birth order, from "
      "2009. Table 12411-10, in the annual population report, gives the average population over the year by "
      "single year of age.",
      "We summed the rates across ages. Because they are single-year rates there is no band width to "
      "multiply by. The age-band comparison divides the births by the population from 12411-10, which is "
      "the concept Destatis itself uses — the mean of the stocks at the start and end of the year.",
      "Two traps we checked and got right. The population table has an all-residents column and a "
      "German-nationals column; the nationals column is 20-30% smaller and using it would push the rate far "
      "too high, so the all-residents column is the one that fits. And the births tables date a mother's age "
      "by subtracting birth years rather than by exact age on a reference date, while the population tables "
      "use exact age — mixing the two biases a recalculation by about 1%, which is why the line stays "
      "Destatis's own summed rates. Rebased on the 2022 census from 2012 onward, so numbers differ slightly "
      "from releases published before the rebasing — our 2023 comes out at 1.385 where the original release "
      "said 1.35. The 2025 figure is provisional. The age-band comparison only starts in 2009, because that "
      "is where the births table begins, and the average-population table ships one year per report edition. "
      "Neither births file could be pulled through the GENESIS API — the access token we have authenticates "
      "as the guest account, which cannot download tables — so both were exported by hand from the web "
      "interface; the population report, by contrast, downloads as a plain spreadsheet with no token at all.",
      "https://www-genesis.destatis.de/genesis/online"),
    C("Thailand", "NSO — Statistical Yearbook tables 1.10 and 1.4", thailand, "Thailand", "complete", True,
      "The Statistical Yearbook has table 1.10, registered live births by age group of mother, and table "
      "1.4, registered population by age group and sex.",
      "We divided births by the female population in each age group and summed. Both tables were read "
      "out of the yearbook PDFs.",
      "Each yearbook edition prints only a rolling three-year window, and the two tables cover different "
      "windows, so only the years where they overlap can be computed. Earlier editions would extend the "
      "series back to about 2017.",
      "https://www.nso.go.th/"),
    C("France", "INSEE — état civil over the mean resident population", france, "France", "complete", False,
      "INSEE publishes fertility rates by single year of age of mother and, separately, births by single year "
      "of age, both from the civil register. Its population pyramid gives women by single year of age at 1 "
      "January, for the whole country from 1991 and for mainland France from 1901.",
      "We summed the rates across ages. The age-band comparison divides the births by the mean population "
      "over the year — the average of the 1 January figures for that year and the next — because that is how "
      "INSEE defines a fertility rate: births to women of an age over \"la population moyenne de l'année des "
      "femmes de même âge\".",
      "Excludes Mayotte before 2014 and includes it after, a small discontinuity we have not corrected. INSEE "
      "does publish a mainland-only series with no such break, going back to 1901, but mainland France is not "
      "the territory the UN figures cover, so switching would trade one mismatch for another. Population "
      "estimates for a given year get revised in later releases — about 1% for one cohort we checked — so "
      "rates and population have to come from the same vintage. The 2025 figure is provisional. The single "
      "year of age population is not in INSEE's documented API, which only carries five-year bands; it comes "
      "from the data file behind INSEE\'s own interactive population pyramid, which answers plain requests but "
      "is not a published interface, so the path could change.",
      "https://www.insee.fr/fr/statistiques/8999017"),
    C("Japan", "MHLW — Vital Statistics via e-Stat", japan, "Japan", "complete", False,
      "e-Stat table 0003411608 gives the total fertility rate and each age group's contribution to it; "
      "table 0003411607 gives births by the same age groups. The Statistics Bureau separately publishes "
      "population by single year of age and sex every October, as a spreadsheet download.",
      "We took MHLW's published total fertility rate directly. For the age-band comparison we divided "
      "MHLW's births by the Statistics Bureau's female population, which is where MHLW says its own "
      "denominator comes from. That gives 1.145 for 2024 against the published 1.15.",
      "Five-yearly before 2000 and annual after, so the early part of the line is coarse. e-Stat "
      "publishes each band as its contribution to the total rather than as a rate per 1,000 women — the "
      "seven bands sum exactly to the published total, so do not multiply those by five. MHLW's rate "
      "counts Japanese women only, not all residents, and the population file gives both columns; using "
      "the wrong one inflates the denominator by the foreign resident population. Births are published "
      "only in five-year bands, so a full recalculation would still approximate. The e-Stat API needs a "
      "key, but registration is email-only and the spreadsheets download without one.",
      "https://www.e-stat.go.jp/"),
    C("Italy", "ISTAT — ANPR / stato civile", italy, "Italy", "complete", False,
      "ISTAT's SDMX service publishes age-specific fertility rates by single year of age of mother, from "
      "2000 onward.",
      "We summed the rates across single ages.",
      "We could not get births or female population by age: ISTAT's population dataflow covers every "
      "municipality and times out when requested unfiltered, so there is no age-band comparison for "
      "Italy yet. Sourced from the national resident register (ANPR).",
      "https://esploradati.istat.it/"),
    C("United States", "CDC / NCHS — natality via data.cdc.gov", united_states, "United States",
      "complete", False,
      "NCHS publishes age-specific birth rates per 1,000 women by maternal age group in two datasets: a "
      "historical one running 1940 to 2018, and a current one covering 2016 to 2024. It also publishes its "
      "own total fertility rate in the annual Births report.",
      "We summed the rates and multiplied by the band width, which is exactly how NCHS builds its own "
      "total. The two datasets are stitched at 2016, where they overlap. Our figures reproduce NCHS's "
      "published totals to the decimal — 1.621 for 2023, 2.056 for 2000.",
      "The current dataset splits 15-19 into 15-17 and 18-19 as well as the whole band, which would "
      "double-count if not filtered, and labels the top band 45-54 where the rate is per woman aged 45-49. "
      "Births by age of mother as counts do exist, in the annual Births report and in the record-level "
      "natality files, so a recalculation against Census population is possible — but which Census vintage "
      "you divide by moves the answer by about 1.4%, so it would no longer match NCHS. The CDC website "
      "itself refuses automated requests; these two datasets come from a separate host that does not.",
      "https://data.cdc.gov/"),
    C("Russia", "Rosstat — Demographic Yearbook of Russia", russia, "Russia", "complete", False,
      "The yearbook is eight chapter spreadsheets behind an HTML index. Chapter 2 gives the total fertility "
      "rate for the whole country back to the early 1960s; chapter 4 gives live births by age of mother as "
      "counts. A separate bulletin gives female population by single year of age at 1 January.",
      "We read the national column of the fertility-rate sheet. Dividing the births by the population gives "
      "1.39 for 2022 against Rosstat's published 1.416, so the age-band comparison uses those counts while "
      "the line stays Rosstat's own figure.",
      "The 1.8% gap is the denominator: Rosstat divides by the average population over the year, and the "
      "only population file on the same census basis is the one for 1 January, which is slightly larger. "
      "There is also a rebasing trap — Rosstat did not rebase its published rates for 2011-21 on the 2020 "
      "census, but the population files are all rebased, so only 2022 is internally consistent. The 2022 "
      "figure excludes the four annexed Ukrainian regions; Crimea has been included since 2014. Rosstat's "
      "site serves a broken certificate chain, so downloads need certificate checking relaxed, and its "
      "unified statistics database refuses requests from outside the country altogether. The series stops "
      "at 2022 because that is the latest yearbook edition.",
      "https://rosstat.gov.ru/folder/12781"),
    C("Zambia", "ZamStats — Demographic and Health Survey", zambia, "Zambia", "survey", False,
      "ZamStats publishes a fertility rate from each survey round, the latest 4.0 for 2024. Its 2022 census "
      "reports 4.6, and publishes the raw births and women behind it in a separate tables volume.",
      "We read the survey rounds. We also computed the census's raw figure from its own counts: 3.35, against "
      "the 4.6 it publishes.",
      "That is a 37% upward correction, and ZamStats names the method — the P/F ratio technique, after "
      "checking reported children ever born against a Brass formula — and says plainly that fertility "
      "measurement relies on indirect estimation because vital registration is underdeveloped. Only 31% of "
      "people under 50 have a registered birth. But it publishes neither the age-specific multipliers nor the "
      "adjusted rates, so the 4.6 cannot be rebuilt from anything public; and its own worked example of the "
      "Brass check does not reproduce from the inputs it quotes — we get 5.66 where the report says 5.921. "
      "The census and the survey also disagree in an informative way: 4.6 for 2022 against 4.0 for 2024, a "
      "0.6 fall in two years where the survey series had been falling about 0.12 a year. The raw counts and "
      "the adjusted figure live in two separate documents, which is worth knowing before citing either.",
      "https://www.zamstats.gov.zm/"),
    C("Vietnam", "National Statistics Office — population change and family planning survey", vietnam,
      "Vietnam", "survey", False,
      "The office publishes a total fertility rate annually from 2001. Its PxWeb database stops at 2023, but "
      "the 2025 survey report prints the whole 2001-2025 series in one table. The report and the 2019 census "
      "volume also publish births by age of mother and population by age group, though only inside long PDFs.",
      "We read the whole-country column of that table. No arithmetic of our own.",
      "This is not a count of registered births. The office estimates the rate from a household sample survey "
      "and then adjusts it upward with the Trussell P/F technique, because women under-report births in the "
      "previous twelve months. Its own report says so but never prints the adjustment. Dividing the 2019 "
      "census's own counts gives 1.85 against the published 2.09, so the correction that year was about 13% — "
      "which makes Vietnam's figure closer in kind to a model estimate than to a count. Doing the same on the "
      "2025 survey tables implies a far larger correction, but the survey's birth table counts mothers rather "
      "than births and may not be scaled the same way as its population table, so we do not trust that "
      "comparison and have not used it. Some years come from a different instrument: 2024 is from the "
      "mid-term population and housing survey, not the annual one. Vietnam does have civil registration, but "
      "it is not what the published rate is built on, and neither PxWeb nor the statistical yearbook carries "
      "any table of births or population by age at all.",
      "https://www.nso.gov.vn/"),
    C("Australia", "ABS — births by age of mother over its population estimates", australia_tfr,
      "Australia", "complete", True,
      "ABS publishes registered births by single year of the mother's age through a data service that needs "
      "no key, and female population by single year of age at 30 June in its population release. It also "
      "publishes its own fertility rate.",
      "We divided the births at each single age by the women of that age and summed. Our 1.482 for 2024 "
      "matches ABS's published 1.481, and 1.497 against its 1.499 for 2023 — the small gap is the population "
      "having been revised since ABS calculated that year's rate.",
      "Australia's rate is at a record low. The one caveat that matters is registration timing: ABS counts "
      "births by the year they were registered, and two states have had backlogs. Victoria registered nearly "
      "a quarter of its 2024 births as having occurred in an earlier year, and clearing a 2023 processing "
      "delay pushed its 2024 registrations up 12.9% against a national rise of 1.9% — ABS says outright that "
      "much of that is administrative rather than real, and warns against comparing years. Western Australia "
      "cleared a backlog in early 2025 that also lands in 2024. ABS does publish an occurrence-year series, "
      "but it is far too incomplete to use for recent years: it has only 248,159 births for 2024 against "
      "292,318 on the registration basis. Births by single year of age are no longer in the downloadable "
      "data cubes — that table exists only through the data service now — and the population file\'s address "
      "carries the release month, so it moves every edition.",
      "https://www.abs.gov.au/statistics/people/population/births-australia/latest-release"),
    C("Bangladesh", "BBS — Sample Vital Registration System", bangladesh, "Bangladesh", "sample", False,
      "The 2023 SVRS report carries a table of every year from 1982 to 2023, giving the total fertility "
      "rate alongside four other fertility measures. Each annual edition also states its own year's figure "
      "in the text, with a confidence interval.",
      "We read the whole 42-year table out of the report. We also summed the age-specific rates for 2023 "
      "and got 2.18 against the printed 2.175, which is what the rounding of those rates allows.",
      "The SVRS is a sample of about 2,000 areas where resident registrars record births monthly — a "
      "continuous sample system, not full civil registration. The health survey run by the government's "
      "population institute puts the rate about 0.2 higher, because it asks women to recall past births "
      "and averages three years, so the two are not measuring quite the same thing. BBS publishes no "
      "birth or population counts by age, only rates, so there is nothing to recalculate from. Everything "
      "is PDF only, served from a cloud bucket behind the BBS site.",
      "https://bbs.gov.bd/"),
    C("Indonesia", "BPS — censuses and inter-censal surveys", indonesia, "Indonesia", "survey", False,
      "BPS publishes a total fertility rate for census and inter-censal survey rounds only. Its 2025 survey "
      "release charts all four rounds together: 2.41 for the 2010 census, 2.28 for the 2015 survey, 2.18 "
      "for the long form of the 2020 census, and 2.13 for the 2025 survey.",
      "We read all four values out of that chart. An earlier version of this page had 2.42 for the 2020 "
      "long form, which is wrong — BPS's own figure is 2.18, and 2.41 belongs to the 2010 census.",
      "BPS says plainly that Indonesian civil registration coverage is still incomplete and cannot be used "
      "for this, so the rate comes from retrospective questions about children ever born. Each round is "
      "placed at the year BPS names it for; the 2020 long form was actually fielded in 2022. BPS's main "
      "website refuses automated requests, so this release had to be read through a web archive; the "
      "separate census microsite does answer, but its fertility tables hold children ever born over a "
      "lifetime, not one year's births, so there is nothing to recalculate from.",
      "https://www.bps.go.id/id/pressrelease/2026/05/05/2645/"),
    C("Pakistan", "PBS — Pakistan Demographic Survey", pakistan, "Pakistan", "survey", False,
      "The Bureau of Statistics ran the Demographic Survey annually until 2007, then again in 2020. Every "
      "edition still online prints a total fertility rate: 4.1 in 2001 falling to 3.7 in 2007, and 3.7 again "
      "in 2020. The 2007 round also publishes live births and female population by age group as counts.",
      "We read each edition's own figure out of its report. For 2007 we also did the arithmetic from the "
      "counts: births divided by women in each age group, summed and multiplied by the band width, gives "
      "3.69 — the Bureau's published 3.7.",
      "Pakistan has no usable civil registration for this. The Bureau's own report explains it revived the "
      "survey after a thirteen-year gap because the national database authority told them vital-event "
      "records were not good enough — hence the empty stretch from 2008 to 2019. The 2020 figure covers "
      "2018-20 rather than one year. The 2020 report has a trap: a table titled live births by age of "
      "mother holds children ever born, not one year's births, and totals over 100 million. The health "
      "survey run by the National Institute of Population Studies gives 3.6 for 2017-18.",
      "https://www.pbs.gov.pk/pds/"),
    C("China", "NBS — census yearbooks, table on fertility by age and birth order", china_tfr, "China",
      "survey", True,
      "For each of the last three censuses the statistics bureau publishes a downloadable table giving, for "
      "every single year of age from 15 to 49, the number of women of childbearing age, the number of "
      "births they had in the year before the count, and the resulting rate. The bureau's director "
      "separately stated a fertility rate of 1.3 for 2020 at the census press conference.",
      "We divided births by women at each single age and summed. This gives 1.221 for 2000, 1.188 for 2010 "
      "and 1.296 for 2020. The bureau's own companion table computes 1.301 for 2020 from five-year bands, "
      "and the press conference rounded that to 1.3.",
      "Only census years. Between censuses the bureau relies on an annual sample survey of about one person "
      "in a thousand, whose age detail appears in a yearbook it does not publish free of charge — so there "
      "is no way to fill the gaps, and the line jumps ten years at a time. The counts are the census long "
      "form's sample, not the whole population, and the reference period is the twelve months to 31 October "
      "of the census year rather than the calendar year. Chinese demographers argued the 2010 census figure "
      "of 1.18 was implausibly low because births went unreported. The bureau's queryable data portal "
      "refuses requests from outside the country, but these static files download without trouble.",
      "https://www.stats.gov.cn/sj/pcsj/rkpc/7rp/zk/html/B0603.xls"),
    C("Nepal", "National Statistics Office — 2021 census", nepal, "Nepal", "survey", False,
      "The census publishes a fertility rate of 1.94 for 2021, and a series back to 1971. It also publishes, "
      "through an open census API, the counts behind it: births in the twelve months before enumeration by "
      "the mother\'s age group, and women by age group.",
      "We show the office\'s published figure. But dividing its own counts gives 1.556, not 1.94 — a quarter "
      "lower — and the same counts reproduce every other fertility measure it prints: a general fertility "
      "rate of 50.16 against its 50.2, and a gross reproduction rate of 0.734 against its 0.73. Had the "
      "fertility rate really been 1.94, that gross reproduction rate would be about 0.92.",
      "So the published 1.94 does not follow from the office\'s own numbers, and no method is named anywhere "
      "in the report that would explain the difference — we searched for every correction technique other "
      "offices use, in English and Nepali, and found none. This is not the documented raw-versus-adjusted "
      "split seen in Tanzania, Uganda, Ghana or Cameroon, nor the computed-then-refused correction of "
      "Madagascar and Niger: it is a figure that contradicts the rest of its own table. We plot what the "
      "office publishes and let the age-band comparison below show the counts, but the 1.94 should not be "
      "relied on. Civil registration is not used for fertility; the census reports 74% of children under six "
      "as registered. The office\'s reports are served inside a JavaScript flipbook with no download link — "
      "the PDF path sits in a script variable — and its old domain now returns a bare 403.",
      "https://censusresults.nsonepal.gov.np/fertility"),
    C("Venezuela", "INE — projection-based fertility series", venezuela, "Venezuela", "projection", False,
      "INE publishes a fertility series in a statistics summary dated August 2024: 2.9 for 2000 falling to "
      "2.3 for 2015, then 2.2 and 2.1 drawn as forward projections. The health ministry separately publishes "
      "registered births by age of mother, but only up to 2014.",
      "We take the observed points up to 2015 and leave the projected ones out. Pairing the ministry\'s 2014 "
      "births against INE\'s population gives about 2.25, close to INE\'s 2.3 for 2015.",
      "Every point in this series comes out of a projection exercise built on the 2011 census and calculated "
      "in 2013 — nothing has been re-estimated from current registration. The last non-projected point is "
      "2015, published nine years later, and no national fertility figure exists for any year after that. "
      "The general statistical yearbook stops at 2003; INE has been digitising volumes from 1909 to 1944 "
      "instead of adding recent ones. The health ministry\'s births-by-age tables stop at 2014, published in "
      "2018. One trap for anyone retracing this: the domain everyone cites, ine.gov.ve, no longer resolves — "
      "the record was removed from the Venezuelan registry, which still answers authoritatively for other "
      "names. The institute moved to ine.gob.ve and is publishing there.",
      "https://ine.gob.ve/"),
    C("Niger", "INS — national fertility and health surveys", niger, "Niger", "survey", False,
      "INS publishes a fertility rate from each survey round: 7.1 for 2006, 7.6 for 2012 and 6.2 for its "
      "2021 fertility survey. Its 2012 census gives 7.5, with the births and women behind it printed as "
      "counts.",
      "We read the survey rounds. We also checked the census: dividing its own counts gives 7.476 against "
      "the published 7.5.",
      "Niger has the highest fertility in the world, and its 2012 census is the second case here — after "
      "Madagascar — where the office computed a correction and refused it. The relational Gompertz method "
      "would have raised the figure from 7.5 to 7.8, but the report found the model a poor fit and decided "
      "to \"favour the field data, which are relatively more accurate than extreme hypothetical estimates\", "
      "so the published number is the lower one. INS also flags that its own census and its own survey "
      "disagree for the same year, 7.5 against 7.6, and attributes the gap to under-declaration in the "
      "census\'s twelve-month window. Civil registration is not used nationally; INS does compute a rate "
      "from registrations for Niamey alone, where completeness is 69% — and it comes out above the survey "
      "figure for the same city, 4.8 against 4.2. The fifth census is still in its pilot phase as of early "
      "2026, more than thirteen years after the fourth. One INS chart gives 7.0 and 7.2 for 1992 and 1998 "
      "where its own primary tables give 7.4 and 7.5; the tables are right.",
      "https://stat-niger.org/"),
    C("North Korea", "Central Bureau of Statistics — 2008 census and 2014 survey", north_korea,
      "North Korea", "survey", False,
      "The bureau's 2008 census reports 2.01 and prints the births and women behind it as counts. Its 2014 "
      "socio-economic and health survey reports 1.89. A 2017 household survey gives 1.9, but its title page "
      "names UNICEF as publisher, so the authorship is shared.",
      "We read both figures. We also checked the census: dividing its own counts gives 2.008 against the "
      "published 2.01.",
      "North Korea is here rather than on the no-figure list, and the reason is worth stating. Every one of "
      "these documents is the bureau's own work — the census foreword is signed by its director-general and "
      "credits the UN population fund only for material and technical support — but not one of them is hosted "
      "on a North Korean server. The country has no statistics website at all: its two public domains resolve "
      "but drop every connection before answering, the same pattern as Iran, and neither has ever carried a "
      "statistics section. So the reports survive only on UN mirrors and in archives. We judge authorship, "
      "not hosting, which is why these count and a UN estimate would not. The planned 2018 census was "
      "cancelled, so nothing newer than 2017 exists. Two loose ends: the bureau\'s own publications give "
      "1993 as both 2.1 and 2.20, and the only analysis of that census available anywhere is the US Census "
      "Bureau\'s, which is not a North Korean source and is not used here.",
      "https://dprkorea.un.org/en"),
    C("Nigeria", "National Population Commission — Nigeria Demographic and Health Survey", nigeria,
      "Nigeria", "survey", False,
      "The 2024 survey report sets every round since 2003 side by side in one table: 5.7 in 2003 and 2008, "
      "then 5.5, 5.3 and 4.8. The Bureau of Statistics separately publishes a row called Calculated TFR, "
      "and its own 2021 household survey found 4.6.",
      "We read the trend table out of the survey report. Summing its age-specific rates and multiplying by "
      "the band width gives 4.79, the published 4.8.",
      "These are measured rates from women's birth histories, not registration — birth registration in "
      "Nigeria is far too incomplete to use. Each round's figure covers the three years before its "
      "fieldwork, so the years are approximate. We do not use the Bureau's Calculated TFR row: the "
      "Population Commission produces it by drawing a straight line between survey rounds, which is why it "
      "falls by almost the same amount every year, and it had Nigeria at 5.14 in 2022 when the next actual "
      "measurement came in at 4.8. Female population by age exists only as projections from the 2006 "
      "census, and they do not reconcile with the survey's own denominators — about 11% apart — so there is "
      "nothing solid to recalculate from.",
      "https://nationalpopulation.gov.ng/publications"),
    C("Iran", "Statistical Center of Iran and the civil registration organization — nothing we could reach",
      None, "Iran", "none", False,
      "Nothing we could get to. We found one real table — registered births by age of mother for the whole "
      "country in 1386, which is 2007-08 — but only through a web archive, and no female population by age "
      "to divide it by. No total fertility rate was found anywhere, live or archived.",
      "Nothing. Iran is on this list rather than filled in from an international compilation, which is the "
      "point of the exercise.",
      "The three hosts fail in three different ways, and the difference matters for anyone trying again. "
      "The statistics center at amar.org.ir accepts a connection and then drops it before sending anything "
      "back, identically across every TLS version, cipher setting and browser header we tried — that is a "
      "perimeter device refusing foreign traffic, not a certificate problem. Its other domain, sci.org.ir, "
      "answers normally but serves a maintenance notice, so it is worth retrying later. The civil "
      "registration organization at sabteahval.ir answers too, but with a bot challenge that wants device "
      "motion data; a real browser might well get through where a script cannot. Do not use nocr.ir — it "
      "is no longer the registration organization's domain and now redirects to a domain reseller.",
      "https://www.amar.org.ir/"),
    C("Turkey", "TurkStat — birth statistics over the address-based population register", turkey_tfr,
      "Turkey", "complete", False,
      "TurkStat's annual birth statistics bulletin publishes age-specific fertility rates and births by "
      "mother's age group, both from 2001. An older population portal gives female population by five-year "
      "age group from the address-based population register, back to 1935.",
      "We summed the age-specific rates and multiplied by the band width, giving 1.484 for 2024 against the "
      "1.48 TurkStat states. We also checked it from the counts: dividing the registered births by the "
      "register's female population gives 1.4832, which agrees with TurkStat's own arithmetic to four "
      "decimal places. The age-band comparison uses those counts.",
      "The two bulletin tables cannot be fetched by a script. TurkStat's current portal is a JavaScript "
      "application whose download links are single-use tokens, so they were downloaded by hand; the "
      "population portal, by contrast, answers a form-encoded POST, though a JSON body gets a 404. TurkStat "
      "splits the teens into 15-17 and 18-19, which we fold together. It also revises birth figures for up "
      "to five years after first release — rows carry an (r) marker — so editions disagree with each other, "
      "and the population figures before 2007 come from censuses rather than the register, a break inside "
      "one table. The population series is complete from 2007, so the age-band comparison cannot go earlier.",
      "https://data.tuik.gov.tr/Bulten/Index?p=Dogum-Istatistikleri-2024-54196"),
    C("Ethiopia", "Statistical Service — Ethiopia Demographic and Health Survey", ethiopia, "Ethiopia",
      "survey", False,
      "The Statistical Service publishes a total fertility rate for each survey round. The 2024-25 "
      "report's trend figure prints the national value for all five rounds since 2000, and its table 3 "
      "gives the age-specific rates behind the latest one.",
      "We read the five round values off the trend figure. Summing the age-specific rates in table 3 and "
      "multiplying by the band width gives 4.05, which is the published 4.0.",
      "These are household surveys asking women about past pregnancies, not registration. Each point is "
      "the average of the three years before the survey, so the years are approximate. Ethiopia's last "
      "completed census was 2007 and the next has been postponed repeatedly, so there is no recent "
      "female population by age to divide by — the 2007 census is the only one, and it is nineteen years "
      "old. The old statsethiopia.gov.et domain is dead; the service is now at ess.gov.et.",
      "https://ess.gov.et/wp-content/uploads/2026/01/edhs-2024-25-kir-01172026.pdf"),
    C("Cote d'Ivoire", "INS — Enquête Démographique et de Santé", cote_divoire, "Cote d'Ivoire", "survey",
      False,
      "INS publishes a fertility rate from each survey round: 5.7 in 1994, 5.2 in 1998-99, 5.0 in 2011-12 "
      "and 4.6 in 2016, each with the age-specific rates behind it.",
      "We read the two rounds inside our window. Summing each round's own rates reproduces its total — 4.95 "
      "against the published 5.0, and 4.595 against 4.6.",
      "The 2021 census publishes no fertility rate at all, and not even births by age of mother — the same "
      "gap as Mozambique. Its own results report lists fertility as one of sixteen thematic volumes to come "
      "later, and none of them had appeared by the time the site was last readable. Nothing newer than 2016 "
      "exists: a survey was fielded in 2021 but no report for it was ever published on the institute's site. "
      "That site is now down — it resolves and answers, but serves only a hosting placeholder, and has done "
      "since January 2026 — so everything here came from a web archive. Civil registration is not used for "
      "fertility, and INS explains why by publishing coverage instead: 55% of under-fives registered in 2006, "
      "65% in 2011-12, 72% in 2016.",
      "https://www.ins.ci/"),
    C("Democratic Republic of Congo", "INS — Enquête Démographique et de Santé", drc,
      "Democratic Republic of Congo", "survey", False,
      "The Institut National de la Statistique publishes a fertility index for each survey round. The "
      "2023-24 report gives 5.5 and states the previous round measured 6.6.",
      "We took both figures as stated. Summing the age-specific rates in table 5.1 and multiplying by the "
      "band width gives 5.47, which is the published 5.5.",
      "There has been no census since 1984 and no civil registration good enough to use, so every "
      "national fertility figure comes from one of the three surveys. The report's age structure is the "
      "surveyed sample, not a population count, so there is nothing to recalculate from. The institute's "
      "ins-rdc.org domain returns a block; the working site is ins.gouv.cd.",
      "https://ins.gouv.cd/publication/RDC-EDS-III.pdf"),
    C("Syria", "Central Bureau of Statistics — family health surveys", syria, "Syria", "survey", False,
      "The bureau's statistical abstract carries one fertility table, giving age-specific rates for its 2001 "
      "and 2009 family health surveys: 3.8 and 3.5.",
      "We read both, from the native spreadsheet inside the abstract's own chapter archive. The rates sum to "
      "the printed totals.",
      "Nothing has been added since 2009. Every edition we recovered — 2016, 2017, 2019 and 2020 — reprints "
      "the same table unchanged, even while the registered-births tables next to it were being updated to "
      "2019; and those birth tables break down by sex and province only, never by the mother's age, so there "
      "is nothing to rebuild from. The bureau\'s site is now gone, and how it went is worth recording: it "
      "kept publishing for months after the change of government in December 2024, with fresh pages as late "
      "as July 2025, then the domain lapsed and a parking service took it. That service cloaks by browser "
      "identity — a plain request gets a 410, which reads as retired, while a browser-like one gets a "
      "for-sale page at 200, which reads as squatted — so testing only one way misdiagnoses it. Everything "
      "here came from a web archive. The health ministry\'s site is live and modern but publishes only "
      "disease surveillance.",
      "https://web.archive.org/web/20241209121919/http://cbssyr.sy/yearbook.htm"),
    C("Tanzania", "NBS — 2022 Population and Housing Census", tanzania, "Tanzania", "survey", False,
      "The census fertility monograph prints age-specific rates twice: as women reported them, and after "
      "adjustment. The reported rates sum to a total fertility rate of 3.2, the adjusted ones to 4.6. NBS "
      "presents 4.6 as the country's figure. The 2022 household health survey separately found 4.8.",
      "We read the adjusted total out of table 3.2. We also checked the arithmetic: the reported rates sum "
      "to 3.195 and the adjusted ones to 4.63, matching both printed totals.",
      "The gap between 3.2 and 4.6 is entirely the adjustment. NBS applies the Arriaga method because "
      "women under-report and misdate births, and says plainly that Tanzania's vital registration is too "
      "incomplete to compute the rate directly. The census does publish births in the last twelve months "
      "and women by age group, so a direct recalculation is possible, but it would reproduce the "
      "unadjusted 3.2 rather than the figure NBS stands behind.",
      "https://www.nbs.go.tz/statistics/topic/demographic-and-socio-economic-statistics"),
    C("Spain", "INE — birth statistics over the continuous population count", spain_tfr, "Spain",
      "complete", True,
      "INE publishes registered births by single year of age of mother, 2009 onward, and female "
      "population by single year of age every quarter back to 1971. Both come out of its open service as "
      "whole-table CSV with no key and no registration.",
      "We divided births at each single age by the women of that age on 1 July and summed. Our figures "
      "land within 0.01 of INE's own published fertility indicator every year — 1.107 against 1.10 for "
      "2024, 1.122 against 1.12 for 2023.",
      "The population series is the Estadística Continua de Población, which replaced the older municipal "
      "register figures and revises as registrations arrive. Births start in 2009 on this table, so the "
      "line is shorter than the population series behind it. The 2024 figure is the first release and will "
      "revise slightly.",
      "https://www.ine.es/dyngs/INEbase/es/operacion.htm?c=Estadistica_C&cid=1254736177007"),
    C("South Korea", "Statistics Korea — annual birth statistics release", korea_tfr, "South Korea",
      "complete", False,
      "Statistics Korea publishes its birth statistics in English every year as a PDF, and each edition's "
      "first table gives an eleven-year run of the total fertility rate. Some editions also print live "
      "births by mother's age group as counts. The interior ministry separately publishes female population "
      "by five-year age group from the resident register, as a spreadsheet download.",
      "We chained five editions of the release to build 2003-2024, taking the newer edition wherever two "
      "overlap because the office revises. As a check we recalculated 2015 from the counts — births by age "
      "group from the release, women by age group from the resident register — and got 1.245 against the "
      "published 1.24.",
      "The obvious source would be the KOSIS database, but its interface refuses scripted requests and its "
      "API rejects everything without a key; getting a key means a Statistics Korea account, and that needs "
      "either a Korean mobile phone registered in your own name or a Korean identity number. There is no "
      "route for a foreigner and no English signup at all, so this uses the published PDFs instead. Editions "
      "before 2013 print only the current and previous year, which is why the series starts in 2003. Recent "
      "editions dropped the full births-by-age table, so a recalculation for the latest year is not "
      "possible from them.",
      "https://mods.go.kr/board.es?mid=a20108010000&bid=11773"),
    C("Cameroon", "INS — Enquête Démographique et de Santé", cameroon, "Cameroon", "survey", False,
      "INS publishes a fertility rate from each survey round: 5.1 for 2011 and 4.8 for 2018. The 2005 census "
      "reports 5.2, and prints the uncorrected figure of 4.1 alongside it.",
      "We read the two survey rounds. We also checked the census arithmetic: summing its corrected "
      "age-specific rates gives 4.16 for urban Cameroon and 6.21 for rural, matching its own printed "
      "4.1607 and 6.2130.",
      "Cameroon has had no census since 2005 and no fertility fieldwork since 2018, so the newest national "
      "figure is eight years old. The fourth census was launched in 2016 but its main enumeration had still "
      "not happened by the last status report anyone published, in September 2019, which was discussing how "
      "to count the crisis-hit North-West and South-West regions at all; nothing has appeared since. The "
      "census bureau\'s own website is gone — parked on an expired hosting account for years — so the 2005 "
      "volumes had to come from a web archive. That census corrects its raw figure upward with the Brass "
      "method, from 4.1 to 5.2, and its published long-run trend uses corrected values throughout while "
      "showing the raw ones only in a technical annex. Civil registration is not used: INS\'s own vital "
      "statistics report says the system\'s coverage problems mean it uses the survey instead, and puts "
      "birth registration completeness at 54%, ranging from 92% in one region to 35% in another.",
      "https://ins-cameroun.cm/"),
    C("Burkina Faso", "INSD — Enquête Démographique et de Santé", burkina_faso, "Burkina Faso", "survey",
      False,
      "INSD publishes a fertility rate from each survey round — 5.9 for 2003, 6.0 for 2010 and 4.4 for 2021 "
      "— and separately from each census, giving 5.4 for 2019 with the births and women behind it as counts.",
      "We read the survey rounds. Summing the census's own published rates reproduces its 5.4.",
      "The two instruments are a full child apart two years apart: the 2019 census says 5.4 and the 2021 "
      "survey says 4.4. Nothing in either report reconciles them, and we do not know which is closer to the "
      "truth, so the survey series is used because it is the more recent and the more consistent run. Burkina "
      "Faso is also the one country here whose correction went down rather than up. INSD found an implausible "
      "spike in fertility at ages 45-49 in its raw census data, tested the Brass method (which gave 5.8) and "
      "then adopted Arriaga instead, which nudged the national figure from 5.5 to 5.4 while cutting the urban "
      "figure from 4.5 to 4.1. Civil registration is not used; the 2021 survey found 85% of under-fives "
      "registered and 73% holding a certificate, and INSD\'s own civil-registration statistics page is a chart "
      "template filled with randomly generated placeholder data. One census volume has an embedded font that "
      "silently defeats text extraction across some two hundred pages, so its fertility chapter needs "
      "character recognition rather than a text layer.",
      "https://www.insd.bf/"),
    C("Canada", "Statistics Canada — births by age of mother over its population estimates", canada_tfr,
      "Canada", "complete", True,
      "Statistics Canada publishes registered live births by age group of mother, female population by "
      "age group at 1 July, and its own fertility rate — all three as plain CSV from one open service "
      "with no key, annually from 1991.",
      "We divided the births by the female population in each age group and summed. Our figures match "
      "Statistics Canada's own published rate to the second decimal in every year we checked: 1.269 "
      "against 1.27 for 2023, 1.255 against 1.25 for 2024, 1.510 against 1.51 for 2018.",
      "Births are dated by the year they occurred, not the year they were registered, and late "
      "registrations are folded back into the year of the birth by annual revision — about a thousand cases "
      "five years on — so recent years do not understate. The 2024 figure is flagged provisional in the "
      "table notes but not in the data itself, so anything automated has to treat the latest year as "
      "provisional by hand. Nova Scotia under-recorded births in 2021. Births to mothers of 50 and over are "
      "folded into the 45-49 row for confidentiality, and mothers whose age was not stated are spread "
      "across the bands. There is no citizens-only denominator to pick wrongly here, but the population does "
      "include non-permanent residents, and the number of women aged 20-24 rose 9% between 2019 and 2024 "
      "partly on student migration — so some of the fall in the rate is the denominator growing rather than "
      "births falling.",
      "https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1310041601"),
    C("Yemen", "CSO and health ministry — National Health and Demographic Survey 2013", yemen, "Yemen",
      "survey", False,
      "One figure inside our window: 4.4 children per woman for the three years to 2013, from the survey "
      "the Central Statistical Organisation ran with the health ministry. The 2004 census is also covered, "
      "but the organisation publishes two different figures for it — 4.93 by one method and 6.1 by another.",
      "We read the national column of table 2. Summing its age-specific rates and multiplying by the band "
      "width gives 4.43, the published 4.4.",
      "We use the 2013 survey rather than the census because the census figures are not one number: the "
      "organisation's own yearbook prints 4.93 from directly observed births and 6.1 from cumulating "
      "age-specific rates, and a separate study of its own reaches 6.1 by indirect estimation. Choosing "
      "between those is a methodological judgement, not a lookup, so the survey — a single figure from "
      "standard birth-history estimation — is the safer one. Nothing has been published since 2013; the war "
      "began in 2015. Three of the organisation's four domains are unusable: cso-yemen.com sits behind a "
      "firewall that rejects anything that is not a browser, cso.gov.ye has its secure port blackholed, and "
      "cso-yemen.org and the planning ministry's domain have both lapsed to squatters — the former now "
      "serves a spam blog, so anything found live there should not be trusted. This report came from a web "
      "archive.",
      "https://web.archive.org/web/20220608201754/https://cso.gov.ye/about_cso"),
    C("Angola", "INE — Inquérito de Indicadores Múltiplos e de Saúde", angola, "Angola", "survey", False,
      "INE publishes a fertility rate for each survey round: 6.2 from the 2015-16 survey and 4.8 from the "
      "2023-24 one, with the age-specific rates behind each. The 2014 census separately gave 5.7.",
      "We read the national figure from each round. Summing the printed age-specific rates reproduces both "
      "totals, at 6.215 and 4.78.",
      "We use the two surveys rather than the census, because they measure the same way — from women\'s own "
      "birth histories, with no correction applied — while the census figures are not one number. INE\'s own "
      "population projection re-derived the 2014 baseline as 5.5 rather than the census\'s 5.7, using a "
      "Gompertz model, because it found women had under-reported their total children and over-reported "
      "births in the last twelve months. The census also sits oddly against the survey: 5.7 in 2014 against "
      "6.2 measured two years later. Angolan civil registration cannot be used at all — the 2023-24 survey "
      "found only 38% of children under five registered, and only 36% of those holding a certificate. No "
      "Angolan source publishes births by age of mother as counts, only rates, so there is nothing to "
      "recalculate from. The 2024 census has published definitive results with no fertility indicator at "
      "all, so a later thematic volume is worth watching for.",
      "https://www.ine.gov.ao/publicacoes/Todas?titulo2=IIMS"),
    C("Argentina", "Health ministry births over INDEC population", argentina_tfr, "Argentina",
      "complete", True,
      "The health ministry publishes registered live births by age group of mother as open CSV, annually "
      "from 2005. INDEC publishes female population by age group in two projection series, one based on the "
      "2010 census and one on the 2022 census. What INDEC does not publish is an annual fertility rate: its "
      "only figures are four projected years, starting at 1.27 for 2025.",
      "We divided the births by the female population in each age group and summed. Because there is no "
      "official annual rate to take, this is the only way to get a year-by-year figure for Argentina. Our "
      "2014 value of 2.35 lines up with the roughly 2.3 usually quoted for that year.",
      "Argentina's fall is the steepest in this whole dataset — from 2.38 in 2010 to 1.19 in 2024, halving in "
      "fourteen years. The population switches vintage at 2022, from the 2010 census basis to the 2022 one, "
      "and we have not smoothed that seam. Births are tabulated by year of registration, but the ministry "
      "defines the series to absorb only one year of lag and its own tables show 95% of a year's "
      "registrations occurred that year and almost all the rest the year before, so unlike Mexico there is no "
      "need to stop the series early. Mothers whose age was not stated — under 1% in recent years and "
      "falling — are spread across the bands. The top group is open-ended at 45 and over, treated here as "
      "45-49. The series starts in 2010 because that is where INDEC\'s population by age begins; births go "
      "back to 2005. INDEC\'s file paths are not linked from its own menus and a stale path returns an HTML "
      "error page with a 200 status, which is easy to mistake for data.",
      "https://datos.salud.gob.ar/dataset/nacidos-vivos-registrados-por-jurisdiccion-de-residencia-de-la-madre-republica-argentina"),
    C("Afghanistan", "CSO and Ministry of Public Health — Demographic and Health Survey 2015", afghanistan,
      "Afghanistan", "survey", False,
      "One figure: 5.3 children per woman for the three years to 2015, from the survey the Central "
      "Statistics Organization ran with the health ministry. The same table gives the age-specific rates "
      "behind it, and the report breaks the figure down by all 34 provinces.",
      "We read the national column of table 5.1. Summing its age-specific rates and multiplying by the band "
      "width gives 5.29, the published 5.3.",
      "Afghanistan has no vital registration usable for this, so the only figure is survey-based, and it is "
      "now over a decade old. The office's old domain, cso.gov.af, no longer resolves at all — its "
      "nameservers were deprovisioned but the delegation was never updated, so this report had to come from "
      "a web archive. Its successor, nsia.gov.af, is alive and actively maintained, with a certificate valid "
      "into late 2026, but serves a JavaScript application that a script cannot read, so we could not check "
      "whether anything newer has been published. The survey's own age structure is a weighted sample, not a "
      "population count, and no Afghan source gives female population by age group outside it, so there is "
      "nothing to recalculate from.",
      "https://nsia.gov.af/"),
    C("Algeria", "ONS — Démographie Algérienne", algeria, "Algeria", "complete", False,
      "The annual bulletin's main indicators table gives a fertility index for most years from 2002, "
      "alongside births and age-specific rates. Female population by five-year age group is in the same "
      "bulletin.",
      "We read the index row out of the 2019 edition, which carries the whole span in one table. Summing its "
      "age-specific rates and multiplying by the band width reproduces the published 3.0 for 2019 exactly, "
      "and multiplying those rates by the female population implies 1,032,000 births against the 1,034,000 "
      "the same bulletin reports.",
      "ONS stopped publishing the fertility index after 2019. The 2020-2023 edition dropped the whole "
      "fertility section — the term survives only in the glossary, with no number attached — so the series "
      "ends there, which is also the year ONS stopped inflating registered births for under-registration "
      "using coverage factors last estimated in 2002. The total is a registration count, but the age split "
      "is not: ONS says the rates for 2010-2019 were readjusted onto the age structure of births taken from "
      "its labour force surveys, and earlier editions say a single 2008 age curve was reused for several "
      "years. The population denominator is rolled forward from the 2008 census by natural increase alone, "
      "assuming no net migration. Some years were never published at all, so the line has gaps. The site "
      "serves its certificate without the intermediate, so downloads need certificate checking relaxed, and "
      "the year headers in several editions carry a digit-transposition typo for 2017.",
      "https://www.ons.dz/spip.php?rubrique182"),
    C("Iraq", "COSIT — household survey rounds", iraq, "Iraq", "survey", False,
      "The Annual Statistical Abstract has a table of the fertility rate for the years it was measured: 4.0 "
      "in 2004 from the living conditions survey, 4.3 in 2006 and 4.5 in 2011 from the two household cluster "
      "surveys, and 4.2 in 2007 from the socio-economic survey. Separately, COSIT publishes a projection "
      "series running 4.08 in 2015 down to 3.82 in 2020.",
      "We read the measured rounds from the abstract's table. We do not use the projection series, on the "
      "same reasoning as Nigeria: it is model output, not measurement.",
      "Iraq has no usable vital statistics for this. COSIT says so itself — the interior ministry holds a "
      "civil registry of more than 46 million people, but it is not yet organised for statistical use, and "
      "COSIT is only now building that pipeline. So there are no births by age of mother to recalculate from, "
      "and the series stops in 2011 because that is the last measured round published. Iraq ran its first "
      "full census since 1987 in November 2024, but only headline counts are out; the fertility results are "
      "not published yet, so this will be worth revisiting. The Kurdistan region's own statistics office "
      "blocks automated requests entirely, and the federal figures do not cover it — an adolescent fertility "
      "table on a COSIT platform reports zero for the three Kurdish governorates.",
      "https://cosit.gov.iq/"),
    C("Uganda", "UBOS — 2024 National Population and Housing Census", uganda_tfr, "Uganda", "survey", False,
      "Table 7.2 of the 2024 census report gives, for every five-year age group, the number of women, the "
      "births they reported in the previous twelve months, the same births after correction, and both the "
      "reported and corrected rates. The 2014 census gave 5.8. The health survey UBOS runs separately gives a "
      "direct series: 6.9 in 2000, 6.7 in 2006, 6.2 in 2011, 5.4 in 2016 and 5.2 in 2022.",
      "We rebuilt the 2024 figure from the corrected rates, getting 4.47 where the report prints 4.5. Summing "
      "the reported rates instead gives 4.15, and dividing the reported births by the women gives the same — "
      "so the published figure sits about 8% above what women actually reported. The age-band comparison uses "
      "the reported counts, because those are what the census counted.",
      "The correction is a Brass P/F adjustment for births women forget or misdate, which UBOS applies because "
      "Uganda's civil registration cannot be used at all: the census found only 10% of children under five had "
      "a registered birth. That also means the census and the health survey are measuring by different methods "
      "— the survey's 5.2 for 2022 against the census's 4.5 for 2024 is partly a real fall and partly a change "
      "of instrument, and we have not tried to separate the two. UBOS's site serves a broken certificate "
      "chain, so downloads need certificate checking relaxed.",
      "https://www.ubos.org/nphc-2024-census-page/"),
    C("South Africa", "Stats SA — mid-year population estimates", south_africa_tfr, "South Africa",
      "projection", False,
      "The fertility rate Stats SA publishes is an input to its population projection, not a rate computed "
      "from births. Its own report says the series was \"derived following a detailed review of TFR estimates "
      "(1985-2024), (both published and unpublished), from various authors, methods and data sources\", "
      "informed by registered births and health-system records. Separately, the recorded live births report "
      "publishes registered births by age of mother as counts, and the mid-year estimates publish female "
      "population by age group.",
      "We read the modelled series from table 2 of the mid-year estimates. The age-band comparison uses the "
      "registered births instead, because those are the only counts the registry produces.",
      "The two disagree enormously. Dividing the registered births by the female population gives 1.53 for "
      "2024 against the published 2.41, and 1.88 for 2016 against 2.33. Some of that is timing — Stats SA "
      "says about 10% of births are registered a year or more late, and a year keeps filling up for years "
      "afterwards — but not all of it: it also estimates that even once late registrations are in, "
      "registration captures only about 90% of births. So South Africa's headline figure is deliberately set "
      "well above what its own registry shows, and is closer in kind to a UN estimate than to a count. "
      "Fertility results from the 2022 census have still not been released, which Stats SA notes as a reason "
      "the census could not feed the estimate. No mid-year estimates edition was published for 2023.",
      "https://www.statssa.gov.za/publications/P0302/P03022024.pdf"),
    C("Sudan", "Central Bureau of Statistics — Multiple Indicator Cluster Survey 2014", sudan, "Sudan",
      "survey", False,
      "One figure: 5.2 children per woman for women aged 15-49, from the Bureau's own 2014 survey, printed "
      "in a short fertility brief. The same brief's longer table stops at a 1990 health survey.",
      "We read the figure from the brief. No arithmetic of our own.",
      "The Bureau's website no longer exists — its own nameservers stopped answering, so the domain does not "
      "even resolve, while the rest of the Sudanese government's web presence is fine. That looks like "
      "abandonment rather than a block, and is unsurprising given the war since 2023. This brief was "
      "therefore recovered from a web archive; it is genuinely the Bureau's own document but carries no "
      "publication date, and one of its other tables has stray percent signs on figures that are children "
      "per woman. There is no age detail to recalculate from, and nothing more recent: the 2008 census files "
      "that survive give population by area only, with no age breakdown. The health ministry's site is alive "
      "but stuck in a redirect loop, so nothing can be read from it either.",
      "http://cbs.gov.sd/"),
    C("Kenya", "KNBS — 2019 Population and Housing Census, volume VI", kenya, "Kenya", "survey", False,
      "The census fertility volume gives a national total fertility rate of 3.4 for 2019 and 4.8 for 2009, "
      "plus age-specific rates for every county. The 2022 household health survey, also run by KNBS, "
      "separately found 3.4.",
      "We read both census values out of the trends table. We also summed the national age-specific rates "
      "and got 3.24, a little below the published 3.4 — the rates are printed to three decimals and the "
      "national total absorbs county-level adjustments, so the two do not have to agree exactly.",
      "The census measures fertility by asking women about births in the three years before the count, not "
      "from registration. Rates for the North-Eastern counties are adjusted with a Gompertz model because "
      "reporting there was inconsistent. The whole report is scanned page images with no text layer, so "
      "everything had to be read by optical character recognition. Births by age of mother as counts are "
      "not in this volume.",
      "https://www.knbs.or.ke/wp-content/uploads/2024/05/2019-Kenya-population-and-Housing-Census-"
      "Analytical-Report-on-Fertility-and-Nuptiality-Vol.VI_.pdf"),
    C("Myanmar", "Department of Population — 2019 Inter-censal Survey", myanmar_tfr, "Myanmar",
      "survey", True,
      "Appendix table D-1 of the Union Report gives, for every five-year age group, the number of women "
      "enumerated and the number of live births in the twelve months before the survey, plus the "
      "department's own age-specific rate.",
      "We divided births by women in each age group and summed, times the band width. Every rate we get "
      "matches the printed one to the second decimal, and our total of 2.007 is the published 2.0.",
      "This is a sample survey, not registration: the counts are the survey's own enumerated households "
      "scaled up, and a twelve-month recall question misses births. The 2014 census gave 2.5 by the same "
      "kind of table. Nothing more recent is published — a 2024 census has been run but its fertility "
      "results are not out.",
      "https://www.dop.gov.mm/sites/dop.gov.mm/files/publication_docs/ics_appendixtables_en.pdf"),
    C("Sri Lanka", "DCS — registered births over the mid-year population", sri_lanka_tfr,
      "Sri Lanka", "complete", True,
      "The Department of Census and Statistics stopped publishing a fertility rate of its own after 2000, "
      "but keeps publishing both ingredients: registered live births by age of mother, and mid-year "
      "population by age group and sex. The 2024 census separately reports 1.3.",
      "We divided the births in each age band by the women in it and summed. The same procedure on the "
      "department's own 2000 data reproduces the 1.9 it published then, which is what tells us the method "
      "is the one it used.",
      "Two things to know. First, the series stops at 2021 — not because registration stopped, but because "
      "the detailed births-by-age tables did. Headline birth totals kept being published through 2025, and "
      "they fall steeply: 319,108 in 2019 to 214,570 in 2025. The age breakdown needed for a rate exists "
      "for none of those years, and 2022 appears on the department's site only as a dead link reusing "
      "2021's files. Second, the 2024 census reports 1.3, far below the 1.64 we get for 2021, but it is a "
      "different measure: it asks ever-married women about their birth histories rather than counting a "
      "year's registrations. Neither number is the other's update. The population is published rounded to "
      "thousands, and its age structure is rolled forward from the 2012 census, not the 2024 one.",
      "https://www.statistics.gov.lk/Population/StaticalInformation/VitalStatistics/Fertility"),
    C("Taiwan", "Ministry of the Interior — births by age of mother over the household register",
      taiwan_tfr, "Taiwan", "complete", True,
      "The interior ministry publishes births by the mother's age group, population by single year of age "
      "and sex, and its own fertility rate — all from the household register, all back to at least 2000.",
      "We divided the births in each age band by the women in it and summed. Our 0.888 for 2024 is the "
      "ministry's published 0.885, and 0.868 against its 0.865 for 2023.",
      "Taiwan has the lowest rate in this collection: 0.71 for 2025 on our arithmetic, against 1.66 in "
      "2000. Zodiac years move it visibly — 2022 was a Tiger year and the rate fell to 0.88, 2024 was a "
      "Dragon year and it rose to 0.89, the only rise in a decade, before falling 20% in 2025. Two "
      "definitions matter. Births are counted by the date they happened, which is why the age breakdown is "
      "only released once a year; 2025 is still filling in, and its occurrence-year total is 2% below the "
      "registration-year total for the same months. And the population is the year-end household register, "
      "not a mid-year estimate and not the de facto resident population that the budget agency publishes "
      "separately. None of this is reachable from the ministry's own portal pages, which are navigation "
      "shells; the query service behind them answers plain requests but has to be addressed directly.",
      "https://www.moi.gov.tw/cl.aspx?n=4404"),
    C("Senegal", "ANSD — continuous Demographic and Health Survey", senegal, "Senegal", "survey", False,
      "ANSD runs its health and demographic survey every year, and the 2023 census report prints the whole "
      "series from 1978 in one trend table. The census publishes its own figure separately, along with the "
      "births and women behind it.",
      "We read the survey series. We also divided the 2023 census's own counts — 487,108 births to women "
      "15-49 over 4,499,636 women — and got 3.69, which is the raw figure ANSD itself prints.",
      "The census is the clearest worked example of an adjustment we have found. ANSD ran three methods on "
      "the same data, published all three, and said which it chose and why: the P/F ratio gave 4.54, a "
      "Gompertz model 4.35, and Arriaga 4.41. It picked Arriaga because that method allows fertility to "
      "have changed between two censuses rather than assuming it constant, and noted that 4.4 sits close to "
      "the survey's 4.0 for the same year. So the published census figure is 4.4 against a counted 3.7. "
      "Its own 2013 census ran the same check and found almost no correction was needed, which is worth "
      "knowing before assuming these adjustments are routine. ANSD's projections document states 4.2 for "
      "2023 rather than 4.4, and we cannot tell which is the slip. Civil registration is not used for any "
      "of this: two thirds of people hold a birth certificate, and ANSD says outright that the registration "
      "system needs strengthening.",
      "https://www.ansd.sn/"),
    C("Malawi", "NSO — census and Demographic and Health Survey rounds", malawi, "Malawi", "survey",
      False,
      "The 2018 census has a dedicated fertility report, and the 2024 health survey charts every round "
      "since 1992. The census also publishes its fertility tables as a spreadsheet — women and births by "
      "age group, which almost no other census in this collection does.",
      "We read the published rates. We also divided the census's own counts — 576,525 births over 4,267,788 "
      "women 15-49 — and got 4.166, matching the 4.167 the report prints as its unadjusted figure.",
      "Malawi is the most legible case of an office deciding how much to correct. It ran three indirect "
      "methods on the 2018 census and published all of them: the P/F ratio gave 4.9 to 5.1, a Gompertz "
      "model 4.9 to 5.4, and Arriaga 4.23. It chose Arriaga and said why — that the other methods assume "
      "fertility is not changing, and Malawi's is falling. So the correction it applied was about 3%, "
      "against the near-doubling Mali applied to its own census. Fertility is measured from census and "
      "survey questions, not registration, though registration itself has risen from 67% of under-fives in "
      "2015 to 78% in 2024. The office's website serves no data at all to a plain request; everything is "
      "behind an undocumented interface that its own pages call from the browser.",
      "https://www.nsomalawi.mw/"),
    C("Somalia", "SNBS — Somali Health and Demographic Survey 2020", somalia, "Somalia", "survey", False,
      "The 2020 survey is the only national fertility figure Somalia has. It gives 6.9, with age-specific "
      "rates and the number of women in each age group.",
      "We read the published figure, and checked that its own age-specific rates sum to it: 6.885 against "
      "the printed 6.9.",
      "Two documents look like they should count and do not. A 2026 survey run by the same office reports "
      "5.7, but its own foreword says it covers five districts and is a step toward national coverage, so "
      "that is not a national figure. And the 2013 population estimation survey, the one that gave Somalia "
      "its population count, is copyrighted and published by a UN agency rather than by the statistics "
      "office — and in any case contains no fertility rate at all. So the 2020 survey stands alone. There "
      "is no census: the last one was in 1975. Birth registration is between 3.5% and 5.9%, depending on "
      "which part of the same report you read.",
      "https://nbs.gov.so/"),
    C("Chad", "INSEED — survey and census rounds", chad, "Chad", "survey", False,
      "Three figures exist: 7.1 from the 2009 census, 6.9 from the 2010 multiple-indicator survey, and 6.4 "
      "from the health survey. Nothing annual, and no age-specific rates at all.",
      "We read the published figures. There is nothing to recompute: no document INSEED hosts publishes "
      "births by age of mother, as counts or as rates.",
      "Chad's own publications disagree about their own data. The 6.4 figure is attributed to the 2014-15 "
      "health survey in one INSEED report and to the 2019 survey in another; we place it at 2015, the "
      "fieldwork years. Neither of those survey reports is on INSEED's site, and neither is the 2009 "
      "census's fertility volume — it is cited by title and page count in another report's bibliography but "
      "was never digitised, so how the 7.1 was computed cannot be checked. Birth registration was 16% in "
      "2010, up from 9% in 2004, and only a quarter of those could show a certificate. The 2009 census was "
      "Chad's most recent for seventeen years; fieldwork for the next one finished in August 2026 and "
      "results are promised progressively. INSEED's website serves nothing to a plain request — the files "
      "sit on a separate host that its own pages call from the browser.",
      "https://inseed.ssn-tchad.td/"),
    C("Chile", "INE — registered births over its population estimates", chile_tfr, "Chile",
      "complete", True,
      "One spreadsheet carries the whole series from 1992: births by five-year age band of the mother, "
      "the women in each band, and INE's own rate.",
      "We divided births by women in each band and summed. Our 1.034 for 2024 is INE's published 1.03, "
      "and 1.159 against its 1.16 for 2023.",
      "Chile is the clearest version of a pattern we keep finding in Latin America. Its registry gives "
      "1.03 for 2024. The population projection INE had in force until early 2026 assumed a flat 1.58 all "
      "the way to 2030 — a gap of 0.55 against its own counted births. INE then rebased the projection onto "
      "the 2024 census in February 2026 and the new one assumes 1.06 for 2024, so the gap has closed; but "
      "anything built on the older vintage still carries it. Which vintage a comparison uses decides "
      "whether Chile looks like Peru or not. The counting itself is sound: births are dated to the year "
      "they happened, and the late-registration correction is small — 98.5% of the births registered "
      "during 2023 had happened in 2023. 2023 and 2024 are still provisional. Santiago was already at 1.03 "
      "in 2023, a year before the country as a whole. The health ministry's own vital-statistics site is "
      "behind a bot challenge and its older domain no longer resolves, but nothing there is needed.",
      "https://www.ine.gob.cl/estadisticas-por-tema/demografia-y-poblacion/estadisticas-vitales"),
    C("Netherlands", "CBS — births by age of mother over its mean population", netherlands_tfr,
      "Netherlands", "complete", True,
      "CBS publishes births by single year of the mother's age from 1950, population by single year of age "
      "and sex, and its own rate — all through an open interface with no key.",
      "We divided births at each single age by the women of that age and summed. Our 1.4299 for 2023 is "
      "CBS's published 1.430, and 1.4262 against its 1.426 for 2024.",
      "This is the cleanest case in the collection. CBS states exactly what its rate is built from — "
      "births in an age group over the mean number of women in it, the mean being half the population on "
      "1 January and half on 31 December — so there is nothing to infer. Births are dated to when they "
      "happened, not when they were reported, and CBS publishes every year as final rather than "
      "provisional. The register covers everyone registered as resident in a municipality whatever their "
      "nationality, so there is no nationals-only variant to pick wrongly. Two practical notes: CBS only "
      "fills in the mean-population column from 1995, so the series starts there rather than in 1950; and "
      "its newer interface refuses connections outright, while the older one works fine.",
      "https://www.cbs.nl/nl-nl/cijfers/detail/85722ned"),
    C("Zimbabwe", "ZIMSTAT — census and Demographic and Health Survey rounds", zimbabwe, "Zimbabwe",
      "survey", False,
      "The 2022 census has a dedicated fertility report, and it publishes the counts behind its figure: "
      "women enumerated and births in the previous twelve months, by age group. The health survey series "
      "runs alongside it.",
      "We read the published rates. We also divided the census's own counts — 438,776 births over 3,814,701 "
      "women 15-49 — and got 3.72, which rounds to the 3.7 it publishes.",
      "Zimbabwe belongs with Madagascar, Niger and Senegal's 2013 census in a group worth naming: offices "
      "that computed an indirect correction and then chose not to use it. ZIMSTAT ran two — an Arriaga "
      "estimate gave 3.8 and a Gompertz curve gave 3.7 against the counted 3.7 — and said that because the "
      "three agree, the direct estimate is robust and the unadjusted figure is what it would use "
      "throughout. That is the opposite outcome from Mali or Ghana, on the same method. Registration is not "
      "used for any of this and could not be: ZIMSTAT's own vital-statistics report puts birth-registration "
      "completeness at 30.9% for 2023 and 26.4% for 2024, falling rather than rising, because mobile "
      "registration drives in 2022 and 2023 pulled registrations forward. That report does publish "
      "registered births by age of mother, but computes no rate from them. The 2012 census report is no "
      "longer on ZIMSTAT's site and is not in any web archive, so its 3.8 survives only as a citation "
      "inside the 2022 report.",
      "https://www.zimstat.co.zw/wp-content/uploads/Census/Fertility_Report.pdf"),
    C("Ecuador", "INEC — registered births over its population estimates", ecuador_tfr, "Ecuador",
      "complete", True,
      "One sheet of INEC's vital-statistics series carries both sides from 2010: births in each "
      "five-year age band of the mother, and the projected women in that band. INEC publishes the "
      "age-specific rates from exactly those two columns, but never their sum.",
      "We divided births by women in each band and summed. There is no published total to check "
      "against — INEC's own bulletin headlines a crude birth rate and adolescent rates, not a fertility "
      "rate at all — but our figures reproduce its printed age-specific rates exactly.",
      "Ecuador is the sixth Latin American country where the registry sits well below the headline, and "
      "the only one where the office says why in writing. Its projection methodology lists four estimates "
      "for 2022 — 2.12 from a P/F ratio, 1.76 from reverse survival, 1.77 from Arriaga, 1.80 from vital "
      "registration — and adopts 1.86, on the stated grounds that it should sit above the registered "
      "births because those always carry some under-registration. So the gap is deliberate, not an "
      "oversight. Its projection assumes 1.82 for 2023 against a counted 1.61, and the 2018 health survey "
      "gave 2.19. Recent years are incomplete by design: a year is provisional until the following March "
      "and semi-definitive for three more, so 2024's 1.44 will rise as late registrations arrive.",
      "https://www.ecuadorencifras.gob.ec/nacidos-vivos-y-defunciones-fetales/"),
    C("Kazakhstan", "Bureau of National Statistics — births by age of mother over its population",
      kazakhstan_tfr, "Kazakhstan", "complete", True,
      "The bureau's database serves births by age of mother, mean annual population by sex and age, and "
      "its own rate — all to a plain request with no key.",
      "We divided births by women in each band and summed. Our figures match the bureau's published rate "
      "to within 0.01 in every year both exist: 2.957 against 2.96 for 2023, 2.798 against 2.80 for 2024.",
      "Kazakhstan is the second country here whose fertility rose sharply and is now falling back, and "
      "like Uzbekistan the rise is in its own counted births, not an artefact. The rate went from 2.84 in "
      "2018 to 3.32 in 2021, then down to 2.57 by 2025 — a fall of 0.75 in four years. Two things bound "
      "what we can say. Births are counted by registration date, and the bureau states outright that a "
      "birth registered this year counts this year even if it happened earlier, so the series is not on an "
      "occurrence basis. And the five-year age classifier was revised in 2025, which is why the mean "
      "annual population only reaches back to 2018 under the current codes; the bureau's own metadata "
      "claims history from 1999, but those years sit under superseded identifiers we could not find.",
      "https://stat.gov.kz/ru/industries/social-statistics/demography/"),
    C("Benin", "INStaD — Demographic and Health Survey rounds", benin, "Benin", "survey", False,
      "The 2017-18 survey's trend table carries the rounds back to 1996. The 2013 census publishes a "
      "single national figure of 4.8 and nothing behind it.",
      "We read the survey series. Nothing can be recomputed: neither births by age of mother nor women "
      "by age group is published for the 2013 census, in any form.",
      "Benin is the case where an office admits to correcting its census figure but never says how. Its "
      "own dissemination volume states that the fertility, nuptiality and mortality indicators were "
      "produced by indirect estimation, by a team of demographers with an international expert — so 4.8 is "
      "a model output, not a count. But no method is named anywhere, and no raw figure is printed beside "
      "it, so unlike Mali, Senegal or Malawi there is nothing to compare. The two instruments also "
      "disagree about direction: the censuses fall steadily, 6.1 in 1992 to 5.5 in 2002 to 4.8 in 2013, "
      "while the surveys are flat or rising since 2012 — 4.9 in the 2011-12 round against 5.7 in 2014 and "
      "5.7 again in 2017-18. The volumes advertised as six thematic census reports are the same twenty-page "
      "scanned brochure under two paths, with no statistical annex. Birth registration is high, at 86% of "
      "under-fives, but is not used for any fertility figure.",
      "https://instad.bj/"),
    C("Cambodia", "NIS — Demographic and Health Survey rounds", cambodia, "Cambodia", "survey", False,
      "The 2021-22 survey charts every round since 2000. Separately, the censuses and inter-censal "
      "surveys of 2008, 2013, 2019 and 2024 each publish a fertility figure with the arithmetic behind it.",
      "We read the survey series. We also summed the 2019 census's own published age-specific rates and "
      "got 2.512 against its printed 2.51, which confirms the table but not the figure — the rates are the "
      "adjusted ones, and the raw counts they were built from are not published.",
      "Cambodia is the most systematic case of census adjustment we have found anywhere. NIS has run the "
      "same exercise four times running and printed both numbers each time: births reported in the twelve "
      "months before the count give 1.6 in 2008 against an adopted 3.1, 2.05 in 2013 against 2.8, 1.67 in "
      "2019 against 2.51, and 1.4 in 2024 against about 2.3. The raw figure is between half and two thirds "
      "of the published one, every time, and the correction is roughly 1.4 to 1.9 times. NIS names the "
      "methods — Brass, Arriaga, Rele, a relational Gompertz curve — says Brass-Arriaga suits Cambodia "
      "best, and explains the under-reporting it is correcting for: children who were born and then died "
      "go undeclared, dates get misplaced, and someone other than the mother often answers. So this is "
      "settled institutional practice, not a one-off. It also means the census series and the survey "
      "series are not measuring the same way and should not be spliced: the surveys are flat at 2.7 from "
      "2014 to 2022 while the adjusted census series falls. One caution about citing across documents — "
      "the 2019 census report looks back at 2008 and gives 2.7, which is one method's value from the 2008 "
      "table rather than the 3.1 that report actually adopted. Registration is not used at all, though "
      "92% of under-fives are registered. The 2008 census and the 2024 survey are scanned images, so their "
      "figures had to be read by optical character recognition, and the 2024 report's own two tables "
      "disagree about its own answer.",
      "https://nis.gov.kh/"),
    C("Guinea", "INS — Demographic and Health Survey rounds", guinea, "Guinea", "survey", False,
      "The 2018 survey's own table carries the rounds back to 1992. The 2014 census publishes the counts "
      "behind its figure in an annex: women by age group, and births in the twelve months before the count.",
      "We read the survey series. We also divided the census's own counts and got 5.19 against the 5.3 it "
      "publishes, then compared age by age — which is where the interesting part is.",
      "Guinea's correction is the most selective one we have found. INS applied the Arriaga method, said "
      "why — the P/F ratios ran from 1.04 to 1.98, well above the 1.02 threshold, so recent births were "
      "under-reported — and the net effect on the total is only about 2%, far smaller than Mali's or "
      "Ghana's. But it is not a level shift. For every age group from 20 to 49 the published rate is within "
      "1% of the counted one; the entire correction lands on 15-19, where 105 per thousand becomes 130, a "
      "24% increase. INS never says this, and it only shows up if you recompute the rates yourself. "
      "Registration is not used and could not be: 62% of under-fives are registered and 51% have a "
      "certificate. One thing readers should know about the source: INS's own website is unreachable, and "
      "archived copies of it from mid-2025 onward serve gambling spam under the same article and file "
      "paths the real reports used — so anything dated 2025 or later and sourced only from that domain "
      "needs checking elsewhere. That includes the claim that the 2024 census has published preliminary "
      "results, which we could not confirm. The documents used here are archived copies from 2018.",
      "https://www.dhsprogram.com/pubs/pdf/FR353/FR353.pdf"),
    C("Romania", "INS — births by age of mother over the resident population", romania_tfr, "Romania",
      "complete", True,
      "The institute's database carries births by the mother's age group on a usual-residence basis "
      "from 2012, resident population by age at 1 July from 2002, and its own age-specific rates — but "
      "no total. It publishes the general fertility rate and the age-specific rates, and leaves the sum "
      "to the reader.",
      "We divided births by women in each band and summed. Our figures match what INS's own published "
      "age-specific rates imply, to four decimals, in all thirteen years: 1.3757 against 1.376 for 2024, "
      "1.8596 against 1.8595 for 2019.",
      "Romania rose and then fell hard: 1.36 in 2012 up to 1.86 by 2019, back to 1.38 by 2024 — the whole "
      "gain given back in five years. The choice of population is the thing to get right, and INS says so "
      "itself. It maintains two: the resident population, meaning everyone whose usual residence is in "
      "the country, and the population by domicile, meaning citizens registered as living there whether "
      "or not they do. Its own guidance is that only the resident figures should be used for "
      "international comparison, and in a country with emigration on Romania's scale the two differ "
      "materially. Births are dated to when they happened, but a year is not final until late "
      "registrations from the following three years are folded in, so recent values still move. Getting "
      "the data out took a browser: the export endpoint needs the selection posted back to it, and asking "
      "for every category at once returns an empty result rather than an error.",
      "http://statistici.insse.ro:8077/tempo-online/"),
    C("Rwanda", "NISR — Demographic and Health Survey rounds", rwanda, "Rwanda", "survey", False,
      "NISR runs the health survey every five years and the 2025 report charts every round since 1992. "
      "It also publishes census fertility, and — unusually — an annual fertility rate built from civil "
      "registration.",
      "We read the survey series. We also recomputed the 2022 census figure from its own women counts "
      "and age-specific rates and got 3.635 against the 3.63 it publishes.",
      "Rwanda is the clearest case of an office changing its mind between census rounds using the same "
      "test. In 2012 the raw census figure was 3.8 against the survey's 4.6 two years earlier; NISR said "
      "a 20% fall in three years was unlikely, concluded births were under-reported, tried three indirect "
      "methods, rejected two on stated grounds and adopted Arriaga, publishing 4.02. In 2022 it ran the "
      "equivalent check, found no evidence of under-reporting, and published the raw 3.6 unadjusted. Same "
      "office, same diagnostic, opposite decisions. There is also a third channel worth knowing about: "
      "NISR publishes a registration-based rate every year, scaling registered births up by its own "
      "measured completeness — 3.2 becomes 3.5 for 2025 — and prints it beside the census and survey "
      "figures in one table. Registration coverage is about 93% and rising, which is high for the region. "
      "The census and survey figures, 14% apart in 2020, have essentially converged: 3.6 against 3.7.",
      "https://statistics.gov.rw/"),
    C("Tunisia", "INS — registered births over the mid-year population", tunisia, "Tunisia", "complete",
      False,
      "INS publishes a fertility rate every year, built from civil registration, and the statistical "
      "yearbook prints the births by the mother's age group and the mid-year population behind it.",
      "We read the published series. We also rebuilt 2023 from those counts: dividing them directly gives "
      "1.489, but 7,709 of the 135,148 births have no age recorded, and spreading those across the bands "
      "in proportion gives 1.579 — INS's published 1.58, and its printed age-specific rates almost "
      "exactly. That is how the office handles them, and dropping them instead would understate the rate "
      "by 6%.",
      "Tunisia's fall is steep and recent: 2.4 in 2015 to 1.58 in 2023. Only the yearbook's most recent "
      "year carries the age breakdown, so a full recalculated series would mean opening one edition per "
      "year. Two things we could not settle. INS never states whether a birth is dated to the year it "
      "happened or the year it was registered, and it mentions no late-registration correction for live "
      "births — though it does say that more than half of stillbirths go unregistered. And the population "
      "is all residents rather than citizens, which in Tunisia hardly matters: the 2024 census puts "
      "foreign nationals at 0.55% of the population.",
      "https://www.ins.tn/statistiques/112"),
    C("Burundi", "ISTEEBU — Demographic and Health Survey rounds", burundi, "Burundi", "survey", False,
      "Two survey rounds, 2010 and 2016-17. The 2008 census publishes its fertility tables as "
      "spreadsheets — women and births by age group, and separately children ever born.",
      "We read the survey figures. We also divided the census's own counts and got 5.954 against the 5.96 "
      "it publishes, which confirms the published figure is the raw one.",
      "No correction was applied and none is mentioned anywhere — and for once we can see why that was "
      "reasonable. The census publishes both the births in the previous twelve months and the children "
      "ever born, so the standard ratio check can be run on it; doing so gives values close to one at the "
      "ages usually trusted, unlike Mali's or Senegal's. So a correction would probably have changed "
      "little. That is our inference, not the office's stated reason. Civil registration produces "
      "quarterly counts but no rate. Two things to know about the source. The office was renamed in 2022 "
      "and its old domain has since been squatted — it now serves a placeholder page, so the census "
      "spreadsheets had to come from a web archive. And the 2024 census has published only preliminary "
      "population totals; there is no fertility page on the new site at all.",
      "https://www.insbu.bi/"),
    C("Haiti", "Ministry of Health — EMMUS survey rounds", haiti, "Haiti", "survey", False,
      "Three survey rounds, the newest from 2016-17. The 2003 census publishes its fertility tables as "
      "scanned images of printed pages.",
      "We read the survey figures. We also divided the census's own counts and got 3.53, which is what "
      "the institute's rounded statement of 4 children is built from — there is no more precise or "
      "adjusted number being simplified away.",
      "Haiti has had no fertility figure since 2016-17, and no census since 2003. The reason is stated "
      "plainly by the statistics institute itself, which is rare: its 2024 population report says the "
      "census cannot be run because of the security situation, that vital registration is too weak to "
      "use, and that it is therefore adopting the UN's own projections rather than producing estimates of "
      "its own. Its fertility and vital-statistics pages are live and empty, marked as pending. One "
      "attribution point matters here. The surveys everyone cites as Haiti's fertility rate are run by a "
      "child-health institute for the health ministry, with an American contractor named as co-preparer; "
      "the statistics institute is credited only as a collaborator. They are still Haitian government "
      "figures, which is why they are plotted, but the institute's own last fertility figure is the 2003 "
      "census. The national archives, which would hold civil registration, no longer resolve at all.",
      "https://ihsi.gouv.ht/recensement/resultat_rgph_2003"),
    C("South Sudan", "NBS — household and multiple-indicator surveys", south_sudan, "South Sudan",
      "survey", False,
      "Two figures exist: 7.5 from the 2010 household health survey, run by the health ministry with the "
      "statistics bureau, and 6.4 from the bureau's own 2025 survey, published as a preliminary report in "
      "2026. The 2008 census tables, from before independence, publish women and births by age group.",
      "We read both figures. We also divided the 2008 census's own counts and got 3.9 — half the survey's "
      "figure two years later.",
      "That gap is the finding, and it is a warning about census fertility questions generally. The 2008 "
      "census asked women how many children they had borne in the previous twelve months; dividing those "
      "answers gives 3.9, while a survey asking for full birth histories two years later gives 7.5. "
      "Nobody thinks fertility doubled. Recent births are simply missed when asked about that way — which "
      "is exactly what the offices that apply Brass and Arriaga corrections are trying to fix, and here we "
      "can see the size of the problem uncorrected. That spread is why South Sudan has the largest gap "
      "against the UN of any country here, by a wide margin — about 2.7 children. Its own instruments range "
      "from 3.9 to 7.5 and the UN fits a smooth decline below all of them, from 6.0 in 2005 to 3.7 now. The "
      "2025 survey is also a preliminary report published in 2026, after the UN's last revision, so it "
      "could not have been taken into account. This is a country where the honest answer is that nobody "
      "knows the level, not one where a single source is wrong. South Sudan has had no census since 2008, none since "
      "independence, and no announced date for one. The bureau's own site is live and well stocked; the "
      "two domains that look like it are an expired-hosting placeholder and a retired domain answering "
      "403. Birth registration stands at 36%, and the finance ministry's own review calls the state of "
      "the system pathetic. One document that looks like it should count does not: the 2013 population "
      "estimation survey has no fertility figure in it.",
      "https://nbs.gov.ss/"),
    C("Bolivia", "INE — Demographic and Health Survey rounds", bolivia, "Bolivia", "survey", False,
      "INE runs the health survey every few years and its 2023 fertility report charts all five rounds "
      "since 1998. The 2024 census asks women how many children they have ever borne, and INE publishes "
      "those counts. Its population projection carries a fertility assumption of its own.",
      "We read the survey series. There is nothing to recompute as a rate: no Bolivian source publishes "
      "births by age of mother, in any year, from any instrument.",
      "Bolivia runs the Latin American pattern backwards, and it is the only country here that does. "
      "Everywhere else — Colombia, Mexico, Peru, Argentina, Chile, Ecuador — the office headlines a figure "
      "above its own registered births. INE's headline projection assumes 1.69 for 2024 against a survey "
      "figure of 2.1 for 2023, so the headline is the lower number. It says why. It treats the survey as "
      "its most reliable source, built a smoothed curve close to it, then checked that curve against three "
      "administrative series — birth registrations, school enrolment, and health-ministry birth records — "
      "found all three implied less fertility, and lowered the adopted figure accordingly. It also "
      "declined to apply the Brass correction it had used on the 2001 and 2012 censuses, saying it was "
      "incompatible with how fast fertility had fallen. Our own rough check agrees with the direction: "
      "registered births over projected women imply something near 1.4 for 2024. One trap to record: the "
      "electoral court's statistical bulletin looks exactly like a vital-statistics release, but its "
      "births table counts birth-certificate printouts — 2.7 million for 2021, about thirteen times the "
      "real number.",
      "https://www.ine.gob.bo/"),
    C("Tajikistan", "Agency on Statistics — registered births over the resident population",
      tajikistan, "Tajikistan", "complete", False,
      "The demographic yearbook prints the fertility rate for every year since 1989, the age-specific "
      "rates behind it, and the female population by single year of age for benchmark years.",
      "We read the published series. We also multiplied the 2023 age-specific rates by the women in each "
      "band and got 3.032 against the published 3.016, and implied births of 250,616 against the 250,285 "
      "the agency separately reports as registered. That 0.13% agreement between two independently "
      "tabulated numbers is a good sign for both.",
      "Tajikistan is the third Central Asian country here whose own counted fertility has risen: 2.64 in "
      "2021 to 3.02 in 2023, after Uzbekistan and Kazakhstan. Raw birth counts by age of mother are not "
      "published anywhere, only the rates, so the series is copied rather than rebuilt. Two caveats from "
      "the agency's own pages. It flags 2002 to 2017 as preliminary or estimated, and 2007's value of "
      "2.35 is a one-year drop of nearly a full child that it never explains — treat it as possibly an "
      "artefact of the series rather than a demographic event. And the denominator is the registered "
      "resident population, which by the agency's own definition includes people temporarily absent. "
      "Tajikistan has one of the highest labour-emigration rates in the world, so a large number of "
      "working-age men abroad are still counted at home; that mostly affects the male side, and our "
      "reconciliation suggests any effect on women of childbearing age is second-order.",
      "https://www.stat.tj/ru/soczialno-demograficheskij-sektor/"),
    C("Sweden", "SCB — births by age of mother over the mean population", sweden_tfr, "Sweden",
      "complete", True,
      "SCB's database serves births by single year of the mother's age from 1968, mean population by "
      "single year of age, and its own rate — all without a key.",
      "We divided births at each single age by the women of that age and summed. Our 1.4245 for 2024 "
      "against SCB's published 1.43, and 1.4466 against 1.45 for 2023.",
      "Sweden's rate is at a record low and still falling: 1.85 in 2016 to 1.42 in 2025 on SCB's own "
      "figures. The half-percent gap in our numbers is a definition rather than an error, and it is worth "
      "knowing about. The public births table records the age the mother had reached by the end of the "
      "year; SCB's own rate uses her age at the birth itself. Summing SCB's own five-year age-specific "
      "rates instead reproduces its published figure exactly, which is how we know that is the whole of "
      "the difference. SCB says outright that the denominator is the mean of the population at the start "
      "and the end of the year, and it covers everyone registered as resident whatever their citizenship. "
      "Two traps in the interface: an oversized request is refused with a bare 403 rather than a truncated "
      "answer, and the current-year population table carries a marital-status code that is a total across "
      "the others, so adding it in doubles the population.",
      "https://www.scb.se/be0101"),
    C("Jordan", "DOS — Population and Family Health Survey", jordan, "Jordan", "survey", False,
      "DOS publishes registered births every year, back to 2000, and female population by age group — but "
      "the births are broken down only by governorate and the child's sex, never by the mother's age. So "
      "the rate comes from the survey, and only from the survey.",
      "We read the survey figures. The registry cannot give a fertility rate, but it does reconcile: "
      "registered births over the mid-year population give a crude birth rate of 15.26 for 2024 against "
      "DOS's published 15.3.",
      "The number to be careful with is which population it describes. About a third of Jordan's residents "
      "are not Jordanian, roughly half of those Syrian, and the 2023 survey publishes the rate separately "
      "for each group: 2.5 for Jordanians, 4.1 for Syrians — 4.9 for those in camps and 3.9 outside — and "
      "2.1 for other nationalities, against 2.6 for everyone. That is a wider spread than Saudi Arabia's, "
      "and unlike Saudi Arabia it appears in one survey table rather than as a standing series. Two things "
      "to watch when citing DOS. Its \"Jordan in Figures\" booklets carry old survey figures under the "
      "current year's column with only a footnote — the 2.7 printed for 2022 is 2017-18 survey data. And "
      "registered births have fallen 16% since 2019, from 197,000 to 166,000, with no explanation offered.",
      "https://dosweb.dos.gov.jo/"),
    C("Honduras", "INE — ENDESA survey rounds", honduras, "Honduras", "survey", False,
      "Four survey rounds since 2001. INE also publishes registered births by the mother's age group every "
      "year from 2010, and the 2013 census publishes both the women and the births in the previous twelve "
      "months.",
      "We read the survey series. We also divided the 2013 census's own counts and got 2.13, against the "
      "2.74 INE adopted for that year — a headline 29% above what its own census counted.",
      "Honduras is the one country here where an office published a registration-based rate and then "
      "stopped. Its vital-statistics releases for 2013-14 and 2015-16 print the full working — age, "
      "births, women, each rate, the total — giving 2.45 to 2.68, which then tracked the survey and "
      "projection figures closely. From the 2021-22 release onward it keeps the birth counts by age and "
      "drops the rate, while the text still says fertility is falling without giving a number. No "
      "correction method is named anywhere; what INE does instead is take the census's age pattern and "
      "substitute the survey's level, for stated reasons of internal consistency rather than any criticism "
      "of the census. Registered births have fallen 27% since 2018, from 181,000 to 132,000. Two "
      "cautions. There has been no census since 2013 — the next is still in preparation — so any recent "
      "denominator is a projection eleven years old, and INE itself points to emigration of women of "
      "childbearing age. And the census's own twelve-month question found fewer births than the registry "
      "recorded for the same period, which is the reverse of the usual direction.",
      "https://ine.gob.hn/"),
    C("Guatemala", "INE — registered births over its population projections", guatemala_tfr,
      "Guatemala", "complete", True,
      "INE publishes no fertility rate of its own from the registry — its bulletin headlines a crude birth "
      "rate and adolescent rates. But it does publish the raw records, one row per birth, with the "
      "mother's age and the year the birth happened, and municipal population projections by single year "
      "of age.",
      "We counted the births by age of mother out of 2.2 million individual records and divided by the "
      "projected women. There is no published total to check against; our figures reproduce the office's "
      "own reported counts exactly, which is as far as verification can go here.",
      "Guatemala runs the Latin American pattern, and the gap is widening fast. Its projection assumes "
      "2.44 for 2022-23 and 2.33 for 2024-25; the registry gives 2.19 for 2023 and 1.90 for 2024. So the "
      "gap roughly doubles in a year, from about 0.2 to about 0.45. Births are dated to the year they "
      "occurred with a six-month window for late registration, which INE says follows international "
      "recommendations, and about 12% of each year's records are late registrations — a stable share, so "
      "the 2024 figure should not move much. Two things bound what we can say. The denominator is a "
      "projection built on the 2018 census, and INE's own projections build in sustained net emigration, "
      "so the population base is not naive. And INE's main website is behind a bot wall that blocks every "
      "request; only its open-data portal answers, and its statistical tables are not reachable at all.",
      "https://datos.ine.gob.gt/dataset/estadisticas-vitales-nacimientos"),
    C("Azerbaijan", "State Statistical Committee — registered births over the average annual population",
      azerbaijan, "Azerbaijan", "complete", False,
      "The committee publishes its own rate for every year since 1959, the age-specific rates behind it, "
      "and births by age of mother as counts back to 1970 — all as plain spreadsheets at stable addresses, "
      "no key and no interface to negotiate.",
      "We read the published series. Its own age-specific rates sum to its own total exactly in every year "
      "we checked. What we could not do is rebuild it independently, and the reason turned out to be "
      "interesting.",
      "There is no annual female population by age group anywhere on the site — only census years and one "
      "current snapshot. Dividing the published births by the census-enumerated women gives 1.75 against a "
      "published 1.8 for 2019, but 1.83 against 2.3 for 2009 and 1.72 against 2.0 for 1999. The recomputed "
      "figure is lower every time, and the gap shrinks to almost nothing at the most recent census. That "
      "is what it looks like when a rate is computed each year from a population estimate rolled forward "
      "from the last census, the next census reveals the estimate was too low, and the historical rates are "
      "never revised. So the published levels for the 2000s may be somewhat overstated — not by our "
      "arithmetic, but by the committee's own later census. Births are dated to when they happened and the "
      "denominator is the average of the population at the start and end of the year, both stated in its "
      "methodology. The 2019 census excluded territories then under occupation, and the population series "
      "since shows no discontinuity from their reincorporation.",
      "https://www.stat.gov.az/source/demoqraphy/az/002_3.xls"),
    C("United Arab Emirates", "FCSC — Emirati women only", None, "United Arab Emirates", "none", False,
      "The federal statistics centre publishes a fertility rate for Emirati women — 3.1 for 2022, down "
      "from 3.7 in 2016 — and a crude birth rate for everybody. It publishes no rate for non-Emirati "
      "residents and no combined rate at all.",
      "Nothing. Every federal and emirate statistics host refuses connections from outside the country or "
      "answers with a bot challenge, so nothing here could be read from the source.",
      "This is the most extreme version of the nationality problem in the collection, and the reason there "
      "is nothing to plot. Emiratis are about 12% of residents. A rate for them alone is not comparable "
      "with the UN's figure for the country, and no comparable figure exists: the federal office publishes "
      "Emirati births and an Emirati rate, but its own current population series carries an explicit note "
      "that the split by nationality is unavailable — so the numerator is public and the denominator is "
      "not. Only Dubai has ever published a rate for non-nationals, a single 1.2 for 2014 against 3.4 for "
      "nationals. There has been no census since 2005; the 2010 one was cancelled and replaced by an "
      "identity register rather than a survey. Every figure quoted here comes from an academic database "
      "that republishes the office's tables with citations, because the office's own site could not be "
      "reached — which is why we are not plotting any of it.",
      "https://uaestat.fcsc.gov.ae/"),
    C("Czechia", "ČSÚ — registered births over the mid-year population", czechia_tfr, "Czechia",
      "complete", False,
      "One spreadsheet carries the office's own rate from 1950 and the fertility rates for every single "
      "year of the woman's age behind it. The yearly demographic yearbooks also publish births by single "
      "year of the mother's age as counts, one archive per year.",
      "We read the published series and its own age decomposition. Summing the single-age rates "
      "reproduces the printed total, and dividing the yearbook's own counts by its mid-year population "
      "gives 1.3675 against a published 1.3679 for 2024 — the same 0.0004 gap in every year, which is "
      "rounding in the printed population.",
      "Czechia rose and fell as sharply as Romania: 1.71 in 2018, a peak of 1.83 in 2021, then 1.37 by "
      "2024 and a preliminary 1.28 for 2025 — the steepest fall in the modern series. The denominator is "
      "the population at midnight between 30 June and 1 July, and it covers all usual residents; from "
      "2022 that explicitly includes people granted temporary protection, which is how Ukrainian refugees "
      "enter it. Both sides of the rate move with them: women of childbearing age jumped by about 77,000 "
      "in 2022, and mothers born in Ukraine went from 1.9% of births in 2021 to 6.3% in 2024. The office "
      "does not flag this on the fertility series itself, but publishes the tables that show it.",
      "https://csu.gov.cz/produkty/demograficka-prirucka-2024"),
    C("Hungary", "KSH — registered births over the mid-year population", hungary_tfr, "Hungary",
      "complete", False,
      "The office publishes its rate for every year since 1900, the age-specific rates by five-year band "
      "since 1980, and population by single year of age — all as small files at stable addresses.",
      "We read the published series. Multiplying its own rates by the mid-year women gives 88,393 births "
      "for 2022 against the 88,491 it reports, and a rate of 1.549 against its 1.55.",
      "Hungary is the clearest case of a rise reversing. Its rate went from 1.23 in 2011 to a peak of "
      "1.61 in 2021 under an explicit pro-natalist policy, then fell every year after: 1.55, 1.51, 1.39, "
      "and 1.31 for 2025 — below where it stood in 2013. Births are down from 93,039 in 2021 to 72,017 in "
      "2025. Raw births by age of mother are not published anywhere free, so the rate cannot be rebuilt "
      "from counts; the office's legacy database, which might hold them, refuses every request. Two "
      "things to note. The population series was revised back to 2013 on the basis of the 2022 census, "
      "and the office does not publish a before-and-after comparison, so the size of that revision is not "
      "visible. And its own pages need a browser-shaped request: a plain one gets a rejection page under "
      "an HTTP 200.",
      "https://www.ksh.hu/stadat_files/nep/hu/nep0006.csv"),
    C("Cuba", "ONEI — registered births over the mean population", cuba, "Cuba", "complete", False,
      "The demographic yearbook publishes the rate, the age-specific rates, births by the mother's age "
      "group and the mean female population by age group — as spreadsheets, one edition per year.",
      "We read the published series. We also divided the 2024 counts and got 1.28897 against ONEI's "
      "published 1.2889651766 — an exact reproduction, once its own convention of folding births under 15 "
      "into the 15-19 band and births at 50 and over into 45-49 is followed.",
      "Cuba is the one country here where an office deliberately cut its own denominator to account for "
      "emigration, and said so. From 2021 it stopped counting the resident population and started "
      "counting the \"effective\" population — everyone actually present for at least 180 of the last "
      "365 days — because, in its words, it wanted to count the population as realistically as possible "
      "given the migratory context. Its published population fell from 11.18 million in 2020 to 9.43 "
      "million in 2025, and it publishes the net migration balance that explains it: about 1.26 million "
      "people over 2021 to 2024. So the sharp fall in the rate to 1.29 in 2024 is happening despite a "
      "denominator that has been shrunk, not because of one that has been left stale. Nothing is revised "
      "backwards, so the pre-2021 and post-2021 series can be joined; the only break is that deliberate "
      "change of definition. One trap in the tables: the row labelled TFG is the general fertility rate "
      "per thousand women, and the actual total sits a few rows below it labelled TGF.",
      "http://www.onei.gob.cu/sites/default/files/publicaciones/2025-07/00-anuario-demografico-2024.pdf"),
    C("Papua New Guinea", "NSO — Demographic and Health Survey and socio-demographic survey",
      papua_new_guinea, "Papua New Guinea", "survey", False,
      "Three figures, all from surveys: 4.4 from 2006, 4.2 from 2016-18, and 3.72 from the 2022 "
      "socio-demographic survey. The 2022 report publishes the women, the children ever born and the "
      "births in the previous twelve months behind its figure.",
      "We read the published figures. We also summed the 2022 survey's own reported rates and got 3.273, "
      "matching the raw figure it prints before adjustment, and the 2016-18 rates give 4.195 against its "
      "published 4.2.",
      "The striking thing about Papua New Guinea is that its censuses have never produced a fertility "
      "rate at all. The 2011 census asked the questions and never published the answers; the 2024 census "
      "asked six questions and fertility was not among them. Every figure the country has comes from a "
      "purpose-built survey. The 2022 one is a fully worked adjustment case: the raw figure is 3.27, a "
      "Brass ratio correction of about 14% gives the adopted 3.72, and a Gompertz model would have given "
      "3.93 — and the office says why it chose the first, that it keeps the age pattern of the original "
      "data and corrects only the level. The census history is its own story: the 2011 census was "
      "followed by a 2021 one that was deferred twice and replaced by a satellite-based population model "
      "putting the country at 11.8 million, against the 10.2 million the census eventually counted in "
      "2024. Nothing published reconciles the two. Birth registration was 13% at the last measurement.",
      "https://www.nso.gov.pg/"),
    C("Dominican Republic", "ONE — ENHOGAR-MICS household survey", dominican_republic,
      "Dominican Republic", "survey", False,
      "The 2019 household survey is the only fertility rate the office publishes from measurement. Its "
      "population projection carries an assumption of its own, and its vital-statistics yearbooks publish "
      "registered births by the mother's age.",
      "We read the survey figure. We also divided the 2010 census's own counts, which gives 2.51, and the "
      "2022 registered births by the projected women, which gives 1.86 — but see the caveat.",
      "That 1.86 is not a fertility rate and should not be read as one, which is why it is not plotted. "
      "The office publishes a full matrix of when births were registered against when they happened, and "
      "it shows registration continuing for two decades: the 2020 birth cohort was 141,548 when first "
      "counted and 159,466 two years later. So any recent year's registered births are a lower bound that "
      "will keep rising, and a rate built on them understates by an unknown and shrinking amount. The "
      "office states two reasons registration lags: 18% of 2022 registrations were late, and a mother "
      "needs an identity card to register a birth at all — it notes explicitly that the fall in "
      "registered births to mothers under 15 may reflect undocumented mothers rather than fewer births. "
      "15% of registered births in 2022 were to Haitian mothers. The 2022 census has published a fertility "
      "volume, but every archived copy of it is truncated and the live site is behind a bot wall, so its "
      "figure is genuinely unknown to us.",
      "https://www.one.gob.do/"),
]

PANELS = [(c["name"], c["src"], c["loader"], c["wpp_name"]) for c in COUNTRIES if c["loader"]]
DOCS = {c["name"]: (c["found"], c["method"], c["caveats"], c["url"]) for c in COUNTRIES}
