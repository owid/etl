# Red-team campaign state

Every one of the hundred countries sits in exactly one list below, and the four always add up to a
hundred. That is the point of the file: it makes the state of the campaign countable instead of
remembered.

- **In flight** — an agent is out on it now. Five at all times, one launched per report received.
- **Reported, awaiting write-up** — the agent came back and nothing has been done with it yet. This
  is the dangerous list. It should be empty by the end of every cycle; anything sitting in it is a
  report that arrived and was not acted on.
- **Analyzed** — written up in a findings log, with its findings applied or explicitly rejected.
- **To do** — not started, in population-rank order.

Countries are spelled exactly as `countries.py` spells them. The collection covers England and Wales
rather than the United Kingdom, so that is the name used for its rank.

Two commands, and both should be run at every launch and every report:

- `python redteam.py --sync` rewrites the four lists. In flight and awaiting write-up are carried
  over as they stand, because only a human knows them; analyzed is read from the findings logs, and
  to do is everything left over. That is what keeps the total at a hundred without anyone counting.
- `python redteam.py --audit` fails if the four do not partition the hundred, if a country appears
  twice, if anything is awaiting write-up, if analyzed disagrees with the logs, or if there are not
  exactly five agents in flight. That last check applies only while To do still has something in it.
  Once it empties the campaign is draining, the count of agents out can only fall, and requiring five
  would fail every cycle from then to the end.

One thing worth reusing: Poland had to be launched twice, because the first agent stalled for ten
minutes trying to enumerate variable IDs in the Polish statistics API. Its replacement was told not
to fight the interface and to fall back to published reports instead. Give that instruction to any
country whose source is an interface rather than a file.

Austria stalled the same way and had to be relaunched, which says the warning belongs in the prompt
from the start rather than only after a stall. The relaunch spelled out what "do not fight it" means:
two failed attempts at any one URL or tool, then switch to the office's static releases; and write the
report even with points unresolved, because a report with three "could not verify" lines beats no
report. Use that wording for any interface-backed country.

A previous session hit its cap of 200 subagents with 21 countries still to do; a fresh session resets
that cap, and the campaign resumed from the To do list without any loss. If it happens again, the state
lives entirely in this file and the findings logs — the brief generator, the standard prompt with its
absence-claim instruction, and `--sync`/`--audit` all work unchanged. Launch the first five from To do,
in the order listed.

Two other things a resuming session should know. The brief generator has caused six false findings, all
fixed: describing the map as an average gap; printing age-band figures at full float precision; printing
the series to four significant figures, so an agent read 1.25486 as "1.255" and reported a rounding bug;
rounding halves to even where offices round up, so an agent reported Cuba's women as one person short of
the source; and, for a country with nothing plotted, printing both a validation label and the standing
"this office publishes fertility rates only" sentence, neither of which any reader sees on such a page —
which had the UAE agent spend two of its five findings on text that is not published. Check anything a
report asserts about the page against the page. And for any country whose source is an interface rather
than a file, tell the agent not to fight it and to fall back on published reports; that is what unstuck
Poland.

## In flight (4)

- Hungary
- Austria
- Switzerland
- Sierra Leone

## Reported, awaiting write-up (0)


## Analyzed (96)

- India
- China
- United States
- Indonesia
- Pakistan
- Nigeria
- Brazil
- Bangladesh
- Russia
- Ethiopia
- Mexico
- Japan
- Egypt
- Philippines
- Democratic Republic of Congo
- Vietnam
- Iran
- Turkey
- Germany
- Tanzania
- Thailand
- England and Wales
- France
- South Africa
- Italy
- Kenya
- Myanmar
- Colombia
- Sudan
- Uganda
- South Korea
- Algeria
- Iraq
- Spain
- Argentina
- Afghanistan
- Yemen
- Canada
- Angola
- Ukraine
- Morocco
- Poland
- Uzbekistan
- Mozambique
- Malaysia
- Ghana
- Saudi Arabia
- Peru
- Madagascar
- Cote d'Ivoire
- Cameroon
- Nepal
- Niger
- Venezuela
- Australia
- North Korea
- Syria
- Mali
- Burkina Faso
- Sri Lanka
- Taiwan
- Malawi
- Zambia
- Chad
- Kazakhstan
- Somalia
- Chile
- Senegal
- Guatemala
- Romania
- Netherlands
- Ecuador
- Cambodia
- Zimbabwe
- Guinea
- Benin
- Rwanda
- Burundi
- Bolivia
- South Sudan
- Tunisia
- Haiti
- Belgium
- Dominican Republic
- Jordan
- United Arab Emirates
- Honduras
- Tajikistan
- Papua New Guinea
- Cuba
- Sweden
- Czechia
- Azerbaijan
- Portugal
- Greece
- Israel

## To do (0)


