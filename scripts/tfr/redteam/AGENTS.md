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
  exactly five agents in flight.

One thing worth reusing: Poland had to be launched twice, because the first agent stalled for ten
minutes trying to enumerate variable IDs in the Polish statistics API. Its replacement was told not
to fight the interface and to fall back to published reports instead. Give that instruction to any
country whose source is an interface rather than a file.

## In flight (5)

- Malawi
- Chad
- Somalia
- Chile
- Senegal

## Reported, awaiting write-up (0)


## Analyzed (63)

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
- Zambia
- Kazakhstan

## To do (32)

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
- Hungary
- Austria
- Switzerland
- Sierra Leone

