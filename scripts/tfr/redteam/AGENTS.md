# Agents in flight

Five red-team agents run at all times: one country each, launched in population-rank order, one
replacement launched per report received.

This file is the ledger. Update it on every launch and every report, and count from it rather than
from memory — counting from memory got the number wrong twice.

The lists below are parsed, so keep the format: one `- Country` bullet per line, spelled exactly as
the registry spells it. `python redteam.py --audit` cross-checks them against the findings logs and
names any country that was launched but never written up. A launched country with no log entry is a
report that arrived and was never acted on, which is the failure this guards against.

## In flight

- Malaysia
- Ghana
- Saudi Arabia
- Peru
- Madagascar

## Reported

- Uganda
- Myanmar
- Sudan
- South Korea
- Algeria
- Democratic Republic of Congo
- Iraq
- Spain
- Poland
- Argentina
- Afghanistan

## Notes

Poland had to be launched twice: the first agent stalled with no output for ten minutes, trying to
enumerate variable IDs in the Polish statistics API. Its replacement was told not to, and to fall
back to published outputs instead. Worth reusing that instruction for any country whose source is an
interface rather than a file.
