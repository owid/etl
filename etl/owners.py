"""Canonical display names for the OWID data team.

Single source of truth for:
- the wizard auto-fill in `apps/wizard/etl_steps/forms.py`
- the backfill heuristic in `apps/owners/propose_owners.py`

The schema enum in `schemas/dataset-schema.json` lists the same names and must
be kept in sync by hand when someone joins or leaves the team.
"""

# Canonical display names (with diacritics). Order is not meaningful;
# the schema enum is the validation contract.
OWID_DATA_TEAM: list[str] = [
    "Pablo Rosado",
    "Pablo Arriagada",
    "Veronika Samborska",
    "Mojmír Vinkler",
    "Lucas Rodés-Guirao",
    "Tuna Acisu",
    "Fiona Spooner",
    "Edouard Mathieu",
]

# Maps a git author name (case-sensitive) or email local-part to a canonical
# display name. Covers the variants we see in `git log` on this repo.
_GIT_AUTHOR_TO_OWNER: dict[str, str] = {
    "Marigold": "Mojmír Vinkler",
    "Mojmir Vinkler": "Mojmír Vinkler",
    "mojmir.vinkler": "Mojmír Vinkler",
    "pabloarosado": "Pablo Rosado",
    "Pablo Rosado": "Pablo Rosado",
    "Pablo A Rosado": "Pablo Rosado",
    "paarriagadap": "Pablo Arriagada",
    "Pablo Arriagada": "Pablo Arriagada",
    "veronikasamborska1994": "Veronika Samborska",
    "Veronika Samborska": "Veronika Samborska",
    "lucasrodes": "Lucas Rodés-Guirao",
    "Lucas Rodés-Guirao": "Lucas Rodés-Guirao",
    "Lucas Rodes-Guirao": "Lucas Rodés-Guirao",
    "antea04": "Tuna Acisu",
    "Tuna Acisu": "Tuna Acisu",
    "spoonerf": "Fiona Spooner",
    "Fiona Spooner": "Fiona Spooner",
    "edomt": "Edouard Mathieu",
    "Edouard Mathieu": "Edouard Mathieu",
}


def resolve_owner(git_name_or_email: str | None) -> str | None:
    """Return the canonical OWID display name for a git author, or None.

    Accepts a committer name, a full email, or the local-part of an email.
    Returns None for bots and anyone not in `_GIT_AUTHOR_TO_OWNER`.
    """
    if not git_name_or_email:
        return None
    s = git_name_or_email.strip()
    if s in _GIT_AUTHOR_TO_OWNER:
        return _GIT_AUTHOR_TO_OWNER[s]
    if "@" in s:
        local = s.split("@", 1)[0]
        if local in _GIT_AUTHOR_TO_OWNER:
            return _GIT_AUTHOR_TO_OWNER[local]
    return None
