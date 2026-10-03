"""Canonical display names for OWID dataset owners.

Single source of truth for:
- the wizard auto-fill in `apps/wizard/etl_steps/forms.py`
- the git-log heuristic in `apps/owners/propose_owners.py`
- the backport heuristic that maps legacy grapher dataset creators

The schema enum in `schemas/dataset-schema.json` lists the union of the two
lists below and must be kept in sync by hand when someone joins or leaves.
"""

# Current data-team members. New datasets default to one of these via
# the Wizard auto-fill.
OWID_DATA_TEAM: list[str] = [
    "Pablo Rosado",
    "Pablo Arriagada",
    "Mojmír Vinkler",
    "Lucas Rodés-Guirao",
    "Tuna Acisu",
    "Fiona Spooner",
    "Edouard Mathieu",
]

# Former employees / non-data-team contributors who originally created
# datasets we still ship. They appear as owners on legacy backported
# datasets (per the dataset.createdByUserId in grapher MySQL). Stay on
# the list once added — matches the article-author convention.
OWID_LEGACY_CREATORS: list[str] = [
    "Hannah Ritchie",
    "Max Roser",
    "Joe Hasell",
    "Esteban Ortiz-Ospina",
    "Diana Beltekian",
    "Sandra Tzvetkova",
    "Saloni Dattani",
    "Sophie Ochmann",
    "Charlie Giattino",
    "Cameron Appel",
    "Bastian Herre",
    "Bobbie Macdonald",
    "Ruby Mittal",
    "Daniel Gavrilov",
    "Aibek Aldabergenov",
    "Gyorgy Attila Ruzicska",
    "Marco Molteni",
    "Jaiden Mispy",
    "Bernadeta",
    "Shahid Ahmad",
    "Breck Yunits",
    "Choo Yan Min",
    "Mallika Snyder",
    # Surfaced from the fast-track Drive CSV
    "Marcel Gerber",
    "Bertha Rohenkohl",
    "Daniel Bachler",
    # Former OWID engineer; original author of several legacy garden steps.
    "Lars Yencken",
    "Veronika Samborska",
]

# Union of the two lists — used by the schema-enum guard and by lookups
# that don't care whether someone is currently active.
ALL_OWNERS: list[str] = OWID_DATA_TEAM + OWID_LEGACY_CREATORS

# Maps a git author name (case-sensitive) or email local-part to a canonical
# display name. Covers the variants we see in `git log` on this repo.
_GIT_AUTHOR_TO_OWNER: dict[str, str] = {
    # Current team
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
    "Tuna": "Tuna Acisu",  # she used to commit under just "Tuna"
    "spoonerf": "Fiona Spooner",
    "Fiona Spooner": "Fiona Spooner",
    "edomt": "Edouard Mathieu",
    "Edouard Mathieu": "Edouard Mathieu",
    # Legacy creators — surface their direct git contributions too, not
    # only the DB-derived backport path.
    "Hannah Ritchie": "Hannah Ritchie",
    "hannahritchie": "Hannah Ritchie",
    "Max Roser": "Max Roser",
    "MaxCRoser": "Max Roser",
    "Joe Hasell": "Joe Hasell",
    "joehasell": "Joe Hasell",
    "Esteban Ortiz-Ospina": "Esteban Ortiz-Ospina",
    "Diana Beltekian": "Diana Beltekian",
    "Sandra Tzvetkova": "Sandra Tzvetkova",
    "Saloni Dattani": "Saloni Dattani",
    "Sophie Ochmann": "Sophie Ochmann",
    "Charlie Giattino": "Charlie Giattino",
    "Cameron Appel": "Cameron Appel",
    "Bastian Herre": "Bastian Herre",
    "bastianherre": "Bastian Herre",
    "Bobbie Macdonald": "Bobbie Macdonald",
    "Ruby Mittal": "Ruby Mittal",
    "Daniel Gavrilov": "Daniel Gavrilov",
    "Aibek Aldabergenov": "Aibek Aldabergenov",
    "Gyorgy Attila Ruzicska": "Gyorgy Attila Ruzicska",
    "Marco Molteni": "Marco Molteni",
    "Jaiden Mispy": "Jaiden Mispy",
    "Bernadeta": "Bernadeta",
    "Shahid Ahmad": "Shahid Ahmad",
    "Breck Yunits": "Breck Yunits",
    "Choo Yan Min": "Choo Yan Min",
    "Mallika Snyder": "Mallika Snyder",
    "Marcel Gerber": "Marcel Gerber",
    "Bertha Rohenkohl": "Bertha Rohenkohl",
    "Daniel Bachler": "Daniel Bachler",
    "Lars Yencken": "Lars Yencken",
    "larsyencken": "Lars Yencken",
}

_ALL_OWNERS_SET: set[str] = set(OWID_DATA_TEAM) | set(OWID_LEGACY_CREATORS)


def resolve_owner(git_name_or_email: str | None) -> str | None:
    """Return the canonical OWID display name for a git author, or None.

    Accepts a committer name, a full email, or the local-part of an email.
    Falls back to an exact-name match against the union of OWID_DATA_TEAM
    and OWID_LEGACY_CREATORS so we don't lose contributions whose git
    author already matches the canonical display name.
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
    if s in _ALL_OWNERS_SET:
        return s
    return None
