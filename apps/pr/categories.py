# Add EMOJIs for each PR type
PR_CATEGORIES = {
    "data": {
        "emoji": "📊",
        "emoji_raw": ":bar_chart:",
        "description": "data update or addition",
    },
    "bug": {
        "emoji": "🐛",
        "emoji_raw": ":bug:",
        "description": "bug fix for the user",
    },
    "refactor": {
        "emoji": "🔨",
        "emoji_raw": ":hammer:",
        "description": "a code change that neither fixes a bug nor adds a feature for the user",
    },
    "enhance": {
        "emoji": "✨",
        "emoji_raw": ":sparkles:",
        "description": "visible improvement over a current implementation without adding a new feature or fixing a bug",
    },
    "feature": {
        "emoji": "🎉",
        "emoji_raw": ":tada:",
        "description": "new feature for the user",
    },
    "docs": {
        "emoji": "📜",
        "emoji_raw": ":scroll:",
        "description": "documentation only changes",
        "shortcut_key": "0",
    },
    "chore": {
        "emoji": "🐝",
        "emoji_raw": ":honeybee:",
        "description": "upgrading dependencies, tooling, etc. No production code change",
    },
    "style": {
        "emoji": "💄",
        "emoji_raw": ":lipstick:",
        "description": "formatting, missing semi colons, etc. No production code change",
    },
    "wip": {
        "emoji": "🚧",
        "emoji_raw": ":construction:",
        "description": "work in progress - intermediate commits that will be explained later on",
    },
    "tests": {
        "emoji": "✅",
        "emoji_raw": ":white_check_mark:",
        "description": "adding missing tests, refactoring tests, etc. No production code change",
    },
}
PR_CATEGORIES_MD_DESCRIPTION = "- " + "\n- ".join(
    f"**{choice}**: {choice_params['description']}" for choice, choice_params in PR_CATEGORIES.items()
)
PR_CATEGORIES_CHOICES = [
    {
        "title": f"{v['emoji']} {k}",
        "value": k,
        "shortcut_key": v.get("shortcut_key", k[0]),
    }
    for k, v in PR_CATEGORIES.items()
]
PR_CATEGORIES_CHOICES = sorted(PR_CATEGORIES_CHOICES, key=lambda x: x["shortcut_key"])
assert len(set([x["shortcut_key"].lower() for x in PR_CATEGORIES_CHOICES])) == len(
    PR_CATEGORIES_CHOICES
), "Shortcut keys must be unique"
