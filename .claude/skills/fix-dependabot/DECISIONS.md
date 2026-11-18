# fix-dependabot: decisions

The calls in `SKILL.md` that were hard-won or easy to undo by accident, each with its reason and the case that caused it. This file isn't loaded when the skill runs; it is for whoever changes the skill.

Before changing `SKILL.md`, check the change against this list. If it reverses or weakens an entry, say so in the PR, then either keep the decision or rewrite the entry with the new reason. When a change settles a call someone could later undo, add an entry; wording fixes don't need one. `#N` is a PR in `owid/etl`.

## Scope

- **The goal is zero open Dependabot PRs, not only resolved alerts.** Dependabot also opens routine version-update PRs with no alert, and PRs stay open after `master` already has the requested version. The skill audits the open PR list, folds the remaining bumps into the one batch PR and closes the originals; a risky or major bump stays open and is flagged instead. (#6144, #6307)
- **The description routes by intent, not by a list of trigger phrases**: Dependabot alerts or PRs, CVEs or other vulnerable dependencies in owid/etl. Generic "upgrade outdated dependencies" requests aren't routed here, since the skill works from Dependabot's alerts and PRs. (#7360)

## Applying the upgrades

- **Bumps are applied on one batch branch rather than by merging Dependabot's branches**, so the whole sweep is one PR with one CI run. (#6307)
- **An npm package becomes a direct dependency only if the extension imports it; otherwise it is forced through `overrides`.** js-yaml was imported in one extension but resolved only transitively, and a pure dev-tooling transitive in another. (#6307)

## Verification and release

- **`make check-all`, not `make check`.** `make check` covers only the top-level code, so an upgrade that breaks a `lib/` package (gdown removing `fuzzy=`, used in `lib/datautils`) passed it. (#6106)
- **A changed `lib/*/pyproject.toml` gets a version bump.** The publish workflow rejects a version already on PyPI, which is what happened after the Dependabot fixes in #6104. (#6106)
