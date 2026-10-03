---
applyTo: ".github/workflows/**,.github/actions/**"
---

# GitHub Actions version pinning

We use [pinact](https://github.com/suzuki-shunsuke/pinact) to manage GitHub Actions
and workflow versions. Action references in `.github/workflows/*` and
`.github/actions/*` must be pinned to immutable commit SHAs with version comments:

```yaml
# Good — pinned to a SHA with a version comment
- uses: actions/checkout@08c6903cd8c0fde910a37f88322edcfb5dd907a8 # v5.0.0

# Bad — mutable tag, not pinned
- uses: actions/checkout@v5
```

When adding or bumping an action, run `pinact run` to pin every reference and
refresh the version comments.
