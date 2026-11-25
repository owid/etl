# GitHub Actions dependency locking

GitHub Actions dependencies, including transitive ones, are locked to commit SHAs in
[`.github/workflows/actions.lock`](https://docs.github.com/actions/concepts/security/workflow-dependency-lockfiles),
and the runner refuses any action that isn't in it. Workflows reference actions by a
full version tag, not a SHA:

```yaml
# Good — full version tag, locked in actions.lock
- uses: actions/checkout@v7.0.1

# Bad — mutable major tag
- uses: actions/checkout@v7
```

After adding or changing any `uses:` in `.github/workflows/` or `.github/actions/`, run
`gh actions-lock` (install once with `gh extension install github/gh-actions-lock`) and
commit the updated lockfile. Never edit `actions.lock` by hand. `owid/actions@main` is
locked too, so new commits there only take effect after `gh actions-lock --relock`.
The `GitHub Actions checks` workflow runs `gh actions-lock --verify` and zizmor on every
change under `.github/`.
