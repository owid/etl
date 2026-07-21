"""Metadata Review — collaborative review of user-facing chart text (FAUST) and metadata.

UI-agnostic core shared by the Metadata Review wizard app and the
`etl metadata-review` CLI:

- `targets`: dataclasses describing reviewable fields and pages.
- `resolution`: DB-based resolution of MDim / indicator fields with provenance
  (override / inherited / missing), ported from the faust-metadata-audit rules.
- `trace`: repo-aware back-tracing of a rendered field to its editable YAML source.
- `export`: the `suggestions.yml` handoff consumed by Claude to implement changes.
"""
