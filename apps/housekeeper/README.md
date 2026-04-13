# Housekeeper

Daily Slack bot that suggests charts for review to keep the OWID chart catalog clean.

## Why

Our database has a growing number of charts. Many are good, but several hundred are unmaintained, outdated, or get very few views. These charts clog our admin, internal search, and external search engines. Housekeeper helps us gradually clean up by surfacing one chart per day for review.

## What it does

Every day, the bot posts **two messages** to [`#chart-reviews`](https://owid.slack.com/archives/C087DMCTYM9):

### 1. Published chart review

Picks the published chart with the **fewest views** (that hasn't been reviewed recently) and asks whether to keep it online or unpublish it.

- Tags the **default reviewer** (configured in [`config.yaml`](config.yaml))
- Includes an AI-generated summary and suggestion (keep / unpublish / improve)
- Shows references (posts, explorers) and revision history in the thread

### 2. Draft chart review

Picks the draft chart with the **oldest last edit** and asks whether to delete or keep it.

- Tags the **chart's creator or last editor** (looked up from the `users` table)
- If the creator/editor is no longer at OWID, falls back to the default reviewer
- Reviewer overrides (e.g. Max -> Hannah) are configured in [`config.yaml`](config.yaml)

## How to run

In production, this runs daily via [`scripts/housekeeper.sh`](../../scripts/housekeeper.sh) on the ETL server.

To test locally, always use the `--dev` flag. It replaces Slack `<@userId>` mentions with code-formatted names (e.g. `` `Tuna Acisu` ``) so nobody gets pinged, and requires `--channel` to prevent accidentally posting to `#chart-reviews`:

```bash
uv run etl d housekeeper --review-type chart --dev --channel "#my-test-channel"
```

## Configuration

All configuration lives in [`config.yaml`](config.yaml):

| Field | Purpose |
|-------|---------|
| `charts.default_slack_id` | Slack user ID tagged on published chart reviews and as fallback for drafts |
| `charts.reviewer_overrides` | Map of Slack user ID replacements (e.g. redirect Max's tags to Hannah) |
| `charts.llm.model_name` | LLM model used for chart summaries |
| `charts.llm.system_prompt` | System prompt for the chart summary LLM |

To find Slack user IDs, check the `slackId` column in the MySQL `users` table.

## Key data sources

| Resource | Link |
|----------|------|
| Chart data (Metabase question 812) | http://metabase.owid.io/question/812 |
| User → Slack ID mapping (Metabase question) | http://metabase.owid.io/question#702 |
| Review history | MySQL table `housekeeper_reviews` |
| User Slack IDs | MySQL table `users` (`slackId` column) |

## Future work

- Add review types beyond charts (e.g. datasets)

## File overview

| File | Role |
|------|------|
| `cli.py` | CLI entry point (`etl d housekeeper`) |
| `charts.py` | Main logic: data fetching, message building, Slack posting |
| `utils.py` | LLM integration, review tracking, config loading |
| `config.yaml` | Reviewer settings and LLM configuration |
