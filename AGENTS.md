# AGENTS.md

## Purpose

This project builds a local agent that:

- Scrapes Bart Torvik men's CBB schedule and T-Rank predictions.
- Pulls Kalshi markets (winner/spread/total) for those games.
- Compares Bart predictions vs Kalshi and outputs CSV + Markdown summary.

## Outputs

- CSV goes to `data/` (path provided via `--csv`).
- Summary Markdown goes to `reports/` (default `reports/bart_kalshi_<date>_summary.md`).

## Summary rules

The summary table and bet notes must:

- Be ordered by game time.
- Exclude games that have already started.
- Mark TTQ >= 70 with a `*`.
- Include Bart predicted scores in each bet note line.

## Time handling

- In the raw HTML, Bart's schedule times are treated as Central (America/Chicago).
- All times shown in output (table + summary) must be converted to local time.
- "Already started" filtering must use the local time.

## Filter criteria

Include a game in the summary only when:

- TTQ > 50, and
- Either |Bart spread - Kalshi spread| >= 2 **or** |Bart total - Kalshi total| >= 2.

## Plain-language bet notes

At the bottom of each summary Markdown file, include a "Plain-language summary" section.
Each note must:

- Explain the spread or total discrepancy in plain language.
- Suggest a lean (side or total).
- Include the Bart predicted score.
- Include a `(TTQ 70+)` marker when TTQ >= 70.

## Files and structure

Keep a clear structure:

- `src/` for code (e.g., `src/bart_kalshi_agent.py`)
- `data/` for CSV outputs
- `reports/` for summary Markdown outputs

## CLI usage

Example:

```
python src/bart_kalshi_agent.py --date 20260111 --csv data/bart_kalshi_20260111.csv
```
