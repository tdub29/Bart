# AGENTS.md

## Dates and year

**All dates use year 2026.** The season and schedule are for 2026. When running for "current day" or when the user does not specify a date, use today's date with **YYYY = 2026** (e.g. 20260213 for Feb 13, 2026). Do not use 2025 or any other year unless the user explicitly provides it.

## Purpose

This project builds a local agent that:

- Scrapes Bart Torvik men's CBB schedule and T-Rank predictions.
- Pulls Kalshi markets (winner/spread/total) for those games.
- Compares Bart predictions vs Kalshi and outputs CSV + Markdown summary.

## Outputs

Reports use subfolders:

- **Bart (CBB)**: `data/bart/` for CSV, `reports/bart/` for summary MD
- **NHL**: `data/nhl/` for CSV, `reports/nhl/` for summary MD

- Bart CSV path via `--csv` (e.g. `data/bart/bart_kalshi_<date>.csv`)
- Bart summary default: `reports/bart/bart_kalshi_<date>_summary.md`
- NHL CSV path via `--csv` (e.g. `data/nhl/nhl_kalshi_<date>.csv`)
- NHL summary default: `reports/nhl/nhl_kalshi_<date>_summary.md`

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

Include a game in the summary when:

- **Hawaii games:** always include (if either team is Hawaii).
- **Northern Iowa games:** always include (if either team is Northern Iowa).
- **Tournament games:** always include (if the game's location indicates a tournament, e.g. conference tournament, NCAA, NIT, championship).
- **All other games:** TTQ > 50, and either |Bart spread - Kalshi spread| >= 2 or |Bart total - Kalshi total| >= 2.

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

## Game not in summary

When the user asks about a specific game that is **not** in the summary report, respond in this format every time:

1. **State** that the game is in the CSV but not in the summary (and briefly why: filter criteria).
2. **Table** with: Time (local), Bart spread (home), Kalshi spread (home), Bart total, Kalshi total, TTQ, and Bart predicted score.
3. **Bullets** for spread difference and total difference (with |diff|).
4. **Explain** why it didn’t make the summary: summary only includes games where TTQ > 50 and either |spread diff| >= 2 or |total diff| >= 2 (or the game is Hawaii, Northern Iowa, or a tournament game).
5. **Conclude** with whether there’s a lean from the comparison or not.

## CLI usage

**Bart (CBB):** Use 2026 for the year (e.g. 20260213 for Feb 13).
```
python src/bart_kalshi_agent.py --date 20260213 --csv data/bart/bart_kalshi_20260213.csv
```

**NHL:** Do not run until 2/25 (Kalshi markets not available before then). Use 2026 for the year.
```
python src/nhl_kalshi_agent.py --date 20260202 --csv data/nhl/nhl_kalshi_20260202.csv
```

**Cursor commands:** `/run` (both), `/bart` (CBB only), `/nhl` (NHL only). Use **current day in 2026** or user-provided YYYYMMDD. Do not run NHL before 2/25.
