# Bart Torvik <-> Kalshi CBB Comparator

This repo contains a small Python agent that:

1. Pulls the men's CBB schedule + T-Rank predictions from `https://barttorvik.com/schedule.php`
2. Pulls Kalshi markets (winner / spread / total) for those games
3. Outputs a per-game comparison (moneylines + implied score vs Bart's predicted score) plus TTQ

## Quick start

```powershell
python -m pip install -r requirements.txt
python src/bart_kalshi_agent.py
```

## Pick a date

Bart Torvik uses a `YYYYMMDD` date parameter.

```powershell
python src/bart_kalshi_agent.py --date 20260108
```

If you omit `--date`, the script uses whatever date Bart Torvik's schedule page defaults to.

## Export to CSV

```powershell
python src/bart_kalshi_agent.py --date 20260108 --csv data/bart_kalshi_20260108.csv
```

## Markdown summary

By default the script writes `reports/bart_kalshi_<date>_summary.md` containing the games where:

- TTQ > 50
- and abs(Bart spread - Kalshi spread) >= 2 OR abs(Bart total - Kalshi total) >= 2

The summary table and picks are ordered by game time, exclude games that have already started, and mark TTQ >= 70 with a star. Times shown in output are converted from Eastern to your local time zone. You can override the path with `--md`.

## Notes / assumptions

- Console output is tab-separated so you can copy/paste into Excel/Sheets easily.
- Bart Torvik blocks non-JS browsers on the first request; the script mimics the required `js_test_submitted=1` POST to get the real schedule HTML.
- Kalshi data is pulled from their public `trade-api/v2` endpoints and is rate-limited; the script uses a small delay + retry/backoff for HTTP 429 responses.
- Matching is done primarily by team names (with a few aliases + fuzzy matching fallback). If you see missing/incorrect joins, tell me which matchups and I'll tune the normalizer.
