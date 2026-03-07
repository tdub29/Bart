# Bart Torvik & NHL vs Kalshi Comparators

This repo contains Python agents that compare predictions to Kalshi markets and output CSV + Markdown summaries.

## Two workflows

| Workflow | Purpose | Script | Data | Reports |
|----------|---------|--------|------|---------|
| **Bart (CBB)** | Men's college basketball: Bart Torvik T-Rank vs Kalshi | `src/bart_kalshi_agent.py` | `data/bart/` | `reports/bart/` |
| **NHL** | Jersey Mike's daily picks: NHL vs Kalshi | `src/nhl_kalshi_agent.py` | `data/nhl/` | `reports/nhl/` |

---

## Bart (CBB) — Bart Torvik vs Kalshi

1. Pulls the men's CBB schedule + T-Rank predictions from `https://barttorvik.com/schedule.php`
2. Pulls Kalshi markets (winner / spread / total) for those games
3. Outputs a per-game comparison (moneylines + implied score vs Bart's predicted score) plus TTQ

### Quick start (Bart)

```powershell
python -m pip install -r requirements.txt
python src/bart_kalshi_agent.py
```

### Pick a date (Bart)

Bart Torvik uses a `YYYYMMDD` date parameter. **Use 2026 for the year** (e.g. 20260213 for Feb 13, 2026).

```powershell
python src/bart_kalshi_agent.py --date 20260213
```

If you omit `--date`, the script uses whatever date Bart Torvik's schedule page defaults to.

### Export to CSV (Bart)

```powershell
python src/bart_kalshi_agent.py --date 20260213 --csv data/bart/bart_kalshi_20260213.csv
```

### Time zone (Bart)

In the raw HTML, Bart's schedule times are in **America/Chicago (Central)**. The script converts all output times to your local timezone (or your chosen `--tz`). On Windows you can force the output timezone:

```powershell
python src/bart_kalshi_agent.py --date 20260213 --tz America/Los_Angeles
```

---

## NHL — Jersey Mike's daily picks vs Kalshi

**Note:** Do not run the NHL agent before 2/25 (Kalshi markets are not available until then). Use **2026** for the year in dates.

The NHL agent compares NHL game lines (used for Jersey Mike's daily picks) to Kalshi markets and writes CSV + summary to `data/nhl/` and `reports/nhl/`.

```powershell
python src/nhl_kalshi_agent.py --date 20260202 --csv data/nhl/nhl_kalshi_20260202.csv
```

---

## Markdown summary (both)

By default each script writes a summary under `reports/bart/` or `reports/nhl/` containing games where:

- TTQ > 50
- and abs(predicted spread - Kalshi spread) >= 2 OR abs(predicted total - Kalshi total) >= 2

The summary table and picks are ordered by game time, exclude games that have already started, and mark TTQ >= 70 with a star. You can override the path with `--md`.

## Notes / assumptions

- Console output is tab-separated so you can copy/paste into Excel/Sheets easily.
- Bart Torvik blocks non-JS browsers on the first request; the Bart script mimics the required `js_test_submitted=1` POST to get the real schedule HTML.
- Kalshi data is pulled from their public `trade-api/v2` endpoints and is rate-limited; the scripts use a small delay + retry/backoff for HTTP 429 responses.
- Matching is done primarily by team names (with aliases + fuzzy matching fallback). If you see missing/incorrect joins, tell me which matchups and I'll tune the normalizer.
