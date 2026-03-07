"""
NHL Kalshi Agent

Fetches NHL game winner markets from Kalshi API for a given date,
outputs CSV + Markdown summary with the biggest favorites (not started yet).

Same workflow as bart_kalshi_agent: Kalshi API -> CSV -> summary MD.
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import requests


KALSHI_API_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
NHL_SERIES_TICKER = "KXNHLGAME"

# Kalshi event ticker suffix maps to (away_abbr, home_abbr) e.g. BUFFLA -> BUF, FLA
# Some use 3-letter codes: NYIWSH, OTTPIT, MTLMIN, STLNSH, etc.
_DISPLAY_NAMES = {
    "New York I": "NY Islanders",
    "New York R": "NY Rangers",
}

_NHL_ABBR = {
    "BUF", "FLA", "NYI", "WSH", "OTT", "PIT", "MTL", "MIN", "STL", "NSH",
    "TOR", "CGY", "VAN", "UTA", "DET", "COL", "WPG", "DAL", "SJ", "CHI",
    "LA", "EDM", "SEA", "VGK", "ANA", "BOS", "TB", "CAR", "CBJ", "NJ", "PHI", "NYR",
}


def _date_token_from_yyyymmdd(date: str) -> str:
    """Convert YYYYMMDD to Kalshi token e.g. 26FEB02."""
    dt = datetime.strptime(date, "%Y%m%d")
    return f"{dt.strftime('%y')}{dt.strftime('%b').upper()}{dt.strftime('%d')}"


def _parse_event_ticker_date(event_ticker: str) -> Optional[str]:
    """Extract YYYYMMDD from event ticker e.g. KXNHLGAME-26FEB02BUFFLA -> 20260202."""
    try:
        _, rest = event_ticker.split("-", 1)
    except ValueError:
        return None
    token = rest[:7]
    if not re.fullmatch(r"\d{2}[A-Z]{3}\d{2}", token):
        return None
    year = int(token[:2])
    mon = token[2:5]
    day = int(token[5:7])
    month_map = {
        "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
        "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
    }
    month = month_map.get(mon)
    if not month:
        return None
    return f"20{year:02d}{month:02d}{day:02d}"


def _parse_ticker_suffix(event_ticker: str) -> tuple[str, str]:
    """Parse team codes from suffix e.g. KXNHLGAME-26FEB02BUFFLA -> (BUF, FLA).
    Handles 5-char suffixes like BUFTB (3+2) and 6+ char like BUFFLA (3+3)."""
    try:
        _, rest = event_ticker.split("-", 1)
    except ValueError:
        return "", ""
    suffix = rest[7:].upper()  # BUFFLA, BUFTB, NYIWSH, etc.
    if len(suffix) >= 5:
        a = suffix[:3]
        h = suffix[3:]
        return a, h
    return "", ""


def _mid_price_cents(market: dict[str, Any]) -> Optional[float]:
    """Mid price from yes_bid/yes_ask in cents."""
    yes_bid = market.get("yes_bid", 0) or 0
    yes_ask = market.get("yes_ask", 0) or 0
    last_price = market.get("last_price", 0) or 0
    if 0 < yes_bid < 100 and 0 < yes_ask < 100:
        return (float(yes_bid) + float(yes_ask)) / 2.0
    if 0 < yes_bid < 100:
        return float(yes_bid)
    if 0 < yes_ask < 100:
        return float(yes_ask)
    if 0 < last_price < 100:
        return float(last_price)
    return None


@dataclass
class NHLGame:
    date: str
    event_ticker: str
    away_team: str
    home_team: str
    away_win_prob: Optional[float]
    home_win_prob: Optional[float]
    favorite: str
    favorite_win_prob: float
    underdog_win_prob: float
    expected_expiration_utc: Optional[str]
    status: str


class KalshiClient:
    def __init__(
        self,
        session: Optional[requests.Session] = None,
        *,
        min_request_interval_s: float = 0.15,
        max_retries: int = 6,
    ) -> None:
        self._session = session or requests.Session()
        self._min_request_interval_s = min_request_interval_s
        self._max_retries = max_retries
        self._last_request_at: Optional[float] = None

    def _get_json(self, url: str, *, params: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        params = params or {}
        attempt = 0
        while True:
            if self._last_request_at is not None:
                elapsed = time.monotonic() - self._last_request_at
                if elapsed < self._min_request_interval_s:
                    time.sleep(self._min_request_interval_s - elapsed)
            resp = self._session.get(url, params=params, timeout=60)
            self._last_request_at = time.monotonic()
            if resp.status_code == 429 and attempt < self._max_retries:
                time.sleep(min(30.0, 2.0**attempt))
                attempt += 1
                continue
            resp.raise_for_status()
            return resp.json()

    def get_nhl_events_for_date(
        self,
        date: str,
        *,
        limit: int = 200,
        max_pages: int = 15,
    ) -> list[dict[str, Any]]:
        """Fetch NHL game events for the given date (YYYYMMDD) with nested markets."""
        target_token = _date_token_from_yyyymmdd(date)
        matched: list[dict[str, Any]] = []
        cursor: Optional[str] = None
        url = f"{KALSHI_API_BASE_URL}/events"
        for _ in range(max_pages):
            params: dict[str, Any] = {
                "series_ticker": NHL_SERIES_TICKER,
                "limit": limit,
                "with_nested_markets": "true",
            }
            if cursor:
                params["cursor"] = cursor
            payload = self._get_json(url, params=params)
            page_events = payload.get("events", [])
            for ev in page_events:
                ticker = ev.get("event_ticker") or ""
                if target_token in ticker:
                    matched.append(ev)
            cursor = payload.get("cursor")
            if not cursor or not page_events:
                break
            for ev in page_events:
                ev_date = _parse_event_ticker_date(ev.get("event_ticker", ""))
                if ev_date and ev_date < date:
                    return matched
        return matched


def _parse_title(title: str) -> tuple[str, str]:
    """Parse 'Away at Home' -> (away, home)."""
    base = (title or "").split(":")[0].strip()
    base = re.sub(r"\s+Winner\??\s*$", "", base)
    if " at " in base:
        away, home = base.split(" at ", 1)
        return away.strip(), home.strip()
    return "", ""


def _abbr_from_yes_sub(yes_sub: str) -> str:
    """Extract team abbr from yes_sub_title e.g. 'FLA Panthers' -> FLA."""
    parts = (yes_sub or "").strip().split()
    if parts and parts[0].upper() in _NHL_ABBR:
        return parts[0].upper()
    return ""


def _build_game(ev: dict[str, Any], game_date: str) -> Optional[NHLGame]:
    """Build NHLGame from event. Use event_ticker suffix to map teams to probs."""
    title = ev.get("title", "")
    away, home = _parse_title(title)
    if not away or not home:
        return None
    event_ticker = ev.get("event_ticker", "")
    away_abbr, home_abbr = _parse_ticker_suffix(event_ticker)
    if not away_abbr or not home_abbr:
        away_abbr, home_abbr = "", ""

    markets = ev.get("markets") or []
    probs: dict[str, float] = {}
    exp_utc: Optional[str] = None
    for m in markets:
        yes_sub = (m.get("yes_sub_title") or "").strip()
        if not yes_sub:
            continue
        cents = _mid_price_cents(m)
        if cents is None:
            continue
        abbr = _abbr_from_yes_sub(yes_sub)
        if abbr:
            probs[abbr] = cents / 100.0
        if exp_utc is None:
            exp_utc = m.get("expected_expiration_time")

    if len(probs) != 2:
        return None
    away_prob = probs.get(away_abbr)
    home_prob = probs.get(home_abbr)
    if away_prob is None or home_prob is None:
        vals = list(probs.values())
        away_prob = vals[0]
        home_prob = vals[1]

    fav = home if home_prob >= away_prob else away
    fav_p = max(home_prob, away_prob)
    udog_p = min(home_prob, away_prob)

    def _display(t: str) -> str:
        return _DISPLAY_NAMES.get(t, t)

    away = _display(away)
    home = _display(home)
    fav = _display(fav)
    return NHLGame(
        date=game_date,
        event_ticker=event_ticker,
        away_team=away,
        home_team=home,
        away_win_prob=away_prob,
        home_win_prob=home_prob,
        favorite=fav,
        favorite_win_prob=fav_p,
        underdog_win_prob=udog_p,
        expected_expiration_utc=exp_utc,
        status="active",
    )


def _is_game_not_started(game: NHLGame) -> bool:
    """True if game has not ended (expected_expiration in future)."""
    if not game.expected_expiration_utc:
        return True
    try:
        exp = datetime.fromisoformat(game.expected_expiration_utc.replace("Z", "+00:00"))
        return datetime.now(timezone.utc) < exp
    except Exception:
        return True


def fetch_nhl_kalshi(date: Optional[str] = None) -> tuple[str, list[NHLGame]]:
    """Fetch NHL games from Kalshi for date (YYYYMMDD). Defaults to today."""
    if not date:
        date = datetime.now().strftime("%Y%m%d")
    client = KalshiClient()
    events = client.get_nhl_events_for_date(date)
    games: list[NHLGame] = []
    for ev in events:
        game_date = _parse_event_ticker_date(ev.get("event_ticker", "")) or date
        g = _build_game(ev, game_date)
        if g:
            games.append(g)
    return date, games


def _write_csv(path: str, date: str, games: list[NHLGame]) -> None:
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "date", "event_ticker", "away_team", "home_team",
        "away_win_prob", "home_win_prob", "favorite", "favorite_win_prob",
        "underdog_win_prob", "expected_expiration_utc",
    ]
    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for g in games:
            w.writerow({
                "date": g.date,
                "event_ticker": g.event_ticker,
                "away_team": g.away_team,
                "home_team": g.home_team,
                "away_win_prob": g.away_win_prob,
                "home_win_prob": g.home_win_prob,
                "favorite": g.favorite,
                "favorite_win_prob": g.favorite_win_prob,
                "underdog_win_prob": g.underdog_win_prob,
                "expected_expiration_utc": g.expected_expiration_utc,
            })


def _write_markdown_summary(path: str, date: str, games: list[NHLGame]) -> None:
    """Write MD with 5 biggest favorites (not started), ordered by game time."""
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    not_started = [g for g in games if _is_game_not_started(g)]
    top5 = sorted(not_started, key=lambda x: -x.favorite_win_prob)[:5]

    lines = [
        f"# NHL Kalshi – 5 biggest favorites ({date})",
        "",
        "Games not started yet, ordered by implied favorite strength.",
        "**Favorite** = team with higher win prob on Kalshi (per market price).",
        "",
    ]
    if not top5:
        lines.append("No games matching criteria.")
    else:
        lines.append("| Rank | Favorite | Implied % | Opponent | Matchup |")
        lines.append("|------|----------|-----------|----------|---------|")
        for i, g in enumerate(top5, 1):
            opp = g.away_team if g.favorite == g.home_team else g.home_team
            matchup = f"{g.away_team} @ {g.home_team}"
            pct = f"{g.favorite_win_prob * 100:.1f}%"
            lines.append(f"| {i} | {g.favorite} | {pct} | {opp} | {matchup} |")
    lines.append("")
    lines.append("## All games (not started)")
    if not not_started:
        lines.append("None.")
    else:
        by_fav = sorted(not_started, key=lambda x: -x.favorite_win_prob)
        lines.append("| Favorite | % | Underdog | Matchup |")
        lines.append("|----------|---|----------|---------|")
        for g in by_fav:
            opp = g.away_team if g.favorite == g.home_team else g.home_team
            lines.append(f"| {g.favorite} | {g.favorite_win_prob*100:.1f}% | {opp} | {g.away_team} @ {g.home_team} |")

    with out.open("w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def _parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fetch NHL Kalshi markets and output CSV + summary.")
    p.add_argument("--date", help="YYYYMMDD (default: today)")
    p.add_argument("--csv", help="Write CSV to this path")
    p.add_argument("--md", help="Write summary MD path (default: reports/nhl/nhl_kalshi_<date>_summary.md)")
    return p.parse_args(argv)


def main(argv: list[str]) -> int:
    args = _parse_args(argv)
    date, games = fetch_nhl_kalshi(date=args.date)
    print(f"Date: {date} | Games: {len(games)}")
    for g in games:
        print(f"  {g.away_team} @ {g.home_team}: {g.favorite} {g.favorite_win_prob*100:.1f}%")
    if args.csv:
        _write_csv(args.csv, date, games)
        print(f"Wrote CSV: {args.csv}")
    md_path = args.md or f"reports/nhl/nhl_kalshi_{date}_summary.md"
    _write_markdown_summary(md_path, date, games)
    print(f"Wrote summary MD: {md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
