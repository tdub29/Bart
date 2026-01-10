from __future__ import annotations

import argparse
import csv
import math
import re
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Iterable, Optional

import requests
from bs4 import BeautifulSoup


BART_SCHEDULE_URL = "https://barttorvik.com/schedule.php"
KALSHI_API_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"


@dataclass(frozen=True)
class BartGame:
    date: str  # YYYYMMDD
    time: str
    away_team: str
    home_team: str
    location: Optional[str]
    bart_ttq: Optional[float]
    bart_predicted_winner: str
    bart_win_prob_home: Optional[float]  # 0..1
    bart_win_prob_away: Optional[float]  # 0..1
    bart_spread_home: Optional[float]  # home line (fav negative)
    bart_total: Optional[float]
    bart_predicted_score_home: Optional[int]
    bart_predicted_score_away: Optional[int]


@dataclass(frozen=True)
class KalshiMoneyline:
    away_win_prob: Optional[float]
    home_win_prob: Optional[float]
    away_moneyline: Optional[int]
    home_moneyline: Optional[int]
    event_ticker: Optional[str]


@dataclass(frozen=True)
class KalshiDerivedLine:
    median_home_margin: Optional[float]  # home - away
    median_total: Optional[float]
    implied_score_home: Optional[float]
    implied_score_away: Optional[float]
    spread_event_ticker: Optional[str]
    total_event_ticker: Optional[str]


@dataclass(frozen=True)
class GameComparison:
    game: BartGame
    kalshi_moneyline: KalshiMoneyline
    kalshi_line: KalshiDerivedLine


_BART_PRED_RE = re.compile(
    r"^(?P<winner>.+?)\s+"
    r"(?P<spread>[+-]?\d+(?:\.\d+)?)\s*,\s*"
    r"(?P<score1>\d+)-(?P<score2>\d+)\s*"
    r"\((?P<win_pct>\d+)%\)\s*$"
)


def _canonical_spaces(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip())


def canonical_team_name(name: str) -> str:
    raw = _canonical_spaces(name)
    raw = re.sub(r"\([^)]*\)", "", raw).strip()
    raw = raw.replace("&", "and")

    lowered = raw.lower()
    lowered_key = re.sub(r"[^a-z0-9]+", " ", lowered).strip()
    alias_map = {
        "umkc": "kansas city",
        "nebraska omaha": "omaha",
        "cal baptist": "california baptist",
        "central connecticut": "central connecticut state",
        "queens": "queens university",
        "sam houston st": "sam houston",
        "sam houston state": "sam houston",
    }
    lowered = alias_map.get(lowered_key, lowered)

    tokens = [t for t in re.split(r"[^a-z0-9]+", lowered) if t]
    out: list[str] = []
    for idx, token in enumerate(tokens):
        if token in {"university", "univ"}:
            continue
        if token == "st":
            out.append("saint" if idx == 0 else "state")
            continue
        out.append(token)
    return "".join(out)


def _pair_key(away: str, home: str) -> tuple[str, str]:
    return canonical_team_name(away), canonical_team_name(home)


def _similarity(a: str, b: str) -> float:
    if not a and not b:
        return 1.0
    return SequenceMatcher(None, a, b).ratio()


def _moneyline_from_prob(prob: float) -> Optional[int]:
    if prob <= 0.0 or prob >= 1.0:
        return None
    if prob >= 0.5:
        return int(round(-100.0 * prob / (1.0 - prob)))
    return int(round(100.0 * (1.0 - prob) / prob))


def _kalshi_mid_price_cents(market: dict[str, Any]) -> Optional[float]:
    yes_bid = market.get("yes_bid", 0) or 0
    yes_ask = market.get("yes_ask", 0) or 0
    last_price = market.get("last_price", 0) or 0

    if 0 < yes_bid < 100 and 0 < yes_ask < 100:
        return (float(yes_bid) + float(yes_ask)) / 2.0

    # If only one side is quoted, avoid treating 0/100 as a meaningful quote.
    if 0 < yes_bid < 100:
        return float(yes_bid)
    if 0 < yes_ask < 100:
        return float(yes_ask)
    if 0 < last_price < 100:
        return float(last_price)
    return None


def _estimate_median_from_survival_points(points: Iterable[tuple[float, float]]) -> Optional[float]:
    pts = [(t, p) for (t, p) in points if p is not None and 0.0 <= p <= 1.0]
    if not pts:
        return None

    # Choose the closest points bracketing p=0.5.
    above = [(t, p) for (t, p) in pts if p >= 0.5]
    below = [(t, p) for (t, p) in pts if p <= 0.5]

    if not above and not below:
        return None
    if not above:
        t, _ = max(below, key=lambda x: x[1])  # closest to 0.5 from below
        return t
    if not below:
        t, _ = min(above, key=lambda x: x[1])  # closest to 0.5 from above
        return t

    t1, p1 = max(above, key=lambda x: x[0])  # largest t with p>=0.5
    t2, p2 = min(below, key=lambda x: x[0])  # smallest t with p<=0.5

    if math.isclose(p1, p2):
        return (t1 + t2) / 2.0
    # Linear interpolation for p(t)=0.5 between (t1,p1) and (t2,p2).
    return t1 + (0.5 - p1) * (t2 - t1) / (p2 - p1)


class BartTorvikClient:
    def __init__(self, session: Optional[requests.Session] = None) -> None:
        self._session = session or requests.Session()
        self._verified = False

    def _verify(self) -> None:
        if self._verified:
            return
        self._session.get(BART_SCHEDULE_URL, timeout=30)
        self._session.post(BART_SCHEDULE_URL, data={"js_test_submitted": "1"}, timeout=30)
        self._verified = True

    def get_default_date(self) -> str:
        self._verify()
        html = self._session.get(BART_SCHEDULE_URL, timeout=30).text
        soup = BeautifulSoup(html, "html.parser")
        form = soup.find_all("form")[-1]
        date_input = form.find("input", attrs={"name": "date"})
        if not date_input or not date_input.get("value"):
            raise RuntimeError("Unable to determine default date from BartTorvik schedule page.")
        return str(date_input["value"])

    def get_schedule(self, date: Optional[str] = None) -> tuple[str, list[BartGame]]:
        self._verify()
        date = date or self.get_default_date()
        html = self._session.get(f"{BART_SCHEDULE_URL}?date={date}", timeout=30).text
        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table", id="tblData")
        if not table:
            raise RuntimeError("Unable to find schedule table (tblData) on BartTorvik schedule page.")

        games: list[BartGame] = []
        for row in table.find_all("tr")[1:]:
            cells = row.find_all("td")
            if len(cells) < 3:
                continue

            time_text = cells[0].get_text(" ", strip=True)
            matchup_td = cells[1]
            location = matchup_td.get("title")
            team_links = matchup_td.find_all("a")
            if len(team_links) < 2:
                continue
            team1 = team_links[0].get_text(" ", strip=True)
            team2 = team_links[1].get_text(" ", strip=True)

            matchup_text = matchup_td.get_text(" ", strip=True)
            if " at " in matchup_text:
                away_team, home_team = team1, team2
            elif " vs " in matchup_text:
                away_team, home_team = team1, team2
            else:
                away_team, home_team = team1, team2

            trank_line = cells[2].get_text(" ", strip=True)
            ttq = _parse_ttq(cells[3].get_text(" ", strip=True)) if len(cells) > 3 else None
            pred = _parse_bart_prediction(trank_line, away_team=away_team, home_team=home_team)
            games.append(
                BartGame(
                    date=date,
                    time=time_text,
                    away_team=away_team,
                    home_team=home_team,
                    location=location,
                    bart_ttq=ttq,
                    bart_predicted_winner=pred.get("winner"),
                    bart_win_prob_home=pred.get("win_prob_home"),
                    bart_win_prob_away=pred.get("win_prob_away"),
                    bart_spread_home=pred.get("spread_home"),
                    bart_total=pred.get("total"),
                    bart_predicted_score_home=pred.get("score_home"),
                    bart_predicted_score_away=pred.get("score_away"),
                )
            )
        return date, games


def _parse_bart_prediction(
    trank_line: str, *, away_team: str, home_team: str
) -> dict[str, Optional[float | int | str]]:
    match = _BART_PRED_RE.match(trank_line)
    if not match:
        return {
            "winner": "",
            "win_prob_home": None,
            "win_prob_away": None,
            "spread_home": None,
            "total": None,
            "score_home": None,
            "score_away": None,
        }

    winner = _canonical_spaces(match.group("winner"))
    spread_winner = float(match.group("spread"))
    score1 = int(match.group("score1"))
    score2 = int(match.group("score2"))
    win_pct = int(match.group("win_pct"))

    away_key, home_key = _pair_key(away_team, home_team)
    winner_key = canonical_team_name(winner)

    win_prob_winner = win_pct / 100.0
    if winner_key == home_key:
        win_prob_home = win_prob_winner
        win_prob_away = 1.0 - win_prob_winner
        spread_home = spread_winner
        score_home, score_away = score1, score2
    elif winner_key == away_key:
        win_prob_away = win_prob_winner
        win_prob_home = 1.0 - win_prob_winner
        spread_home = -spread_winner
        score_away, score_home = score1, score2
    else:
        # Fall back: treat the first score as the home team's score if it's closer.
        # This is only used when names don't match cleanly.
        win_prob_home = None
        win_prob_away = None
        spread_home = None
        score_home = None
        score_away = None

    total = (score_home + score_away) if (score_home is not None and score_away is not None) else None
    return {
        "winner": winner,
        "win_prob_home": win_prob_home,
        "win_prob_away": win_prob_away,
        "spread_home": spread_home,
        "total": total,
        "score_home": score_home,
        "score_away": score_away,
    }


def _parse_ttq(value: str) -> Optional[float]:
    cleaned = value.strip()
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _parse_kalshi_event_title(title: str) -> Optional[tuple[str, str]]:
    base = title.split(":", 1)[0].strip()
    if " at " in base:
        away, home = base.split(" at ", 1)
        return away.strip(), home.strip()
    if " vs " in base:
        away, home = base.split(" vs ", 1)
        return away.strip(), home.strip()
    return None


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

    def _get_json(self, url: str, *, params: dict[str, Any]) -> dict[str, Any]:
        attempt = 0
        while True:
            if self._last_request_at is not None:
                elapsed = time.monotonic() - self._last_request_at
                if elapsed < self._min_request_interval_s:
                    time.sleep(self._min_request_interval_s - elapsed)

            resp = self._session.get(url, params=params, timeout=60)
            self._last_request_at = time.monotonic()

            if resp.status_code == 429 and attempt < self._max_retries:
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    try:
                        delay = float(retry_after)
                    except ValueError:
                        delay = 2.0**attempt
                else:
                    delay = 2.0**attempt
                time.sleep(min(30.0, max(0.5, delay)))
                attempt += 1
                continue

            resp.raise_for_status()
            return resp.json()

    def list_events_for_date(
        self,
        *,
        series_ticker: str,
        date: str,
        limit: int = 200,
        max_pages: int = 25,
    ) -> list[dict[str, Any]]:
        url = f"{KALSHI_API_BASE_URL}/events"
        params: dict[str, Any] = {"series_ticker": series_ticker, "limit": limit}
        target_token = _date_token_from_yyyymmdd(date)
        target_dt = datetime.strptime(date, "%Y%m%d").date()
        matched: list[dict[str, Any]] = []
        cursor: Optional[str] = None
        for _ in range(max_pages):
            if cursor:
                params["cursor"] = cursor
            payload = self._get_json(url, params=params)
            page_events = payload.get("events", [])
            for e in page_events:
                if target_token in (e.get("event_ticker") or ""):
                    matched.append(e)
            cursor = payload.get("cursor")
            if not cursor or not page_events:
                break

            # Fast stop once we've paged past the target date (events are returned newest-first).
            page_dates = [_event_ticker_date(e.get("event_ticker", "")) for e in page_events]
            page_dates = [d for d in page_dates if d is not None]
            if page_dates:
                oldest = min(page_dates)
                if oldest < target_dt:
                    break
        return matched

    def list_markets(self, *, event_ticker: str, limit: int = 200) -> list[dict[str, Any]]:
        url = f"{KALSHI_API_BASE_URL}/markets"
        params = {"event_ticker": event_ticker, "limit": limit}
        payload = self._get_json(url, params=params)
        return payload.get("markets", [])


def _date_token_from_yyyymmdd(date: str) -> str:
    dt = datetime.strptime(date, "%Y%m%d")
    return f"{dt.strftime('%y')}{dt.strftime('%b').upper()}{dt.strftime('%d')}"


def _event_ticker_date(event_ticker: str) -> Optional[date]:
    # Example: KXNCAAMBGAME-26JAN10TEXALA
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
        "JAN": 1,
        "FEB": 2,
        "MAR": 3,
        "APR": 4,
        "MAY": 5,
        "JUN": 6,
        "JUL": 7,
        "AUG": 8,
        "SEP": 9,
        "OCT": 10,
        "NOV": 11,
        "DEC": 12,
    }
    month = month_map.get(mon)
    if not month:
        return None
    return datetime(year=2000 + year, month=month, day=day).date()


def _build_kalshi_event_index(
    events: list[dict[str, Any]],
) -> tuple[dict[tuple[str, str], str], list[tuple[str, str, str]]]:
    index: dict[tuple[str, str], str] = {}
    parsed: list[tuple[str, str, str]] = []
    for ev in events:
        title = ev.get("title", "")
        parsed_title = _parse_kalshi_event_title(title)
        if not parsed_title:
            continue
        away, home = parsed_title
        key = _pair_key(away, home)
        index[key] = ev["event_ticker"]
        parsed.append((away, home, ev["event_ticker"]))
    return index, parsed


def _find_best_event_ticker(
    *,
    away_team: str,
    home_team: str,
    index: dict[tuple[str, str], str],
    parsed_events: list[tuple[str, str, str]],
    min_score: float = 0.80,
) -> Optional[str]:
    key = _pair_key(away_team, home_team)
    if key in index:
        return index[key]

    away_can = canonical_team_name(away_team)
    home_can = canonical_team_name(home_team)
    best: tuple[float, str] | None = None
    second_best_score = -1.0
    for away_ev, home_ev, ticker in parsed_events:
        score = (_similarity(away_can, canonical_team_name(away_ev)) + _similarity(home_can, canonical_team_name(home_ev))) / 2.0
        if best is None or score > best[0]:
            second_best_score = best[0] if best is not None else second_best_score
            best = (score, ticker)
        elif score > second_best_score:
            second_best_score = score

    if not best:
        return None
    best_score, best_ticker = best
    if best_score < min_score:
        return None
    if second_best_score >= 0 and (best_score - second_best_score) < 0.03:
        return None
    return best_ticker


def _kalshi_moneyline_for_game(
    client: KalshiClient, *, event_ticker: str, away_team: str, home_team: str
) -> KalshiMoneyline:
    markets = client.list_markets(event_ticker=event_ticker, limit=20)
    away_can = canonical_team_name(away_team)
    home_can = canonical_team_name(home_team)

    away_prob: Optional[float] = None
    home_prob: Optional[float] = None

    for m in markets:
        team = m.get("yes_sub_title") or ""
        team_can = canonical_team_name(team)
        price_cents = _kalshi_mid_price_cents(m)
        if price_cents is None:
            continue
        prob = price_cents / 100.0
        if team_can == away_can:
            away_prob = prob
        elif team_can == home_can:
            home_prob = prob

    return KalshiMoneyline(
        away_win_prob=away_prob,
        home_win_prob=home_prob,
        away_moneyline=_moneyline_from_prob(away_prob) if away_prob is not None else None,
        home_moneyline=_moneyline_from_prob(home_prob) if home_prob is not None else None,
        event_ticker=event_ticker,
    )


_SPREAD_TITLE_RE = re.compile(r"^(?P<team>.+?)\s+wins\s+by\s+over\s+(?P<thresh>\d+(?:\.\d+)?)\s+Points\??$")
_TOTAL_YES_RE = re.compile(r"^Over\s+(?P<thresh>\d+(?:\.\d+)?)\s+points\s+scored$")


def _kalshi_derived_line_for_game(
    client: KalshiClient,
    *,
    spread_event_ticker: Optional[str],
    total_event_ticker: Optional[str],
    away_team: str,
    home_team: str,
) -> KalshiDerivedLine:
    away_can = canonical_team_name(away_team)
    home_can = canonical_team_name(home_team)

    median_margin: Optional[float] = None
    if spread_event_ticker:
        markets = client.list_markets(event_ticker=spread_event_ticker, limit=200)
        points: list[tuple[float, float]] = []
        for m in markets:
            title = (m.get("title") or "").strip()
            mt = _SPREAD_TITLE_RE.match(title)
            if not mt:
                continue
            team = mt.group("team").strip()
            team_can = canonical_team_name(team)
            threshold = float(mt.group("thresh"))
            price_cents = _kalshi_mid_price_cents(m)
            if price_cents is None:
                continue
            prob = price_cents / 100.0
            if team_can == home_can:
                points.append((threshold, prob))  # P(home_margin > threshold)
            elif team_can == away_can:
                points.append((-threshold, 1.0 - prob))  # P(home_margin > -threshold)
        median_margin = _estimate_median_from_survival_points(points)

    median_total: Optional[float] = None
    if total_event_ticker:
        markets = client.list_markets(event_ticker=total_event_ticker, limit=200)
        points: list[tuple[float, float]] = []
        for m in markets:
            yes_sub = (m.get("yes_sub_title") or "").strip()
            mt = _TOTAL_YES_RE.match(yes_sub)
            if not mt:
                # Fallback to ticker suffix like “…-165” => “Over 165.5”
                ticker = m.get("ticker", "")
                suffix = ticker.rsplit("-", 1)[-1]
                if suffix.isdigit():
                    thresh = float(suffix) + 0.5
                else:
                    continue
            else:
                thresh = float(mt.group("thresh"))
            price_cents = _kalshi_mid_price_cents(m)
            if price_cents is None:
                continue
            prob = price_cents / 100.0  # P(total > thresh)
            points.append((thresh, prob))
        median_total = _estimate_median_from_survival_points(points)

    implied_home: Optional[float] = None
    implied_away: Optional[float] = None
    if median_margin is not None and median_total is not None:
        implied_home = (median_total + median_margin) / 2.0
        implied_away = (median_total - median_margin) / 2.0

    return KalshiDerivedLine(
        median_home_margin=median_margin,
        median_total=median_total,
        implied_score_home=implied_home,
        implied_score_away=implied_away,
        spread_event_ticker=spread_event_ticker,
        total_event_ticker=total_event_ticker,
    )


def compare_bart_vs_kalshi(*, date: Optional[str]) -> tuple[str, list[GameComparison]]:
    bart = BartTorvikClient()
    used_date, games = bart.get_schedule(date=date)

    kalshi = KalshiClient()

    series = {
        "moneyline": "KXNCAAMBGAME",
        "spread": "KXNCAAMBSPREAD",
        "total": "KXNCAAMBTOTAL",
    }
    events_by_kind: dict[str, list[dict[str, Any]]] = {}
    for kind, series_ticker in series.items():
        events_by_kind[kind] = kalshi.list_events_for_date(series_ticker=series_ticker, date=used_date, limit=200)

    ml_index, ml_parsed = _build_kalshi_event_index(events_by_kind["moneyline"])
    sp_index, sp_parsed = _build_kalshi_event_index(events_by_kind["spread"])
    tot_index, tot_parsed = _build_kalshi_event_index(events_by_kind["total"])

    out: list[GameComparison] = []
    for g in games:
        ml_event = _find_best_event_ticker(
            away_team=g.away_team, home_team=g.home_team, index=ml_index, parsed_events=ml_parsed, min_score=0.78
        )
        sp_event = _find_best_event_ticker(
            away_team=g.away_team, home_team=g.home_team, index=sp_index, parsed_events=sp_parsed, min_score=0.78
        )
        tot_event = _find_best_event_ticker(
            away_team=g.away_team, home_team=g.home_team, index=tot_index, parsed_events=tot_parsed, min_score=0.78
        )

        kalshi_ml = _kalshi_moneyline_for_game(kalshi, event_ticker=ml_event, away_team=g.away_team, home_team=g.home_team) if ml_event else KalshiMoneyline(None, None, None, None, None)
        kalshi_line = _kalshi_derived_line_for_game(
            kalshi, spread_event_ticker=sp_event, total_event_ticker=tot_event, away_team=g.away_team, home_team=g.home_team
        )
        out.append(GameComparison(game=g, kalshi_moneyline=kalshi_ml, kalshi_line=kalshi_line))
    return used_date, out


def _fmt_pct(prob: Optional[float]) -> str:
    return "" if prob is None else f"{prob*100:.1f}%"


def _fmt_num(value: Optional[float], *, digits: int = 1) -> str:
    if value is None:
        return ""
    return f"{value:.{digits}f}"


def _fmt_ml(value: Optional[int]) -> str:
    if value is None:
        return ""
    return f"{value:+d}"


def _kalshi_spread_home_value(line: KalshiDerivedLine) -> Optional[float]:
    if line.median_home_margin is None:
        return None
    return -line.median_home_margin


def _diffs_for_game(row: GameComparison) -> tuple[Optional[float], Optional[float]]:
    kalshi_spread_home = _kalshi_spread_home_value(row.kalshi_line)
    spread_diff = None
    if row.game.bart_spread_home is not None and kalshi_spread_home is not None:
        spread_diff = row.game.bart_spread_home - kalshi_spread_home

    total_diff = None
    if row.game.bart_total is not None and row.kalshi_line.median_total is not None:
        total_diff = row.game.bart_total - row.kalshi_line.median_total
    return spread_diff, total_diff


def _print_table(date: str, rows: list[GameComparison]) -> None:
    header = [
        "Time",
        "Away",
        "Home",
        "Bart P(Home)",
        "Bart Line(H)",
        "Bart Score",
        "TTQ",
        "Kalshi P(Away)",
        "Kalshi ML(A)",
        "Kalshi P(Home)",
        "Kalshi ML(H)",
        "Kalshi Line(H)",
        "Kalshi Total",
        "Kalshi Implied Score",
    ]
    print(f"Date: {date}")
    print("\t".join(header))
    for r in rows:
        g = r.game
        bart_score = ""
        if g.bart_predicted_score_away is not None and g.bart_predicted_score_home is not None:
            bart_score = f"{g.bart_predicted_score_away}-{g.bart_predicted_score_home}"

        kalshi_implied = ""
        if r.kalshi_line.implied_score_away is not None and r.kalshi_line.implied_score_home is not None:
            kalshi_implied = f"{r.kalshi_line.implied_score_away:.1f}-{r.kalshi_line.implied_score_home:.1f}"

        kalshi_spread_home = _kalshi_spread_home_value(r.kalshi_line)

        print(
            "\t".join(
                [
                    g.time,
                    g.away_team,
                    g.home_team,
                    _fmt_pct(g.bart_win_prob_home),
                    _fmt_num(g.bart_spread_home),
                    bart_score,
                    _fmt_num(g.bart_ttq, digits=0),
                    _fmt_pct(r.kalshi_moneyline.away_win_prob),
                    _fmt_ml(r.kalshi_moneyline.away_moneyline),
                    _fmt_pct(r.kalshi_moneyline.home_win_prob),
                    _fmt_ml(r.kalshi_moneyline.home_moneyline),
                    _fmt_num(kalshi_spread_home),
                    _fmt_num(r.kalshi_line.median_total),
                    kalshi_implied,
                ]
            )
        )


def _write_csv(path: str, date: str, rows: list[GameComparison]) -> None:
    out_path = Path(path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "date",
        "time",
        "away_team",
        "home_team",
        "bart_predicted_winner",
        "bart_ttq",
        "bart_win_prob_home",
        "bart_win_prob_away",
        "bart_spread_home",
        "bart_total",
        "bart_predicted_score_away",
        "bart_predicted_score_home",
        "kalshi_event_ticker_moneyline",
        "kalshi_win_prob_away",
        "kalshi_win_prob_home",
        "kalshi_moneyline_away",
        "kalshi_moneyline_home",
        "kalshi_event_ticker_spread",
        "kalshi_median_home_margin",
        "kalshi_implied_spread_home",
        "diff_spread_bart_minus_kalshi",
        "kalshi_event_ticker_total",
        "kalshi_median_total",
        "diff_total_bart_minus_kalshi",
        "kalshi_implied_score_away",
        "kalshi_implied_score_home",
    ]
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            g = r.game
            implied_spread_home = _kalshi_spread_home_value(r.kalshi_line)
            spread_diff, total_diff = _diffs_for_game(r)
            writer.writerow(
                {
                    "date": date,
                    "time": g.time,
                    "away_team": g.away_team,
                    "home_team": g.home_team,
                    "bart_predicted_winner": g.bart_predicted_winner,
                    "bart_ttq": g.bart_ttq,
                    "bart_win_prob_home": g.bart_win_prob_home,
                    "bart_win_prob_away": g.bart_win_prob_away,
                    "bart_spread_home": g.bart_spread_home,
                    "bart_total": g.bart_total,
                    "bart_predicted_score_away": g.bart_predicted_score_away,
                    "bart_predicted_score_home": g.bart_predicted_score_home,
                    "kalshi_event_ticker_moneyline": r.kalshi_moneyline.event_ticker,
                    "kalshi_win_prob_away": r.kalshi_moneyline.away_win_prob,
                    "kalshi_win_prob_home": r.kalshi_moneyline.home_win_prob,
                    "kalshi_moneyline_away": r.kalshi_moneyline.away_moneyline,
                    "kalshi_moneyline_home": r.kalshi_moneyline.home_moneyline,
                    "kalshi_event_ticker_spread": r.kalshi_line.spread_event_ticker,
                    "kalshi_median_home_margin": r.kalshi_line.median_home_margin,
                    "kalshi_implied_spread_home": implied_spread_home,
                    "diff_spread_bart_minus_kalshi": spread_diff,
                    "kalshi_event_ticker_total": r.kalshi_line.total_event_ticker,
                    "kalshi_median_total": r.kalshi_line.median_total,
                    "diff_total_bart_minus_kalshi": total_diff,
                    "kalshi_implied_score_away": r.kalshi_line.implied_score_away,
                    "kalshi_implied_score_home": r.kalshi_line.implied_score_home,
                }
            )


def _write_markdown_summary(path: str, date: str, rows: list[GameComparison]) -> None:
    out_path = Path(path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    lines.append(f"# Bart vs Kalshi discrepancies ({date})")
    lines.append("")
    lines.append("Criteria: TTQ > 50 and |spread diff| >= 2 or |total diff| >= 2.")
    lines.append("Notes: games that have already started are excluded; TTQ >= 70 is marked with a star.")
    lines.append("")

    header = [
        "Time",
        "Away",
        "Home",
        "TTQ",
        "Bart Line(H)",
        "Kalshi Line(H)",
        "Spread Diff",
        "Bart Total",
        "Kalshi Total",
        "Total Diff",
        "Bart Score",
        "Kalshi Implied Score",
    ]
    def time_key(value: str) -> tuple[int, str]:
        try:
            parsed = datetime.strptime(value.strip(), "%I:%M %p")
            return (parsed.hour * 60 + parsed.minute, value)
        except ValueError:
            return (10**9, value)

    def is_started(game_date: str, game_time: str) -> bool:
        # Bart times are typically Eastern; convert local Pacific -> Eastern by adding 3 hours.
        now_local = datetime.now()
        now_et = now_local + timedelta(hours=3)
        try:
            game_day = datetime.strptime(game_date, "%Y%m%d").date()
        except ValueError:
            return False
        if game_day < now_et.date():
            return True
        if game_day > now_et.date():
            return False
        try:
            game_dt = datetime.strptime(f"{game_date} {game_time}", "%Y%m%d %I:%M %p")
        except ValueError:
            return False
        return game_dt <= now_et

    filtered_rows: list[GameComparison] = []
    table: list[list[str]] = []
    for row in rows:
        g = row.game
        if is_started(g.date, g.time):
            continue
        if g.bart_ttq is None or g.bart_ttq <= 50:
            continue
        spread_diff, total_diff = _diffs_for_game(row)
        if spread_diff is None and total_diff is None:
            continue
        if (spread_diff is None or abs(spread_diff) < 2.0) and (total_diff is None or abs(total_diff) < 2.0):
            continue

        bart_score = ""
        if g.bart_predicted_score_away is not None and g.bart_predicted_score_home is not None:
            bart_score = f"{g.bart_predicted_score_away}-{g.bart_predicted_score_home}"

        kalshi_implied = ""
        if row.kalshi_line.implied_score_away is not None and row.kalshi_line.implied_score_home is not None:
            kalshi_implied = f"{row.kalshi_line.implied_score_away:.1f}-{row.kalshi_line.implied_score_home:.1f}"

        kalshi_spread_home = _kalshi_spread_home_value(row.kalshi_line)

        filtered_rows.append(row)
        ttq_display = _fmt_num(g.bart_ttq, digits=0)
        if g.bart_ttq is not None and g.bart_ttq >= 70:
            ttq_display = f"{ttq_display}*"
        table.append(
            [
                g.time,
                g.away_team,
                g.home_team,
                ttq_display,
                _fmt_num(g.bart_spread_home),
                _fmt_num(kalshi_spread_home),
                _fmt_num(spread_diff),
                _fmt_num(g.bart_total),
                _fmt_num(row.kalshi_line.median_total),
                _fmt_num(total_diff),
                bart_score,
                kalshi_implied,
            ]
        )

    picks: list[str] = []
    if not table:
        lines.append("No games matched the filter.")
    else:
        table_sorted = sorted(table, key=lambda r: time_key(r[0]))
        lines.append("| " + " | ".join(header) + " |")
        lines.append("| " + " | ".join(["---"] * len(header)) + " |")
        for row in table_sorted:
            lines.append("| " + " | ".join(row) + " |")

        # Add plain-language bet notes.
        for row in sorted(filtered_rows, key=lambda r: time_key(r.game.time)):
            g = row.game
            spread_diff, total_diff = _diffs_for_game(row)

            kalshi_spread_home = _kalshi_spread_home_value(row.kalshi_line)
            matchup = f"{g.away_team} at {g.home_team}"
            bart_score_note = ""
            if g.bart_predicted_score_away is not None and g.bart_predicted_score_home is not None:
                bart_score_note = f" Bart predicted score: {g.bart_predicted_score_away}-{g.bart_predicted_score_home}."
            star_note = ""
            if g.bart_ttq is not None and g.bart_ttq >= 70:
                star_note = " (TTQ 70+)"

            if spread_diff is not None and abs(spread_diff) >= 2.0 and kalshi_spread_home is not None:
                if spread_diff <= -2.0:
                    picks.append(
                        f"{matchup}{star_note}: Bart makes {g.home_team} ~{abs(spread_diff):.1f} pts stronger than Kalshi; lean {g.home_team} {kalshi_spread_home:+.1f}.{bart_score_note}"
                    )
                else:
                    away_line = -kalshi_spread_home
                    picks.append(
                        f"{matchup}{star_note}: Bart makes {g.away_team} ~{abs(spread_diff):.1f} pts stronger than Kalshi; lean {g.away_team} {away_line:+.1f}.{bart_score_note}"
                    )

            if total_diff is not None and abs(total_diff) >= 2.0 and row.kalshi_line.median_total is not None:
                if total_diff >= 2.0:
                    picks.append(
                        f"{matchup}{star_note}: Bart total {g.bart_total:.1f} is {abs(total_diff):.1f} pts higher than Kalshi {row.kalshi_line.median_total:.1f}; lean Over.{bart_score_note}"
                    )
                else:
                    picks.append(
                        f"{matchup}{star_note}: Bart total {g.bart_total:.1f} is {abs(total_diff):.1f} pts lower than Kalshi {row.kalshi_line.median_total:.1f}; lean Under.{bart_score_note}"
                    )

    lines.append("")
    lines.append("## Plain-language summary")
    if not picks:
        lines.append("No suggested bets based on the filter.")
    else:
        for pick in picks:
            lines.append(f"- {pick}")

    with out_path.open("w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def _parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Compare Bart Torvik schedule predictions vs Kalshi markets.")
    p.add_argument("--date", help="YYYYMMDD (defaults to BartTorvik's default schedule date)")
    p.add_argument("--csv", help="Write results to CSV at this path")
    p.add_argument("--md", help="Write summary markdown at this path (defaults to bart_kalshi_<date>_summary.md)")
    return p.parse_args(argv)


def main(argv: list[str]) -> int:
    args = _parse_args(argv)
    date, rows = compare_bart_vs_kalshi(date=args.date)
    _print_table(date, rows)
    if args.csv:
        _write_csv(args.csv, date, rows)
        print(f"Wrote CSV: {args.csv}")
    md_path = args.md or f"reports/bart_kalshi_{date}_summary.md"
    _write_markdown_summary(md_path, date, rows)
    print(f"Wrote summary MD: {md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
