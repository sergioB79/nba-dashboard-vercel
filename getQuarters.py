"""
getQuarters.py (NHL)

Cria/atualiza um ficheiro CSV com estatísticas por jogo/equipa
usando o endpoint oficial da NHL (`statsapi.web.nhl.com`).

Inclui golos por período (P1, P2, P3, OT) e métricas básicas
como remates e power-play.
"""

import os
from datetime import date, datetime, timedelta
from typing import Dict, List

import pandas as pd
import requests

SEASON_ID = "20242025"
SEASON_TYPE = "Regular"
START_DATE = date(2024, 10, 1)
OUTPUT_FILE = os.path.join("data", f"nhl_periods_{SEASON_ID}.csv")

SCHEDULE_URL = "https://statsapi.web.nhl.com/api/v1/schedule"
TEAM_URL = "https://statsapi.web.nhl.com/api/v1/teams"


def fetch_team_map() -> Dict[int, Dict[str, str]]:
    try:
        r = requests.get(TEAM_URL, timeout=10)
        r.raise_for_status()
        teams = r.json().get("teams", [])
    except Exception as exc:  # noqa: BLE001
        print("❌ Falha ao obter equipas NHL:", exc)
        return {}

    mapping = {}
    for t in teams:
        mapping[t.get("id")] = {
            "abbr": t.get("abbreviation") or (t.get("teamName", "")[:3].upper()),
            "name": t.get("teamName"),
            "full": t.get("name"),
        }
    return mapping


def fetch_schedule(start: date, end: date) -> List[Dict]:
    params = {
        "startDate": start.isoformat(),
        "endDate": end.isoformat(),
    }
    r = requests.get(SCHEDULE_URL, params=params, timeout=15)
    r.raise_for_status()
    return r.json().get("dates", [])


def fetch_game_feed(game_pk: int) -> Dict:
    url = f"https://statsapi.web.nhl.com/api/v1/game/{game_pk}/feed/live"
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    return r.json()


def build_rows(team_map: Dict[int, Dict[str, str]]) -> pd.DataFrame:
    today = date.today()
    schedule = fetch_schedule(START_DATE, today)

    all_rows: List[Dict] = []

    for day in schedule:
        game_date = day.get("date")
        for g in day.get("games", []):
            game_pk = g.get("gamePk")
            matchup = g.get("teams", {})
            away_info = matchup.get("away", {})
            home_info = matchup.get("home", {})

            try:
                feed = fetch_game_feed(game_pk)
            except Exception as exc:  # noqa: BLE001
                print(f"⚠️ Falha ao obter boxscore de {game_pk}: {exc}")
                continue

            linescore = feed.get("liveData", {}).get("linescore", {})
            periods = linescore.get("periods", [])

            def period_goals(side: str, num: int) -> int:
                for p in periods:
                    if p.get("num") == num:
                        return p.get(side, {}).get("goals", 0)
                return 0

            def ot_goals(side: str) -> int:
                total = sum(period_goals(side, n) for n in [1, 2, 3])
                return max(0, linescore.get(side, {}).get("goals", 0) - total)

            box_teams = feed.get("liveData", {}).get("boxscore", {}).get("teams", {})

            def build_team_row(side: str, info: Dict) -> Dict:
                team = info.get("team", {})
                tm = team_map.get(team.get("id"), {})
                stats = box_teams.get(side, {}).get("teamStats", {}).get("teamSkaterStats", {})

                p1 = period_goals(side, 1)
                p2 = period_goals(side, 2)
                p3 = period_goals(side, 3)
                ot = ot_goals(side)

                return {
                    "GAME_ID": str(game_pk),
                    "GAME_DATE": game_date,
                    "MATCHUP": f"{away_team['team'].get('abbreviation', '')} @ {home_team['team'].get('abbreviation', '')}",
                    "TEAM_ID": team.get("id"),
                    "TEAM_ABBREVIATION": tm.get("abbr") or team.get("abbreviation"),
                    "TEAM_NAME": tm.get("name") or team.get("name"),
                    # períodos NHL
                    "P1": p1,
                    "P2": p2,
                    "P3": p3,
                    "OT": ot,
                    # aliases para compatibilidade com UI antiga
                    "Q1": p1,
                    "Q2": p2,
                    "Q3": p3,
                    "Q4": ot,
                    "PTS": stats.get("goals", 0),
                    "GOALS": stats.get("goals", 0),
                    "SHOTS": stats.get("shots", 0),
                    "POWER_PLAY_GOALS": stats.get("powerPlayGoals", 0),
                    "POWER_PLAY_OPPORTUNITIES": stats.get("powerPlayOpportunities", 0),
                    "PIM": stats.get("pim", 0),
                    "HITS": stats.get("hits", 0),
                    "BLOCKED": stats.get("blocked", 0),
                    "TAKEAWAYS": stats.get("takeaways", 0),
                    "GIVEAWAYS": stats.get("giveaways", 0),
                    "SEASON": SEASON_ID,
                    "SEASON_TYPE": SEASON_TYPE,
                }

            away_team = matchup.get("away", {})
            home_team = matchup.get("home", {})

            away_row = build_team_row("away", away_team)
            home_row = build_team_row("home", home_team)

            all_rows.extend([away_row, home_row])

    df = pd.DataFrame(all_rows)
    if not df.empty:
        df.sort_values(["GAME_DATE", "GAME_ID", "TEAM_ABBREVIATION"], inplace=True)
    return df


def main():
    try:
        team_map = fetch_team_map()
        df = build_rows(team_map)
    except Exception as exc:  # noqa: BLE001
        print("❌ Erro geral:", exc)
        return

    if df.empty:
        print("⚠️ Sem dados para gravar.")
        return

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")
    print(f"✅ Ficheiro atualizado: {OUTPUT_FILE} ({len(df)} linhas)")


if __name__ == "__main__":
    main()
