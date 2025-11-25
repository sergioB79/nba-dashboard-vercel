"""
getGames.py (versão NHL)

Vai buscar o calendário de HOJE e AMANHÃ da NHL
(`statsapi.web.nhl.com`) e grava em data/games_cache.json.

Os blocos home/away mantêm chaves simples (tricode, name, city, wins, losses, score)
para que o frontend possa reutilizar o mesmo contrato.
"""

import os
import json
from datetime import datetime, timedelta, timezone
from typing import Dict, Tuple

import requests

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(BASE_DIR, "data", "games_cache.json")

SCHEDULE_URL = "https://statsapi.web.nhl.com/api/v1/schedule"
TEAMS_URL = "https://statsapi.web.nhl.com/api/v1/teams"


def get_team_map() -> Dict[int, Dict[str, str]]:
    try:
        r = requests.get(TEAMS_URL, timeout=10)
        r.raise_for_status()
        data = r.json()
        teams = data.get("teams", [])
    except Exception as exc:  # noqa: BLE001
        print("❌ Erro ao obter lista de equipas NHL:", exc)
        return {}

    mapping = {}
    for t in teams:
        tid = t.get("id")
        mapping[tid] = {
            "tricode": t.get("abbreviation") or (t.get("teamName", "")[:3].upper()),
            "name": t.get("teamName"),
            "city": t.get("locationName"),
            "full": t.get("name"),
        }
    return mapping


def build_team(team_raw: dict, team_map: Dict[int, Dict[str, str]]) -> dict:
    info = team_map.get(team_raw.get("id"), {})
    record = team_raw.get("leagueRecord", {})
    wins = record.get("wins")
    losses = record.get("losses")

    return {
        "teamId": team_raw.get("id"),
        "teamTricode": info.get("tricode"),
        "teamName": info.get("name") or team_raw.get("name"),
        "teamCity": info.get("city"),
        "wins": wins,
        "losses": losses,
        "score": team_raw.get("score"),

        "tricode": info.get("tricode"),
        "name": info.get("name") or team_raw.get("name"),
        "city": info.get("city"),
        "record": f"{wins}-{losses}" if wins is not None and losses is not None else "",
    }


def fetch_linescore(game_pk: int) -> Tuple[int, str]:
    url = f"https://statsapi.web.nhl.com/api/v1/game/{game_pk}/linescore"
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        period = data.get("currentPeriod")
        clock = data.get("currentPeriodTimeRemaining")
        return period, clock
    except Exception:
        return None, None


def fetch_games_for_date(date_str: str, team_map: Dict[int, Dict[str, str]]):
    params = {"date": date_str}
    try:
        r = requests.get(SCHEDULE_URL, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
    except Exception as e:  # noqa: BLE001
        print(f"❌ Erro ao obter jogos da NHL ({date_str}):", e)
        return []

    dates = data.get("dates", [])
    if not dates:
        return []

    games_raw = dates[0].get("games", [])
    games_list = []

    for g in games_raw:
        status_code = g.get("status", {}).get("statusCode", "0")
        detailed = g.get("status", {}).get("detailedState")
        start_time = g.get("gameDate")
        game_pk = g.get("gamePk")

        # NHL status codes: 1/2/3 pre, 4 live, 5 final, 6 future etc
        if status_code in {"1", "2"}:
            status = 1  # scheduled
        elif status_code in {"3", "4"}:
            status = 2  # live
        else:
            status = 3  # final/other

        home_raw = g.get("teams", {}).get("home", {}).get("team", {}) | {
            "score": g.get("teams", {}).get("home", {}).get("score"),
            "leagueRecord": g.get("teams", {}).get("home", {}).get("leagueRecord", {}),
        }
        away_raw = g.get("teams", {}).get("away", {}).get("team", {}) | {
            "score": g.get("teams", {}).get("away", {}).get("score"),
            "leagueRecord": g.get("teams", {}).get("away", {}).get("leagueRecord", {}),
        }

        period, clock = fetch_linescore(game_pk)

        games_list.append(
            {
                "game_id": str(game_pk),
                "status": status,
                "status_text": detailed,
                "period": period,
                "clock": clock,
                "start_time_utc": start_time,
                "home": build_team(home_raw, team_map),
                "away": build_team(away_raw, team_map),
            }
        )

    return games_list


def fetch_games():
    now_iso = datetime.now(timezone.utc).isoformat()
    team_map = get_team_map()

    today = datetime.now(timezone.utc).date()
    tomorrow = today + timedelta(days=1)

    today_str = today.isoformat()
    tomorrow_str = tomorrow.isoformat()

    today_games = fetch_games_for_date(today_str, team_map)
    tomorrow_games = fetch_games_for_date(tomorrow_str, team_map)

    live = [g for g in today_games if g.get("status") == 2]
    today_upcoming = [g for g in today_games if g.get("status") == 1]
    tomorrow_upcoming = [g for g in tomorrow_games if g.get("status") == 1]

    return {
        "ok": True,
        "live_games": live,
        "today_upcoming": today_upcoming,
        "tomorrow_upcoming": tomorrow_upcoming,
        "warnings": [],
        "generated_at_utc": now_iso,
    }


def main():
    data = fetch_games()

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"✅ games_cache.json atualizado em {OUTPUT_FILE} (ok={data['ok']})")


if __name__ == "__main__":
    main()
