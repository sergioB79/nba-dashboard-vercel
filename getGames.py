"""
getGames.py

Vai buscar os jogos de HOJE à API oficial JSON da NBA
(cdn.nba.com) e grava em data/games_cache.json.

Este script é pensado para correr em GitHub Actions.
"""

import os
import json
from datetime import datetime, timezone

import requests

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(BASE_DIR, "data", "games_cache.json")

NBA_URL = "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json"


def fetch_games():
    now_iso = datetime.now(timezone.utc).isoformat()

    try:
        r = requests.get(NBA_URL, timeout=10)
        r.raise_for_status()
        data = r.json()
    except Exception as e:  # noqa: BLE001
        print("❌ Erro ao obter jogos da NBA:", e)
        return {
            "ok": False,
            "live_games": [],
            "today_upcoming": [],
            "tomorrow_upcoming": [],
            "warnings": [f"Erro ao obter jogos: {e}"],
            "generated_at_utc": now_iso,
        }

    games = data.get("scoreboard", {}).get("games", [])

    live = []
    upcoming = []

    for g in games:
        status = g.get("gameStatus", 0)
        simple = {
            "game_id": g.get("gameId"),
            "status": status,
            "status_text": g.get("gameStatusText"),
            "period": g.get("period"),
            "clock": g.get("gameClock"),
            "start_time_utc": g.get("gameTimeUTC"),
            "home": g.get("homeTeam", {}),
            "away": g.get("awayTeam", {}),
        }

        if status == 2:  # live
            live.append(simple)
        else:
            upcoming.append(simple)

    return {
        "ok": True,
        "live_games": live,
        "today_upcoming": upcoming,
        "tomorrow_upcoming": [],  # esta API só dá o dia atual
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
