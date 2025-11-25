import os
import json
from datetime import datetime, timezone
import requests

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(BASE_DIR, "data", "games_cache.json")

NBA_URL = "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json"


def fetch_games():
    try:
        r = requests.get(NBA_URL, timeout=10)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print("❌ Erro ao obter jogos:", e)
        return {
            "ok": False,
            "live_games": [],
            "today_upcoming": [],
            "tomorrow_upcoming": [],
            "warnings": [str(e)],
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        }

    games = data.get("scoreboard", {}).get("games", [])

    live = []
    upcoming = []

    for g in games:
        status = g.get("gameStatus", 0)

        simplified = {
            "game_id": g.get("gameId"),
            "status": status,
            "status_text": g.get("gameStatusText"),
            "period": g.get("period"),
            "clock": g.get("gameClock"),
            "home": g.get("homeTeam", {}),
            "away": g.get("awayTeam", {}),
            "start_time_utc": g.get("gameTimeUTC"),
        }

        if status == 2:  # live
            live.append(simplified)
        else:  # scheduled or final
            upcoming.append(simplified)

    return {
        "ok": True,
        "live_games": live,
        "today_upcoming": upcoming,
        "tomorrow_upcoming": [],  # esta API só dá o dia atual
        "warnings": [],
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }


def main():
    data = fetch_games()

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print("✅ Atualizado:", OUTPUT_FILE)


if __name__ == "__main__":
    main()
