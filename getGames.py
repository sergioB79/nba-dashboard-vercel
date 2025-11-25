"""
getGames.py

Vai buscar os jogos de HOJE à API oficial JSON da NBA
(cdn.nba.com) e grava em data/games_cache.json.

Forma os campos home/away com *dois conjuntos* de chaves:
- as originais da NBA (teamTricode, teamName, teamCity, wins, losses, score)
- aliases simples usados pelo frontend (tricode, name, city)

Assim o frontend antigo continua a funcionar sem mexer no JS.
"""

import os
import json
from datetime import datetime, timezone

import requests

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(BASE_DIR, "data", "games_cache.json")

NBA_URL = "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json"


def build_team(team_raw: dict) -> dict:
    """
    Converte o bloco homeTeam/awayTeam da NBA num dicionário
    com:
      - chaves originais: teamId, teamTricode, teamName, teamCity, wins, losses, score
      - aliases: tricode, name, city
    """
    if team_raw is None:
        team_raw = {}

    team_tricode = team_raw.get("teamTricode")
    team_name = team_raw.get("teamName")
    team_city = team_raw.get("teamCity")

    return {
        # campos originais
        "teamId": team_raw.get("teamId"),
        "teamTricode": team_tricode,
        "teamName": team_name,
        "teamCity": team_city,
        "wins": team_raw.get("wins"),
        "losses": team_raw.get("losses"),
        "score": team_raw.get("score"),
        "seed": team_raw.get("seed"),
        "inBonus": team_raw.get("inBonus"),
        "timeoutsRemaining": team_raw.get("timeoutsRemaining"),
        "periods": team_raw.get("periods", []),

        # aliases para o frontend antigo
        "tricode": team_tricode,
        "name": team_name,
        "city": team_city,
    }


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

        home = build_team(g.get("homeTeam"))
        away = build_team(g.get("awayTeam"))

        simple = {
            "game_id": g.get("gameId"),
            "status": status,
            "status_text": g.get("gameStatusText"),
            "period": g.get("period"),
            "clock": g.get("gameClock"),
            "start_time_utc": g.get("gameTimeUTC"),
            "home": home,
            "away": away,
        }

        # 1 = agendado, 2 = live, 3 = final
        if status == 2:
            live.append(simple)
        else:
            # por agora mandamos tudo o resto para "upcoming";
            # se quiseres só jogos com status == 1, é trocar esta condição
            upcoming.append(simple)

    return {
        "ok": True,
        "live_games": live,
        "today_upcoming": upcoming,
        "tomorrow_upcoming": [],
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
