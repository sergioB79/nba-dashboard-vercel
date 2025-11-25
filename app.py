import os
import csv
import collections
from datetime import datetime, timezone, timedelta

from flask import Flask, jsonify, Response, send_from_directory

# Usamos scoreboardv3 (stats) para jogos de hoje + amanhã
from nba_api.stats.endpoints import scoreboardv3

# -----------------------------------------------------------------------------
# Configuração básica
# -----------------------------------------------------------------------------

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# CSV estático dentro da pasta data/
CSV_PATH = os.path.join(BASE_DIR, "data", "nba_quarters_202526.csv")

# Conferência por equipa (tricode)
CONF_BY_TRICODE = {
    "ATL": "East", "BOS": "East", "BKN": "East", "CHA": "East", "CHI": "East",
    "CLE": "East", "DET": "East", "IND": "East", "MIA": "East", "MIL": "East",
    "NYK": "East", "ORL": "East", "PHI": "East", "TOR": "East", "WAS": "East",
    "DAL": "West", "DEN": "West", "GSW": "West", "HOU": "West", "LAC": "West",
    "LAL": "West", "MEM": "West", "MIN": "West", "NOP": "West", "OKC": "West",
    "PHX": "West", "POR": "West", "SAC": "West", "SAS": "West", "UTA": "West",
}

# -----------------------------------------------------------------------------
# Helpers gerais
# -----------------------------------------------------------------------------

def safe_int(value, default=None):
    try:
        if value is None:
            return default
        return int(value)
    except (ValueError, TypeError):
        return default


def compute_streak(results):
    """Recebe lista de 'W'/'L' e devolve '+N' ou '-N'."""
    if not results:
        return ""
    last = results[-1]
    count = 0
    for r in reversed(results):
        if r == last:
            count += 1
        else:
            break
    sign = "+" if last == "W" else "-"
    return f"{sign}{count}"


# -----------------------------------------------------------------------------
# Jogos de hoje + amanhã via ScoreboardV3
# -----------------------------------------------------------------------------

def get_live_and_upcoming_games_from_scoreboardv3():
    """
    Usa ScoreboardV3 para:
      - Jogos de HOJE (live + agendados)
      - Jogos de AMANHÃ (agendados)

    Devolve:
      live_games, today_upcoming, tomorrow_upcoming, warnings
    """
    warnings = []

    today = datetime.now(timezone.utc).date()
    tomorrow = today + timedelta(days=1)

    def _simplify_team(team_dict):
        if not team_dict:
            return {
                "team_id": None,
                "tricode": "",
                "city": "",
                "name": "",
                "score": 0,
                "wins": None,
                "losses": None,
                "record": "",
            }

        wins = team_dict.get("wins")
        losses = team_dict.get("losses")

        if wins is not None and losses is not None:
            record = f"{wins}-{losses}"
        else:
            record = ""

        return {
            "team_id": safe_int(team_dict.get("teamId")),
            "tricode": team_dict.get("teamTricode") or "",
            "city": team_dict.get("teamCity") or "",
            "name": team_dict.get("teamName") or "",
            "score": safe_int(team_dict.get("score"), default=0),
            "wins": safe_int(wins),
            "losses": safe_int(losses),
            "record": record,
        }

    def _fetch_day(day):
        day_str = day.strftime("%Y-%m-%d")
        try:
            sb = scoreboardv3.ScoreboardV3(game_date=day_str, league_id="00", timeout=8)
            data = sb.get_dict()
            games = data.get("scoreboard", {}).get("games", [])
        except Exception as e:  # noqa: BLE001
            msg = f"Erro ao obter ScoreboardV3 para {day_str}: {e}"
            print(msg)
            warnings.append(msg)
            return [], []

        live_list = []
        upcoming_list = []

        for g in games:
            status = safe_int(g.get("gameStatus"), default=0)
            # 1 = agendado, 2 = live, 3 = final
            if status not in (1, 2):
                continue

            home = _simplify_team(g.get("homeTeam") or {})
            away = _simplify_team(g.get("awayTeam") or {})

            period_raw = g.get("period")
            if isinstance(period_raw, dict):
                period = period_raw.get("current") or period_raw.get("period")
            else:
                period = period_raw

            simple = {
                "game_id": g.get("gameId"),
                "status": status,
                "status_text": g.get("gameStatusText") or "",
                "start_time_utc": g.get("gameTimeUTC"),
                "date": day_str,
                "time_local": g.get("gameTimeLocal") or "",
                "period": safe_int(period, 0),
                "clock": g.get("gameClock") or "",
                "home": home,
                "away": away,
            }

            if status == 2:
                live_list.append(simple)
            elif status == 1:
                upcoming_list.append(simple)

        return live_list, upcoming_list

    live_today, today_upcoming = _fetch_day(today)
    _, tomorrow_upcoming = _fetch_day(tomorrow)

    return live_today, today_upcoming, tomorrow_upcoming, warnings


# -----------------------------------------------------------------------------
# Standings a partir do CSV (sem API ao vivo)
# -----------------------------------------------------------------------------

def compute_standings_from_csv():
    """
    Lê o ficheiro nba_quarters_202526.csv e calcula standings por equipa:
      - wins, losses
      - home/away record
      - streak geral, casa, fora
      - conferência (East/West)
    """
    warnings = []
    rows_final = []

    if not os.path.exists(CSV_PATH):
        warnings.append(f"CSV não encontrado em {CSV_PATH}")
        return rows_final, warnings

    try:
        games = collections.defaultdict(list)
        with open(CSV_PATH, encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                gid = row.get("GAME_ID")
                if not gid:
                    continue
                games[gid].append(row)

        team_stats = {}

        def get_stats(team_id, tricode, name):
            s = team_stats.get(team_id)
            if not s:
                conf = CONF_BY_TRICODE.get(tricode, "")
                s = {
                    "team_id": safe_int(team_id),
                    "tricode": tricode,
                    "team": name,
                    "city": "",
                    "name": name,
                    "conf": conf,
                    "wins": 0,
                    "losses": 0,
                    "home_w": 0,
                    "home_l": 0,
                    "away_w": 0,
                    "away_l": 0,
                    "results": [],
                    "home_results": [],
                    "away_results": [],
                }
                team_stats[team_id] = s
            return s

        for gid, team_rows in games.items():
            if len(team_rows) != 2:
                # jogos estranhos (não deviam acontecer)
                continue

            a, b = team_rows
            matchup = a.get("MATCHUP") or b.get("MATCHUP") or ""
            parts = matchup.split()
            if "@" in parts:
                # "HOU @ OKC"
                away_code = parts[0]
                home_code = parts[-1]
            else:
                # assumir "X vs. Y"
                if len(parts) >= 3:
                    home_code = parts[0]
                    away_code = parts[-1]
                else:
                    # fallback tosco
                    home_code = a.get("TEAM_ABBREVIATION")
                    away_code = b.get("TEAM_ABBREVIATION")

            rows_by_code = {row.get("TEAM_ABBREVIATION"): row for row in team_rows}
            home_row = rows_by_code.get(home_code) or a
            away_row = rows_by_code.get(away_code) or b

            try:
                home_pts = int(home_row.get("PTS") or 0)
                away_pts = int(away_row.get("PTS") or 0)
            except ValueError:
                continue

            if home_pts > away_pts:
                home_result, away_result = "W", "L"
            elif home_pts < away_pts:
                home_result, away_result = "L", "W"
            else:
                # empate não existe na NBA; ignorar
                continue

            for row, is_home, result in [
                (home_row, True, home_result),
                (away_row, False, away_result),
            ]:
                tid = row.get("TEAM_ID")
                tri = row.get("TEAM_ABBREVIATION")
                name = row.get("TEAM_NAME")
                if not tid or not tri:
                    continue

                s = get_stats(tid, tri, name)

                if result == "W":
                    s["wins"] += 1
                    if is_home:
                        s["home_w"] += 1
                    else:
                        s["away_w"] += 1
                else:
                    s["losses"] += 1
                    if is_home:
                        s["home_l"] += 1
                    else:
                        s["away_l"] += 1

                s["results"].append(result)
                if is_home:
                    s["home_results"].append(result)
                else:
                    s["away_results"].append(result)

        # Construir linhas finais
        for team_id, s in team_stats.items():
            wins = s["wins"]
            losses = s["losses"]
            games_played = wins + losses
            win_pct = wins / games_played if games_played else 0.0

            row = {
                "team_id": s["team_id"],
                "tricode": s["tricode"],
                "team": s["team"],
                "city": s["city"],
                "name": s["name"],
                "conf": s["conf"],
                "wins": wins,
                "losses": losses,
                "win_pct": win_pct,
                "home_w": s["home_w"],
                "home_l": s["home_l"],
                "road_w": s["away_w"],
                "road_l": s["away_l"],
                "streak": compute_streak(s["results"]),
                "streak_home": compute_streak(s["home_results"]),
                "streak_away": compute_streak(s["away_results"]),
                "league_rank": None,
                "playoff_rank": None,
            }
            rows_final.append(row)

        rows_final.sort(
            key=lambda r: (-r["win_pct"], -r["wins"], (r["team"] or ""))
        )

    except Exception as exc:  # noqa: BLE001
        warnings.append(f"Erro ao processar CSV de standings: {exc}")
        rows_final = []

    return rows_final, warnings


# -----------------------------------------------------------------------------
# Endpoints
# -----------------------------------------------------------------------------

@app.route("/")
def index():
    return send_from_directory(BASE_DIR, "index.html")


@app.route("/api/quarters_csv")
def quarters_csv():
    if not os.path.exists(CSV_PATH):
        return jsonify({"error": f"CSV não encontrado em {CSV_PATH}"}), 500

    try:
        with open(CSV_PATH, "r", encoding="utf-8") as f:
            csv_text = f.read()
        return Response(csv_text, mimetype="text/csv")
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": f"Falha ao ler CSV: {exc}"}), 500


@app.route("/api/games")
def api_games():
    live_games, today_upcoming, tomorrow_upcoming, warnings = (
        get_live_and_upcoming_games_from_scoreboardv3()
    )

    has_any = bool(live_games or today_upcoming or tomorrow_upcoming)

    data = {
        "ok": has_any,
        "live_games": live_games,
        "today_upcoming": today_upcoming,
        "tomorrow_upcoming": tomorrow_upcoming,
        "warnings": warnings,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }

    return jsonify(data)


@app.route("/api/standings")
def api_standings():
    rows, warnings = compute_standings_from_csv()
    ok = bool(rows)
    return jsonify({"ok": ok, "rows": rows, "warnings": warnings})


@app.route("/api/health")
def api_health():
    return jsonify(
        {
            "ok": True,
            "message": "NBA backend a funcionar",
            "time_utc": datetime.now(timezone.utc).isoformat(),
        }
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
