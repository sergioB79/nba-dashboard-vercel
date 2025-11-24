import os
import csv
import collections
from datetime import datetime, timezone

from flask import Flask, jsonify, Response, send_from_directory

# NBA API (apenas para jogos live / agendados)
from nba_api.live.nba.endpoints import scoreboard as live_scoreboard

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
# Helpers para LIVE scoreboard
# -----------------------------------------------------------------------------

def simplify_live_team(team_dict):
    """Normaliza estrutura da equipa vinda do live ScoreBoard()."""
    wins = team_dict.get("wins") or team_dict.get("teamWins")
    losses = team_dict.get("losses") or team_dict.get("teamLosses")

    if wins is not None and losses is not None:
        record = f"{wins}-{losses}"
    else:
        record = ""

    return {
        "team_id": team_dict.get("teamId"),
        "tricode": team_dict.get("teamTricode"),
        "city": team_dict.get("teamCity"),
        "name": team_dict.get("teamName"),
        "score": safe_int(team_dict.get("score"), 0),
        "wins": safe_int(wins),
        "losses": safe_int(losses),
        "record": record,
    }


def simplify_live_game(game_dict):
    """Normaliza um jogo da estrutura do live ScoreBoard()."""
    home_raw = game_dict.get("homeTeam", {}) or {}
    away_raw = game_dict.get("awayTeam", {}) or {}

    home = simplify_live_team(home_raw)
    away = simplify_live_team(away_raw)

    period_raw = game_dict.get("period")
    if isinstance(period_raw, dict):
        period = period_raw.get("current") or period_raw.get("period")
    else:
        period = period_raw

    return {
        "game_id": game_dict.get("gameId"),
        "status": game_dict.get("gameStatus"),
        "status_text": game_dict.get("gameStatusText") or "",
        "period": safe_int(period, 0),
        "clock": game_dict.get("gameClock") or "",
        "start_time_utc": game_dict.get("gameTimeUTC"),
        "home": home,
        "away": away,
    }


def fetch_today_games_from_live():
    """
    Usa nba_api.live.nba.endpoints.scoreboard.ScoreBoard()
    para devolver:
      - live_games: em andamento ou concluídos
      - today_upcoming: agendados para hoje
    """
    live_games = []
    today_upcoming = []
    warnings = []

    try:
        sb = live_scoreboard.ScoreBoard()
        games = sb.games.get_dict()  # lista de jogos
        for g in games:
            simplified = simplify_live_game(g)
            status = g.get("gameStatus")
            # gameStatus == 1 → agendado; 2 / 3 → em jogo / final
            if status == 1:
                today_upcoming.append(simplified)
            else:
                live_games.append(simplified)
    except Exception as exc:  # noqa: BLE001
        warnings.append(f"Erro ao obter live scoreboard: {exc}")

    return live_games, today_upcoming, warnings


# -----------------------------------------------------------------------------
# Standings a partir do CSV (sem depender da NBA Stats API)
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
                # jogos estranhos (não deveriam aparecer aqui)
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
                # empate não acontece na NBA; ignorar
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

        # Ordenar por win_pct e vitórias (só para output mais organizado)
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
    """
    Para uso local: devolve o index.html da raiz.
    No Vercel, o ficheiro pode ser servido como static file em vez deste route.
    """
    return send_from_directory(BASE_DIR, "index.html")


@app.route("/api/quarters_csv")
def quarters_csv():
    """
    Devolve o CSV tal como está no disco.
    O frontend espera *texto*, não JSON.
    Em caso de erro devolvemos JSON com 'error'.
    """
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
    """
    Devolve:
      {
        ok: true/false,
        live_games: [...],
        today_upcoming: [...],
        tomorrow_upcoming: [],   # por agora fica vazio
        warnings: [...]
      }
    """
    live_games, today_upcoming, warnings = fetch_today_games_from_live()

    data = {
        "ok": True,
        "live_games": live_games,
        "today_upcoming": today_upcoming,
        # Para já não usamos scoreboardv3 – menos sources de bug.
        "tomorrow_upcoming": [],
        "warnings": warnings,
    }
    return jsonify(data)


@app.route("/api/standings")
def api_standings():
    """
    Devolve standings calculados a partir do CSV:
      {
        ok: true/false,
        rows: [...],
        warnings: [...]
      }
    O frontend faz: standingsApiData = data.rows || [];
    """
    rows, warnings = compute_standings_from_csv()
    ok = bool(rows)

    return jsonify({"ok": ok, "rows": rows, "warnings": warnings})


@app.route("/api/health")
def api_health():
    """Endpoint simples para testar se o backend está operacional."""
    return jsonify(
        {
            "ok": True,
            "message": "NBA backend a funcionar",
            "time_utc": datetime.now(timezone.utc).isoformat(),
        }
    )


# -----------------------------------------------------------------------------
# Execução local
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    # Para testes no teu PC:
    #   python app.py
    #   e depois abre http://127.0.0.1:5000
    app.run(host="0.0.0.0", port=5000, debug=True)
