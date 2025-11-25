import os
import csv
import json
import collections
from datetime import datetime, timezone

from flask import Flask, jsonify, Response, send_from_directory

# -----------------------------------------------------------------------------
# Configuração básica
# -----------------------------------------------------------------------------

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# CSV estático com períodos / standings (tenta NHL, depois fallback antigo)
CSV_CANDIDATES = [
    os.path.join(BASE_DIR, "data", "nhl_periods_20242025.csv"),
    os.path.join(BASE_DIR, "data", "nba_quarters_202526.csv"),
]

# JSON estático com jogos (criado pelo getGames.py via GitHub Actions)
GAMES_JSON_PATH = os.path.join(BASE_DIR, "data", "games_cache.json")

# Conferência por equipa (tricode)
CONF_BY_TRICODE = {
    "BOS": "East", "BUF": "East", "DET": "East", "FLA": "East", "MTL": "East",
    "OTT": "East", "TBL": "East", "TOR": "East", "CAR": "East", "CBJ": "East",
    "NJD": "East", "NYI": "East", "NYR": "East", "PHI": "East", "PIT": "East",
    "WSH": "East", "ARI": "West", "CGY": "West", "CHI": "West", "COL": "West",
    "DAL": "West", "EDM": "West", "LAK": "West", "MIN": "West", "NSH": "West",
    "SJS": "West", "SEA": "West", "STL": "West", "VAN": "West", "VGK": "West",
    "WPG": "West", "ANA": "West",
}

# -----------------------------------------------------------------------------
# Helpers
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
# Standings a partir do CSV (sem API ao vivo)
# -----------------------------------------------------------------------------


def select_csv_path():
    for idx, path in enumerate(CSV_CANDIDATES):
        if os.path.exists(path):
            warn = []
            if idx > 0:
                warn.append(
                    "CSV NHL não encontrado; a usar ficheiro de legado "
                    f"({os.path.basename(path)})."
                )
            return path, warn
    primary = CSV_CANDIDATES[0]
    return primary, [f"CSV não encontrado em {primary}"]


def compute_standings_from_csv():
    """Calcula standings a partir do CSV disponível (NHL se existir)."""
    warnings = []
    rows_final = []

    csv_path, selection_warnings = select_csv_path()
    warnings.extend(selection_warnings)

    if selection_warnings:
        return rows_final, warnings

    try:
        games = collections.defaultdict(list)
        with open(csv_path, encoding="utf-8-sig") as f:
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
                continue

            a, b = team_rows
            matchup = a.get("MATCHUP") or b.get("MATCHUP") or ""
            parts = matchup.split()
            if "@" in parts:
                away_code = parts[0]
                home_code = parts[-1]
            else:
                if len(parts) >= 3:
                    home_code = parts[0]
                    away_code = parts[-1]
                else:
                    home_code = a.get("TEAM_ABBREVIATION")
                    away_code = b.get("TEAM_ABBREVIATION")

            rows_by_code = {row.get("TEAM_ABBREVIATION"): row for row in team_rows}
            home_row = rows_by_code.get(home_code) or a
            away_row = rows_by_code.get(away_code) or b

            try:
                home_pts = int(home_row.get("GOALS") or 0)
                away_pts = int(away_row.get("GOALS") or 0)
            except ValueError:
                continue

            if home_pts > away_pts:
                home_result, away_result = "W", "L"
            elif home_pts < away_pts:
                home_result, away_result = "L", "W"
            else:
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
    csv_path, selection_warnings = select_csv_path()

    if selection_warnings:
        return jsonify({"error": selection_warnings[0]}), 500

    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            csv_text = f.read()
        return Response(csv_text, mimetype="text/csv")
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": f"Falha ao ler CSV: {exc}"}), 500

@app.route("/api/games")
def api_games():
    if not os.path.exists(GAMES_JSON_PATH):
        return jsonify({
            "ok": False,
            "live_games": [],
            "today_upcoming": [],
            "tomorrow_upcoming": [],
            "warnings": ["games_cache.json não encontrado"],
            "generated_at_utc": datetime.now(timezone.utc).isoformat()
        })

    try:
        with open(GAMES_JSON_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return jsonify(data)
    except Exception as e:
        return jsonify({
            "ok": False,
            "live_games": [],
            "today_upcoming": [],
            "tomorrow_upcoming": [],
            "warnings": [str(e)],
            "generated_at_utc": datetime.now(timezone.utc).isoformat()
        })


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
            "message": "NHL backend a funcionar",
            "time_utc": datetime.now(timezone.utc).isoformat(),
        }
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
