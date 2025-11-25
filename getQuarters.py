"""
getQuarters.py (versão com GAME_ID normalizado + update incremental limpo)

- Cria / atualiza um ficheiro CSV com TODOS os jogos da época 2025-26 (Regular Season),
  com pontos por período (Q1..Q4) e total de OT (OT) por equipa, usando ScoreboardV3.
- Normaliza sempre GAME_ID para 10 dígitos com zeros à esquerda (ex: 0022500001).
- Se o ficheiro já existir, só chama a API para os jogos que ainda não estão no CSV.
- No fim, mantém apenas os GAME_ID que aparecem no LeagueGameLog dessa época (limpa lixo).
"""

import os
import time
from typing import Dict, List, Optional, Set

import pandas as pd
from nba_api.stats.endpoints import leaguegamelog, scoreboardv3


SEASON = "2025-26"               # época atual
SEASON_TYPE = "Regular Season"   # ou "Playoffs"
# grava diretamente dentro de data/
OUTPUT_FILE = os.path.join("data", f"nba_quarters_{SEASON.replace('-', '')}.csv")
SLEEP_SECONDS = 0.8              # pausa entre chamadas à API (ajusta se precisares)


# ==============================
#  HELPERS
# ==============================

def normalize_game_id(x) -> str:
    """
    Converte GAME_ID para string com 10 dígitos, com zeros à esquerda se necessário.
    Ex.: 22500001 -> '0022500001'
    """
    if pd.isna(x):
        return ""
    s = str(x).strip()
    # remover possíveis ".0" vindos de float
    if s.endswith(".0"):
        s = s[:-2]
    return s.zfill(10)


def load_existing_output(path: str) -> Optional[pd.DataFrame]:
    """
    Lê o CSV existente (se houver). Se não existir, devolve None.
    """
    if not os.path.exists(path):
        print("ℹ️ Ficheiro de saída ainda não existe. Vai ser criado de raiz.")
        return None

    try:
        df = pd.read_csv(path)
        print(f"📂 Ficheiro existente encontrado: {path} (linhas: {len(df)})")
        return df
    except Exception as e:
        print(f"⚠️ Erro a ler ficheiro existente ({path}): {e}")
        print("   Vai ser ignorado e recriado de raiz.")
        return None


def get_existing_game_ids(df: Optional[pd.DataFrame]) -> Set[str]:
    """
    Extrai o conjunto de GAME_ID já presentes no CSV.
    """
    if df is None or df.empty:
        return set()
    # garantir que estão normalizados
    return set(df["GAME_ID"].astype(str).apply(normalize_game_id).unique())


def get_season_games(season: str, season_type: str) -> pd.DataFrame:
    """
    Vai ao LeagueGameLog buscar TODOS os jogos (home e away) dessa época e tipo de época.

    Devolve DataFrame com colunas importantes: GAME_ID, GAME_DATE, MATCHUP, TEAM_ID, PTS, etc.
    """
    lg = leaguegamelog.LeagueGameLog(
        league_id="00",
        season=season,
        season_type_all_star=season_type,
        counter=0,
        direction="ASC",
        player_or_team="T",  # T -> Team
        sorter="DATE"
    )
    df = lg.get_data_frames()[0]

    # Converter GAME_DATE para datetime e criar coluna de "dia" (yyyy-mm-dd)
    df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"])
    df["GAME_DAY"] = df["GAME_DATE"].dt.floor("D")

    # Normalizar GAME_ID aqui para já vir limpo
    df["GAME_ID"] = df["GAME_ID"].apply(normalize_game_id)

    print(f"✅ LeagueGameLog devolveu {len(df)} linhas (equipa/jogo).")
    return df


def fetch_day_from_scoreboard(day: pd.Timestamp) -> List[Dict]:
    """
    Vai ao ScoreboardV3 para um determinado dia (yyyy-mm-dd) e devolve
    uma lista de dicts com estatísticas por equipa/jogo:

    [
      {
        "GAME_ID": "0022500001",
        "TEAM_ID": 1610612737,
        "TEAM_ABBREVIATION": "ATL",
        "TEAM_NAME": "Atlanta Hawks",
        "MATCHUP": "ATL @ BOS",
        "Q1": 25,
        "Q2": 31,
        "Q3": 22,
        "Q4": 27,
        "OT": 10,
        "PTS": 115,
      },
      ...
    ]
    """
    date_str = day.strftime("%Y-%m-%d")
    sb = scoreboardv3.ScoreboardV3(game_date=date_str, league_id="00", day_offset=0)
    games = sb.get_data_frames()

    # scoreboardv3 devolve vários DataFrames; o que nos interessa é o "LineScore"
    # mas, na versão atual da nba_api, ele costuma vir como o segundo DataFrame.
    if len(games) < 2:
        print(f"⚠️ ScoreboardV3({date_str}) não devolveu LineScore esperado.")
        return []

    # Tentamos identificar o DF que tenha colunas "GAME_ID", "TEAM_ID" etc.
    linescore_df = None
    for df in games:
        if {"GAME_ID", "TEAM_ID", "TEAM_ABBREVIATION", "TEAM_NAME"}.issubset(df.columns):
            linescore_df = df
            break

    if linescore_df is None or linescore_df.empty:
        print(f"⚠️ ScoreboardV3({date_str}) não tem LineScore com as colunas esperadas.")
        return []

    # Alguns ScoreboardV3 já trazem Q1, Q2, Q3, Q4, OT, PTS
    cols_needed = ["GAME_ID", "TEAM_ID", "TEAM_ABBREVIATION", "TEAM_NAME", "PTS"]
    quarter_cols = ["PTS_QTR1", "PTS_QTR2", "PTS_QTR3", "PTS_QTR4"]

    for c in cols_needed + quarter_cols:
        if c not in linescore_df.columns:
            print(f"⚠️ Coluna {c} em falta em LineScore({date_str}).")
            return []

    rows = []
    for _, row in linescore_df.iterrows():
        game_id = normalize_game_id(row["GAME_ID"])
        team_id = int(row["TEAM_ID"])
        team_abbr = str(row["TEAM_ABBREVIATION"])
        team_name = str(row["TEAM_NAME"])

        q1 = int(row["PTS_QTR1"])
        q2 = int(row["PTS_QTR2"])
        q3 = int(row["PTS_QTR3"])
        q4 = int(row["PTS_QTR4"])
        pts_total = int(row["PTS"])

        ot = pts_total - (q1 + q2 + q3 + q4)

        rows.append(
            {
                "GAME_ID": game_id,
                "TEAM_ID": team_id,
                "TEAM_ABBREVIATION": team_abbr,
                "TEAM_NAME": team_name,
                "Q1": q1,
                "Q2": q2,
                "Q3": q3,
                "Q4": q4,
                "OT": ot,
                "PTS": pts_total,
            }
        )

    return rows


def cleanup_and_write(df: pd.DataFrame, games_df: pd.DataFrame) -> None:
    """
    Limpa / normaliza o DataFrame final e grava no OUTPUT_FILE:

    - Normaliza GAME_ID.
    - Remove linhas com GAME_ID que não existam no LeagueGameLog dessa época.
    - Garante tipos numéricos em Q1..Q4, OT, PTS.
    - Ordena por GAME_DATE + TEAM_ABBREVIATION.
    """
    print("🧹 A limpar e normalizar DataFrame final...")

    df = df.copy()

    # Normalizar GAME_ID
    df["GAME_ID"] = df["GAME_ID"].apply(normalize_game_id)

    # Lista de GAME_ID válidos (que existem no LeagueGameLog da época)
    valid_ids = set(games_df["GAME_ID"].astype(str).apply(normalize_game_id).unique())
    before = len(df)
    df = df[df["GAME_ID"].isin(valid_ids)].copy()
    after = len(df)
    print(f"   Removidas {before - after} linhas com GAME_ID fora da época {SEASON}.")

    # Converter Q1..Q4, OT, PTS para numérico
    for col in ["Q1", "Q2", "Q3", "Q4", "OT", "PTS"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    # Unir com meta de jogos (GAME_DATE, MATCHUP) vindas de games_df
    meta = (
        games_df[["GAME_ID", "GAME_DATE", "MATCHUP"]]
        .drop_duplicates("GAME_ID")
        .copy()
    )
    meta["GAME_ID"] = meta["GAME_ID"].apply(normalize_game_id)

    df = df.merge(meta, on="GAME_ID", how="left")

    # Ordenar por GAME_DATE, GAME_ID, TEAM_ABBREVIATION
    df.sort_values(["GAME_DATE", "GAME_ID", "TEAM_ABBREVIATION"], inplace=True)

    print(f"💾 A gravar ficheiro final: {OUTPUT_FILE} (linhas: {len(df)})")
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")


# ==============================
#  MAIN
# ==============================

def main():
    # 1) Ler ficheiro existente (se houver)
    existing_df = load_existing_output(OUTPUT_FILE)
    if existing_df is not None and not existing_df.empty:
        # Normalizar GAME_ID imediatamente
        existing_df["GAME_ID"] = existing_df["GAME_ID"].apply(normalize_game_id)

    # 2) Jogos da época (todos) via LeagueGameLog
    try:
        games_df = get_season_games(SEASON, SEASON_TYPE)
    except Exception as exc:
        # Não deixamos o script rebentar se a API da NBA estiver lenta/offline.
        print("⚠️ Não foi possível obter LeagueGameLog da stats.nba.com:")
        print(f"   {exc}")
        print("⚠️ A atualização será tentada novamente na próxima execução.")
        return

    if games_df.empty:
        print("❌ LeagueGameLog devolveu vazio. Nada para fazer.")
        return

    all_game_ids = set(games_df["GAME_ID"].astype(str).unique())

    # 3) Quais são os jogos já existentes no CSV?
    existing_ids = get_existing_game_ids(existing_df)

    # 4) Quais são os jogos em falta?
    missing_ids = all_game_ids - existing_ids
    print(f"🔍 Jogos em falta nesta época (ainda não no CSV): {len(missing_ids)}")

    # 5) Se não há jogos em falta, ainda assim limpamos/normalizamos e regravamos
    if len(missing_ids) == 0:
        if existing_df is None or existing_df.empty:
            print("❌ Não há ficheiro existente e não há missing_ids (situação estranha).")
            return
        print("✅ Não há jogos novos para adicionar. A limpar/normalizar ficheiro existente...")
        cleanup_and_write(existing_df, games_df)
        return

    # 6) Restringir o DataFrame de jogos apenas aos missing_ids
    games_missing_df = games_df[games_df["GAME_ID"].astype(str).isin(missing_ids)].copy()

    # Mapa rápido GAME_ID -> (GAME_DATE, MATCHUP)
    game_meta = (
        games_df[["GAME_ID", "GAME_DATE", "MATCHUP"]]
        .drop_duplicates("GAME_ID")
        .set_index("GAME_ID")
        .to_dict(orient="index")
    )

    # Lista de dias com jogos em falta
    days = sorted(games_missing_df["GAME_DAY"].unique())
    print(f"📅 Número de dias com jogos em falta: {len(days)}")

    all_rows: List[Dict] = []

    for idx, d in enumerate(days, start=1):
        print(f"[{idx}/{len(days)}] {d} → a ler ScoreboardV3...")
        day_rows = fetch_day_from_scoreboard(d)
        if not day_rows:
            continue

        # Enriquecer com GAME_DATE / MATCHUP vindos do LeagueGameLog
        for r in day_rows:
            gid = str(r["GAME_ID"])
            # só queremos guardar se este jogo estiver em missing_ids
            if gid not in missing_ids:
                continue

            meta = game_meta.get(gid)
            if meta:
                # meta["GAME_DATE"] é datetime (porque convertida em get_season_games)
                r["GAME_DATE"] = meta["GAME_DATE"].strftime("%Y-%m-%d")
                r["MATCHUP"] = meta["MATCHUP"]
            else:
                # fallback se algum GAME_ID não estiver em LeagueGameLog
                r["GAME_DATE"] = d.strftime("%Y-%m-%d")
                r["MATCHUP"] = ""

            r["SEASON"] = SEASON
            r["SEASON_TYPE"] = SEASON_TYPE

            all_rows.append(r)

        time.sleep(SLEEP_SECONDS)

    if not all_rows:
        print("❌ Scoreboard não devolveu dados novos para nenhum dia (jogos podem ainda não ter boxscore disponível).")
        # Mesmo assim limpamos/normalizamos o que já existe
        if existing_df is not None and not existing_df.empty:
            cleanup_and_write(existing_df, games_df)
        return

    new_df = pd.DataFrame(all_rows)
    print(f"➕ Novas linhas obtidas (equipa/jogo): {len(new_df)}")

    # 7) Juntar com dados antigos (se existirem)
    if existing_df is not None and not existing_df.empty:
        full_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        full_df = new_df

    # 8) Limpar, normalizar e gravar ficheiro final
    cleanup_and_write(full_df, games_df)


if __name__ == "__main__":
    main()
