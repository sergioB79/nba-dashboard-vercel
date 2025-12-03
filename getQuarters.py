"""
getQuarters_3.py

Atualiza um CSV com TODOS os jogos da época 2025-26 (Regular Season),
com pontos por período (Q1..Q4) + total de OT (OT) por EQUIPA/JOGO.

Diferenças desta versão:
- Deixa de usar ScoreboardV3 (o schema mudou e já não traz os parciais).
- Passa a usar BoxScoreSummaryV3, que expõe o LineScore com PTS_QTR*.
- Corrige LeagueGameLog para usar o novo argumento player_or_team_abbreviation="T".
"""

import os
import time
from typing import Dict, List, Optional, Set

import pandas as pd
from nba_api.stats.endpoints import leaguegamelog, boxscoresummaryv3

# ==============================
#  CONFIG
# ==============================

SEASON = "2025-26"               # época atual
SEASON_TYPE = "Regular Season"   # ou "Playoffs"

DATA_DIR = "data"
OUTPUT_FILE = os.path.join(DATA_DIR, f"nba_quarters_{SEASON.replace('-', '')}.csv")

# pausa entre chamadas à API (em segundos) – ajusta se começares a ver muitos timeouts
SLEEP_SECONDS = 0.8


# ==============================
#  HELPERS
# ==============================

def normalize_game_id(x) -> str:
    """
    Normaliza GAME_ID para string de 10 dígitos, com zeros à esquerda.
    Ex: 22500001   -> "000022500001" (se viesse assim)
        0022500001 -> "0022500001"  (já vem OK)
    Na prática, os GAME_ID's da NBA já vêm no formato 10 dígitos, mas isto torna a
    limpeza robusta.
    """
    s = str(x).strip()
    if len(s) < 10:
        s = s.zfill(10)
    return s


def ensure_data_dir() -> None:
    if not os.path.isdir(DATA_DIR):
        os.makedirs(DATA_DIR, exist_ok=True)


def load_existing_df() -> Optional[pd.DataFrame]:
    """
    Lê o CSV existente (se houver).
    """
    if not os.path.exists(OUTPUT_FILE):
        print("📂 Nenhum ficheiro existente encontrado. Vai ser criado de raiz.")
        return None

    df = pd.read_csv(OUTPUT_FILE)
    print(f"📂 Ficheiro existente encontrado: {OUTPUT_FILE} (linhas: {len(df)})")

    # Normalizar GAME_ID logo aqui
    if "GAME_ID" in df.columns:
        df["GAME_ID"] = df["GAME_ID"].astype(str).apply(normalize_game_id)

    return df


def get_existing_game_ids(df: Optional[pd.DataFrame]) -> Set[str]:
    """
    Extrai o conjunto de GAME_ID já presentes no CSV.
    """
    if df is None or df.empty:
        return set()
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
        player_or_team_abbreviation="T",  # T -> Team (novo nome do argumento)
        sorter="DATE",
        direction="ASC",
    )
    df = lg.get_data_frames()[0]

    # Normalizar tipos e GAME_ID
    df["GAME_ID"] = df["GAME_ID"].astype(str).apply(normalize_game_id)
    df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"])
    df["GAME_DAY"] = df["GAME_DATE"].dt.floor("D")

    print(f"✅ LeagueGameLog devolveu {len(df)} linhas (equipa/jogo).")
    return df


# ==============================
#  BOX SCORE SUMMARY (QUARTERS)
# ==============================

def fetch_game_from_boxscoresummary(game_id: str, games_df: pd.DataFrame) -> List[Dict]:
    """
    Vai ao BoxScoreSummaryV3 para UM game_id e devolve os parciais por equipa.

    Usa o dataset com colunas:
      - gameId, teamId, teamCity, teamName, teamTricode
      - period1Score..period4Score
      - score
    """
    gid_norm = normalize_game_id(game_id)

    try:
        bs = boxscoresummaryv3.BoxScoreSummaryV3(game_id=gid_norm)
    except Exception as exc:
        print(f"⚠️ Erro em BoxScoreSummaryV3(game_id={gid_norm}): {exc}")
        return []

    line_df = None
    for df in bs.get_data_frames():
        cols = set(df.columns)
        if {"gameId", "teamId", "score",
            "period1Score", "period2Score", "period3Score", "period4Score"
           }.issubset(cols):
            line_df = df
            break

    if line_df is None or line_df.empty:
        print(f"⚠️ BoxScoreSummaryV3({gid_norm}) não tem dataset com period1Score..4/score. Colunas disponíveis:")
        for idx, df in enumerate(bs.get_data_frames()):
            print(f"   Dataset {idx} colunas: {list(df.columns)}")
        return []

    line_df = line_df.copy()
    # normalizar gameId -> GAME_ID
    line_df["gameId"] = line_df["gameId"].astype(str).apply(normalize_game_id)

    rows: List[Dict] = []

    # meta desse jogo vinda do LeagueGameLog (para MATCHUP, GAME_DATE, etc.)
    meta_rows = games_df[games_df["GAME_ID"] == gid_norm]

    for _, row in line_df.iterrows():
        game_id_norm = row["gameId"]
        team_id = int(row["teamId"])

        team_abbr = str(row.get("teamTricode", ""))
        team_name = str(row.get("teamName", ""))

        # parciais (se faltarem, ficam 0)
        def get_int(col: str) -> int:
            v = row.get(col)
            if pd.notna(v):
                return int(v)
            return 0

        q1 = get_int("period1Score")
        q2 = get_int("period2Score")
        q3 = get_int("period3Score")
        q4 = get_int("period4Score")
        pts_total = get_int("score")

        ot_total = pts_total - (q1 + q2 + q3 + q4)

        # MATCHUP a partir do LeagueGameLog (se houver meta p/ este TEAM_ID)
        matchup = ""
        if not meta_rows.empty:
            meta_team = meta_rows[meta_rows["TEAM_ID"] == team_id]
            if not meta_team.empty and "MATCHUP" in meta_team.columns:
                matchup = str(meta_team["MATCHUP"].iloc[0])

        rows.append(
            {
                "GAME_ID": game_id_norm,
                "TEAM_ID": team_id,
                "TEAM_ABBREVIATION": team_abbr,
                "TEAM_NAME": team_name,
                "MATCHUP": matchup,
                "Q1": q1,
                "Q2": q2,
                "Q3": q3,
                "Q4": q4,
                "OT": ot_total,
                "PTS": pts_total,
            }
        )

    return rows



# ==============================
#  CLEANUP + WRITE
# ==============================

def cleanup_and_write(df: pd.DataFrame, games_df: pd.DataFrame) -> None:
    """
    Limpa / normaliza o DataFrame final e grava no OUTPUT_FILE:

    - Normaliza GAME_ID
    - Garante colunas numéricas para Q1..Q4, OT, PTS
    - Junta meta (GAME_DATE, MATCHUP) vinda do LeagueGameLog
    - Remove jogos fora da época SEASON
    """
    if df.empty:
        print("❌ DataFrame final está vazio, nada para gravar.")
        return

    df = df.copy()

    # Normalizar GAME_ID
    df["GAME_ID"] = df["GAME_ID"].astype(str).apply(normalize_game_id)

    # Remover possíveis duplicados exactos (por segurança)
    before = len(df)
    df.drop_duplicates(
        subset=["GAME_ID", "TEAM_ID"], keep="last", inplace=True
    )
    after = len(df)
    if after != before:
        print(f"🧹 Removidas {before - after} linhas duplicadas (GAME_ID, TEAM_ID).")

    # Converter Q1..Q4, OT, PTS para numérico
    for col in ["Q1", "Q2", "Q3", "Q4", "OT", "PTS"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
        else:
            df[col] = 0

    # Meta (GAME_DATE, MATCHUP) vinda do LeagueGameLog
    meta = games_df[["GAME_ID", "GAME_DATE", "MATCHUP"]].drop_duplicates(
        ["GAME_ID", "MATCHUP"]
    )
    meta["GAME_ID"] = meta["GAME_ID"].apply(normalize_game_id)

    df = df.merge(meta, on=["GAME_ID", "MATCHUP"], how="left")

    # Opcional: remover qualquer GAME_ID que não exista em games_df
    valid_ids = set(games_df["GAME_ID"].astype(str).apply(normalize_game_id).unique())
    before = len(df)
    df = df[df["GAME_ID"].isin(valid_ids)].copy()
    after = len(df)
    print(f"   Removidas {before - after} linhas com GAME_ID fora da época {SEASON}.")

    # Ordenar por data e GAME_ID
    if "GAME_DATE" in df.columns:
        df.sort_values(["GAME_DATE", "GAME_ID", "TEAM_ABBREVIATION"], inplace=True)
    else:
        df.sort_values(["GAME_ID", "TEAM_ABBREVIATION"], inplace=True)

    # Gravar
    ensure_data_dir()
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"💾 A gravar ficheiro final: {OUTPUT_FILE} (linhas: {len(df)})")


# ==============================
#  MAIN
# ==============================

def main() -> None:
    ensure_data_dir()

    # 1) Ler CSV existente (se houver)
    existing_df = load_existing_df()

    # 2) Obter todos os jogos da época via LeagueGameLog
    try:
        games_df = get_season_games(SEASON, SEASON_TYPE)
    except Exception as exc:
        print("⚠️ Não foi possível obter LeagueGameLog da stats.nba.com:")
        print(f"   {exc}")
        print("⚠️ A atualização será tentada novamente na próxima execução.")
        return

    if games_df.empty:
        print("❌ LeagueGameLog devolveu vazio. Nada para fazer.")
        return

    all_game_ids = set(games_df["GAME_ID"].astype(str).apply(normalize_game_id).unique())

    # 3) Quais são os jogos já existentes no CSV?
    existing_ids = get_existing_game_ids(existing_df)

    # 4) Quais são os jogos em falta?
    missing_ids = sorted(all_game_ids - existing_ids)
    print(f"🔍 Jogos em falta nesta época (ainda não no CSV): {len(missing_ids)}")

    if not missing_ids:
        print("✅ Não há jogos novos. Apenas a normalizar e regravar o ficheiro.")
        # Mesmo assim limpamos/normalizamos o existente
        if existing_df is not None and not existing_df.empty:
            cleanup_and_write(existing_df, games_df)
        else:
            print("❌ Não há dados existentes para normalizar.")
        return

    # 5) Ir buscar BoxScoreSummaryV3 para cada jogo em falta
    all_rows: List[Dict] = []
    total_missing = len(missing_ids)

    for idx, gid in enumerate(missing_ids, start=1):
        print(f"[{idx}/{total_missing}] GAME_ID {gid} → a ler BoxScoreSummaryV3...")
        rows = fetch_game_from_boxscoresummary(gid, games_df)
        if rows:
            all_rows.extend(rows)
        time.sleep(SLEEP_SECONDS)

    if not all_rows:
        print("❌ BoxScoreSummaryV3 não devolveu dados novos para nenhum jogo em falta.")
        # Ainda assim, podemos apenas normalizar o existente
        if existing_df is not None and not existing_df.empty:
            cleanup_and_write(existing_df, games_df)
        else:
            print("❌ Não há dados existentes para normalizar.")
        return

    new_df = pd.DataFrame(all_rows)
    print(f"➕ Novas linhas obtidas (equipa/jogo): {len(new_df)}")

    # 6) Juntar com dados antigos (se existirem)
    if existing_df is not None and not existing_df.empty:
        full_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        full_df = new_df

    # 7) Limpar, normalizar e gravar ficheiro final
    cleanup_and_write(full_df, games_df)


if __name__ == "__main__":
    main()
