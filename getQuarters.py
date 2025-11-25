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

# ==============================
#  CONFIG
# ==============================

SEASON = "2025-26"               # época atual
SEASON_TYPE = "Regular Season"   # ou "Playoffs"
OUTPUT_FILE = os.path.join("data", f"nba_quarters_{SEASON.replace('-', '')}.csv")
SLEEP_SECONDS = 0.8              # pausa entre chamadas à API (ajusta se precisares)


# ==============================
#  HELPERS
# ==============================

def normalize_game_id(x) -> str:
    """
    Converte GAME_ID para string com 10 dígitos, com zeros à esquerda.
    Ex:
      22500001      -> '0022500001'
      '0022500001'  -> '0022500001'
      2.2500001e7   -> '0022500001'
    """
    s = "".join(ch for ch in str(x) if ch.isdigit())
    return s.zfill(10)


def safe_int(val) -> int:
    try:
        return int(val)
    except Exception:
        return 0


def get_season_games(season: str, season_type: str) -> pd.DataFrame:
    """
    Vai buscar todos os jogos da época via LeagueGameLog (uma linha por equipa),
    depois reduz para uma linha por GAME_ID.
    """
    print(f"📝 A obter jogos da época {season} ({season_type}) via LeagueGameLog...")
    lg = leaguegamelog.LeagueGameLog(
        season=season,
        season_type_all_star=season_type,
    )
    df = lg.get_data_frames()[0]

    games = (
        df[["GAME_ID", "GAME_DATE", "MATCHUP"]]
        .drop_duplicates("GAME_ID")
        .copy()
    )

    # Normalizar GAME_ID aqui logo
    games["GAME_ID"] = games["GAME_ID"].apply(normalize_game_id)

    # GAME_DATE como datetime + coluna só com date para agrupar por dia
    games["GAME_DATE"] = pd.to_datetime(games["GAME_DATE"])
    games["GAME_DAY"] = games["GAME_DATE"].dt.date

    print(f"✅ Encontrados {len(games)} jogos únicos nesta época (LeagueGameLog).")
    return games


def fetch_day_from_scoreboard(game_day, request_timeout: int = 8) -> List[Dict]:
    """
    Usa ScoreboardV3 para um dia específico e devolve linhas
    com Q1..Q4 + OT por equipa.
    """
    day_str = game_day.strftime("%Y-%m-%d")
    try:
        sb = scoreboardv3.ScoreboardV3(game_date=day_str, timeout=request_timeout)
        data = sb.get_dict()
        games = data.get("scoreboard", {}).get("games", [])
    except Exception as e:
        print(f"  ⚠️ Falha no ScoreboardV3 para {day_str}: {e}")
        return []

    rows: List[Dict] = []

    for g in games:
        game_id_raw = g.get("gameId")
        if not game_id_raw:
            continue
        game_id = normalize_game_id(game_id_raw)

        # home / away
        for side in ["homeTeam", "awayTeam"]:
            t = g.get(side, {}) or {}
            periods = t.get("periods", []) or []

            if not periods:
                continue

            # primeiros 4 períodos = Q1..Q4
            q_scores = [safe_int(p.get("score")) for p in periods[:4]]
            while len(q_scores) < 4:
                q_scores.append(0)

            total = safe_int(t.get("score"))
            ot_total = max(0, total - sum(q_scores))

            try:
                team_id = int(t.get("teamId"))
            except Exception:
                continue

            rows.append(
                {
                    "GAME_ID": game_id,
                    "TEAM_ID": team_id,
                    "TEAM_ABBREVIATION": t.get("teamTricode") or "",
                    "TEAM_NAME": t.get("teamName") or "",
                    "Q1": q_scores[0],
                    "Q2": q_scores[1],
                    "Q3": q_scores[2],
                    "Q4": q_scores[3],
                    "OT": ot_total,
                    "OT_FLAG": 1 if len(periods) > 4 else 0,
                    "OT_PERIODS": max(0, len(periods) - 4),
                    "PTS": total,
                }
            )

    return rows


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


def get_existing_game_ids(existing: Optional[pd.DataFrame]) -> Set[str]:
    """
    Devolve o conjunto de GAME_ID já presentes no CSV para esta SEASON.
    Se não houver ficheiro, devolve conjunto vazio.
    IMPORTANTE: assume GAME_ID já normalizado (normalize_game_id).
    """
    if existing is None:
        return set()

    if "GAME_ID" not in existing.columns:
        return set()

    # Se tiver coluna SEASON, filtramos só essa época; se não, usamos todos.
    if "SEASON" in existing.columns:
        mask = existing["SEASON"].astype(str) == SEASON
        ids = existing.loc[mask, "GAME_ID"].astype(str).unique()
    else:
        ids = existing["GAME_ID"].astype(str).unique()

    existing_ids = set(ids)
    print(f"📌 GAME_ID já existentes para {SEASON}: {len(existing_ids)}")
    return existing_ids


def cleanup_and_write(full_df: pd.DataFrame, games_df: pd.DataFrame) -> None:
    """
    - Normaliza GAME_ID no full_df
    - Remove jogos cujo GAME_ID não está em games_df (garante só Regular Season 2025-26)
    - Remove duplicados (GAME_ID + TEAM_ID)
    - Ordena e grava o CSV final.
    """
    if full_df is None or full_df.empty:
        print("❌ Nada para gravar (full_df vazio).")
        return

    # Normalizar GAME_ID
    full_df = full_df.copy()
    full_df["GAME_ID"] = full_df["GAME_ID"].apply(normalize_game_id)

    # Conjunto de GAME_ID válidos para esta época (já normalizados em get_season_games)
    allowed_ids = set(games_df["GAME_ID"].astype(str).unique())

    before_filter = len(full_df)
    full_df = full_df[full_df["GAME_ID"].isin(allowed_ids)]
    after_filter = len(full_df)
    removed_lixo = before_filter - after_filter
    if removed_lixo > 0:
        print(f"🧹 Removidos {removed_lixo} registos cujo GAME_ID não pertence à época {SEASON}.")

    # TEAM_ID como int se existir
    if "TEAM_ID" in full_df.columns:
        full_df["TEAM_ID"] = full_df["TEAM_ID"].apply(safe_int)

    # Remover duplicados por GAME_ID + TEAM_ID
    if {"GAME_ID", "TEAM_ID"}.issubset(full_df.columns):
        before_dups = len(full_df)
        full_df = full_df.drop_duplicates(subset=["GAME_ID", "TEAM_ID"], keep="last")
        after_dups = len(full_df)
        removed_dups = before_dups - after_dups
        if removed_dups > 0:
            print(f"🧹 Removidos {removed_dups} registos duplicados (GAME_ID+TEAM_ID).")

    # Ordenar e gravar
    sort_cols = [c for c in ["GAME_DATE", "GAME_ID", "TEAM_ID"] if c in full_df.columns]
    if sort_cols:
        full_df = full_df.sort_values(sort_cols).reset_index(drop=True)

    full_df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
    print(f"\n✅ Ficheiro atualizado em: {OUTPUT_FILE}")
    print(f"   Linhas (equipa/jogo): {len(full_df)}")
    print("   Jogos únicos (GAME_ID):", full_df["GAME_ID"].nunique())
    print("   Colunas:", ", ".join(full_df.columns))


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
    games_df = get_season_games(SEASON, SEASON_TYPE)
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
