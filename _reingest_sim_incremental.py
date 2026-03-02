"""
Re-ingesta do SIM com schema expandido (linha_a, linha_b, linha_c, linha_d).

Lógica incremental por ano:
  - Verifica se raw.fact_sim tem as colunas linha_a/b/c/d.
    Se não tiver, faz ALTER TABLE para adicioná-las (sem apagar dados).
  - Para cada ano no intervalo configurado:
      * Se o ano já foi re-ingerido com o novo schema (linha_a IS NOT NULL em algum
        registro daquele ano), PULA.
      * Se não, DELETE os registros do ano e re-faz o INSERT via BigQuery.
  - Isso permite interromper e retomar sem re-custo de anos já processados.

Custo BigQuery: ~1 requisição por ano × tamanho do estado de SP.
"""

import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import sys
sys.path.insert(0, '.')

from ingestion.config import config
import basedosdados as bd


def _bq_fetch_ano(ano: int) -> pd.DataFrame:
    uf = config.ingest_uf
    print(f"  [BQ] Buscando {ano} (UF={uf})...")
    sql = f"""
        SELECT
            ano,
            sigla_uf,
            id_municipio_residencia  AS id_municipio,
            ocupacao                 AS cbo_2002,
            causa_basica             AS cid_10_causa,
            acidente_trabalho,
            sexo,
            idade,
            linha_a,
            linha_b,
            linha_c,
            linha_d,
            linha_ii,
            COUNT(*)                 AS total_obitos
        FROM `basedosdados.br_ms_sim.microdados`
        WHERE sigla_uf = '{uf}'
          AND ano = {ano}
          AND ocupacao IS NOT NULL
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
    """
    return bd.read_sql(sql, billing_project_id=config.gcp_project_id)


def _ensure_columns(conn):
    """Adiciona colunas linha_a/b/c/d se não existirem."""
    cur = conn.cursor()
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema='raw' AND table_name='fact_sim'
        AND column_name IN ('linha_a','linha_b','linha_c','linha_d')
    """)
    existing = {r[0] for r in cur.fetchall()}
    for col in ['linha_a', 'linha_b', 'linha_c', 'linha_d']:
        if col not in existing:
            print(f"  [DDL] Adicionando coluna raw.fact_sim.{col}...")
            cur.execute(f"ALTER TABLE raw.fact_sim ADD COLUMN {col} TEXT")
    conn.commit()
    cur.close()


def _anos_pendentes(conn, ano_ini: int, ano_fim: int) -> list[int]:
    """Anos que ainda não têm linha_a populada (precisam de re-ingestão)."""
    cur = conn.cursor()
    # Anos que já existem COM linha_a preenchida — estão ok
    cur.execute("""
        SELECT DISTINCT ano FROM raw.fact_sim
        WHERE linha_a IS NOT NULL
        ORDER BY ano
    """)
    ja_migrados = {r[0] for r in cur.fetchall()}
    # Anos que existem mas sem linha_a — precisam re-buscar
    cur.execute("SELECT DISTINCT ano FROM raw.fact_sim ORDER BY ano")
    todos_no_raw = {r[0] for r in cur.fetchall()}
    cur.close()

    pendentes = []
    for ano in range(ano_ini, ano_fim + 1):
        if ano not in ja_migrados:
            pendentes.append(ano)
    return pendentes


def main():
    ano_ini = config.ingest_ano_inicio
    ano_fim = config.ingest_ano_fim

    conn = psycopg2.connect(config.database_url)
    engine = create_engine(config.database_url)

    print("=== Re-ingestão incremental SIM (linha_a/b/c/d) ===")

    # 1. Garantir colunas novas existem
    _ensure_columns(conn)

    # 2. Identificar anos pendentes
    pendentes = _anos_pendentes(conn, ano_ini, ano_fim)
    if not pendentes:
        print("  Nada a fazer — todos os anos já têm linha_a populada.")
        conn.close()
        return

    print(f"  Anos a re-ingerir: {pendentes}")

    cur = conn.cursor()
    for ano in pendentes:
        print(f"\n--- Ano {ano} ---")
        df = _bq_fetch_ano(ano)
        n_bq = len(df)
        print(f"  [BQ] {n_bq:,} combinações retornadas")

        # Deletar registros do ano no raw (schema antigo ou vazio)
        cur.execute("DELETE FROM raw.fact_sim WHERE ano = %s", (ano,))
        n_del = cur.rowcount
        print(f"  [PG] {n_del:,} registros deletados (ano={ano})")

        # Inserir novo schema
        df.to_sql(
            'fact_sim',
            engine,
            schema='raw',
            if_exists='append',
            index=False,
            method='multi',
            chunksize=5000,
        )
        print(f"  [PG] {n_bq:,} registros inseridos (ano={ano})")
        conn.commit()

    # Resumo final
    cur.execute("SELECT COUNT(*), MIN(ano), MAX(ano) FROM raw.fact_sim")
    total, ano_min, ano_max = cur.fetchone()
    cur.execute("SELECT COUNT(*) FROM raw.fact_sim WHERE linha_a IS NOT NULL")
    com_linha_a = cur.fetchone()[0]
    print(f"\n=== Concluído ===")
    print(f"  raw.fact_sim: {total:,} linhas | anos {ano_min}–{ano_max}")
    print(f"  Com linha_a:  {com_linha_a:,} ({com_linha_a*100//total}%)")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
