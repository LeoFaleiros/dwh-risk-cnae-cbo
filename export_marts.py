"""
Exporta as tabelas de mart para CSV em output/.

Uso:
    python export_marts.py

Gera:
    output/mart_risco_teorico_vs_observado.csv
    output/mart_cat_cnae_cbo_mes.csv
    output/mart_top_cbo_por_cnae.csv
    output/mart_perfil_risco_cnae.csv
    output/mart_severidade_cat_cnae.csv
    output/mart_mortalidade_por_cnae.csv
"""

import os
import pandas as pd
from sqlalchemy import create_engine
from ingestion.config import config

OUTPUT_DIR = "output"

MARTS = [
    "mart_risco_teorico_vs_observado",
    "mart_cat_cnae_cbo_mes",
    "mart_top_cbo_por_cnae",
    "mart_perfil_risco_cnae",
    "mart_severidade_cat_cnae",
    "mart_mortalidade_por_cnae",
]


def main() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    engine = create_engine(config.database_url)

    for mart in MARTS:
        df = pd.read_sql_table(mart, engine, schema="mart")
        path = os.path.join(OUTPUT_DIR, f"{mart}.csv")
        df.to_csv(path, index=False, encoding="utf-8-sig")  # utf-8-sig: BOM para Excel abrir corretamente
        print(f"{mart}: {len(df):,} linhas → {path}")


if __name__ == "__main__":
    main()
