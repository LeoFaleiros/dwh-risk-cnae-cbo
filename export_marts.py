"""
Exporta as tabelas de mart para CSV em output/.

Uso:
    python export_marts.py

Gera (marts de produto/cobertura — principais para o ticket):
    output/mart_risco_morte.csv         → cobertura Morte Natural + Morte Acidental
    output/mart_risco_acidente.csv      → cobertura IPA + DIT proxy
    output/mart_risco_doenca.csv        → cobertura IFPD + Doenças Graves

Gera (marts exploratórios/diagnósticos):
    output/mart_risco_teorico_vs_observado.csv
    output/mart_cat_cnae_cbo_mes.csv
    output/mart_top_cbo_por_cnae.csv
    output/mart_perfil_risco_cnae.csv
    output/mart_severidade_cat_cnae.csv
    output/mart_mortalidade_por_cnae.csv
    output/mart_comorbidades_por_cnae.csv
    output/mart_causas_externas_por_cnae.csv
"""

import os
import pandas as pd
from sqlalchemy import create_engine
from ingestion.config import config

OUTPUT_DIR = "output"

# Marts orientados a cobertura de seguro (prioridade para o ticket)
MARTS_PRODUTO = [
    "mart_risco_morte",
    "mart_risco_acidente",
    "mart_risco_doenca",
]

# Marts exploratórios / diagnósticos
MARTS_EXPLORATORIOS = [
    "mart_risco_teorico_vs_observado",
    "mart_cat_cnae_cbo_mes",
    "mart_top_cbo_por_cnae",
    "mart_perfil_risco_cnae",
    "mart_severidade_cat_cnae",
    "mart_mortalidade_por_cnae",
    "mart_comorbidades_por_cnae",
    "mart_causas_externas_por_cnae",
]

MARTS = MARTS_PRODUTO + MARTS_EXPLORATORIOS


def main() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    engine = create_engine(config.database_url)

    print("\n=== Marts de produto (ticket) ===")
    for mart in MARTS_PRODUTO:
        df = pd.read_sql_table(mart, engine, schema="mart")
        path = os.path.join(OUTPUT_DIR, f"{mart}.csv")
        df.to_csv(path, index=False, encoding="utf-8-sig")  # utf-8-sig: BOM para Excel abrir corretamente
        print(f"  {mart}: {len(df):,} linhas → {path}")

    print("\n=== Marts exploratórios ===")
    for mart in MARTS_EXPLORATORIOS:
        df = pd.read_sql_table(mart, engine, schema="mart")
        path = os.path.join(OUTPUT_DIR, f"{mart}.csv")
        df.to_csv(path, index=False, encoding="utf-8-sig")
        print(f"  {mart}: {len(df):,} linhas → {path}")


if __name__ == "__main__":
    main()
