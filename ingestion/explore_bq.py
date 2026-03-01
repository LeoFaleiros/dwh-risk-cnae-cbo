"""
Exploracao das fontes BigQuery.
Rodar uma vez para confirmar acesso e documentar o schema real.
Nao e parte do pipeline.
"""
import os
import sys
import basedosdados as bd
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
if not PROJECT_ID:
    sys.exit("GCP_PROJECT_ID nao definido no .env")


TABLES = [
    ("br_bd_diretorios_brasil", "cnae_2"),
    ("br_bd_diretorios_brasil", "cbo_2002"),
    ("br_bd_diretorios_brasil", "municipio"),
    ("br_me_rais",              "microdados_vinculos"),
    ("br_ms_sim",               "microdados"),
]


def explore(dataset: str, table: str) -> None:
    print(f"\n{'='*60}")
    print(f"  {dataset}.{table}")
    print(f"{'='*60}")

    # Schema
    df_schema = bd.read_sql(
        f"SELECT column_name, data_type "
        f"FROM `basedosdados.{dataset}.INFORMATION_SCHEMA.COLUMNS` "
        f"WHERE table_name = '{table}' ORDER BY ordinal_position",
        billing_project_id=PROJECT_ID,
    )
    print("\nColunas:")
    print(df_schema.to_string(index=False))

    # Contagem (sem scan completo para economizar custo)
    try:
        df_count = bd.read_sql(
            f"SELECT COUNT(*) AS total FROM `basedosdados.{dataset}.{table}`",
            billing_project_id=PROJECT_ID,
        )
        print(f"\nTotal de registros: {df_count['total'].iloc[0]:,}")
    except Exception as e:
        print(f"\nContagem indisponivel: {e}")

    # Amostra
    df_sample = bd.read_sql(
        f"SELECT * FROM `basedosdados.{dataset}.{table}` LIMIT 3",
        billing_project_id=PROJECT_ID,
    )
    print("\nAmostra (3 linhas):")
    print(df_sample.to_string(index=False))


if __name__ == "__main__":
    for ds, tbl in TABLES:
        try:
            explore(ds, tbl)
        except Exception as e:
            print(f"\nERRO em {ds}.{tbl}: {e}")
