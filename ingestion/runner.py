import argparse
import logging
import sys

import pandas as pd
from sqlalchemy import create_engine

from ingestion.config import config
from ingestion.ops.load_history import (
    compute_checksum,
    is_already_loaded,
    register_error,
    register_finish,
    register_start,
)
from ingestion.sources.bigquery import fetch_dim_cnae, fetch_dim_cbo, fetch_dim_municipio, fetch_rais_vinculos, fetch_sim_obitos
from ingestion.sources.cid_loader import fetch_dim_cid
from ingestion.sources.csv_loader import fetch_dim_grau_risco
from ingestion.sources.inss_cat import fetch_cat_microdados

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("runner")


def load_table(
    engine, df: pd.DataFrame, table: str, source_id: str, force: bool = False
) -> None:
    checksum = compute_checksum(source_id)

    if not force and is_already_loaded(engine, table, checksum):
        log.info("Skipping %s (already loaded)", table)
        return

    load_id = register_start(engine, table, source_id, checksum)
    try:
        df.to_sql(table, engine, schema="raw", if_exists="replace", index=False)
        register_finish(engine, load_id, len(df))
        log.info("Loaded %s: %d rows", table, len(df))
    except Exception as exc:
        register_error(engine, load_id, str(exc))
        log.error("Failed %s: %s", table, exc, exc_info=True)
        raise


def main(force: bool = False) -> None:
    engine = create_engine(config.database_url)
    log.info("Starting ingestion")

    tasks = [
        ("dim_cnae",       fetch_dim_cnae,      "bq:br_bd_diretorios_brasil.cnae_2"),
        ("dim_cbo",        fetch_dim_cbo,        "bq:br_bd_diretorios_brasil.cbo_2002"),
        ("dim_municipio",  fetch_dim_municipio,  "bq:br_bd_diretorios_brasil.municipio"),
        ("dim_grau_risco", fetch_dim_grau_risco, "csv:input/AnexoI_CNAE_GR_NR04_2023_COMPLETO.xlsx"),
        ("dim_cid",        fetch_dim_cid,        "csv:input/CID/CID-10-SUBCATEGORIAS.CSV+CID-10-CATEGORIAS.CSV"),
        (
            "rais_vinculos",
            fetch_rais_vinculos,
            f"bq:rais.{config.ingest_uf}.{config.ingest_ano_inicio}-{config.ingest_ano_fim}",
        ),
        (
            "fact_sim",
            fetch_sim_obitos,
            f"bq:sim.{config.ingest_uf}.{config.ingest_ano_inicio}-{config.ingest_ano_fim}",
        ),
        (
            "cat_microdados",
            fetch_cat_microdados,
            f"csv:cat.{config.ingest_uf}.{config.ingest_ano_inicio}-{config.ingest_ano_fim}",
        ),
    ]

    for table_name, fetch_fn, source_id in tasks:
        try:
            checksum = compute_checksum(source_id)
            if not force and is_already_loaded(engine, table_name, checksum):
                log.info("Skipping %s (already loaded)", table_name)
                continue
            log.info("Fetching %s...", table_name)
            df = fetch_fn()
            load_table(engine, df, table_name, source_id, force=True)
        except Exception as exc:
            log.error("Table %s failed, continuing: %s", table_name, exc)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    main(force=args.force)
