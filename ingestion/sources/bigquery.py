import basedosdados as bd
import pandas as pd

from ingestion.config import config


def _query(sql: str) -> pd.DataFrame:
    return bd.read_sql(sql, billing_project_id=config.gcp_project_id)


def fetch_dim_cnae() -> pd.DataFrame:
    return _query("""
        SELECT
            subclasse,
            descricao_subclasse,
            classe,
            descricao_classe,
            grupo,
            descricao_grupo,
            divisao,
            descricao_divisao,
            secao,
            descricao_secao
        FROM `basedosdados.br_bd_diretorios_brasil.cnae_2`
    """)


def fetch_dim_cbo() -> pd.DataFrame:
    return _query("""
        SELECT
            cbo_2002,
            descricao,
            grande_grupo,
            descricao_grande_grupo,
            indicador_cbo_2002_ativa
        FROM `basedosdados.br_bd_diretorios_brasil.cbo_2002`
    """)


def fetch_dim_municipio() -> pd.DataFrame:
    return _query("""
        SELECT
            id_municipio,
            id_municipio_6,
            nome,
            sigla_uf,
            nome_uf,
            nome_regiao
        FROM `basedosdados.br_bd_diretorios_brasil.municipio`
    """)
