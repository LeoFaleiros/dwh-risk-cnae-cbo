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


def fetch_sim_obitos() -> pd.DataFrame:
    uf = config.ingest_uf
    ano_ini = config.ingest_ano_inicio
    ano_fim = config.ingest_ano_fim

    # CBO column in SIM is 'ocupacao', not 'cbo_2002'
    # Use id_municipio_residencia for residence-based risk attribution
    return _query(f"""
        SELECT
            ano,
            sigla_uf,
            id_municipio_residencia  AS id_municipio,
            ocupacao                 AS cbo_2002,
            causa_basica             AS cid_10_causa,
            acidente_trabalho,
            COUNT(*)                 AS total_obitos
        FROM `basedosdados.br_ms_sim.microdados`
        WHERE sigla_uf = '{uf}'
          AND ano BETWEEN {ano_ini} AND {ano_fim}
          AND ocupacao IS NOT NULL
        GROUP BY 1, 2, 3, 4, 5, 6
    """)


def fetch_rais_vinculos() -> pd.DataFrame:
    uf = config.ingest_uf
    ano_ini = config.ingest_ano_inicio
    ano_fim = config.ingest_ano_fim

    return _query(f"""
        SELECT
            ano,
            sigla_uf,
            id_municipio,
            cnae_2_subclasse,
            cbo_2002,
            COUNT(*) AS quantidade_vinculos
        FROM `basedosdados.br_me_rais.microdados_vinculos`
        WHERE sigla_uf = '{uf}'
          AND ano BETWEEN {ano_ini} AND {ano_fim}
          AND vinculo_ativo_3112 = '1'
          AND cnae_2_subclasse IS NOT NULL
          AND cbo_2002 IS NOT NULL
        GROUP BY 1, 2, 3, 4, 5
    """)
