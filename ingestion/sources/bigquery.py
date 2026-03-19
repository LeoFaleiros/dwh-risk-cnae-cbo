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
    ano_ini = config.sim_ano_inicio
    ano_fim = config.sim_ano_fim

    # CBO column in SIM is 'ocupacao', not 'cbo_2002'
    # Use id_municipio_residencia for residence-based risk attribution
    # sexo: '1'=Masculino, '2'=Feminino, '9'=Ignorado (DATASUS encoding)
    # idade: already in years as float64 in the BigQuery dataset
    #
    # Parte I — cadeia causal (linhas a → d):
    #   linha_a: causa imediata da morte (o que matou no momento)
    #   linha_b: causa que originou a linha_a
    #   linha_c: causa que originou a linha_b
    #   linha_d: causa mais remota (raramente preenchida)
    #   Formato: '*CID' — um único CID por linha (ex: '*I219', '*J690')
    #   causa_basica é o resultado do algoritmo DATASUS sobre essa cadeia.
    #
    # Parte II — outras condições significativas (linha_ii):
    #   Comorbidades que contribuíram para a morte mas não fazem parte da cadeia causal.
    #   Formato: '*I10X*E149' — múltiplos CIDs concatenados com '*'.
    #   O 'X' é filler DATASUS para códigos de 3 caracteres sem subdivisão.
    #
    # Todas as linhas são armazenadas raw; normalização e unnesting ocorrem no staging/mart.
    return _query(f"""
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
          AND ano BETWEEN {ano_ini} AND {ano_fim}
          AND ocupacao IS NOT NULL
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
    """)


def fetch_rais_vinculos() -> pd.DataFrame:
    ano_ini = config.rais_ano_inicio
    ano_fim = config.rais_ano_fim

    # Agrupado por UF (sem município) para reduzir volume de ~41M para ~500K linhas.
    # Município foi removido pois o denominador de risco é calculado por CNAE×CBO nacional.
    # sigla_uf mantido para validação de cobertura e eventual análise regional futura.
    return _query(f"""
        SELECT
            ano,
            sigla_uf,
            cnae_2_subclasse,
            cbo_2002,
            COUNT(*) AS quantidade_vinculos
        FROM `basedosdados.br_me_rais.microdados_vinculos`
        WHERE ano BETWEEN {ano_ini} AND {ano_fim}
          AND vinculo_ativo_3112 = '1'
          AND cnae_2_subclasse IS NOT NULL
          AND cbo_2002 IS NOT NULL
        GROUP BY 1, 2, 3, 4
    """)
