-- Top 10 CBOs por classe CNAE, ordenados por número de vínculos ativos (RAIS).
-- Responde: "quais ocupações dominam cada setor, e isso bate com o risco regulatório?"
--
-- Fonte: stg_fact_rais (vínculos ativos em 31/12, RAIS)
-- Join keys: cnae_classe (5-digit) -> stg_dim_grau_risco, stg_dim_cnae
--            cbo_codigo (6-digit)  -> stg_dim_cbo
--
-- Granularidade: 1 linha por (cnae_classe, rank_cbo). Máximo 10 linhas por classe.
-- Se uma classe tem menos de 10 CBOs presentes no RAIS, mostra apenas os disponíveis.

with cnae_info as (
    select distinct
        cnae_classe,
        cnae_descricao_classe,
        cnae_descricao_secao,
        cnae_secao
    from {{ ref('stg_dim_cnae') }}
),

vinculos_por_classe_cbo as (
    select
        cnae_classe,
        cbo_codigo,
        sum(quantidade_vinculos)    as total_vinculos,
        min(ano)                    as ano_inicio,
        max(ano)                    as ano_fim,
        count(distinct ano)         as anos_cobertos
    from {{ ref('stg_fact_rais') }}
    where cbo_codigo is not null
    group by 1, 2
),

ranked as (
    select
        *,
        sum(total_vinculos) over (partition by cnae_classe)     as total_vinculos_cnae,
        row_number() over (
            partition by cnae_classe
            order by total_vinculos desc
        )                                                        as rank_cbo
    from vinculos_por_classe_cbo
)

select
    gr.grau_risco,
    gr.grau_risco_descricao,
    r.cnae_classe,
    cnae.cnae_descricao_classe                                  as cnae_descricao,
    cnae.cnae_secao,
    cnae.cnae_descricao_secao,
    r.rank_cbo,
    r.cbo_codigo,
    cbo.cbo_descricao,
    cbo.cbo_grande_grupo_codigo,
    cbo.cbo_grande_grupo_descricao,
    r.total_vinculos,
    r.total_vinculos_cnae,
    round(
        r.total_vinculos::numeric / nullif(r.total_vinculos_cnae, 0) * 100, 2
    )                                                           as pct_vinculos_na_classe,
    r.ano_inicio,
    r.ano_fim

from ranked as r
inner join {{ ref('stg_dim_grau_risco') }} as gr
    on r.cnae_classe = gr.cnae_classe
left join cnae_info as cnae
    on r.cnae_classe = cnae.cnae_classe
left join {{ ref('stg_dim_cbo') }} as cbo
    on r.cbo_codigo = cbo.cbo_codigo
where r.rank_cbo <= 10

order by gr.grau_risco desc, r.cnae_classe, r.rank_cbo
