-- stg_dim_cnae has one row per subclass (7-digit); use DISTINCT at class level (5-digit)
-- CAT carries a 4-digit code (no check digit) -> join via left(cnae_classe, 4)
with cnae_classe as (
    select distinct
        cnae_classe,
        left(cnae_classe, 4)    as cnae_cod_4,
        cnae_descricao_classe,
        cnae_descricao_secao
    from {{ ref('stg_dim_cnae') }}
),

acidentes_por_cnae as (
    select
        cnae_cod_4,
        count(*)                                            as total_acidentes,
        sum(case when obito then 1 else 0 end)              as total_obitos
    from {{ ref('stg_fact_cat') }}
    group by 1
),

vinculos_por_cnae as (
    select
        cnae_classe,
        sum(quantidade_vinculos)                            as total_vinculos
    from {{ ref('stg_fact_rais') }}
    group by 1
)

select
    gr.cnae_classe,
    cnae.cnae_descricao_classe                              as cnae_descricao,
    cnae.cnae_descricao_secao                               as cnae_secao,
    gr.grau_risco,
    gr.grau_risco_descricao,
    coalesce(v.total_vinculos, 0)                           as total_vinculos,
    coalesce(a.total_acidentes, 0)                          as total_acidentes,
    coalesce(a.total_obitos, 0)                             as total_obitos,
    -- Accidents per 1,000 bonds (only meaningful where we have RAIS data)
    case
        when coalesce(v.total_vinculos, 0) > 0
        then round(a.total_acidentes::numeric / v.total_vinculos * 1000, 2)
    end                                                     as taxa_acidentes_por_mil,
    rank() over (order by coalesce(a.total_acidentes, 0) desc)
                                                            as rank_por_acidentes

from {{ ref('stg_dim_grau_risco') }} as gr
left join cnae_classe as cnae
    on left(gr.cnae_classe, 4) = cnae.cnae_cod_4
left join acidentes_por_cnae as a
    on left(gr.cnae_classe, 4) = a.cnae_cod_4
left join vinculos_por_cnae as v
    on gr.cnae_classe = v.cnae_classe

order by gr.grau_risco desc, total_acidentes desc nulls last
