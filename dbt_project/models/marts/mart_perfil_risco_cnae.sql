-- Perfil de risco por CNAE: top CBOs por acidente x top CIDs por acidente dentro do CBO.
--
-- Um WHERE em cnae_classe retorna o perfil completo de risco de um setor:
-- quais ocupações mais se acidentam, com quais diagnósticos, qual a exposição.
--
-- Granularidade: (cnae_classe, rank_cbo, rank_cid)
--   Top 10 CBOs por CNAE (por volume de acidentes no CAT)
--   Top 5 CIDs por CNAE×CBO (por volume de acidentes no CAT)
--   Máximo 50 linhas por CNAE.
--
-- Join keys:
--   CAT:  cnae_cod_4 (4-digit) -> cnae_classe via stg_dim_cnae
--   RAIS: cnae_classe (5-digit) -> vinculos por ocupação (denominador da taxa)
--
-- Registros sem CBO ou CID são excluídos (não contribuem para o ranking de risco).

{{ config(materialized='table') }}

with cnae_info as (
    select distinct
        cnae_classe,
        left(cnae_classe, 4)   as cnae_cod_4,
        cnae_descricao_classe,
        cnae_secao,
        cnae_descricao_secao
    from {{ ref('stg_dim_cnae') }}
),

-- acidentes agregados no nível CNAE × CBO × CID
acidentes_brutos as (
    select
        cat.cnae_cod_4,
        cat.cbo_codigo,
        cat.cid_10,
        count(*)                                        as acidentes_cid,
        sum(case when cat.obito then 1 else 0 end)      as obitos_cid
    from {{ ref('stg_fact_cat') }} as cat
    where cat.cbo_codigo is not null
      and cat.cid_10     is not null
    group by 1, 2, 3
),

-- total por CBO dentro do CNAE + ranking por acidente
acidentes_por_cbo as (
    select
        cnae_cod_4,
        cbo_codigo,
        sum(acidentes_cid)                              as acidentes_cbo,
        sum(obitos_cid)                                 as obitos_cbo,
        row_number() over (
            partition by cnae_cod_4
            order by sum(acidentes_cid) desc
        )                                               as rank_cbo
    from acidentes_brutos
    group by 1, 2
),

-- mantém apenas os top 10 CBOs e adiciona ranking de CID dentro de cada CBO
acidentes_rankeados as (
    select
        ab.cnae_cod_4,
        ab.cbo_codigo,
        ab.cid_10,
        ab.acidentes_cid,
        ab.obitos_cid,
        ap.acidentes_cbo,
        ap.obitos_cbo,
        ap.rank_cbo,
        row_number() over (
            partition by ab.cnae_cod_4, ab.cbo_codigo
            order by ab.acidentes_cid desc
        )                                               as rank_cid
    from acidentes_brutos as ab
    inner join acidentes_por_cbo as ap
        on  ab.cnae_cod_4  = ap.cnae_cod_4
        and ab.cbo_codigo  = ap.cbo_codigo
    where ap.rank_cbo <= 10
),

-- vínculos RAIS por CNAE × CBO (denominador para taxa de acidente)
vinculos_cbo as (
    select
        cnae_classe,
        cbo_codigo,
        sum(quantidade_vinculos)    as total_vinculos
    from {{ ref('stg_fact_rais') }}
    where cbo_codigo is not null
    group by 1, 2
)

select
    -- risco regulatório
    gr.grau_risco,
    gr.grau_risco_descricao,

    -- CNAE
    cnae.cnae_classe,
    cnae.cnae_descricao_classe                          as cnae_descricao,
    cnae.cnae_secao,
    cnae.cnae_descricao_secao,

    -- ocupação (CBO) e sua posição no ranking de acidentes do CNAE
    ar.rank_cbo,
    ar.cbo_codigo,
    cbo.cbo_descricao,
    cbo.cbo_grande_grupo_codigo,
    cbo.cbo_grande_grupo_descricao,

    -- exposição e sinistralidade no nível CBO
    coalesce(v.total_vinculos, 0)                       as vinculos_cbo,
    ar.acidentes_cbo,
    ar.obitos_cbo,
    case
        when coalesce(v.total_vinculos, 0) > 0
        then round(ar.acidentes_cbo::numeric / v.total_vinculos * 1000, 2)
    end                                                 as taxa_acidentes_por_mil,

    -- diagnóstico (CID) e sua posição dentro do CBO
    ar.rank_cid,
    ar.cid_10,
    cid.cid_descricao,
    ar.acidentes_cid,
    ar.obitos_cid,
    round(
        ar.acidentes_cid::numeric / nullif(ar.acidentes_cbo, 0) * 100, 1
    )                                                   as pct_acidentes_no_cbo

from acidentes_rankeados as ar
inner join cnae_info as cnae
    on ar.cnae_cod_4 = cnae.cnae_cod_4
inner join {{ ref('stg_dim_grau_risco') }} as gr
    on cnae.cnae_classe = gr.cnae_classe
left join {{ ref('stg_dim_cbo') }} as cbo
    on ar.cbo_codigo = cbo.cbo_codigo
left join {{ ref('stg_dim_cid') }} as cid
    on ar.cid_10 = cid.cid_codigo
left join vinculos_cbo as v
    on  cnae.cnae_classe = v.cnae_classe
    and ar.cbo_codigo    = v.cbo_codigo
where ar.rank_cid <= 5

order by gr.grau_risco desc, cnae.cnae_classe, ar.rank_cbo, ar.rank_cid
