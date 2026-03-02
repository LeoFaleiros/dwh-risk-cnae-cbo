-- Severidade de acidentes de trabalho por CNAE.
--
-- Responde: além de quantos acidentes ocorrem, quão graves são?
-- Quais lesões, partes do corpo e agentes causadores dominam cada setor?
-- Qual é o perfil demográfico (sexo, faixa etária) dos acidentados?
--
-- Granularidade: 1 linha por CNAE (classe 5 dígitos).
-- Joins via cnae_cod_4 (4 dígitos) entre CAT e dim_cnae.

{{ config(materialized='table') }}

with cnae_info as (
    select distinct
        cnae_classe,
        left(cnae_classe, 4)    as cnae_cod_4,
        cnae_descricao_classe,
        cnae_secao,
        cnae_descricao_secao
    from {{ ref('stg_dim_cnae') }}
),

-- base: CAT aggregated by CNAE × natureza × parte × agente (keeping sexo for pivot)
cat_base as (
    select
        cnae_cod_4,
        natureza_lesao,
        parte_corpo,
        agente_causador,
        sexo,
        faixa_etaria,
        count(*)                                        as acidentes,
        sum(case when obito then 1 else 0 end)          as obitos
    from {{ ref('stg_fact_cat') }}
    where cnae_cod_4 is not null
    group by 1, 2, 3, 4, 5, 6
),

-- CNAE-level totals
cnae_totais as (
    select
        cnae_cod_4,
        sum(acidentes)                                          as total_acidentes,
        sum(obitos)                                             as total_obitos,
        sum(acidentes) filter (where sexo = 'M')                as acidentes_m,
        sum(acidentes) filter (where sexo = 'F')                as acidentes_f
    from cat_base
    group by 1
),

-- top natureza_lesao per CNAE (by accident count)
top_natureza as (
    select distinct on (cnae_cod_4)
        cnae_cod_4,
        natureza_lesao                                          as top_natureza_lesao,
        sum(acidentes)                                          as acidentes_top_natureza
    from cat_base
    where natureza_lesao is not null
    group by cnae_cod_4, natureza_lesao
    order by cnae_cod_4, sum(acidentes) desc
),

-- top parte_corpo per CNAE
top_parte as (
    select distinct on (cnae_cod_4)
        cnae_cod_4,
        parte_corpo                                             as top_parte_corpo,
        sum(acidentes)                                          as acidentes_top_parte
    from cat_base
    where parte_corpo is not null
    group by cnae_cod_4, parte_corpo
    order by cnae_cod_4, sum(acidentes) desc
),

-- top agente_causador per CNAE
top_agente as (
    select distinct on (cnae_cod_4)
        cnae_cod_4,
        agente_causador                                         as top_agente_causador,
        sum(acidentes)                                          as acidentes_top_agente
    from cat_base
    where agente_causador is not null
    group by cnae_cod_4, agente_causador
    order by cnae_cod_4, sum(acidentes) desc
),

-- faixa etária dominante per CNAE
top_faixa as (
    select distinct on (cnae_cod_4)
        cnae_cod_4,
        faixa_etaria                                            as faixa_etaria_dominante,
        sum(acidentes)                                          as acidentes_top_faixa
    from cat_base
    where faixa_etaria is not null
    group by cnae_cod_4, faixa_etaria
    order by cnae_cod_4, sum(acidentes) desc
)

select
    -- risk context
    gr.grau_risco,
    gr.grau_risco_descricao,

    -- CNAE identification
    cn.cnae_classe,
    cn.cnae_descricao_classe                                    as cnae_descricao,
    cn.cnae_secao,
    cn.cnae_descricao_secao,

    -- accident volume
    ct.total_acidentes,
    ct.total_obitos,
    round(ct.total_obitos::numeric / nullif(ct.total_acidentes, 0) * 100, 2)
                                                                as taxa_obito_pct,

    -- gender breakdown
    ct.acidentes_m,
    ct.acidentes_f,
    round(ct.acidentes_m::numeric / nullif(ct.total_acidentes, 0) * 100, 1)
                                                                as pct_masculino,

    -- dominant injury profile
    tn.top_natureza_lesao,
    round(tn.acidentes_top_natureza::numeric / nullif(ct.total_acidentes, 0) * 100, 1)
                                                                as pct_top_natureza,

    tp.top_parte_corpo,
    round(tp.acidentes_top_parte::numeric / nullif(ct.total_acidentes, 0) * 100, 1)
                                                                as pct_top_parte,

    ta.top_agente_causador,
    round(ta.acidentes_top_agente::numeric / nullif(ct.total_acidentes, 0) * 100, 1)
                                                                as pct_top_agente,

    -- dominant age band
    tf.faixa_etaria_dominante,
    round(tf.acidentes_top_faixa::numeric / nullif(ct.total_acidentes, 0) * 100, 1)
                                                                as pct_faixa_dominante

from cnae_totais as ct
inner join cnae_info as cn
    on ct.cnae_cod_4 = cn.cnae_cod_4
inner join {{ ref('stg_dim_grau_risco') }} as gr
    on cn.cnae_classe = gr.cnae_classe
left join top_natureza as tn on ct.cnae_cod_4 = tn.cnae_cod_4
left join top_parte    as tp on ct.cnae_cod_4 = tp.cnae_cod_4
left join top_agente   as ta on ct.cnae_cod_4 = ta.cnae_cod_4
left join top_faixa    as tf on ct.cnae_cod_4 = tf.cnae_cod_4

order by gr.grau_risco desc, cn.cnae_classe
