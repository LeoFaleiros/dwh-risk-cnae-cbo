-- Mortalidade SIM atribuída por CNAE, cruzada com exposição RAIS.
--
-- Responde: qual é a taxa de mortalidade por mil vínculos em cada CNAE?
-- Qual proporção dos óbitos é por acidente de trabalho?
-- Como se distribui por sexo e faixa etária?
--
-- Metodologia de atribuição: óbito SIM é atribuído ao CNAE dominante do CBO
-- (aquele com mais vínculos RAIS para aquela ocupação). Simplificação intencional —
-- CBOs que atuam em múltiplos setores podem ter distorção marginal.
--
-- Granularidade: CNAE × sexo × faixa_etaria.
-- Pre-requisito: raw.fact_sim re-ingerido com colunas sexo e idade.

{{ config(materialized='table') }}

with cnae_info as (
    select distinct
        cnae_classe,
        cnae_descricao_classe,
        cnae_secao,
        cnae_descricao_secao
    from {{ ref('stg_dim_cnae') }}
),

-- vinculos RAIS por CBO × CNAE (para mapear CBO → CNAE dominante)
rais_cbo_cnae as (
    select
        cbo_codigo,
        cnae_classe,
        sum(quantidade_vinculos)    as total_vinculos
    from {{ ref('stg_fact_rais') }}
    where cbo_codigo  is not null
      and cnae_classe is not null
    group by 1, 2
),

-- CNAE dominante por CBO (mais vínculos)
cbo_cnae_dom as (
    select distinct on (cbo_codigo)
        cbo_codigo,
        cnae_classe,
        total_vinculos              as vinculos_dom
    from rais_cbo_cnae
    order by cbo_codigo, total_vinculos desc
),

-- vinculos RAIS por CNAE (denominador total)
vinculos_cnae as (
    select
        cnae_classe,
        sum(quantidade_vinculos)    as total_vinculos
    from {{ ref('stg_fact_rais') }}
    where cnae_classe is not null
    group by 1
),

-- óbitos SIM aggregados por CBO × sexo × faixa_etaria
obitos_cbo as (
    select
        cbo_codigo,
        coalesce(sexo, 'Não informado')         as sexo,
        coalesce(faixa_etaria, 'Não informada') as faixa_etaria,
        sum(total_obitos)                       as total_obitos,
        sum(
            case when acidente_trabalho = '1'
            then total_obitos else 0 end
        )                                       as obitos_acidente_trabalho
    from {{ ref('stg_fact_sim') }}
    where cbo_codigo is not null
    group by 1, 2, 3
)

select
    -- risk context
    gr.grau_risco,
    gr.grau_risco_descricao,

    -- CNAE identification
    ci.cnae_classe,
    ci.cnae_descricao_classe                                    as cnae_descricao,
    ci.cnae_secao,
    ci.cnae_descricao_secao,

    -- demographics
    ob.sexo,
    ob.faixa_etaria,

    -- mortality (SIM)
    ob.total_obitos,
    ob.obitos_acidente_trabalho,
    round(
        ob.obitos_acidente_trabalho::numeric / nullif(ob.total_obitos, 0) * 100, 1
    )                                                           as pct_acidente_trabalho,

    -- exposure (RAIS denominator)
    coalesce(vc.total_vinculos, 0)                              as vinculos_cnae,
    case
        when coalesce(vc.total_vinculos, 0) > 0
        then round(ob.total_obitos::numeric / vc.total_vinculos * 1000, 3)
    end                                                         as taxa_obitos_por_mil_vinculos

from obitos_cbo as ob
inner join cbo_cnae_dom as cd
    on ob.cbo_codigo = cd.cbo_codigo
inner join cnae_info as ci
    on cd.cnae_classe = ci.cnae_classe
inner join {{ ref('stg_dim_grau_risco') }} as gr
    on cd.cnae_classe = gr.cnae_classe
left join vinculos_cnae as vc
    on cd.cnae_classe = vc.cnae_classe

order by gr.grau_risco desc, ci.cnae_classe, ob.sexo, ob.faixa_etaria
