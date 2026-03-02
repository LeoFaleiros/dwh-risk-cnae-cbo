-- Causas externas de óbito (morte acidental) por CNAE.
--
-- Responde: em cada setor econômico, quais mecanismos de morte acidental/violenta
-- são mais frequentes? Qual proporção desses óbitos é classificada como acidente
-- de trabalho?
--
-- Fonte: campo causa_basica da Declaração de Óbito — causa raiz do óbito.
-- Escopo: capítulos CID-10 V, W, X, Y, S, T (causas externas / traumatismos).
--   V — Acidentes de transporte
--   W — Quedas, afogamentos, exposição a forças inanimadas
--   X — Causas externas intencionais ou indeterminadas (incl. X59 lesão NE)
--   Y — Intenção não determinada / complicação de intervenção médica
--   S — Traumatismos (fraturas, ferimentos — geralmente como sequela)
--   T — Envenenamentos, queimaduras, complicações cirúrgicas
--
-- Diferença do mart_comorbidades_por_cnae: aqui a causa externa É a causa básica
-- do óbito (não uma comorbidade registrada na Parte II da DO).
--
-- Uso atuarial: precificação de coberturas de morte acidental, diária por
-- internação hospitalar, invalidez por acidente. Alta sobreposição com
-- acidente_trabalho='Sim', mas também captura acidentes fora do trabalho
-- em populações de trabalhadores de setores específicos.
--
-- Método de atribuição CNAE: CBO → CNAE dominante (mais vínculos RAIS).

{{ config(materialized='table') }}

with cnae_info as (
    select distinct
        cnae_classe,
        cnae_descricao_classe,
        cnae_secao,
        cnae_descricao_secao
    from {{ ref('stg_dim_cnae') }}
),

-- CNAE dominante por CBO (mais vínculos RAIS)
cbo_cnae_dom as (
    select distinct on (cbo_codigo)
        cbo_codigo,
        cnae_classe
    from {{ ref('stg_fact_rais') }}
    where cbo_codigo  is not null
      and cnae_classe is not null
    group by cbo_codigo, cnae_classe, quantidade_vinculos
    order by cbo_codigo, quantidade_vinculos desc
),

-- óbitos por causa externa, mapeados ao CNAE dominante do CBO
sim_causa_externa as (
    select
        c.cnae_classe,
        s.cid_10_causa,
        s.acidente_trabalho,
        s.total_obitos
    from {{ ref('stg_fact_sim') }} as s
    inner join cbo_cnae_dom as c
        on s.cbo_codigo = c.cbo_codigo
    where s.cid_10_causa is not null
      and left(s.cid_10_causa, 1) in ('S','T','V','W','X','Y')
),

-- agrega por CNAE × CID subcategoria
agg as (
    select
        cnae_classe,
        cid_10_causa                                as cid_causa,
        left(cid_10_causa, 1)                       as capitulo,
        sum(total_obitos)                           as total_obitos,
        sum(case when acidente_trabalho = 'Sim'
                 then total_obitos else 0 end)      as total_obitos_trabalho
    from sim_causa_externa
    group by 1, 2, 3
),

-- total de óbitos por causa externa por CNAE (denominador do pct)
total_por_cnae as (
    select
        cnae_classe,
        sum(total_obitos) as total_obitos_causa_ext_cnae
    from agg
    group by cnae_classe
)

select
    gr.grau_risco,
    gr.grau_risco_descricao,
    a.cnae_classe,
    ci.cnae_descricao_classe,
    ci.cnae_secao,
    ci.cnae_descricao_secao,
    row_number() over (
        partition by a.cnae_classe
        order by a.total_obitos desc
    )                                               as rank_causa,
    a.capitulo,
    case a.capitulo
        when 'V' then 'Acidente de transporte'
        when 'W' then 'Queda / Afogamento / Exposicao'
        when 'X' then 'Causa externa intencional ou indeterminada'
        when 'Y' then 'Intencao nao determinada / Complicacao medica'
        when 'S' then 'Traumatismo'
        when 'T' then 'Envenenamento / Queimadura / Complicacao cirurgica'
    end                                             as tipo_causa_externa,
    a.cid_causa,
    d_sub.cid_descricao                             as causa_descricao,
    left(a.cid_causa, 3)                            as cid_categoria,
    d_cat.cid_descricao                             as categoria_descricao,
    a.total_obitos,
    a.total_obitos_trabalho,
    t.total_obitos_causa_ext_cnae,
    round(a.total_obitos * 100.0 /
          nullif(t.total_obitos_causa_ext_cnae, 0), 2) as pct_causa_no_cnae,
    round(a.total_obitos_trabalho * 100.0 /
          nullif(a.total_obitos, 0), 2)               as pct_trabalho_na_causa

from agg as a
inner join total_por_cnae as t
    using (cnae_classe)
left join cnae_info as ci
    using (cnae_classe)
inner join {{ ref('stg_dim_grau_risco') }} as gr
    on a.cnae_classe = gr.cnae_classe
left join {{ ref('stg_dim_cid') }} as d_sub
    on a.cid_causa = d_sub.cid_codigo
left join {{ ref('stg_dim_cid') }} as d_cat
    on left(a.cid_causa, 3) = d_cat.cid_codigo

order by gr.grau_risco desc, a.cnae_classe, a.total_obitos desc
