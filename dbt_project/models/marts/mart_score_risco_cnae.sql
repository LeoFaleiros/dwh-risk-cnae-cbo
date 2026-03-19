-- =============================================================================
-- mart_score_risco_cnae — Score de risco ocupacional por CNAE (1-5)
-- =============================================================================
-- Score empírico para triagem de subscrição. Complementar ao Grau de Risco NR-04.
--
-- METODOLOGIA:
--   1. taxa_acidente = acidentes (típico + doença) / vínculos RAIS
--      Trajeto excluído — risco de trajeto não caracteriza o CNAE.
--   2. score_base (1-5) = faixas fixas determinísticas sobre taxa_acidente:
--        1: < 0.003  |  2: 0.003–0.006  |  3: 0.006–0.010
--        4: 0.010–0.015  |  5: ≥ 0.015
--      Limiares calibrados via medianas do Grau de Risco NR-04 como âncora.
--   3. agravante_obito = +1 se taxa_obito > 0.010 (P90 da distribuição)
--      taxa_obito = óbitos em acidentes de trabalho / total acidentes trabalho
--   4. score_risco = min(score_base + agravante_obito, 5)
--
-- JANELA TEMPORAL:
--   Interseção CAT ∩ RAIS = anos com dados em ambas as fontes.
--   Garante numerador e denominador no mesmo período.
--
-- CALIBRAÇÃO DOS LIMIARES (referência):
--   Score | Limiar         | Mediana GR NR-04 equivalente
--   1     | < 0.003        | GR 1 (mediana 0.0018)
--   2     | 0.003 – 0.006  | GR 2 (mediana 0.0034)
--   3     | 0.006 – 0.010  | entre GR 2 e GR 3
--   4     | 0.010 – 0.015  | GR 3-4 (medianas 0.0088–0.0108)
--   5     | ≥ 0.015        | acima do P75 do GR 4
--
-- NOTA: cnae_cod_4 (CAT) faz join com left(cnae_classe,4) (RAIS/dim_cnae)
--   pois a CAT registra CNAE no nível classe sem o dígito verificador.
--
-- EXCLUSÃO — CNAE 1.0:
--   ~412 códigos da CAT (0,52% dos acidentes) são CNAE 1.0 (pré-2007) sem
--   correspondência na tabela CNAE 2.0. Removidos via inner join em cnae_info.

{{ config(materialized='table') }}

-- Limiares determinísticos (calibrados em 2025-03 com dados 2018-2024)
{% set limiar_score_2 = 0.003 %}
{% set limiar_score_3 = 0.006 %}
{% set limiar_score_4 = 0.010 %}
{% set limiar_score_5 = 0.015 %}
{% set limiar_agravante_obito = 0.010 %}

-- Interseção temporal: anos com dados em ambas as fontes
with janela as (
    select
        greatest(cat_min, rais_min) as ano_inicio,
        least(cat_max, rais_max)    as ano_fim
    from (
        select min(ano) as cat_min, max(ano) as cat_max
        from {{ ref('stg_fact_cat') }}
        where ano is not null
    ) c
    cross join (
        select min(ano) as rais_min, max(ano) as rais_max
        from {{ ref('stg_fact_rais') }}
        where ano is not null
    ) r
),

cat_por_cnae as (
    select
        c.cnae_cod_4,
        count(*)                                                        as total_registros,
        count(*) filter (where c.tipo_acidente in ('típico', 'doença')) as acidentes_trabalho,
        count(*) filter (where c.tipo_acidente = 'trajeto')             as acidentes_trajeto,
        count(*) filter (where c.obito = true)                          as obitos,
        count(*) filter (
            where c.obito = true
              and c.tipo_acidente in ('típico', 'doença')
        )                                                               as obitos_trabalho
    from {{ ref('stg_fact_cat') }} c
    cross join janela j
    where c.cnae_cod_4 is not null
      and c.ano between j.ano_inicio and j.ano_fim
    group by 1
),

rais_por_cnae as (
    select
        left(cnae_classe, 4)        as cnae_cod_4,
        sum(quantidade_vinculos)    as total_vinculos
    from {{ ref('stg_fact_rais') }} r
    cross join janela j
    where r.ano between j.ano_inicio and j.ano_fim
    group by 1
),

cnae_info as (
    select distinct on (left(cnae_classe, 4))
        left(cnae_classe, 4)    as cnae_cod_4,
        cnae_classe,
        cnae_descricao_classe
    from {{ ref('stg_dim_cnae') }}
    order by left(cnae_classe, 4), cnae_classe
),

base as (
    select
        i.cnae_classe,
        i.cnae_descricao_classe,
        j.ano_inicio,
        j.ano_fim,
        c.acidentes_trabalho,
        c.acidentes_trajeto,
        c.obitos,
        c.obitos_trabalho,
        r.total_vinculos,
        case
            when r.total_vinculos > 0
            then round(c.acidentes_trabalho::numeric / r.total_vinculos, 6)
        end                                      as taxa_acidente,
        case
            when c.acidentes_trabalho > 0
            then round(c.obitos_trabalho::numeric / c.acidentes_trabalho, 6)
        end                                      as taxa_obito
    from cat_por_cnae c
    cross join janela j
    inner join cnae_info i using (cnae_cod_4)
    left join rais_por_cnae r using (cnae_cod_4)
    where c.acidentes_trabalho > 0
)

select
    cnae_classe,
    cnae_descricao_classe,
    ano_inicio,
    ano_fim,
    acidentes_trabalho,
    acidentes_trajeto,
    obitos,
    obitos_trabalho,
    total_vinculos,
    taxa_acidente,
    taxa_obito,
    -- Score base: faixas fixas determinísticas
    case
        when taxa_acidente is null then null
        when taxa_acidente < {{ limiar_score_2 }} then 1
        when taxa_acidente < {{ limiar_score_3 }} then 2
        when taxa_acidente < {{ limiar_score_4 }} then 3
        when taxa_acidente < {{ limiar_score_5 }} then 4
        else 5
    end                                          as score_base,
    -- Agravante: alta letalidade nos acidentes
    case
        when taxa_obito > {{ limiar_agravante_obito }} then 1
        else 0
    end                                          as agravante_obito,
    -- Score final: base + agravante, capped em 5
    case
        when taxa_acidente is null then null
        else least(
            (case
                when taxa_acidente < {{ limiar_score_2 }} then 1
                when taxa_acidente < {{ limiar_score_3 }} then 2
                when taxa_acidente < {{ limiar_score_4 }} then 3
                when taxa_acidente < {{ limiar_score_5 }} then 4
                else 5
            end)
            + (case when taxa_obito > {{ limiar_agravante_obito }} then 1 else 0 end),
            5
        )
    end                                          as score_risco

from base
order by score_risco desc nulls last, acidentes_trabalho desc
