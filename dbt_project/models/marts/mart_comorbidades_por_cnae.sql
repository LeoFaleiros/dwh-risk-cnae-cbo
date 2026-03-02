-- Comorbidades associadas a óbitos por CNAE.
--
-- Responde: em cada setor econômico, quais doenças preexistentes (comorbidades)
-- estavam presentes nos trabalhadores que morreram?
--
-- Fonte: campo linha_ii da Declaração de Óbito (Parte II da DO) — "outras condições
-- significativas que contribuíram para a morte mas não foram a causa básica".
-- Exatamente o campo de comorbidades: diabetes, hipertensão, IRC, DPOC, câncer, etc.
--
-- Formato bruto linha_ii: '*I10X*E149' — múltiplos CIDs concatenados com '*'.
-- Filler 'X' ao final é padrão DATASUS para subcategorias sem dígito terminal
-- (ex: 'I10X' = código I10, hipertensão essencial — sem subdivisão).
--
-- Exclusões intencionais:
--   Capítulos S e T (traumatismos): não são comorbidades, são causa direta da lesão.
--   Capítulos V/W/X/Y (causas externas): idem — mecanismo de morte, não condição prévia.
--   Capítulo Z (contato com serviços de saúde): contexto clínico, não doença.
--
-- Método de atribuição CNAE: CBO → CNAE dominante (mais vínculos RAIS). Mesma
-- simplificação usada em mart_mortalidade_por_cnae.
--
-- Granularidade: CNAE × CID comorbidade.

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

-- registros SIM com linha_ii presente, mapeados ao CNAE dominante do CBO
sim_com_comorbidade as (
    select
        s.cbo_codigo,
        s.total_obitos,
        s.linha_ii_raw
    from {{ ref('stg_fact_sim') }} as s
    where s.linha_ii_raw is not null
      and s.cbo_codigo   is not null
),

-- unnest: cada '* código' vira uma linha separada
-- regexp_split_to_table quebra no '*', gerando tokens; trim e upper limpam.
comorbidade_unnested as (
    select
        sc.cbo_codigo,
        sc.total_obitos,
        upper(trim(token))  as token_raw
    from sim_com_comorbidade as sc
    cross join lateral regexp_split_to_table(sc.linha_ii_raw, '\*') as token
    where trim(token) <> ''         -- descarta fragmentos vazios antes do 1º '*'
),

-- normalizar cada código individual:
--   'I10X' → 'I10'  (X = filler DATASUS para código sem subcategoria)
--   'E149' → 'E14.9' (dot normalization)
--   'R54'  → 'R54'  (3 chars: já ok)
comorbidade_normalizada as (
    select
        cbo_codigo,
        total_obitos,
        case
            -- 4 chars ending in X → strip X: 'I10X' → 'I10'
            when length(token_raw) = 4 and right(token_raw, 1) = 'X'
            then left(token_raw, 3)
            -- 4 chars not ending in X → add dot: 'E149' → 'E14.9'
            when length(token_raw) = 4
            then left(token_raw, 3) || '.' || right(token_raw, 1)
            -- 3 chars → as-is
            else token_raw
        end                         as cid_comorbidade
    from comorbidade_unnested
    -- filtrar capítulos irrelevantes para análise de comorbidades:
    --   S, T = traumatismos e envenenamentos (causa média, não prévia)
    --   V, W, X, Y = causas externas de mortalidade
    --   Z = contato com serviços de saúde
    where left(token_raw, 1) not in ('S', 'T', 'V', 'W', 'X', 'Y', 'Z')
      and length(token_raw) >= 3
),

-- agregar por CNAE × comorbidade (via CNAE dominante do CBO)
comorbidade_por_cnae as (
    select
        cd.cnae_classe,
        cn.cid_comorbidade,
        sum(cn.total_obitos)    as total_obitos_com_comorbidade
    from comorbidade_normalizada as cn
    inner join cbo_cnae_dom as cd
        on cn.cbo_codigo = cd.cbo_codigo
    group by 1, 2
),

-- total de óbitos por CNAE (com linha_ii preenchida — denominador correto para pct)
obitos_com_linhaii as (
    select
        cd.cnae_classe,
        sum(sc.total_obitos)    as total_obitos_com_linhaii
    from sim_com_comorbidade as sc
    inner join cbo_cnae_dom as cd
        on sc.cbo_codigo = cd.cbo_codigo
    group by 1
)

select
    -- risco regulatório
    gr.grau_risco,
    gr.grau_risco_descricao,

    -- CNAE
    ci.cnae_classe,
    ci.cnae_descricao_classe                                    as cnae_descricao,
    ci.cnae_secao,
    ci.cnae_descricao_secao,

    -- ranking da comorbidade dentro do CNAE
    row_number() over (
        partition by cc.cnae_classe
        order by cc.total_obitos_com_comorbidade desc
    )                                                           as rank_comorbidade,

    -- comorbidade
    cc.cid_comorbidade,
    cid.cid_descricao                                           as comorbidade_descricao,
    left(cc.cid_comorbidade, 3)                                 as cid_categoria,
    cat_cid.cid_descricao                                       as categoria_descricao,
    -- capítulo CID derivado da letra inicial do código
    case left(cc.cid_comorbidade, 1)
        when 'A' then 'I – Infecciosas e parasitárias'
        when 'B' then 'I – Infecciosas e parasitárias'
        when 'C' then 'II – Neoplasias'
        when 'D' then 'II/III – Neoplasias / Sangue'
        when 'E' then 'IV – Endócrinas, nutricionais e metabólicas'
        when 'F' then 'V – Transtornos mentais'
        when 'G' then 'VI – Sistema nervoso'
        when 'H' then 'VII/VIII – Olho / Ouvido'
        when 'I' then 'IX – Aparelho circulatório'
        when 'J' then 'X – Aparelho respiratório'
        when 'K' then 'XI – Aparelho digestivo'
        when 'L' then 'XII – Pele'
        when 'M' then 'XIII – Osteomuscular'
        when 'N' then 'XIV – Aparelho geniturinário'
        when 'O' then 'XV – Gravidez e parto'
        when 'P' then 'XVI – Afecções perinatais'
        when 'Q' then 'XVII – Malformações congênitas'
        when 'R' then 'XVIII – Sintomas e sinais'
        else 'Outro'
    end                                                         as capitulo_cid,

    -- volume e proporção
    cc.total_obitos_com_comorbidade,
    ol.total_obitos_com_linhaii,
    round(
        cc.total_obitos_com_comorbidade::numeric
        / nullif(ol.total_obitos_com_linhaii, 0) * 100, 2
    )                                                           as pct_obitos_com_comorbidade

from comorbidade_por_cnae as cc
inner join cnae_info as ci
    on cc.cnae_classe = ci.cnae_classe
inner join {{ ref('stg_dim_grau_risco') }} as gr
    on cc.cnae_classe = gr.cnae_classe
left join obitos_com_linhaii as ol
    on cc.cnae_classe = ol.cnae_classe
left join {{ ref('stg_dim_cid') }} as cid
    on cc.cid_comorbidade = cid.cid_codigo
left join {{ ref('stg_dim_cid') }} as cat_cid
    on left(cc.cid_comorbidade, 3) = cat_cid.cid_codigo

order by gr.grau_risco desc, ci.cnae_classe, cc.total_obitos_com_comorbidade desc
