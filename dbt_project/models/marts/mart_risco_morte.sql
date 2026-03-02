-- =============================================================================
-- mart_risco_morte — Risco de mortalidade por CNAE e CBO
-- =============================================================================
-- Coberturas suportadas: MORTE (qualquer causa) | MORTE ACIDENTAL
--
-- IMPORTANTE — ATRIBUIÇÃO DE CNAE:
--   O SIM não registra o CNAE do empregador — apenas o CBO (ocupação).
--   A coluna cnae_classe é inferida via: CBO → CNAE com mais vínculos na RAIS.
--   Isso é uma APROXIMAÇÃO. Para análise de empresa específica, cruzar com o
--   CNAE declarado pela empresa e usar este mart como REFERÊNCIA de setor.
--
-- ESTRUTURA:
--   Granularidade: CNAE × CBO × categoria_morte × faixa_etaria × sexo
--   Cada linha representa o total de óbitos com aquela combinação.
--
-- COLUNA GUIA: categoria_morte
--   Classifica a causa básica em categorias mutuamente exclusivas para
--   permitir ao subscritor distinguir coberturas sem pré-filtrar dados:
--
--   'Natural – [Capítulo]'  → Óbito por doença (capítulos A-R, U)
--   'Acidental – Trabalho'  → Capítulos V/W com acidente_trabalho='Sim'
--   'Acidental – Trânsito'  → Capítulo V (acidentes de transporte)
--   'Acidental – Queda'     → Capítulo W (quedas, afogamentos, exposição)
--   'Suicídio'              → X60-X84 (lesão autoprovocada intencional)
--   'Homicídio'             → X85-Y09 (agressão)
--   'Indeterminado'         → Y10-Y34 (intenção não determinada)
--   'Traumatismo'           → Capítulo S (fraturas, ferimentos)
--   'Intoxicação/Complicação'→ Capítulo T
--   'Causa externa – outra' → Demais capítulos XX
--
-- NOTA SOBRE MORTE ACIDENTAL E SUICÍDIO:
--   Coberturas de Morte Acidental tipicamente EXCLUEM suicídio (X60-X84).
--   Este mart separa explicitamente 'Suicídio' para que o subscritor aplique
--   a regra correta sem necessidade de filtrar manualmente no CID.
--
-- EXEMPLO de consulta para Morte Acidental (excl. suicídio) por CNAE 41204:
--   SELECT * FROM mart_risco_morte
--   WHERE cnae_classe = '41204'
--     AND categoria_morte IN ('Acidental – Trabalho','Acidental – Trânsito',
--                             'Acidental – Queda','Acidental – Outra')
--   ORDER BY total_obitos DESC

{{ config(materialized='table') }}

with cbo_cnae_dom as (
    -- CBO → CNAE dominante (mais vínculos RAIS). 1 linha por CBO.
    select distinct on (cbo_codigo)
        cbo_codigo,
        cnae_classe
    from {{ ref('stg_fact_rais') }}
    where cbo_codigo  is not null
      and cnae_classe is not null
    group by cbo_codigo, cnae_classe, quantidade_vinculos
    order by cbo_codigo, quantidade_vinculos desc
),

sim_com_cnae as (
    select
        s.cbo_codigo,
        s.cid_10_causa,
        s.acidente_trabalho,
        s.total_obitos,
        s.sexo,
        s.faixa_etaria,
        s.ano,
        c.cnae_classe
    from {{ ref('stg_fact_sim') }} s
    inner join cbo_cnae_dom c using (cbo_codigo)
    where s.cid_10_causa is not null
),

categorizado as (
    select
        *,
        case
            -- Acidental com registro explícito de acidente do trabalho
            when left(cid_10_causa, 1) in ('V','W','X','Y')
             and acidente_trabalho = 'Sim'
            then 'Acidental – Trabalho'

            -- Acidentes de transporte
            when left(cid_10_causa, 1) = 'V'
            then 'Acidental – Trânsito'

            -- Quedas, afogamentos, exposição a forças inanimadas
            when left(cid_10_causa, 1) = 'W'
            then 'Acidental – Queda / Afogamento'

            -- Suicídio (X60-X84): lesão autoprovocada intencional
            -- Importante: Morte Acidental tipicamente EXCLUI esta categoria.
            when cid_10_causa between 'X60' and 'X84.9'
            then 'Suicídio'

            -- Homicídio (X85-Y09)
            when cid_10_causa between 'X85' and 'Y09.9'
            then 'Homicídio / Agressão'

            -- Intenção indeterminada (Y10-Y34)
            when cid_10_causa between 'Y10' and 'Y34.9'
            then 'Causa externa – Intenção indeterminada'

            -- Complicações de procedimentos / intervenção médica (Y83-Y84)
            when cid_10_causa between 'Y83' and 'Y84.9'
            then 'Complicação de procedimento médico'

            -- Demais capítulo Y
            when left(cid_10_causa, 1) = 'Y'
            then 'Causa externa – Outra (Y)'

            -- Demais capítulo X
            when left(cid_10_causa, 1) = 'X'
            then 'Causa externa – Outra (X)'

            -- Traumatismos (S): geralmente sequela, mas registrado como causa básica
            when left(cid_10_causa, 1) = 'S'
            then 'Traumatismo / Lesão (S)'

            -- Envenenamentos, queimaduras, complicações cirúrgicas (T)
            when left(cid_10_causa, 1) = 'T'
            then 'Intoxicação / Queimadura / Complicação cirúrgica (T)'

            -- Fatores situacionais e histórico clínico (Z)
            when left(cid_10_causa, 1) = 'Z'
            then 'Fator situacional / Histórico (Z)'

            -- Doenças naturais — capítulo A-R, U
            when left(cid_10_causa, 1) in ('A','B') then 'Natural – I: Infecciosas'
            when left(cid_10_causa, 1) in ('C','D') then 'Natural – II/III: Neoplasias'
            when left(cid_10_causa, 1) =  'E'       then 'Natural – IV: Endócrinas/Metabólicas'
            when left(cid_10_causa, 1) =  'F'       then 'Natural – V: Transtornos mentais'
            when left(cid_10_causa, 1) =  'G'       then 'Natural – VI: Sistema nervoso'
            when left(cid_10_causa, 1) =  'H'       then 'Natural – VII/VIII: Olho/Ouvido'
            when left(cid_10_causa, 1) =  'I'       then 'Natural – IX: Cardiovascular'
            when left(cid_10_causa, 1) =  'J'       then 'Natural – X: Respiratório'
            when left(cid_10_causa, 1) =  'K'       then 'Natural – XI: Digestivo'
            when left(cid_10_causa, 1) =  'L'       then 'Natural – XII: Pele'
            when left(cid_10_causa, 1) =  'M'       then 'Natural – XIII: Osteomuscular'
            when left(cid_10_causa, 1) =  'N'       then 'Natural – XIV: Geniturinário'
            when left(cid_10_causa, 1) =  'O'       then 'Natural – XV: Maternidade'
            when left(cid_10_causa, 1) =  'P'       then 'Natural – XVI: Perinatal'
            when left(cid_10_causa, 1) =  'Q'       then 'Natural – XVII: Congênita'
            when left(cid_10_causa, 1) =  'R'       then 'Natural – XVIII: Sintomas inespecíficos'
            when left(cid_10_causa, 1) =  'U'       then 'Natural – XXII: Especial (COVID-19)'
            else 'Não classificado'
        end as categoria_morte
    from sim_com_cnae
)

select
    gr.grau_risco,
    gr.grau_risco_descricao,
    a.cnae_classe,
    ci.cnae_descricao_classe,
    ci.cnae_secao,
    ci.cnae_descricao_secao,
    a.cbo_codigo,
    cb.cbo_descricao,
    cb.cbo_grande_grupo_codigo,
    a.categoria_morte,
    a.faixa_etaria,
    a.sexo,
    a.cid_10_causa,
    d.cid_descricao                  as causa_descricao,
    sum(a.total_obitos)              as total_obitos

from categorizado a
left join (
    select distinct cnae_classe, cnae_descricao_classe, cnae_secao, cnae_descricao_secao
    from {{ ref('stg_dim_cnae') }}
) ci using (cnae_classe)
inner join {{ ref('stg_dim_grau_risco') }} gr using (cnae_classe)
left join {{ ref('stg_dim_cbo') }} cb on a.cbo_codigo = cb.cbo_codigo
left join {{ ref('stg_dim_cid') }} d on a.cid_10_causa = d.cid_codigo

group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
order by gr.grau_risco desc, a.cnae_classe, sum(a.total_obitos) desc
