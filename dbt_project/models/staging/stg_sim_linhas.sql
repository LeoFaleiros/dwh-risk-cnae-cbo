-- =============================================================================
-- stg_sim_linhas — CIDs da Declaração de Óbito expandidos por linha (unnested)
-- =============================================================================
-- Cada registro de stg_fact_sim pode ter até 5 campos com CIDs:
--   linha_a_raw : causa imediata  (Parte I, linha A)
--   linha_b_raw : causa anterior  (Parte I, linha B)
--   linha_c_raw : causa anterior  (Parte I, linha C)
--   linha_d_raw : causa originária (Parte I, linha D)
--   linha_ii_raw: outras condições significativas (Parte II — comorbidades)
--
-- Este model transforma tudo isso em um formato longo:
--   1 linha por (registro_chave + fonte_linha + cid_normalizado)
--
-- COLUNA GUIA: fonte_linha
--   'a'  → linha_a — causa imediata da morte (o que finalizou o processo)
--   'b'  → linha_b — causa que gerou a linha A
--   'c'  → linha_c — causa que gerou a linha B
--   'd'  → linha_d — causa mais remota da cadeia causal
--   'ii' → linha_ii — comorbidade / outra condição significativa (NÃO é causa)
--
-- DIFERENÇA CRÍTICA entre Parte I e Parte II:
--   Parte I (a/b/c/d): relação causal direta. O óbito ocorreu PORQUE o paciente
--     teve essas condições em sequência. Ex: Sépsis (a) ← Pneumonia (b) ← Queda (c).
--   Parte II (ii): condições que CONTRIBUÍRAM mas não são a causa. Ex: paciente caiu
--     e teve sépsis, mas tinha diabetes e hipertensão que agravam — diabetes e HAS
--     vão para a linha II.
--
-- NORMALIZAÇÃO CID:
--   Formato bruto: '*I10X', '*B342', '*J690'
--   Após normalização:
--     I10X → I10   (3 chars, X é filler DATASUS)
--     B342 → B34.2 (4 chars sem X → inserir ponto)
--     J690 → J69.0 (4 chars sem X → inserir ponto)
--     I10  → I10   (3 chars, já correto)
--
-- CAPÍTULOS: TODOS estão presentes, sem filtro.
--   É responsabilidade do subscritor/analista decidir quais capítulos usar
--   para cada cobertura (Morte, Morte Acidental, IFPD, Doenças Graves, etc.).
--
-- EXEMPLO de saída para uma DO com I219, J690, A418, I10, E149:
--   cbo_codigo | fonte_linha | cid_raw | cid_normalizado | capitulo_cid
--   761309     | a           | *R570   | R57.0           | XVIII
--   761309     | b           | *A418   | A41.8           | I
--   761309     | c           | *J690   | J69.0           | X
--   761309     | ii          | *I219   | I21.9           | IX
--   761309     | ii          | *E149   | E14.9           | IV

{{ config(materialized='view') }}

with base as (
    select
        ano,
        sigla_uf,
        municipio_id,
        cbo_codigo,
        cid_10_causa        as causa_basica,
        acidente_trabalho,
        total_obitos,
        sexo,
        faixa_etaria,
        linha_a_raw,
        linha_b_raw,
        linha_c_raw,
        linha_d_raw,
        linha_ii_raw
    from {{ ref('stg_fact_sim') }}
),

-- Cada linha_x tem NO MÁXIMO UM CID. Empilhamos as 4 linhas da Parte I.
parte_i as (
    select ano, sigla_uf, municipio_id, cbo_codigo, causa_basica,
           acidente_trabalho, total_obitos, sexo, faixa_etaria,
           'a'          as fonte_linha,
           linha_a_raw  as cid_raw
    from base where linha_a_raw is not null

    union all

    select ano, sigla_uf, municipio_id, cbo_codigo, causa_basica,
           acidente_trabalho, total_obitos, sexo, faixa_etaria,
           'b', linha_b_raw
    from base where linha_b_raw is not null

    union all

    select ano, sigla_uf, municipio_id, cbo_codigo, causa_basica,
           acidente_trabalho, total_obitos, sexo, faixa_etaria,
           'c', linha_c_raw
    from base where linha_c_raw is not null

    union all

    select ano, sigla_uf, municipio_id, cbo_codigo, causa_basica,
           acidente_trabalho, total_obitos, sexo, faixa_etaria,
           'd', linha_d_raw
    from base where linha_d_raw is not null
),

-- Parte II: linha_ii tem múltiplos CIDs separados por '*'. Unnesta cada um.
parte_ii_unnested as (
    select
        ano, sigla_uf, municipio_id, cbo_codigo, causa_basica,
        acidente_trabalho, total_obitos, sexo, faixa_etaria,
        'ii'                                               as fonte_linha,
        trim(token)                                        as cid_raw
    from base,
         lateral unnest(regexp_split_to_array(linha_ii_raw, '[*]')) as token
    where linha_ii_raw is not null
      and trim(token) != ''
),

-- Combina Parte I e Parte II em formato longo
todas_linhas as (
    select * from parte_i
    union all
    select * from parte_ii_unnested
),

-- Normalização do CID:
--   Formato bruto: '*I10X', 'B342', 'J690'
--   Remover '*' prefix se houver (linha_a/b/c/d trazem '*CID')
normalizado as (
    select
        ano, sigla_uf, municipio_id, cbo_codigo, causa_basica,
        acidente_trabalho, total_obitos, sexo, faixa_etaria,
        fonte_linha,
        cid_raw,
        -- Remover asterisco inicial
        ltrim(cid_raw, '*')                                as cid_sem_prefixo
    from todas_linhas
    where cid_raw is not null and trim(cid_raw) != ''
),

com_cid_norm as (
    select
        *,
        case
            -- 4 chars terminando em X → strip X (ex: I10X → I10)
            when length(cid_sem_prefixo) = 4
             and right(cid_sem_prefixo, 1) = 'X'
            then left(cid_sem_prefixo, 3)
            -- 4 chars sem X → inserir ponto (ex: B342 → B34.2)
            when length(cid_sem_prefixo) = 4
            then left(cid_sem_prefixo, 3) || '.' || right(cid_sem_prefixo, 1)
            -- 3 chars → já normalizado (ex: I10)
            when length(cid_sem_prefixo) in (3, 5, 6)
            then cid_sem_prefixo
            else null
        end                                                as cid_normalizado
    from normalizado
)

select
    ano,
    sigla_uf,
    municipio_id,
    cbo_codigo,
    causa_basica,
    acidente_trabalho,
    total_obitos,
    sexo,
    faixa_etaria,
    fonte_linha,
    cid_raw,
    cid_normalizado,
    left(cid_normalizado, 3)                               as cid_categoria,

    -- Capítulo CID-10: letra inicial do código
    -- Incluído para facilitar filtragem downstream pelo subscritor.
    -- Todos os capítulos estão presentes — sem exclusão.
    case left(cid_normalizado, 1)
        when 'A' then 'I – Infecciosas e parasitárias'
        when 'B' then 'I – Infecciosas e parasitárias'
        when 'C' then 'II – Neoplasias malignas'
        when 'D' then 'II/III – Neoplasias / Sangue e órgãos hematopoéticos'
        when 'E' then 'IV – Endócrinas, nutricionais e metabólicas'
        when 'F' then 'V – Transtornos mentais e comportamentais'
        when 'G' then 'VI – Doenças do sistema nervoso'
        when 'H' then 'VII/VIII – Olho / Ouvido'
        when 'I' then 'IX – Doenças do aparelho circulatório'
        when 'J' then 'X – Doenças do aparelho respiratório'
        when 'K' then 'XI – Doenças do aparelho digestivo'
        when 'L' then 'XII – Doenças da pele'
        when 'M' then 'XIII – Doenças do sistema osteomuscular'
        when 'N' then 'XIV – Doenças do aparelho geniturinário'
        when 'O' then 'XV – Gravidez, parto e puerpério'
        when 'P' then 'XVI – Afecções do período perinatal'
        when 'Q' then 'XVII – Malformações congênitas'
        when 'R' then 'XVIII – Sintomas, sinais e achados anormais'
        when 'S' then 'XIX – Lesões e traumatismos'
        when 'T' then 'XIX/XX – Envenenamentos e causas externas'
        when 'V' then 'XX – Causas externas: acidentes de transporte'
        when 'W' then 'XX – Causas externas: quedas e afogamentos'
        when 'X' then 'XX – Causas externas: intencionais/indeterminadas'
        when 'Y' then 'XX – Causas externas: intenção indeterminada / intervenção'
        when 'Z' then 'XXI – Fatores que influenciam o estado de saúde'
        when 'U' then 'XXII – Códigos especiais (ex: COVID-19)'
        else 'Não classificado'
    end                                                    as capitulo_cid,

    -- Flag semântica para facilitar filtragem por cobertura
    -- O subscritor usa este campo para selecionar o subconjunto relevante.
    case left(cid_normalizado, 1)
        when 'A' then 'Doença crônica/infecciosa'
        when 'B' then 'Doença crônica/infecciosa'
        when 'C' then 'Neoplasia'
        when 'D' then 'Neoplasia/Hematológica'
        when 'E' then 'Doença crônica/metabólica'
        when 'F' then 'Saúde mental'
        when 'G' then 'Doença crônica/neurológica'
        when 'H' then 'Doença crônica/sensorial'
        when 'I' then 'Doença crônica/cardiovascular'
        when 'J' then 'Doença crônica/respiratória'
        when 'K' then 'Doença crônica/digestiva'
        when 'L' then 'Doença crônica/dermatológica'
        when 'M' then 'Doença crônica/osteomuscular'
        when 'N' then 'Doença crônica/renal-urológica'
        when 'O' then 'Maternidade'
        when 'P' then 'Perinatal'
        when 'Q' then 'Congênita'
        when 'R' then 'Sintoma inespecífico'
        when 'S' then 'Traumatismo/Lesão'
        when 'T' then 'Intoxicação/Complicação cirúrgica'
        when 'V' then 'Acidente de transporte'
        when 'W' then 'Acidente: queda/afogamento/exposição'
        when 'X' then 'Causa externa intencional/indeterminada'
        when 'Y' then 'Causa externa: intenção indeterminada/intervenção médica'
        when 'Z' then 'Histórico clínico/situacional'
        when 'U' then 'Código especial'
        else 'Não classificado'
    end                                                    as tipo_condicao

from com_cid_norm
where cid_normalizado is not null
