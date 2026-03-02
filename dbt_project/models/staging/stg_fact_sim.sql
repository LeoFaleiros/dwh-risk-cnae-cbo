-- =============================================================================
-- stg_fact_sim — Registros de óbito do SIM com ocupação (CBO)
-- =============================================================================
-- Fonte: raw.fact_sim (ingestão via BigQuery basedosdados.br_ms_sim.microdados)
--
-- O SIM é a Declaração de Óbito (DO). Cada DO tem dois blocos de CIDs:
--
--   PARTE I — Cadeia causal (quatro linhas em sequência):
--     linha_a : causa imediata — o que produziu a morte no momento final
--     linha_b : causa que originou a linha_a (se houver)
--     linha_c : causa que originou a linha_b (se houver)
--     linha_d : causa originária mais remota (raramente preenchida)
--     causa_basica : CID determinado pelo algoritmo DATASUS sobre a Parte I.
--                    Geralmente é o CID mais primário da cadeia (linha_d > c > b > a).
--                    É este campo que classifica o óbito para publicações estatísticas.
--
--   PARTE II — Outras condições significativas (linha_ii):
--     Doenças ou condições que CONTRIBUÍRAM para a morte mas NÃO fazem parte da
--     cadeia causal. Exemplos: diabetes, hipertensão, insuficiência renal crônica,
--     histórico de neoplasia. É o campo de comorbidades/multimorbidade.
--
-- Formato bruto das linhas: '*CID' (ex: '*I219', '*J690X').
--   O asterisco '*' é separador DATASUS.
--   O 'X' ao final é filler para códigos de 3 caracteres sem subdivisão (ex: 'I10X' = I10).
--   linha_ii pode ter múltiplos CIDs: '*I10X*E149' = I10 + E14.9.
--
-- Normalização CID aplicada aqui:
--   4 chars terminando em X → remover X (ex: I10X → I10)
--   4 chars sem X → inserir ponto antes do último (ex: B342 → B34.2)
--   3 chars → manter como-está (ex: I10 → I10)
--
-- LIMITAÇÃO DA ATRIBUIÇÃO CNAE:
--   O SIM não registra o CNAE do empregador — apenas o CBO (ocupação).
--   O CNAE é inferido por: CBO → CNAE dominante via RAIS (mais vínculos).
--   Isso é uma APROXIMAÇÃO nacional, não o CNAE específico do empregador.
--   Para análises por empresa específica, usar CAT (que tem CNPJ/CNAE direto).
--
-- Outros campos normalizados:
--   sexo            : DATASUS '1' → 'M', '2' → 'F'
--   acidente_trabalho: '1'/'1.0' → 'Sim', '2'/'2.0' → 'Não', '9' → 'Ignorado'
--   faixa_etaria    : calculada de 'idade' (anos, float) — NOT do encoding DATASUS 4XX
--   total_obitos    : COUNT(*) agrupado na ingestão — cada linha representa N óbitos
--                     com a mesma combinação de atributos.

select
    ano::integer                        as ano,
    upper(trim(sigla_uf))               as sigla_uf,
    trim(id_municipio)                  as municipio_id,
    trim(cbo_2002)                      as cbo_codigo,

    -- causa_basica: CID determinado pelo DATASUS via algoritmo da OMS sobre a Parte I.
    -- Normalizado para formato com ponto (ex: B342 → B34.2).
    case
        when length(trim(cid_10_causa)) = 4
        then left(trim(cid_10_causa), 3) || '.' || right(trim(cid_10_causa), 1)
        else trim(cid_10_causa)
    end                                 as cid_10_causa,

    -- acidente_trabalho: '1'/'1.0' = Sim | '2'/'2.0' = Não | '9' = Ignorado
    -- pandas converte inteiros para float na ingestão BigQuery → normalizar via left(1).
    case left(trim(acidente_trabalho::text), 1)
        when '1' then 'Sim'
        when '2' then 'Não'
        when '9' then 'Ignorado'
        else null
    end                                 as acidente_trabalho,

    total_obitos::integer               as total_obitos,

    -- sexo: DATASUS '1' = Masculino → 'M', '2' = Feminino → 'F'
    case trim(sexo::text)
        when '1' then 'M'
        when '2' then 'F'
        else null
    end                                 as sexo,

    -- faixa_etaria: 'idade' no BigQuery já está em anos (float, ex: 45.0).
    -- Valores > 130 são erros de fonte — excluídos.
    case
        when idade::numeric between 0  and 17  then '0-17'
        when idade::numeric between 18 and 24  then '18-24'
        when idade::numeric between 25 and 34  then '25-34'
        when idade::numeric between 35 and 44  then '35-44'
        when idade::numeric between 45 and 54  then '45-54'
        when idade::numeric between 55 and 64  then '55-64'
        when idade::numeric between 65 and 130 then '65+'
    end                                 as faixa_etaria,

    -- ---- Parte I: cadeia causal ---- --
    -- Cada campo tem UM CID no formato '*CID' (ex: '*I219').
    -- Passados raw para stg_sim_linhas, que os unnesta e normaliza por linha.
    nullif(trim(linha_a), '')           as linha_a_raw,
    nullif(trim(linha_b), '')           as linha_b_raw,
    nullif(trim(linha_c), '')           as linha_c_raw,
    nullif(trim(linha_d), '')           as linha_d_raw,

    -- ---- Parte II: comorbidades ---- --
    -- Múltiplos CIDs concatenados: '*I10X*E149'.
    -- Passado raw para stg_sim_linhas, que unnesta cada CID por separado.
    nullif(trim(linha_ii), '')          as linha_ii_raw

from {{ source('raw', 'fact_sim') }}
where cbo_2002  is not null
  and sigla_uf  is not null
