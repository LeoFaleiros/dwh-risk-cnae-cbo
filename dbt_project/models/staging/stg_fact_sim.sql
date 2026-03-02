-- SIM mortality records with occupation (CBO)
-- 'ocupacao' was aliased to cbo_2002 during ingestion
-- sexo: DATASUS encoding '1'=Masculino, '2'=Feminino → mapped to 'M'/'F'/NULL
-- idade: BigQuery br_ms_sim already delivers age in years as float (e.g. 45.0)
--        Values > 130 are source data errors — excluded from faixa_etaria.
-- cid_10_causa: SIM stores without dot (B342); add dot to match stg_dim_cid (B34.2).
-- acidente_trabalho: pandas float conversion produces '1.0'/'2.0' alongside '1'/'2';
--                    normalise to 'Sim'/'Não'/'Ignorado' via first character.

select
    ano::integer                        as ano,
    upper(trim(sigla_uf))               as sigla_uf,
    trim(id_municipio)                  as municipio_id,
    trim(cbo_2002)                      as cbo_codigo,
    -- CID: undotted 4-char (B342) → dotted (B34.2) to match stg_dim_cid; 3-char kept as-is.
    case
        when length(trim(cid_10_causa)) = 4
        then left(trim(cid_10_causa), 3) || '.' || right(trim(cid_10_causa), 1)
        else trim(cid_10_causa)
    end                                 as cid_10_causa,
    -- acidente_trabalho: normalise '1'/'1.0' → 'Sim', '2'/'2.0' → 'Não', '9'/'9.0' → 'Ignorado'
    case left(trim(acidente_trabalho::text), 1)
        when '1' then 'Sim'
        when '2' then 'Não'
        when '9' then 'Ignorado'
        else null
    end                                 as acidente_trabalho,
    total_obitos::integer               as total_obitos,
    -- sexo: DATASUS '1'=M '2'=F '9'=Ignorado
    case trim(sexo::text)
        when '1' then 'M'
        when '2' then 'F'
        else null
    end                                 as sexo,
    -- faixa_etaria: idade já está em anos (float). Valores > 130 são erros de fonte.
    case
        when idade::numeric between 0  and 17  then '0-17'
        when idade::numeric between 18 and 24  then '18-24'
        when idade::numeric between 25 and 34  then '25-34'
        when idade::numeric between 35 and 44  then '35-44'
        when idade::numeric between 45 and 54  then '45-54'
        when idade::numeric between 55 and 64  then '55-64'
        when idade::numeric between 65 and 130 then '65+'
    end                                 as faixa_etaria

from {{ source('raw', 'fact_sim') }}
where cbo_2002  is not null
  and sigla_uf  is not null
