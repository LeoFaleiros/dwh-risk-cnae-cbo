-- SIM mortality records with occupation (CBO)
-- 'ocupacao' was aliased to cbo_2002 during ingestion

select
    ano::integer                        as ano,
    upper(trim(sigla_uf))               as sigla_uf,
    trim(id_municipio)                  as municipio_id,
    trim(cbo_2002)                      as cbo_codigo,
    trim(cid_10_causa)                  as cid_10_causa,
    trim(acidente_trabalho)             as acidente_trabalho,
    total_obitos::integer               as total_obitos

from {{ source('raw', 'fact_sim') }}
where cbo_2002 is not null
  and sigla_uf is not null
