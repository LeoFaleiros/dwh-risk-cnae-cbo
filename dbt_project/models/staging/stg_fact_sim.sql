-- SIM mortality records with occupation (CBO)
-- 'ocupacao' was aliased to cbo_2002 during ingestion
-- sexo: DATASUS encoding '1'=Masculino, '2'=Feminino, '9'=Ignorado → mapped to 'M'/'F'/NULL
-- idade: DATASUS encoding — prefix 4XX = years (e.g. 440 = 40 yrs), 5XX = 100+ yrs

select
    ano::integer                        as ano,
    upper(trim(sigla_uf))               as sigla_uf,
    trim(id_municipio)                  as municipio_id,
    trim(cbo_2002)                      as cbo_codigo,
    trim(cid_10_causa)                  as cid_10_causa,
    trim(acidente_trabalho)             as acidente_trabalho,
    total_obitos::integer               as total_obitos,
    -- sexo: DATASUS '1'=M '2'=F '9'=Ignorado
    case trim(sexo::text)
        when '1' then 'M'
        when '2' then 'F'
        else null
    end                                 as sexo,
    -- faixa_etaria decoded from DATASUS idade encoding (prefix 4XX = years, 5XX = 100+)
    case
        when left(trim(idade::text), 1) = '4' then
            case
                when (trim(idade::text)::int - 400) between 0  and 17 then '0-17'
                when (trim(idade::text)::int - 400) between 18 and 24 then '18-24'
                when (trim(idade::text)::int - 400) between 25 and 34 then '25-34'
                when (trim(idade::text)::int - 400) between 35 and 44 then '35-44'
                when (trim(idade::text)::int - 400) between 45 and 54 then '45-54'
                when (trim(idade::text)::int - 400) between 55 and 64 then '55-64'
                when (trim(idade::text)::int - 400) >= 65             then '65+'
            end
        when left(trim(idade::text), 1) = '5' then '65+'
        else null
    end                                 as faixa_etaria

from {{ source('raw', 'fact_sim') }}
where cbo_2002  is not null
  and sigla_uf  is not null
