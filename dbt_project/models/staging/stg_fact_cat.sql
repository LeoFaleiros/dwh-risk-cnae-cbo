-- CAT (work accident communications) from INSS portal
-- cbo column: "515105-Agente Comunitário de Saúde" -> extract 6-digit code
-- cnae_empregador_cod: 4-digit integer stored as text -> left-pad to 5 for cnae_classe
-- data_competencia: "YYYY/MM" -> extract year and month
-- munic_empregador: "354780-Santo André-Sp" -> extract 6-digit IBGE code
-- indica_obito: "Sim"/"Não" -> boolean

select
    split_part(data_competencia, '/', 1)::integer       as ano,
    split_part(data_competencia, '/', 2)::integer       as mes,
    -- CBO: first 6 chars before the dash
    left(trim(cbo), 6)                                  as cbo_codigo,
    -- CNAE class: zero-pad to 5 digits
    lpad(trim(cnae_empregador_cod), 5, '0')             as cnae_classe,
    -- Municipality: first 6 digits of "NNNNNN-Nome-UF"
    left(trim(munic_empregador), 6)                     as municipio_id_6,
    lower(trim(tipo_acidente))                          as tipo_acidente,
    trim(cid_10)                                        as cid_10,
    (indica_obito ilike 'Sim')                         as obito,
    trim(uf_empregador)                                 as uf_empregador,
    trim(_source_file)                                  as source_file

from {{ source('raw', 'cat_microdados') }}
where cbo is not null
  and cnae_empregador_cod is not null
  and data_competencia is not null
