-- CAT (work accident communications) from INSS portal
-- Python ingestion now normalizes across 3 source schemas (A/B/C):
--   cbo: always 6-digit code (e.g. "515105")
--   cid_10: always code only (e.g. "B34.2", "S681")
--   data_competencia: always "YYYY/MM"
--   data_acidente: always "DD/MM/YYYY"
--   indica_obito: "Sim"/"Não"
-- cnae_empregador_cod: 4-digit integer stored as text -> left-pad to 4 for cnae_cod_4
-- munic_empregador: "354780-Santo André-Sp" -> extract 6-digit IBGE code

with base as (
    select
        -- Guard against '*****' or non-numeric values in data_competencia
        case when data_competencia ~ '^\d{4}/\d{2}$'
            then split_part(data_competencia, '/', 1)::integer end  as ano,
        case when data_competencia ~ '^\d{4}/\d{2}$'
            then split_part(data_competencia, '/', 2)::integer end  as mes,
        case
            when left(trim(cbo), 1) = '{'       then null
            when left(trim(cbo), 6) = '000000'  then null
            else left(trim(cbo), 6)
        end                                                          as cbo_codigo,
        lpad(trim(cnae_empregador_cod), 4, '0')                      as cnae_cod_4,
        left(trim(munic_empregador), 6)                              as municipio_id_6,
        -- Normalize tipo_acidente: handle double-encoded UTF-8 variants
        case
            when lower(trim(tipo_acidente)) ilike '%pico%'   then 'típico'
            when lower(trim(tipo_acidente)) ilike '%trajeto%' then 'trajeto'
            when lower(trim(tipo_acidente)) ilike '%doen%'   then 'doença'
            when lower(trim(tipo_acidente)) ilike '%ignora%' then 'ignorado'
            else lower(trim(tipo_acidente))
        end                                                          as tipo_acidente,
        case
            when left(trim(cid_10), 1) = '{' then null
            else trim(cid_10)
        end                                                  as cid_10,
        (indica_obito ilike 'Sim')                          as obito,
        trim(uf_empregador)                                  as uf_empregador,
        trim(_source_file)                                   as source_file,
        case sexo
            when 'Masculino' then 'M'
            when 'Feminino'  then 'F'
            else null
        end                                                  as sexo,
        case
            when left(trim(natureza_lesao), 1) = '{' then null
            else trim(natureza_lesao)
        end                                                  as natureza_lesao,
        case
            when left(trim(parte_corpo), 1) = '{' then null
            else trim(parte_corpo)
        end                                                  as parte_corpo,
        case
            when left(trim(agente_causador), 1) = '{' then null
            else trim(agente_causador)
        end                                                  as agente_causador,
        case
            when data_nascimento is not null
             and data_acidente   is not null
             and left(data_nascimento, 1) != '{'
             and left(data_acidente,   1) != '{'
             and data_nascimento != ''
             and data_acidente   != ''
            then extract(year from age(
                    to_date(data_acidente,   'DD/MM/YYYY'),
                    to_date(data_nascimento, 'DD/MM/YYYY')
                ))::int
        end                                                  as idade_no_acidente

    from {{ source('raw', 'cat_microdados') }}
    where cnae_empregador_cod is not null
      and lpad(trim(cnae_empregador_cod), 4, '0') != '0000'
      and data_competencia    is not null
)

select
    ano,
    mes,
    cbo_codigo,
    cnae_cod_4,
    municipio_id_6,
    tipo_acidente,
    cid_10,
    obito,
    uf_empregador,
    source_file,
    sexo,
    natureza_lesao,
    parte_corpo,
    agente_causador,
    case
        when idade_no_acidente between 0  and 17 then '0-17'
        when idade_no_acidente between 18 and 24 then '18-24'
        when idade_no_acidente between 25 and 34 then '25-34'
        when idade_no_acidente between 35 and 44 then '35-44'
        when idade_no_acidente between 45 and 54 then '45-54'
        when idade_no_acidente between 55 and 64 then '55-64'
        when idade_no_acidente >= 65             then '65+'
    end                                                      as faixa_etaria

from base
