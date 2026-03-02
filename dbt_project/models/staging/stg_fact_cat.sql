-- CAT (work accident communications) from INSS portal
-- cbo column: "515105-Agente Comunitário de Saúde" -> extract 6-digit code
-- cnae_empregador_cod: 4-digit integer stored as text -> left-pad to 5 for cnae_classe
-- data_competencia: "YYYY/MM" -> extract year and month
-- munic_empregador: "354780-Santo André-Sp" -> extract 6-digit IBGE code
-- indica_obito: "Sim"/"Não" -> boolean
-- data_nascimento / data_acidente: "DD/MM/YYYY" -> used to compute faixa_etaria
-- sexo: "Masculino"/"Feminino"/"Não Informado" -> "M"/"F"/NULL

with base as (
    select
        split_part(data_competencia, '/', 1)::integer       as ano,
        split_part(data_competencia, '/', 2)::integer       as mes,
        -- CBO: first 6 chars before the dash-separated description.
        -- Raw value '{ñ class}' means 'not classified' in the source — mapped to NULL.
        -- Code '000000' is a placeholder used for missing occupation — also NULL.
        case
            when left(trim(cbo), 1) = '{'       then null
            when left(trim(cbo), 6) = '000000'  then null
            else left(trim(cbo), 6)
        end                                                  as cbo_codigo,
        -- CNAE: raw field is a 4-digit integer without check digit (e.g. 8630)
        -- dim_grau_risco and dim_cnae store 5-digit codes with check digit (e.g. 86305)
        -- join downstream using left(cnae_classe, 4) = cnae_cod_4
        lpad(trim(cnae_empregador_cod), 4, '0')              as cnae_cod_4,
        -- Municipality: first 6 digits of "NNNNNN-Nome-UF"
        left(trim(munic_empregador), 6)                      as municipio_id_6,
        lower(trim(tipo_acidente))                           as tipo_acidente,
        -- CID-10 in CAT is "B34.2 Infecc p/Coronavirus Ne" — extract code only.
        -- Raw value '{ñ class}' means 'not classified' in the source — mapped to NULL.
        -- Note: SIM stores CID without dot ("B342"); join with stg_dim_cid uses CAT format.
        case
            when left(trim(cid_10), 1) = '{' then null
            else split_part(trim(cid_10), ' ', 1)
        end                                                  as cid_10,
        (indica_obito ilike 'Sim')                          as obito,
        trim(uf_empregador)                                  as uf_empregador,
        trim(_source_file)                                   as source_file,
        -- Demographics
        case sexo
            when 'Masculino' then 'M'
            when 'Feminino'  then 'F'
            else null
        end                                                  as sexo,
        -- natureza_lesao, parte_corpo, agente_causador: '{ñ class}' → NULL
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
        -- Age at time of accident (years). Dates format: DD/MM/YYYY.
        -- Guard against '{ñ class}' placeholders and blank values.
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
