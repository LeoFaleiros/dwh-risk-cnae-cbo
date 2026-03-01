-- Municipios IBGE, standardized identifiers
-- Source: basedosdados br_bd_diretorios_brasil.municipio

select
    trim(id_municipio)      as municipio_id,
    trim(id_municipio_6)    as municipio_id_6,
    trim(nome)              as municipio_nome,
    upper(trim(sigla_uf))   as sigla_uf,
    trim(nome_uf)           as uf_nome,
    trim(nome_regiao)       as regiao_nome

from {{ source('raw', 'dim_municipio') }}
where id_municipio is not null
