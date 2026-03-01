-- CBO 2002 dimension, standardized keys and descriptions
-- Source: basedosdados br_bd_diretorios_brasil.cbo_2002

select
    trim(cbo_2002)                  as cbo_codigo,
    trim(descricao)                 as cbo_descricao,
    trim(grande_grupo)              as cbo_grande_grupo_codigo,
    trim(descricao_grande_grupo)    as cbo_grande_grupo_descricao,
    left(trim(cbo_2002), 4)         as cbo_subgrupo_codigo,
    indicador_cbo_2002_ativa        as cbo_ativa

from {{ source('raw', 'dim_cbo') }}
where cbo_2002 is not null
