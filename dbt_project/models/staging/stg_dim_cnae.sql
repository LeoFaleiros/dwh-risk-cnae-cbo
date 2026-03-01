-- Standardizes CNAE 2.x dimension
-- Source columns from basedosdados br_bd_diretorios_brasil.cnae_2

select
    trim(subclasse)           as cnae_subclasse,
    trim(descricao_subclasse) as cnae_descricao_subclasse,
    trim(classe)              as cnae_classe,
    trim(descricao_classe)    as cnae_descricao_classe,
    trim(grupo)               as cnae_grupo,
    trim(descricao_grupo)     as cnae_descricao_grupo,
    trim(divisao)             as cnae_divisao,
    trim(descricao_divisao)   as cnae_descricao_divisao,
    trim(secao)               as cnae_secao,
    trim(descricao_secao)     as cnae_descricao_secao

from {{ source('raw', 'dim_cnae') }}
where subclasse is not null
