-- CNAEs válidos (CNAE 2.0) sem score de risco — sem acidentes típicos ou doença registrados.
-- Complemento do mart_score_risco_cnae para cobertura total das classes CNAE.

{{ config(materialized='table') }}

with classes_cnae as (
    select distinct
        cnae_classe,
        cnae_descricao_classe
    from {{ ref('stg_dim_cnae') }}
),

classes_com_score as (
    select distinct left(cnae_classe, 4) as cnae_cod_4
    from {{ ref('mart_score_risco_cnae') }}
),

cat_check as (
    select
        cnae_cod_4,
        count(*) as total_cat,
        count(*) filter (where tipo_acidente in ('típico', 'doença')) as acidentes_trabalho,
        count(*) filter (where tipo_acidente = 'trajeto') as acidentes_trajeto
    from {{ ref('stg_fact_cat') }}
    where cnae_cod_4 is not null
    group by 1
),

rais_check as (
    select
        cnae_classe,
        sum(quantidade_vinculos) as total_vinculos
    from {{ ref('stg_fact_rais') }}
    group by 1
)

select
    c.cnae_classe,
    c.cnae_descricao_classe,
    coalesce(cat.total_cat, 0)            as total_cat,
    coalesce(cat.acidentes_trabalho, 0)   as acidentes_trabalho,
    coalesce(cat.acidentes_trajeto, 0)    as acidentes_trajeto,
    coalesce(rais.total_vinculos, 0)      as total_vinculos,
    'Sem acidentes típicos/doença registrados' as motivo_exclusao

from classes_cnae c
left join cat_check cat on left(c.cnae_classe, 4) = cat.cnae_cod_4
left join rais_check rais on c.cnae_classe = rais.cnae_classe
where left(c.cnae_classe, 4) not in (select cnae_cod_4 from classes_com_score)
order by c.cnae_classe
