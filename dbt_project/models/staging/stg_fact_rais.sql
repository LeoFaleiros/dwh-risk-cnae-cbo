-- RAIS employment bonds aggregated by CNAE x CBO x municipality x year
-- cnae_2_subclasse (7-digit) links to stg_dim_cnae.cnae_subclasse
-- left 5 digits link to stg_dim_grau_risco.cnae_classe

select
    ano::integer                        as ano,
    upper(trim(sigla_uf))               as sigla_uf,
    trim(id_municipio)                  as municipio_id,
    trim(cnae_2_subclasse)              as cnae_subclasse,
    left(trim(cnae_2_subclasse), 5)     as cnae_classe,
    trim(cbo_2002)                      as cbo_codigo,
    quantidade_vinculos::integer        as quantidade_vinculos

from {{ source('raw', 'rais_vinculos') }}
where cnae_2_subclasse is not null
  and cbo_2002 is not null
  and sigla_uf is not null
