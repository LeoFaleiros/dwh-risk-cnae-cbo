-- Monthly work accident counts by CNAE class, CBO, and CID-10
-- Join keys: cnae_classe (5-digit), cbo_codigo (6-digit), cid_10 (dotted code)

-- stg_dim_cnae has one row per subclass (7-digit); use DISTINCT at class level (5-digit)
-- CAT carries a 4-digit code without check digit -> join via left(cnae_classe, 4)
with cnae_classe as (
    select distinct
        cnae_classe,
        left(cnae_classe, 4)    as cnae_cod_4,
        cnae_descricao_classe,
        cnae_descricao_secao
    from {{ ref('stg_dim_cnae') }}
)

select
    cat.ano,
    cat.mes,
    cat.uf_empregador,
    cat.cnae_cod_4,
    cnae.cnae_classe,
    cnae.cnae_descricao_classe                              as cnae_descricao,
    cnae.cnae_descricao_secao                               as cnae_secao,
    cat.cbo_codigo,
    cbo.cbo_descricao,
    cbo.cbo_grande_grupo_codigo,
    cbo.cbo_grande_grupo_descricao,
    cat.tipo_acidente,
    cat.cid_10,
    cid.cid_descricao,
    count(*)                                                as total_acidentes,
    sum(case when cat.obito then 1 else 0 end)              as total_obitos

from {{ ref('stg_fact_cat') }} as cat
left join cnae_classe as cnae
    on cat.cnae_cod_4 = cnae.cnae_cod_4
left join {{ ref('stg_dim_cbo') }} as cbo
    on cat.cbo_codigo = cbo.cbo_codigo
left join {{ ref('stg_dim_cid') }} as cid
    on cat.cid_10 = cid.cid_codigo

group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
