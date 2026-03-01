-- Grau de Risco por classe CNAE conforme NR-04 (Anexo I)
-- Source: input/AnexoI_CNAE_GR_NR04_2023_COMPLETO.xlsx

select
    trim(cnae_classe)   as cnae_classe,
    grau_risco,
    case grau_risco
        when 1 then 'Leve'
        when 2 then 'Medio'
        when 3 then 'Alto'
        when 4 then 'Critico'
    end                 as grau_risco_descricao

from {{ source('raw', 'dim_grau_risco') }}
where cnae_classe is not null
  and grau_risco is not null
