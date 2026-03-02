-- =============================================================================
-- mart_risco_acidente — Frequência e perfil de acidentes por CNAE e CBO
-- =============================================================================
-- Coberturas suportadas: IPA (Invalidez Permanente por Acidente) | DIT (proxy)
--
-- FONTE: CAT (Comunicação de Acidente de Trabalho) via INSS.
--
-- VANTAGEM SOBRE SIM:
--   A CAT registra CNAE e CNPJ do empregador DIRETAMENTE — sem inferência via RAIS.
--   A atribuição CNAE < CBO neste mart é DIRETA e confiável.
--
-- LIMITAÇÃO PARA DIT:
--   A CAT não registra dias de afastamento nem duração da incapacidade.
--   O campo especie_beneficio (B31, B91, B94, etc.) indica o tipo de benefício
--   concedido, o que é um proxy para a gravidade, mas não substitui a duração real.
--   Para DIT precisa as bases do INSS/CAGED com dias de benefício (não ingerida).
--
-- ESTRUTURA:
--   Granularidade: CNAE × CBO × natureza_lesao × parte_corpo
--   Cada linha representa o total de acidentes com aquela combinação.
--
-- COLUNAS GUIA:
--   natureza_lesao  → tipo de lesão (fratura, contusão, amputação, etc.)
--   parte_corpo     → localização anatômica (mão, coluna, olho, etc.)
--   tipo_acidente   → Típico (no trabalho) | Trajeto | Doença Profissional
--   obito           → true/false — permite calcular taxa de mortalidade por acidente
--
-- EXEMPLO de consulta para IPA em construção civil (CNAE começa com '41'):
--   SELECT natureza_lesao, parte_corpo, sum(total_acidentes), sum(total_obitos)
--   FROM mart_risco_acidente
--   WHERE cnae_classe LIKE '41%'
--   GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 20

{{ config(materialized='table') }}

with cat_base as (
    select
        -- CNAE direto do empregador (4 dígitos na CAT, zero-padded)
        -- Exemplo: '4120' refere-se à classe '41204' — JOIN por left(cnae_classe,4)
        cnae_cod_4,
        cbo_codigo,
        tipo_acidente,
        cid_10,
        obito,
        sexo,
        faixa_etaria,
        natureza_lesao,
        parte_corpo,
        agente_causador,
        count(*) as total_acidentes
    from {{ ref('stg_fact_cat') }}
    where cnae_cod_4 is not null
    group by 1,2,3,4,5,6,7,8,9,10
),

-- Enriquece com a classe CNAE de 5 dígitos via left join no prefixo de 4 chars
cnae_match as (
    select distinct on (cnae_classe)
        left(cnae_classe, 4)    as cnae_cod_4,
        cnae_classe,
        cnae_descricao_classe,
        cnae_secao,
        cnae_descricao_secao
    from {{ ref('stg_dim_cnae') }}
    order by cnae_classe
)

select
    gr.grau_risco,
    gr.grau_risco_descricao,
    n.cnae_classe,
    n.cnae_descricao_classe,
    n.cnae_secao,
    n.cnae_descricao_secao,
    c.cbo_codigo,
    cb.cbo_descricao,
    cb.cbo_grande_grupo_codigo,
    c.tipo_acidente,
    c.natureza_lesao,
    c.parte_corpo,
    c.agente_causador,
    c.cid_10,
    d.cid_descricao          as cid_descricao,
    c.sexo,
    c.faixa_etaria,
    sum(c.total_acidentes)   as total_acidentes,
    sum(case when c.obito then c.total_acidentes else 0 end) as total_obitos_acidente,
    round(
        sum(case when c.obito then c.total_acidentes else 0 end) * 100.0
        / nullif(sum(c.total_acidentes), 0), 2
    )                        as pct_obito_no_acidente

from cat_base c
inner join cnae_match n using (cnae_cod_4)
inner join {{ ref('stg_dim_grau_risco') }} gr using (cnae_classe)
left join {{ ref('stg_dim_cbo') }} cb on c.cbo_codigo = cb.cbo_codigo
left join {{ ref('stg_dim_cid') }} d on c.cid_10 = d.cid_codigo

group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
order by gr.grau_risco desc, n.cnae_classe, sum(c.total_acidentes) desc
