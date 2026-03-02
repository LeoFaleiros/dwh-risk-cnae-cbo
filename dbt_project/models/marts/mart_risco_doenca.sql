-- =============================================================================
-- mart_risco_doenca — Condições clínicas associadas a óbitos por CNAE e CBO
-- =============================================================================
-- Coberturas suportadas: IFPD (Invalidez Funcional Permanente por Doença)
--                        DOENÇAS GRAVES (DG)
--
-- FONTE: stg_sim_linhas — todos os CIDs de todas as linhas da Declaração de Óbito,
--        expandidos em formato longo (1 linha por CID por óbito).
--
-- CAMPOS DA DECLARAÇÃO DE ÓBITO USADOS (ver stg_sim_linhas para documentação completa):
--   fonte_linha = 'a'  → causa imediata (Parte I)
--   fonte_linha = 'b'  → causa anterior (Parte I)
--   fonte_linha = 'c'  → causa anterior (Parte I)
--   fonte_linha = 'd'  → causa originária (Parte I)
--   fonte_linha = 'ii' → outras condições significativas / comorbidades (Parte II)
--
-- DECISÃO DE DESIGN — SEM FILTRO DE CAPÍTULO:
--   Este mart NÃO filtra capítulos CID. Todos os capítulos (A–Z, U) estão presentes.
--   A escolha de quais capítulos são relevantes para cada cobertura é do SUBSCRITOR:
--     - Doenças Graves: tipicamente C (neoplasias), I21/I22 (IAM), I60-I64 (AVC),
--       N18 (IRC), J40-J47 (DPOC), F00 (demência), etc.
--     - IFPD: qualquer condição que cause incapacidade permanente antes da morte.
--     - Histórico familiar/pessoal: capítulo Z (Z80-Z99).
--   Use a coluna tipo_condicao como guia rápido de triagem.
--
-- COLUNA GUIA: tipo_condicao
--   Classifica semanticamente o tipo de condição (ex: 'Doença crônica/cardiovascular',
--   'Neoplasia', 'Traumatismo/Lesão', 'Histórico clínico/situacional').
--   Ver stg_sim_linhas para a tabela completa de mapeamento capítulo → tipo.
--
-- LIMITAÇÃO DE ATRIBUIÇÃO CNAE:
--   Mesmo que stg_fact_sim: CBO → CNAE inferido via RAIS (mais vínculos).
--   Não é o CNAE do empregador específico.
--
-- EXEMPLO — Doenças Graves elegíveis (SUSEP rol habitual) em construção civil:
--   SELECT cid_normalizado, d.cid_descricao, sum(total_obitos_com_condicao)
--   FROM mart_risco_doenca
--   WHERE cnae_classe = '41204'
--     AND fonte_linha = 'ii'    -- comorbidades
--     AND left(cid_normalizado,1) IN ('C','I','N','J','F')
--   GROUP BY 1,2 ORDER BY 3 DESC LIMIT 20
--
-- EXEMPLO — Histórico familiar/pessoal (Z80-Z99) para subscrição avançada:
--   SELECT cid_normalizado, d.cid_descricao, sum(total_obitos_com_condicao)
--   FROM mart_risco_doenca
--   WHERE fonte_linha = 'ii'
--     AND left(cid_normalizado,1) = 'Z'
--     AND cid_normalizado >= 'Z80'
--   GROUP BY 1,2 ORDER BY 3 DESC

{{ config(materialized='table') }}

with cbo_cnae_dom as (
    select distinct on (cbo_codigo)
        cbo_codigo,
        cnae_classe
    from {{ ref('stg_fact_rais') }}
    where cbo_codigo  is not null
      and cnae_classe is not null
    group by cbo_codigo, cnae_classe, quantidade_vinculos
    order by cbo_codigo, quantidade_vinculos desc
),

linhas_com_cnae as (
    select
        l.cbo_codigo,
        l.fonte_linha,
        l.cid_normalizado,
        l.cid_categoria,
        l.capitulo_cid,
        l.tipo_condicao,
        l.total_obitos,
        l.sexo,
        l.faixa_etaria,
        c.cnae_classe
    from {{ ref('stg_sim_linhas') }} l
    inner join cbo_cnae_dom c using (cbo_codigo)
    where l.cid_normalizado is not null
),

agregado as (
    select
        cnae_classe,
        cbo_codigo,
        fonte_linha,
        cid_normalizado,
        cid_categoria,
        capitulo_cid,
        tipo_condicao,
        sum(total_obitos)   as total_obitos_com_condicao
    from linhas_com_cnae
    group by 1,2,3,4,5,6,7
),

-- Total de óbitos por CNAE × CBO × fonte_linha (denominador para pct)
total_por_cnae_cbo_fonte as (
    select cnae_classe, cbo_codigo, fonte_linha, sum(total_obitos_com_condicao) as total_base
    from agregado
    group by 1,2,3
)

select
    gr.grau_risco,
    gr.grau_risco_descricao,
    a.cnae_classe,
    ci.cnae_descricao_classe,
    ci.cnae_secao,
    ci.cnae_descricao_secao,
    a.cbo_codigo,
    cb.cbo_descricao,
    cb.cbo_grande_grupo_codigo,
    a.fonte_linha,
    -- Descrição legível da fonte para facilitar leitura
    case a.fonte_linha
        when 'a'  then 'Parte I – Linha A (causa imediata)'
        when 'b'  then 'Parte I – Linha B (causa anterior)'
        when 'c'  then 'Parte I – Linha C (causa anterior)'
        when 'd'  then 'Parte I – Linha D (causa originária)'
        when 'ii' then 'Parte II – Comorbidades / outras condições'
    end                          as fonte_linha_descricao,
    a.cid_normalizado,
    a.cid_categoria,
    d_sub.cid_descricao          as cid_descricao,
    d_cat.cid_descricao          as categoria_descricao,
    a.capitulo_cid,
    a.tipo_condicao,
    a.total_obitos_com_condicao,
    t.total_base                 as total_obitos_na_fonte,
    round(
        a.total_obitos_com_condicao * 100.0
        / nullif(t.total_base, 0), 2
    )                            as pct_na_fonte,
    row_number() over (
        partition by a.cnae_classe, a.cbo_codigo, a.fonte_linha
        order by a.total_obitos_com_condicao desc
    )                            as rank_na_fonte

from agregado a
inner join total_por_cnae_cbo_fonte t
    using (cnae_classe, cbo_codigo, fonte_linha)
left join (
    select distinct cnae_classe, cnae_descricao_classe, cnae_secao, cnae_descricao_secao
    from {{ ref('stg_dim_cnae') }}
) ci using (cnae_classe)
inner join {{ ref('stg_dim_grau_risco') }} gr using (cnae_classe)
left join {{ ref('stg_dim_cbo') }} cb on a.cbo_codigo = cb.cbo_codigo
left join {{ ref('stg_dim_cid') }} d_sub on a.cid_normalizado = d_sub.cid_codigo
left join {{ ref('stg_dim_cid') }} d_cat on a.cid_categoria   = d_cat.cid_codigo

order by gr.grau_risco desc, a.cnae_classe, a.cbo_codigo,
         a.fonte_linha, a.total_obitos_com_condicao desc
