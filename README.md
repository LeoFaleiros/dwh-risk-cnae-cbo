# dwh-risk-cnae-cbo

Pipeline de ingestao e transformacao de dados publicos brasileiros para analise de risco ocupacional.

Cruza CNAE (atividade economica) e CBO (ocupacao) com Grau de Risco (NR-04),
Comunicacoes de Acidente de Trabalho (CAT) e dados de mortalidade (SIM).

## Arquitetura

```
Fontes → Ingestao Python (raw) → dbt staging (stg) → dbt marts (mart)
```

**Banco:** PostgreSQL 16 (docker-compose)

| Schema | Descricao |
|--------|-----------|
| `raw`  | Dados brutos carregados pelo pipeline Python |
| `stg`  | Dados padronizados (views dbt) |
| `mart` | Agregacoes analiticas (tables dbt) |
| `ops`  | Controle de cargas (`load_history`) |

## Fontes de dados

| Dado | Origem |
|------|--------|
| CNAE 2.3 | BigQuery: `basedosdados.br_bd_diretorios_brasil.cnae_2` |
| CBO 2002 | BigQuery: `basedosdados.br_bd_diretorios_brasil.cbo_2002` |
| Municipios IBGE | BigQuery: `basedosdados.br_bd_diretorios_brasil.municipio` |
| RAIS vinculos ativos | BigQuery: `basedosdados.br_me_rais.microdados_vinculos` |
| SIM obitos | BigQuery: `basedosdados.br_ms_sim.microdados` |
| Grau de Risco NR-04 | CSV local: `input/AnexoI_CNAE_GR_NR04_2023_COMPLETO.xlsx` |
| CAT acidentes de trabalho | INSS portal (CKAN API + ZIP/CSV) |

## Pre-requisitos

- Docker e Docker Compose
- Python 3.11+
- Conta GCP com billing ativo: https://basedosdados.org/

## Setup

```bash
git clone <repo>
cd dwh-risk-cnae-cbo

cp .env.example .env
# Editar .env: preencher GCP_PROJECT_ID, INGEST_UF e INGEST_ANO_INICIO/FIM

gcloud auth application-default login

python -m venv venv
source venv/bin/activate          # Linux/Mac
# ou: .\venv\Scripts\Activate.ps1  # Windows

pip install -r requirements.txt

docker compose up -d
```

## Rodar

```bash
# 1. Ingerir dados brutos no PostgreSQL
python -m ingestion.runner

# 2. Executar transformacoes dbt
cd dbt_project
dbt run --profiles-dir .

# 3. Rodar testes de integridade
dbt test --profiles-dir .
```

## Testes

`dbt test` executa 28 testes sobre os modelos de staging:

- `unique` e `not_null` nas PKs de todas as dimensoes
- `not_null` nas chaves de juncao dos fatos
- `accepted_values` em `grau_risco` (valores 1-4)
- `relationships` de `stg_fact_rais` → `stg_dim_cnae` (severity: warn — subclasses historicas podem nao ter match)

Resultado esperado: `PASS=27 WARN=1 ERROR=0`

## Queries de exemplo

```sql
-- Top 10 CNAEs com mais acidentes de trabalho
SELECT cnae_classe, cnae_descricao, SUM(total_acidentes) AS acidentes
FROM mart.mart_cat_cnae_cbo_mes
GROUP BY 1, 2
ORDER BY 3 DESC
LIMIT 10;

-- CNAEs grau Leve com mais acidentes (possivel subestimacao de risco)
SELECT cnae_classe, cnae_descricao, grau_risco_descricao, total_acidentes, total_vinculos,
       taxa_acidentes_por_mil
FROM mart.mart_risco_teorico_vs_observado
WHERE grau_risco_descricao = 'Leve'
ORDER BY total_acidentes DESC
LIMIT 10;

-- Cobertura por grau de risco
SELECT grau_risco_descricao,
       COUNT(*) AS classes,
       COUNT(*) FILTER (WHERE total_acidentes > 0) AS com_acidentes,
       SUM(total_acidentes) AS total_acidentes,
       SUM(total_vinculos) AS total_vinculos
FROM mart.mart_risco_teorico_vs_observado
GROUP BY 1 ORDER BY 1;
```
