# Fonte da CAT - Decisao

## Opcoes testadas

### basedosdados (BigQuery)
- Existe? Nao
- Motivo: Dataset basedosdados nao esta disponivel no projeto GCP
- Status: Descartado

### Portal INSS (dadosabertos.inss.gov.br)
- Existe? Sim
- Endpoint: https://dadosabertos.inss.gov.br/api/3/action/package_show?id=inss-comunicacao-de-acidente-de-trabalho-cat
- Recursos: CAT trimestral/mensal de JUL/2018 ate o presente
- Formato: CSV (2018-2020), ZIP com CSV (2021+)
- Dicionario: XLSX disponivel
- Armazenamento: S3 AWS, acesso HTTP GET publico, sem autenticacao
- Status: Escolhido

### dados.gov.br (CKAN)
- Status: API nao retorna JSON valido
- Descartado

### SmartLab / PowerBI (MTE)
- Status: Dashboard acessivel mas sem API estruturada
- Descartado

---

## Decisao final

**Fonte escolhida:** Portal Dados Abertos INSS

**Justificativa:**
- Dados estruturados em CSV/ZIP com historico desde JUL/2018
- Sem autenticacao requerida
- Dicionario de dados disponivel (XLSX)
- Atualizacao regular
- Acesso mais conveniente que PowerBI

**Como acessar:**
1. Consultar API CKAN para listar recursos
2. Baixar arquivo ZIP ou CSV via URL S3
3. Processar com pandas ou COPY em SQL

---

## Schema esperado para stg.fact_cat

| Coluna | Tipo | Obrigatorio | Nota |
|--------|------|-------------|------|
| id_cat | BIGINT | Sim | Chave primaria |
| ano | INT | Sim | De data_acidente |
| sigla_uf | TEXT(2) | Sim | JOIN com dim_municipio |
| id_municipio | TEXT(7) | Sim | IBGE |
| cnae_2_subclasse | TEXT(7) | Nao | JOIN com dim_cnae |
| cbo_2002 | TEXT(6) | Nao | JOIN com dim_cbo |
| data_acidente | DATE | Sim | Campo central |
| tipo_acidente | TEXT | Nao | Tipico/trajeto/doenca |
| obito | BOOLEAN | Nao | Flag obito |

---

## Load em raw.cat_microdados

- Sem transformacoes: colunas exatas do CSV original
- Encoding: Latin-1 (validar no T08)
- Mapeamento de colunas do INSS sera documentado em dbt
