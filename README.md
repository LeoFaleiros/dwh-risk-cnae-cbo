# dwh-risk-cnae-cbo

Pipeline de ingestão e transformação de dados públicos brasileiros para análise de risco ocupacional.

Cruza CNAE (atividade econômica) e CBO (ocupação) com Grau de Risco (NR-04),
Comunicações de Acidente de Trabalho (CAT) e dados de mortalidade (SIM).

## Arquitetura

```
Fontes → Ingestão Python (raw) → dbt staging (stg) → dbt marts (mart)
```

**Banco:** PostgreSQL 16 (docker-compose)

| Schema | Descrição |
|--------|-----------|
| `raw`  | Dados brutos carregados pelo pipeline Python |
| `stg`  | Dados padronizados (views dbt) |
| `mart` | Agregações analíticas (tables dbt) |
| `ops`  | Controle de cargas (`load_history`) |

## Fontes de dados

| Dado | Origem |
|------|--------|
| CNAE 2.3 | BigQuery: `basedosdados.br_bd_diretorios_brasil.cnae_2` |
| CBO 2002 | BigQuery: `basedosdados.br_bd_diretorios_brasil.cbo_2002` |
| Municípios IBGE | BigQuery: `basedosdados.br_bd_diretorios_brasil.municipio` |
| RAIS vínculos ativos | BigQuery: `basedosdados.br_me_rais.microdados_vinculos` |
| SIM óbitos | BigQuery: `basedosdados.br_ms_sim.microdados` |
| Grau de Risco NR-04 | CSV local: `input/AnexoI_CNAE_GR_NR04_2023_COMPLETO.xlsx` |
| CAT acidentes de trabalho | INSS portal (CKAN API + ZIP/CSV) |
| CID-10 | CSV local: `input/CID/` (DATASUS, cp1252) |

## Pré-requisitos

- Docker e Docker Compose
- Python 3.11+
- Conta GCP com billing ativo: https://basedosdados.org/

## Setup

```bash
git clone <repo>
cd dwh-risk-cnae-cbo

cp .env.example .env
# Editar .env: preencher GCP_PROJECT_ID, INGEST_UF e INGEST_ANO_INICIO/FIM

python -m venv venv
source venv/bin/activate          # Linux/Mac
# ou: .\venv\Scripts\Activate.ps1  # Windows

pip install -r requirements.txt

docker compose up -d
```

> **Autenticação GCP:** na primeira execução do runner, o `basedosdados`
> abre automaticamente o browser para autenticação OAuth com sua conta Google.
> Não é necessário instalar nem executar o `gcloud` CLI.

## Rodar

```bash
# 1. Ingerir dados brutos no PostgreSQL
python -m ingestion.runner

# 2. Executar transformações dbt
cd dbt_project
dbt run --profiles-dir .

# 3. Rodar testes de integridade
dbt test --profiles-dir .
```

## Testes

`dbt test` executa 28 testes sobre os modelos de staging:

- `unique` e `not_null` nas PKs de todas as dimensões
- `not_null` nas chaves de junção dos fatos
- `accepted_values` em `grau_risco` (valores 1–4)
- `relationships` de `stg_fact_rais` → `stg_dim_cnae` (severity: warn — subclasses históricas podem não ter match)

Resultado esperado: `PASS=27 WARN=1 ERROR=0`

## Marts

### `mart_risco_teorico_vs_observado`

Uma linha por classe CNAE. Cruza o grau de risco teórico (NR-04) com
acidentes reais (CAT) e vínculos (RAIS). Útil para identificar classes
com taxa de acidentes desproporcional ao risco classificado.

Top 25 por volume de acidentes (SP, 2020–2023):

```
 gr | grau_risco_desc | cnae_classe | cnae_descricao                                   | acidentes | vinculos | taxa/mil
----+-----------------+-------------+--------------------------------------------------+-----------+----------+---------
  3 | Alto            | 86101       | Atividades de atendimento hospitalar             |    19 543 |  475 683 |    41.1
  2 | Medio           | 47113       | Comércio varejista de mercadorias em geral       |     7 045 |  431 337 |    16.3
  1 | Leve            | 84116       | Administração pública em geral                   |     6 008 | 1366 316 |     4.4
  3 | Alto            | 49302       | Transporte rodoviário de carga                   |     5 411 |  358 204 |    15.1
  3 | Alto            | 86305       | Atividades de atenção ambulatorial executadas    |     4 305 |  168 403 |    25.6
  2 | Medio           | 56112       | Restaurantes e outros estabelecimentos           |     4 299 |  416 426 |    10.3
  1 | Leve            | 78205       | Locação de mão de obra temporária                |     3 974 |  278 251 |    14.3
  3 | Alto            | 81214       | Limpeza em prédios e em domicílios               |     2 871 |  241 643 |    11.9
  3 | Alto            | 41204       | Construção de edifícios                          |     2 666 |  193 366 |    13.8
  2 | Medio           | 47440       | Com. varej. de ferragens, madeira e vidraçaria   |     2 518 |  183 230 |    13.7
  3 | Alto            | 86402       | Atividades de serviços de complementação diagn.  |     2 492 |   80 625 |    30.9
  2 | Medio           | 56201       | Serviços de catering, bufê e outros              |     2 368 |  127 149 |    18.6
  3 | Alto            | 29492       | Fabricação de peças e acessórios para veículos   |     2 165 |   99 480 |    21.8
  3 | Alto            | 38114       | Coleta de resíduos não perigosos                 |     2 002 |   47 884 |    41.8
  1 | Leve            | 86909       | Atividades de atenção à saúde humana não espec.  |     1 916 |   37 410 |    51.2
  3 | Alto            | 22293       | Fabricação de artefatos de material plástico     |     1 787 |   81 306 |    22.0
  3 | Alto            | 10716       | Fabricação de açúcar em bruto                    |     1 779 |  115 398 |    15.4
  2 | Medio           | 45307       | Comércio de peças e acessórios para veículos     |     1 650 |  134 915 |    12.2
  1 | Leve            | 86607       | Atividades de apoio à gestão de saúde            |     1 617 |   35 350 |    45.7
  2 | Medio           | 82997       | Atividades de serviços prestados principalmente  |     1 488 |  152 787 |     9.7
  3 | Alto            | 22226       | Fabricação de embalagens de material plástico    |     1 434 |   54 306 |    26.4
  1 | Leve            | 64221       | Bancos múltiplos, com carteira comercial         |     1 390 |  166 604 |     8.3
  3 | Alto            | 25993       | Fabricação de produtos de metal não especif.     |     1 389 |   43 868 |    31.7
  2 | Medio           | 53105       | Atividades de Correio                            |     1 388 |   30 654 |    45.3
  2 | Medio           | 81117       | Serviços combinados para apoio a edifícios       |     1 357 |  180 650 |     7.5
```

> Grau Leve com taxa > 40/mil (86909, 86607) pode indicar subclassificação
> ou exposição a risco biológico não refletida na NR-04.

---

### `mart_cat_cnae_cbo_mes`

Granularidade: CNAE × CBO × CID × mês. Permite detalhar quais ocupações
e diagnósticos concentram os acidentes dentro de cada atividade econômica.

Top 25 combinações CNAE-CBO-CID por volume agregado (SP, 2020–2023):

```
 cnae  | cnae_descricao                          | cbo    | cbo_descricao            | cid_descricao                    | acid. | óbitos
-------+-----------------------------------------+--------+--------------------------+----------------------------------+-------+-------
 86101 | Atendimento hospitalar                  | 322205 | Técnico De Enfermagem    | Contato c/ e exposição a doenças |  1451 |      0
 86101 | Atendimento hospitalar                  | 322205 | Técnico De Enfermagem    | Contato com objeto cortante      |   728 |      0
 86101 | Atendimento hospitalar                  | 322230 | Auxiliar De Enfermagem   | Contato c/ e exposição a doenças |   552 |      0
 86101 | Atendimento hospitalar                  | 322205 | Técnico De Enfermagem    | Infecção por coronavírus         |   516 |      0
 86101 | Atendimento hospitalar                  | 322205 | Técnico De Enfermagem    | Ferimento de dedo(s) sem lesão   |   364 |      0
 47113 | Com. varej. de mercadorias em geral     | 848510 | Açougueiro               | Ferimento de dedo(s) sem lesão   |   311 |      0
 86305 | Atenção ambulatorial executada p/ méd.  | 322205 | Técnico De Enfermagem    | Infecção por coronavírus         |   283 |      0
 86101 | Atendimento hospitalar                  | 223505 | Enfermeiro               | Contato c/ e exposição a doenças |   277 |      0
 47113 | Com. varej. de mercadorias em geral     | 848510 | Açougueiro               | Ferimento de dedo(s) com lesão   |   272 |      0
 86101 | Atendimento hospitalar                  | 322205 | Técnico De Enfermagem    | Entorse e distensão do tornozelo |   256 |      0
 86101 | Atendimento hospitalar                  | 322230 | Auxiliar De Enfermagem   | Contato com objeto cortante      |   230 |      0
 86101 | Atendimento hospitalar                  | 322205 | Técnico De Enfermagem    | Contusão do joelho               |   215 |      0
 86305 | Atenção ambulatorial executada p/ méd.  | 322205 | Técnico De Enfermagem    | Contato c/ e exposição a doenças |   193 |      0
 86101 | Atendimento hospitalar                  | 322205 | Técnico De Enfermagem    | Exame e observação após acidente |   175 |      0
 86101 | Atendimento hospitalar                  | 223505 | Enfermeiro               | Infecção por coronavírus         |   163 |      0
 86402 | Serviços de complementação diagnóstica  | 322205 | Técnico De Enfermagem    | Contato c/ e exposição a doenças |   150 |      0
 86101 | Atendimento hospitalar                  | 223505 | Enfermeiro               | Contato com objeto cortante      |   147 |      0
 86305 | Atenção ambulatorial executada p/ méd.  | 223505 | Enfermeiro               | Infecção por coronavírus         |   145 |      0
 86101 | Atendimento hospitalar                  | 322230 | Auxiliar De Enfermagem   | Ferimento de dedo(s) sem lesão   |   142 |      0
 86101 | Atendimento hospitalar                  | 322205 | Técnico De Enfermagem    | Corte, punção, perfuração        |   142 |      0
 86101 | Atendimento hospitalar                  | 322205 | Técnico De Enfermagem    | Contusão de dedo(s) sem lesão    |   138 |      0
 86101 | Atendimento hospitalar                  | 322230 | Auxiliar De Enfermagem   | Infecção por coronavírus         |   135 |      0
 86101 | Atendimento hospitalar                  | 322205 | Técnico De Enfermagem    | Ferimento de dedo(s) com lesão   |   124 |      0
 86305 | Atenção ambulatorial executada p/ méd.  | 322230 | Auxiliar De Enfermagem   | Contato com objeto cortante      |   114 |      0
 47113 | Com. varej. de mercadorias em geral     | 521125 | Repositor De Mercadorias | Ferimento de dedo(s) com lesão   |   113 |      0
```

---

### `mart_top_cbo_por_cnae`

Top 10 ocupações (CBO) por classe CNAE, ordenadas por vínculos RAIS.
Útil para mapear a força de trabalho predominante em cada atividade.

Exemplo — 3 CNAEs representativos (30 linhas):

```
 gr | cnae  | cnae_descricao                    | rank | cbo    | cbo_descricao              | vinculos |   pct
----+-------+-----------------------------------+------+--------+----------------------------+----------+------
  3 | 10716 | Fabricação de açúcar em bruto     |    1 | 641015 | Tratorista Agrícola        |   15 019 | 13.0%
  3 | 10716 | Fabricação de açúcar em bruto     |    2 | 782510 | Motorista De Caminhão      |   13 783 | 11.9%
  3 | 10716 | Fabricação de açúcar em bruto     |    3 | 622110 | Trabalhador Da Cultura De  |   12 563 | 10.9%
  3 | 10716 | Fabricação de açúcar em bruto     |    4 | 622020 | Trabalhador Volante Da Agr |    5 390 |  4.7%
  3 | 10716 | Fabricação de açúcar em bruto     |    5 | 641005 | Operador De Colheitadeira  |    2 938 |  2.6%
  3 | 10716 | Fabricação de açúcar em bruto     |    6 | 914405 | Mecânico De Manutenção     |    2 519 |  2.2%
  3 | 10716 | Fabricação de açúcar em bruto     |    7 | 841310 | Operador De Equipamentos   |    2 495 |  2.2%
  3 | 10716 | Fabricação de açúcar em bruto     |    8 | 911305 | Mecânico De Manutenção     |    2 441 |  2.1%
  3 | 10716 | Fabricação de açúcar em bruto     |    9 | 724315 | Soldador                   |    2 331 |  2.0%
  3 | 10716 | Fabricação de açúcar em bruto     |   10 | 641010 | Operador De Máquinas       |    2 144 |  1.9%
  3 | 41204 | Construção de edifícios           |    1 | 717020 | Servente De Obras          |   45 341 | 23.5%
  3 | 41204 | Construção de edifícios           |    2 | 715210 | Pedreiro                   |   30 741 | 15.9%
  3 | 41204 | Construção de edifícios           |    3 | 710205 | Mestre (Construção Civil)  |    9 439 |  4.9%
  3 | 41204 | Construção de edifícios           |    4 | 715505 | Carpinteiro                |    7 889 |  4.1%
  3 | 41204 | Construção de edifícios           |    5 | 411010 | Assistente Administrativo  |    5 335 |  2.8%
  3 | 41204 | Construção de edifícios           |    6 | 411005 | Auxiliar De Escritório     |    5 033 |  2.6%
  3 | 41204 | Construção de edifícios           |    7 | 715525 | Carpinteiro De Obras       |    4 171 |  2.2%
  3 | 41204 | Construção de edifícios           |    8 | 514320 | Faxineiro                  |    4 115 |  2.1%
  3 | 41204 | Construção de edifícios           |    9 | 715305 | Armador De Estrutura       |    4 095 |  2.1%
  3 | 41204 | Construção de edifícios           |   10 | 716610 | Pintor De Obras            |    4 039 |  2.1%
  2 | 47113 | Com. varej. de mercadorias em g.  |    1 | 421125 | Operador De Caixa          |   90 214 | 20.9%
  2 | 47113 | Com. varej. de mercadorias em g.  |    2 | 521125 | Repositor De Mercadorias   |   83 022 | 19.3%
  2 | 47113 | Com. varej. de mercadorias em g.  |    3 | 521140 | Atendente De Lojas         |   40 276 |  9.3%
  2 | 47113 | Com. varej. de mercadorias em g.  |    4 | 848510 | Açougueiro                 |   29 723 |  6.9%
  2 | 47113 | Com. varej. de mercadorias em g.  |    5 | 521110 | Vendedor De Comércio Varej |   21 798 |  5.1%
  2 | 47113 | Com. varej. de mercadorias em g.  |    6 | 784105 | Embalador, A Mão           |   15 265 |  3.5%
  2 | 47113 | Com. varej. de mercadorias em g.  |    7 | 517425 | Fiscal De Loja             |   10 394 |  2.4%
  2 | 47113 | Com. varej. de mercadorias em g.  |    8 | 513505 | Auxiliar Nos Serviços      |    9 529 |  2.2%
  2 | 47113 | Com. varej. de mercadorias em g.  |    9 | 141415 | Gerente De Loja            |    9 311 |  2.2%
  2 | 47113 | Com. varej. de mercadorias em g.  |   10 | 411005 | Auxiliar De Escritório     |    8 522 |  2.0%
```
