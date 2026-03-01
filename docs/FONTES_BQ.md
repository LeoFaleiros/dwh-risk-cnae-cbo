# Fontes BigQuery - Schema e Notas

Explorado em: 2026-03-01
Script: `ingestion/explore_bq.py`

---

## br_bd_diretorios_brasil.cnae_2

Total de registros: 1.356

Colunas utilizadas:
- `subclasse` — codigo de 7 digitos sem mascara (ex: "0111301") — chave de join com RAIS
- `descricao_subclasse`
- `classe`, `descricao_classe`
- `grupo`, `descricao_grupo`
- `divisao`, `descricao_divisao`
- `secao`, `descricao_secao`

Colunas ignoradas: `indicador_cnae_2_0/1/2/3` (flags de versao, irrelevantes para o pipeline)

Observacoes:
- Ja contem toda a hierarquia (subclasse -> classe -> grupo -> divisao -> secao) em uma unica tabela — dispensa o Excel CNAE local para a dimensao
- Formato sem mascara: "0111301", nao "01.11-3-01". O join com RAIS usa este campo diretamente

---

## br_bd_diretorios_brasil.cbo_2002

Total de registros: 2.812

Colunas utilizadas:
- `cbo_2002` — codigo de 6 digitos (ex: "010105") — chave de join com RAIS e SIM
- `descricao`
- `grande_grupo`, `descricao_grande_grupo`
- `indicador_cbo_2002_ativa` — flag 0/1 para filtrar ocupacoes ativas

Colunas ignoradas: hierarquia intermediaria (familia, subgrupo, subgrupo_principal) — disponivel se necessario

---

## br_bd_diretorios_brasil.municipio

Total de registros: 5.571

Colunas utilizadas:
- `id_municipio` — codigo IBGE de 7 digitos (ex: "5101837") — chave de join com RAIS e SIM
- `id_municipio_6` — codigo IBGE de 6 digitos (sem digito verificador)
- `nome`
- `sigla_uf`, `nome_uf`
- `nome_regiao`

Colunas ignoradas: regioes especificas (saude, imediata, intermediaria, metropolitana), `centroide`, codigos TSE/RF/BCB

Observacao: RAIS e SIM usam `id_municipio` de 7 digitos — join direto sem transformacao

---

## br_me_rais.microdados_vinculos

Total de registros: ~2,07 bilhoes (microdado individual por vinculo empregaticio)

Colunas utilizadas:
- `ano` (INT64)
- `sigla_uf` (STRING)
- `id_municipio` (STRING, 7 digitos)
- `cnae_2_subclasse` (STRING, 7 digitos sem mascara — ex: "0115600")
- `cbo_2002` (STRING, 6 digitos)
- `vinculo_ativo_3112` (STRING — filtrar `= '1'` para vinculos ativos em 31/12)

Colunas de interesse secundario: `valor_remuneracao_media`, `sexo`, `raca_cor`, `grau_instrucao_apos_2005`, `idade`

Aviso de volume: tabela particionada por `ano`. Sempre filtrar `sigla_uf` e `ano` para evitar full scan.
Estrategia de ingestao: agregar no BQ (GROUP BY ano, sigla_uf, id_municipio, cnae_2_subclasse, cbo_2002) antes de trazer para o Postgres — nao trazer microdado bruto.

---

## br_ms_sim.microdados

Total de registros: ~31,2 milhoes (microdado individual por obito)

Colunas utilizadas:
- `ano` (INT64)
- `sigla_uf` (STRING)
- `id_municipio_residencia` (STRING, 7 digitos) — municipio de residencia do falecido
- `id_municipio_ocorrencia` (STRING, 7 digitos) — municipio onde ocorreu o obito
- `causa_basica` (STRING — CID-10, ex: "J47")
- `ocupacao` (STRING — CBO 6 digitos, **nao se chama `cbo_2002`**)
- `acidente_trabalho` (STRING — flag de acidente de trabalho)

Observacoes:
- A coluna de CBO no SIM chama `ocupacao`, nao `cbo_2002` — atencao no join com a dimensao CBO
- `causa_basica` e CID-10, nao CBO — cruzar com CAT para identificar causas ocupacionais
- Periodo disponivel na amostra: a partir de 2012
