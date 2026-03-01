# Data Quality Notes

Decisões e limitações conhecidas do pipeline. Atualizado conforme novos issues são identificados.

---

## 1. Join CNAE: código de 4 dígitos x código de 5 dígitos

### Contexto

A fonte CAT (INSS portal) armazena o código CNAE do empregador como inteiro de 4 dígitos
(ex.: `8630`). As dimensões `dim_grau_risco` e `dim_cnae` usam o padrão CNAE 2.x com 5
dígitos, onde o 5º é um dígito verificador calculado (ex.: `86305`).

O join correto é via os 4 primeiros dígitos do código de 5:

```sql
left('86305', 4) = '8630'  -- correto
lpad('8630', 5, '0') = '08630' != '86305'  -- errado (bug corrigido em b917421)
```

### Risco de fan-out

Um join `left(cnae_classe, 4) = cnae_cod_4` poderia gerar múltiplas linhas por
acidente se existissem dois códigos de 5 dígitos com o mesmo prefixo de 4 (ex.:
`86305` e `86306`). **Isso não ocorre na prática**: o 5º dígito do CNAE brasileiro
é determinístico — cada código de 4 dígitos produz exatamente um código de 5.
Auditado com `GROUP BY left(cnae_classe, 4) HAVING COUNT(DISTINCT cnae_classe) > 1` →
zero resultados em `stg_dim_cnae` e `stg_dim_grau_risco`.

Se o CNAE for revisado no futuro e novos códigos forem publicados, revalidar esta
premissa.

### Cobertura temporal

O dado CAT disponível via API do INSS cobre apenas 2022 para SP no momento desta
implementação. A configuração `INGEST_ANO_INICIO`/`INGEST_ANO_FIM` no `.env` permite
ampliar o range quando dados históricos ficarem disponíveis — o pipeline detecta
automaticamente a mudança via `source_id`.

---

## 2. Valor `{ñ class}` nos campos CBO e CID-10

### Origem

O arquivo CSV do INSS-CAT usa o valor literal `{ñ class}` (possivelmente herança de
sistema legado) para indicar "não classificado" nos campos de ocupação e diagnóstico.

| Campo raw | Valor problemático | Ocorrências (SP 2022) |
|-----------|-------------------|----------------------|
| `cbo`     | `{ñ class}`       | ~22.853 registros     |
| `cid_10`  | `{ñ class} ...`   | ~9.677 registros      |

### Tratamento aplicado

Em `stg_fact_cat`, os valores são mapeados para `NULL`:

```sql
case
    when trim(cbo) ilike '{%'         then null
    when left(trim(cbo), 6) = '000000' then null
    else left(trim(cbo), 6)
end  as cbo_codigo

case
    when trim(cid_10) ilike '{%' then null
    else split_part(trim(cid_10), ' ', 1)
end  as cid_10
```

Em `stg_dim_cid`, a linha com `{ñ class}` é excluída do filtro `WHERE`.

O código `000000` no campo CBO também é mapeado para `NULL` (placeholder do sistema
fonte para ocupação não informada).

### Impacto residual

- `cbo_codigo` pode ser `NULL` em ~12% dos registros do `stg_fact_cat`.
- O teste `not_null` foi removido de `stg_fact_cat.cbo_codigo` — NULL é válido.
- Joins subsequentes com `stg_dim_cbo` retornam `NULL` para esses registros
  (comportamento correto via `LEFT JOIN`).

---

## 3. Fonte oficial de CID-10

### Situação atual

As descrições de diagnóstico em `stg_dim_cid` são derivadas do próprio texto do
campo `cid_10` no CAT. O formato bruto é `"B34.2 Infecc p/Coronavirus Ne"` —
descrições abreviadas, truncadas e em português informal.

### Problema

- Descrições inconsistentes entre registros do mesmo código.
- Truncamento em campos longos.
- Nenhuma cobertura para códigos que não aparecem no CAT (ex.: diagnósticos só
  presentes no SIM).

### Fonte recomendada

O DATASUS disponibiliza a tabela oficial CID-10 em:
- **CIDABr** (Tabnet): https://datasus.saude.gov.br/transferencia-de-arquivos/
  → Arquivos / CID-10 / Tab CID10
- Formato: ZIP com DBF, contém código + descrição curta + descrição longa por capítulo.

Implementar como nova tarefa de ingestão (`dim_cid` via CSV/DBF official), substituindo
`stg_dim_cid` atual que é derivada.

---

## 4. Join CBO entre CAT e dimensão

### Situação

O campo `cbo` no CAT segue o formato `"515105-Agente Comunitário de Saúde"`. O
staging extrai os 6 primeiros dígitos (`left(trim(cbo), 6) = "515105"`).

Match rate atual (SP 2022):

| Total registros | CBOs distintos no CAT | Com match na dim | Sem match |
|----------------|-----------------------|------------------|-----------|
| 188.774        | 1.456                 | 1.452            | ~22.906   |

Os ~22.906 sem match são majoritariamente (`{ñ class}` → NULL após fix). Os 4 CBOs
sem match restantes (`722225`, `000000`, `314605`) são códigos não presentes na
versão da CBO 2002 carregada — provavelmente inativados ou erros de digitação na fonte.

---

## 5. Diferença de formato CID-10 entre CAT e SIM

O SIM armazena CID sem ponto (`B342`); o CAT usa formato com ponto (`B34.2`). Um
join direto entre `stg_fact_sim` e `stg_dim_cid` exigiria normalização:

```sql
replace(cid_10_causa, '.', '')  -- SIM: B342 -> B342 (já sem ponto)
replace(cid_codigo, '.', '')    -- dim_cid: B34.2 -> B342
```

Fora do escopo atual. Quando implementado, considerar usar a `dim_cid` oficial
(item 3 acima) como fonte única de referência para os dois fatos.

---

## 6. NULLs em `mart_cat_cnae_cbo_mes`

Resumo por coluna (SP 2022, 87.600 linhas):

| Coluna | NULLs | % | Causa |
|--------|-------|---|-------|
| `cnae_classe` | 453 | 0.5% | CNAEs 1.0 depreciados (ver item 7) |
| `cnae_descricao` | 453 | 0.5% | idem |
| `cbo_codigo` | 9.859 | 11.3% | `{ñ class}` + `000000` → NULL (esperado) |
| `cbo_descricao` | 9.883 | 11.3% | idem + 4 CBOs não presentes na dim |
| `cid_10` | 4.033 | 4.6% | `{ñ class}` → NULL (esperado) |
| `cid_descricao` | 4.033 | 4.6% | idem |

Todos os NULLs são esperados ou explicados por problemas da fonte. Nenhum indica
falha no pipeline.

---

## 7. Códigos CNAE 1.0 depreciados no CAT

O INSS CAT aceita comunicações com o código CNAE do empregador na versão vigente
à época do acidente. Para acidentes de anos anteriores, parte dos registros pode
conter códigos CNAE 1.0 (pré-2006) que não existem na estrutura CNAE 2.x atual.

Exemplos encontrados (SP 2022): `6411`, `5244`, `5050`, `7040`, ... (124 classes).
Total afetado: 453 registros (0.5%).

Esses registros aparecem no `mart_cat_cnae_cbo_mes` com `cnae_classe = NULL`.
**Não são erros de pipeline** — são dados historicamente válidos sem equivalente
exato na tabela de dimensão atual.

Se necessário, um mapa de equivalência CNAE 1.0 → 2.x pode ser adicionado como
dimensão auxiliar futuramente.

---

## 8. Qualidade das descrições CID-10

As descrições em `stg_dim_cid` são derivadas do próprio campo `cid_10` dos
registros CAT, no formato `"B34.2 Infecc p/Coronavirus Ne"`. Características:

- **Truncadas**: o INSS limita a descrição a 39 caracteres.
- **Não padronizadas**: a mesma doença pode aparecer com abreviações diferentes
  entre registros (ex.: "Infecc" vs "Infec").
- **Cobertura parcial**: só cobre diagnósticos presentes no CAT, não a tabela
  CID-10 completa.

Fonte recomendada para resolução: DATASUS CIDABr (ver item 3).


O SIM armazena CID sem ponto (`B342`); o CAT usa formato com ponto (`B34.2`). Um
join direto entre `stg_fact_sim` e `stg_dim_cid` exigiria normalização:

```sql
replace(cid_10_causa, '.', '')  -- SIM: B342 -> B342 (já sem ponto)
replace(cid_codigo, '.', '')    -- dim_cid: B34.2 -> B342
```

Fora do escopo atual. Quando implementado, considerar usar a `dim_cid` oficial
(item 3 acima) como fonte única de referência para os dois fatos.
