# Dicionário de Dados

## 📚 Visão Geral

Este documento descreve todas as tabelas, colunas, tipos de dados e regras de negócio do Data Lake IBGE e Banco Central do Brasil.

---

## 🥉 BRONZE LAYER

### bronze.populacao

Dados brutos de população municipal do IBGE.

| Coluna | Tipo | Nulo | Descrição |
|--------|------|------|-----------|
| codigo_municipio | STRING | Não | Código IBGE do município (7 dígitos) |
| nome_municipio | STRING | Não | Nome oficial do município |
| ano | INTEGER | Não | Ano de referência |
| valor | DOUBLE | Sim | População residente |
| variavel | STRING | Sim | Código da variável IBGE |
| fonte | STRING | Não | Fonte dos dados ("populacao") |
| nivel_territorial | STRING | Sim | Nível territorial (6 = município) |
| dt_processamento | TIMESTAMP | Não | Timestamp de processamento |
| dt_carga | DATE | Não | Data da carga |

**Chave Primária:** codigo_municipio + ano  
**Particionamento:** ano, fonte  
**Origem:** https://servicodados.ibge.gov.br/api/v3/agregados/6579/

---

### bronze.pib

Dados brutos de PIB municipal do IBGE.

| Coluna | Tipo | Nulo | Descrição |
|--------|------|------|-----------|
| codigo_municipio | STRING | Não | Código IBGE do município (7 dígitos) |
| nome_municipio | STRING | Não | Nome oficial do município |
| ano | INTEGER | Não | Ano de referência |
| valor | DOUBLE | Sim | PIB per capita (R$) |
| variavel | STRING | Sim | Código da variável IBGE |
| fonte | STRING | Não | Fonte dos dados ("pib") |
| nivel_territorial | STRING | Sim | Nível territorial (6 = município) |
| dt_processamento | TIMESTAMP | Não | Timestamp de processamento |
| dt_carga | DATE | Não | Data da carga |

**Chave Primária:** codigo_municipio + ano  
**Particionamento:** ano, fonte  
**Origem:** https://servicodados.ibge.gov.br/api/v3/agregados/5938/

---

### bronze.localidades

Metadados geográficos dos municípios.

| Coluna | Tipo | Nulo | Descrição |
|--------|------|------|-----------|
| codigo_municipio | STRING | Não | Código IBGE do município |
| nome_municipio | STRING | Não | Nome oficial do município |
| codigo_uf | STRING | Não | Código da UF (2 dígitos) |
| sigla_uf | STRING | Não | Sigla da UF (ex: SP, RJ) |
| nome_uf | STRING | Não | Nome completo da UF |
| codigo_regiao | STRING | Não | Código da região (1 dígito) |
| nome_regiao | STRING | Não | Nome da região geográfica |
| sigla_regiao | STRING | Não | Sigla da região (ex: SE, NE) |
| dt_processamento | TIMESTAMP | Não | Timestamp de processamento |
| dt_carga | DATE | Não | Data da carga |

**Chave Primária:** codigo_municipio  
**Particionamento:** Não particionado

---

### bronze.bacen_* 

Dados brutos de indicadores econômicos do Banco Central do Brasil.

**Tabelas:**
- `bronze.bacen_ipca_mensal` - IPCA Mensal (Série 433)
- `bronze.bacen_ipca_12m` - IPCA Acumulado 12 Meses (Série 13522)
- `bronze.bacen_pib_mensal` - PIB Mensal (Série 4380)
- `bronze.bacen_desemprego` - Taxa de Desemprego (Série 24369)
- `bronze.bacen_igpm` - IGP-M (Série 189)
- `bronze.bacen_igpdi` - IGP-DI (Série 190)

| Coluna | Tipo | Nulo | Descrição |
|--------|------|------|-----------|
| data | STRING | Não | Data no formato dd/MM/yyyy |
| valor | STRING | Não | Valor da série (string da API) |
| serie_codigo | INTEGER | Não | Código da série BACEN |
| serie_nome | STRING | Não | Nome da série (ipca_mensal, etc) |
| fonte | STRING | Não | Fonte dos dados ("banco_central") |
| data_parsed | DATE | Sim | Data convertida para tipo DATE |
| ano | INTEGER | Sim | Ano extraído da data |
| mes | INTEGER | Sim | Mês extraído da data |
| valor_decimal | DECIMAL(18,4) | Sim | Valor convertido para decimal |
| dt_processamento | TIMESTAMP | Não | Timestamp de processamento |
| dt_carga | TIMESTAMP | Não | Timestamp da carga |

**Chave Primária:** serie_codigo + data_parsed  
**Particionamento:** ano, fonte  
**Origem:** https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados

**Nota:** Cada série é armazenada em uma tabela separada para facilitar o particionamento e processamento.

---

## 🥈 SILVER LAYER 

### silver.populacao

Dados de população limpos e padronizados.

| Coluna | Tipo | Nulo | Descrição |
|--------|------|------|-----------|
| codigo_municipio | STRING | Não | Código IBGE do município |
| nome_municipio | STRING | Não | Nome padronizado (UPPER, TRIM) |
| codigo_uf | STRING | Não | Código UF extraído (primeiros 2 dígitos) |
| ano | INTEGER | Não | Ano de referência |
| populacao | BIGINT | Não | População total do município |
| dt_processamento | TIMESTAMP | Não | Timestamp de processamento |
| dt_carga | DATE | Não | Data da carga |

**Chave Primária:** codigo_municipio + ano  
**Particionamento:** ano  
**Validações:**
- populacao > 0
- populacao < 20.000.000
- Sem nulos em colunas críticas
- Sem duplicatas

---

### silver.pib

Dados de PIB limpos e padronizados.

| Coluna | Tipo | Nulo | Descrição |
|--------|------|------|-----------|
| codigo_municipio | STRING | Não | Código IBGE do município |
| nome_municipio | STRING | Não | Nome padronizado (UPPER, TRIM) |
| codigo_uf | STRING | Não | Código UF extraído |
| ano | INTEGER | Não | Ano de referência |
| pib_per_capita | DOUBLE | Não | PIB per capita em reais (R$) |
| dt_processamento | TIMESTAMP | Não | Timestamp de processamento |
| dt_carga | DATE | Não | Data da carga |

**Chave Primária:** codigo_municipio + ano  
**Particionamento:** ano  
**Validações:**
- pib_per_capita > 0
- pib_per_capita < 1.000.000
- Sem nulos em colunas críticas
- Sem duplicatas

---

### silver.localidades

Dimensão geográfica limpa.

| Coluna | Tipo | Nulo | Descrição |
|--------|------|------|-----------|
| codigo_municipio | STRING | Não | Código IBGE do município |
| nome_municipio | STRING | Não | Nome padronizado |
| codigo_uf | STRING | Não | Código da UF |
| sigla_uf | STRING | Não | Sigla da UF |
| nome_uf | STRING | Não | Nome da UF |
| codigo_regiao | STRING | Não | Código da região |
| nome_regiao | STRING | Não | Nome da região |
| sigla_regiao | STRING | Não | Sigla da região |
| dt_processamento | TIMESTAMP | Não | Timestamp de processamento |
| dt_carga | DATE | Não | Data da carga |

**Chave Primária:** codigo_municipio  
**Particionamento:** Não particionado

---

### silver.bacen_*_clean (Séries do Banco Central Processadas)

Dados limpos e padronizados das séries econômicas do Banco Central.

**Tabelas:**
- `silver.bacen_ipca_mensal_clean`
- `silver.bacen_ipca_12m_clean`
- `silver.bacen_pib_mensal_clean`
- `silver.bacen_desemprego_clean`
- `silver.bacen_igpm_clean`
- `silver.bacen_igpdi_clean`

| Coluna | Tipo | Nulo | Descrição |
|--------|------|------|-----------|
| serie_codigo | INTEGER | Não | Código da série BACEN |
| serie_nome | STRING | Não | Nome da série |
| data | STRING | Não | Data original (dd/MM/yyyy) |
| data_parsed | DATE | Não | Data convertida |
| ano | INTEGER | Não | Ano extraído |
| mes | INTEGER | Não | Mês extraído |
| valor | STRING | Sim | Valor original (string) |
| valor_decimal | DECIMAL(18,4) | Não | Valor numérico |
| fonte | STRING | Não | Fonte ("banco_central") |
| dt_processamento | TIMESTAMP | Não | Timestamp de processamento |
| dt_carga | TIMESTAMP | Não | Timestamp da carga |

**Chave Primária:** serie_codigo + data_parsed  
**Particionamento:** ano  
**Validações:**
- data_parsed não nulo
- valor_decimal não nulo
- Sem duplicatas por série + data
- Ano entre 2010 e ano atual

---

## 🥇 GOLD LAYER (Analytics)

### gold.dim_municipio

Dimensão de municípios (SCD Type 1).

| Coluna | Tipo | Nulo | Descrição |
|--------|------|------|-----------|
| sk_municipio | BIGINT | Não | Surrogate key (PK) |
| codigo_municipio | STRING | Não | Business key - Código IBGE |
| nome_municipio | STRING | Não | Nome do município |
| codigo_uf | STRING | Não | Código da UF |
| sigla_uf | STRING | Não | Sigla da UF (SP, RJ, etc) |
| nome_uf | STRING | Não | Nome completo da UF |
| codigo_regiao | STRING | Não | Código da região (1-5) |
| nome_regiao | STRING | Não | Nome da região |
| sigla_regiao | STRING | Não | Sigla da região (N, NE, SE, S, CO) |

**Chave Primária:** sk_municipio  
**Business Key:** codigo_municipio  
**Tipo SCD:** Type 1 (overwrite)  
**Registros:** ~5.570 municípios

**Regiões:**
- 1 - Norte (N)
- 2 - Nordeste (NE)
- 3 - Sudeste (SE)
- 4 - Sul (S)
- 5 - Centro-Oeste (CO)

---

### gold.dim_tempo

Dimensão temporal.

| Coluna | Tipo | Nulo | Descrição |
|--------|------|------|-----------|
| sk_tempo | BIGINT | Não | Surrogate key (PK) |
| ano | INTEGER | Não | Ano (2010-2024) |
| semestre | INTEGER | Não | Semestre (1 ou 2) |
| trimestre | INTEGER | Não | Trimestre (1-4) |

**Chave Primária:** sk_tempo  
**Business Key:** ano  
**Granularidade:** Anual

---

### gold.fact_indicadores_municipio

Fato principal com indicadores por município-ano.

| Coluna | Tipo | Nulo | Descrição |
|--------|------|------|-----------|
| sk_municipio | BIGINT | Não | FK para dim_municipio |
| sk_tempo | BIGINT | Não | FK para dim_tempo |
| codigo_municipio | STRING | Não | Código IBGE (desnormalizado) |
| ano | INTEGER | Não | Ano (desnormalizado) |
| populacao_total | BIGINT | Sim | População total do município |
| pib_total | DOUBLE | Sim | PIB total (R$ mil) |
| pib_per_capita | DOUBLE | Sim | PIB per capita (R$) |
| yoy_pib | DOUBLE | Sim | Variação % PIB vs ano anterior |
| yoy_populacao | DOUBLE | Sim | Variação % população vs ano anterior |
| share_pib_uf | DOUBLE | Sim | Participação % no PIB da UF |
| share_pib_brasil | DOUBLE | Sim | Participação % no PIB do Brasil |
| rank_pib_uf | INTEGER | Sim | Ranking PIB dentro da UF |
| rank_pib_brasil | INTEGER | Sim | Ranking PIB nacional |

**Chaves:**
- **PK Composta:** sk_municipio + sk_tempo
- **FK:** sk_municipio → dim_municipio
- **FK:** sk_tempo → dim_tempo

**Granularidade:** 1 registro por município por ano  
**Tipo:** Transaction Fact Table  
**Registros:** ~11.000 (2 anos × 5.570 municípios)

**Cálculos:**
```
pib_total = pib_per_capita × populacao_total / 1000
yoy_pib = ((pib_total_atual - pib_total_anterior) / pib_total_anterior) × 100
share_pib_uf = (pib_total_municipio / pib_total_uf) × 100
rank_pib_brasil = ROW_NUMBER() OVER (PARTITION BY ano ORDER BY pib_total DESC)
```

---

### gold.fact_indicadores_uf

Fato agregado por UF-ano.

| Coluna | Tipo | Nulo | Descrição |
|--------|------|------|-----------|
| codigo_uf | STRING | Não | Código da UF |
| sigla_uf | STRING | Não | Sigla da UF |
| nome_uf | STRING | Não | Nome da UF |
| ano | INTEGER | Não | Ano de referência |
| qtd_municipios | BIGINT | Não | Quantidade de municípios na UF |
| populacao_total | BIGINT | Sim | População total da UF |
| pib_total | DOUBLE | Sim | PIB total da UF (R$ mil) |
| pib_per_capita_medio | DOUBLE | Sim | PIB per capita médio da UF |
| yoy_pib | DOUBLE | Sim | Variação % PIB vs ano anterior |
| yoy_populacao | DOUBLE | Sim | Variação % população vs ano anterior |
| share_pib_brasil | DOUBLE | Sim | Participação % no PIB do Brasil |
| rank_pib_brasil | INTEGER | Sim | Ranking PIB entre UFs |

**Chave Primária:** codigo_uf + ano  
**Granularidade:** 1 registro por UF por ano  
**Tipo:** Aggregate Fact Table  
**Registros:** ~54 (27 UFs × 2 anos)

**Agregações:**
```
populacao_total = SUM(populacao_municipios)
pib_total = SUM(pib_municipios)
pib_per_capita_medio = AVG(pib_per_capita_municipios)
```

---

### gold.fact_indicadores_brasil

Fato agregado por Brasil-ano, enriquecido com indicadores macroeconômicos do Banco Central.

| Coluna | Tipo | Nulo | Descrição |
|--------|------|------|-----------|
| ano | INTEGER | Não | Ano de referência |
| qtd_ufs | BIGINT | Sim | Quantidade de UFs (27) - apenas se dados IBGE disponíveis |
| populacao_total | BIGINT | Sim | População total do Brasil - apenas se dados IBGE disponíveis |
| pib_total | DOUBLE | Sim | PIB total do Brasil (R$ mil) - apenas se dados IBGE disponíveis |
| pib_per_capita | DOUBLE | Sim | PIB per capita nacional - apenas se dados IBGE disponíveis |
| yoy_pib | DOUBLE | Sim | Variação % PIB vs ano anterior - apenas se dados IBGE disponíveis |
| yoy_populacao | DOUBLE | Sim | Variação % população vs ano anterior - apenas se dados IBGE disponíveis |
| **Indicadores Banco Central (quando disponíveis):** |
| ipca_mensal_medio | DECIMAL(18,4) | Sim | IPCA Mensal - média anual |
| ipca_mensal_max | DECIMAL(18,4) | Sim | IPCA Mensal - máximo anual |
| ipca_mensal_min | DECIMAL(18,4) | Sim | IPCA Mensal - mínimo anual |
| ipca_12m_medio | DECIMAL(18,4) | Sim | IPCA 12 Meses - média anual |
| ipca_12m_max | DECIMAL(18,4) | Sim | IPCA 12 Meses - máximo anual |
| ipca_12m_min | DECIMAL(18,4) | Sim | IPCA 12 Meses - mínimo anual |
| pib_mensal_medio | DECIMAL(18,4) | Sim | PIB Mensal - média anual |
| pib_mensal_max | DECIMAL(18,4) | Sim | PIB Mensal - máximo anual |
| pib_mensal_min | DECIMAL(18,4) | Sim | PIB Mensal - mínimo anual |
| desemprego_medio | DECIMAL(18,4) | Sim | Taxa de Desemprego - média anual |
| desemprego_max | DECIMAL(18,4) | Sim | Taxa de Desemprego - máximo anual |
| desemprego_min | DECIMAL(18,4) | Sim | Taxa de Desemprego - mínimo anual |
| igpm_medio | DECIMAL(18,4) | Sim | IGP-M - média anual |
| igpm_max | DECIMAL(18,4) | Sim | IGP-M - máximo anual |
| igpm_min | DECIMAL(18,4) | Sim | IGP-M - mínimo anual |
| igpdi_medio | DECIMAL(18,4) | Sim | IGP-DI - média anual |
| igpdi_max | DECIMAL(18,4) | Sim | IGP-DI - máximo anual |
| igpdi_min | DECIMAL(18,4) | Sim | IGP-DI - mínimo anual |

**Chave Primária:** ano  
**Granularidade:** 1 registro por ano  
**Tipo:** Aggregate Fact Table  
**Registros:** ~10-15 (anos com dados BACEN)

**Agregações (quando dados IBGE disponíveis):**
```
populacao_total = SUM(populacao_ufs)
pib_total = SUM(pib_ufs)
pib_per_capita = pib_total / populacao_total
```

**Agregações Banco Central:**
```
ipca_mensal_medio = AVG(valor_decimal) WHERE serie_codigo = 433 AND ano = X
ipca_mensal_max = MAX(valor_decimal) WHERE serie_codigo = 433 AND ano = X
ipca_mensal_min = MIN(valor_decimal) WHERE serie_codigo = 433 AND ano = X
```

**Nota:** Esta tabela pode ser construída de duas formas:
1. **Com dados IBGE:** Versão completa com população, PIB e indicadores BACEN
2. **Apenas BACEN:** Versão alternativa apenas com indicadores econômicos (quando IBGE não disponível)

---

## 📊 Relacionamentos

```
dim_municipio (1) ──< (*) fact_indicadores_municipio
dim_tempo (1) ──< (*) fact_indicadores_municipio
```

**Cardinalidade:**
- dim_municipio → fact_indicadores_municipio: 1:N
- dim_tempo → fact_indicadores_municipio: 1:N

**Integridade Referencial:**
- Todas as FKs devem existir nas dimensões
- Validado por Quality Checks

---

## 🎯 Glossário de Termos

| Termo | Definição |
|-------|-----------|
| **PIB** | Produto Interno Bruto - soma de todos os bens e serviços finais produzidos |
| **PIB per capita** | PIB dividido pela população |
| **YoY** | Year over Year - comparação com o mesmo período do ano anterior |
| **Share** | Participação percentual em relação ao total |
| **Granularidade** | Nível de detalhe de uma tabela fato |
| **SK** | Surrogate Key - chave técnica gerada |
| **FK** | Foreign Key - chave estrangeira |
| **SCD** | Slowly Changing Dimension - dimensão que muda lentamente |
| **BACEN** | Banco Central do Brasil |
| **IPCA** | Índice Nacional de Preços ao Consumidor Amplo |
| **IGP-M** | Índice Geral de Preços do Mercado |
| **IGP-DI** | Índice Geral de Preços - Disponibilidade Interna |

---

## 📏 Regras de Negócio

### Cálculos de PIB

1. **PIB Total do Município:**
   ```
   PIB Total = PIB per capita × População / 1000
   ```
   *(Dividido por 1000 para expressar em milhares de reais)*

2. **PIB Total da UF:**
   ```
   PIB Total UF = Σ (PIB Total de todos municípios da UF)
   ```

3. **PIB Per Capita Médio da UF:**
   ```
   PIB PC Médio UF = Média (PIB per capita dos municípios da UF)
   ```

### Cálculos de Variação (YoY)

```
YoY = ((Valor Ano Atual - Valor Ano Anterior) / Valor Ano Anterior) × 100
```

- Positivo: crescimento
- Negativo: retração
- NULL: primeiro ano (sem ano anterior para comparar)

### Cálculos de Participação (Share)

```
Share UF = (PIB Município / PIB Total UF) × 100
Share Brasil = (PIB Município / PIB Total Brasil) × 100
```

### Rankings

```sql
-- Ranking Nacional
ROW_NUMBER() OVER (PARTITION BY ano ORDER BY pib_total DESC)

-- Ranking por UF
ROW_NUMBER() OVER (PARTITION BY codigo_uf, ano ORDER BY pib_total DESC)
```

---

---

## 📊 Fontes de Dados

### IBGE (Instituto Brasileiro de Geografia e Estatística)
- **População Municipal:** Agregado 6579
- **PIB Municipal:** Agregado 5938
- **Localidades:** Metadados geográficos

### Banco Central do Brasil (BACEN)
- **IPCA Mensal:** Série 433
- **IPCA 12 Meses:** Série 13522
- **PIB Mensal:** Série 4380
- **Taxa de Desemprego:** Série 24369
- **IGP-M:** Série 189
- **IGP-DI:** Série 190

---

## 🔄 Fluxo de Dados

```
APIs (IBGE + BACEN)
    ↓
BRONZE
    ├── IBGE: populacao, pib, localidades
    └── BACEN: bacen_ipca_mensal, bacen_ipca_12m, bacen_pib_mensal, 
               bacen_desemprego, bacen_igpm, bacen_igpdi
    ↓
SILVER
    ├── IBGE: populacao, pib, localidades (limpos)
    └── BACEN: bacen_*_clean (processados)
    ↓
GOLD
    ├── Dimensões: dim_municipio, dim_tempo
    └── Fatos: fact_indicadores_municipio, fact_indicadores_uf, 
               fact_indicadores_brasil (com BACEN)
```

---
