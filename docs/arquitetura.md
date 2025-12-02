# Arquitetura

## Visão Geral

Este documento detalha a arquitetura end-to-end da pipeline do processamento de dados de APIs.

## Camadas da Arquitetura

### 1. Camada de Origem 

**Fonte de Dados:**

**APIs REST do IBGE (Se Habilitado em .yaml):** 
- População Municipal (Agregado 6579)
- PIB Municipal (Agregado 5938)
- Localidades (Metadados geográficos)

**APIs REST do Banco Central do Brasil (BACEN):**
- IPCA Mensal (Série 433)
- IPCA Acumulado 12 Meses (Série 13522)
- PIB Mensal (Série 4380)
- Taxa de Desemprego (Série 24369)
- IGP-M (Série 189)
- IGP-DI (Série 190)

**Informações extras:**
- Protocolo: HTTPS
- Formato: JSON
- Autenticação: Não requerida (APIs públicas)

### 2. Camada Bronze

**Objetivo:** Armazenar dados brutos exatamente como recebidos da fonte

**Características Técnicas:**
- Formato: Parquet (compressão Snappy)
- Particionamento: `ano`, `fonte`
- Schema: Schema-on-read (flexível)
- Retenção: 90 dias

**Estrutura de Dados:**
```
bronze/
├── populacao/
│   └── ano=2024/
│       └── fonte=populacao/
│           └── *.parquet
├── pib/
│   └── ano=2021/
│       └── fonte=pib/
│           └── *.parquet
├── localidades/
│   └── *.parquet
└── bacen_*/
    ├── bacen_ipca_mensal/
    ├── bacen_ipca_12m/
    ├── bacen_pib_mensal/
    ├── bacen_desemprego/
    ├── bacen_igpm/
    └── bacen_igpdi/
        └── *.parquet
```

**Operações:**
- Ingestão via API Client (Python requests)
- Conversão JSON → Pandas → Spark DataFrame → Parquet
- Adição de metadados: timestamp de processamento, data de carga
- Modo: Overwrite

### 3. Camada Silver

**Objetivo:** Dados limpos, padronizados e validados

**Transformações Aplicadas:**
- Remoção de nulos em colunas críticas
- Padronização de tipos de dados
- Normalização de nomes (UPPER, TRIM)
- Deduplicação por chave de negócio
- Validação de ranges numéricos
- Enriquecimento com códigos geográficos
- Processamento de séries temporais
- Conversão de formatos de data
- Agregação de indicadores econômicos

**Características Técnicas:**
- Formato: Parquet (compressão Snappy)
- Particionamento: `ano`
- Schema: Schema-on-write (rígido)
- Retenção: 365 dias

**Qualidade de Dados:**
- Completeness: > 95%
- Validity: 100% em campos críticos
- Uniqueness: Sem duplicatas
- Consistency: Padrões estabelecidos

### 4. Camada Gold (DataMart)

**Objetivo:** Modelo dimensional otimizado para análises

**Modelo de Dados:**

#### Dimensões

**dim_municipio**
```sql
sk_municipio (PK)        BIGINT
codigo_municipio         STRING
nome_municipio           STRING
codigo_uf                STRING
sigla_uf                 STRING
nome_uf                  STRING
codigo_regiao            STRING
nome_regiao              STRING
sigla_regiao             STRING
```

**dim_tempo**
```sql
sk_tempo (PK)            BIGINT
ano                      INT
semestre                 INT
trimestre                INT
```

#### Fatos

**fact_indicadores_municipio**
```sql
sk_municipio (FK)        BIGINT
sk_tempo (FK)            BIGINT
codigo_municipio         STRING
ano                      INT
populacao_total          BIGINT
pib_total                DOUBLE
pib_per_capita           DOUBLE
yoy_pib                  DOUBLE
yoy_populacao            DOUBLE
share_pib_uf             DOUBLE
share_pib_brasil         DOUBLE
rank_pib_uf              INT
rank_pib_brasil          INT
```

**fact_indicadores_uf** (Agregado)
```sql
codigo_uf                STRING
sigla_uf                 STRING
nome_uf                  STRING
ano                      INT
qtd_municipios           BIGINT
populacao_total          BIGINT
pib_total                DOUBLE
pib_per_capita_medio     DOUBLE
yoy_pib                  DOUBLE
yoy_populacao            DOUBLE
share_pib_brasil         DOUBLE
rank_pib_brasil          INT
```

**fact_indicadores_brasil** (Agregado)
```sql
ano                      INT
qtd_ufs                  BIGINT
populacao_total          BIGINT
pib_total                DOUBLE
pib_per_capita           DOUBLE
yoy_pib                  DOUBLE
yoy_populacao            DOUBLE
ipca_mensal_medio        DOUBLE
ipca_12m_medio           DOUBLE
pib_mensal_medio         DOUBLE
desemprego_medio         DOUBLE
igpm_medio               DOUBLE
igpdi_medio              DOUBLE
```

**Granularidade:** 
- Município: 1 registro por município-ano
- UF: 1 registro por UF-ano
- Brasil: 1 registro por ano

**Métricas Calculadas:**
- YoY (Year over Year): Variação percentual ano sobre ano
- Share: Participação percentual (UF e Brasil)
- Rank: Posição no ranking (UF e Brasil)
- PIB Total: Calculado como PIB per capita × População

## 🔄 Pipeline ETL

### Orquestração

```python
DataPipeline
├── BronzeLoader
│   ├── IBGEAPIClient
│   │   ├── get_populacao_municipios()
│   │   ├── get_pib_municipios()
│   │   └── get_localidades_info()
│   ├── BACENAPIClient
│   │   ├── get_bacen_series()
│   │   └── parse_bacen_response()
│   └── write_parquet()
├── SilverProcessor
│   ├── process_populacao()
│   ├── process_pib()
│   ├── process_localidades()
│   └── process_bacen_indicadores()
├── GoldBuilder
│   ├── build_dim_municipio()
│   ├── build_dim_tempo()
│   ├── build_fact_indicadores_municipio()
│   ├── build_fact_indicadores_uf()
│   ├── build_fact_indicadores_brasil()
│   └── build_fact_indicadores_brasil_bacen_only() 
├── QualityChecker
│   ├── run_bronze_checks()
│   ├── run_silver_checks()
│   └── run_gold_checks()
└── SQLExporter (Delivery Layer)
    ├── export_table()
    ├── export_all_indicators()
    └── create_tables_if_not_exists()
```

### Fluxo de Execução

```
1. BRONZE
   ├─ Ingestão API População (IBGE)
   ├─ Ingestão API PIB (IBGE)
   ├─ Ingestão Localidades (IBGE)
   ├─ Ingestão Séries BACEN (IPCA, PIB, Desemprego, IGP-M, IGP-DI)
   ├─ Write Parquet (particionado)
   └─ Quality Checks Bronze
   
2. SILVER
   ├─ Read Bronze Parquet
   ├─ Limpeza e Padronização (IBGE)
   ├─ Processamento Séries Temporais (BACEN)
   ├─ Deduplicação
   ├─ Validações
   ├─ Write Parquet (particionado)
   └─ Quality Checks Silver
   
3. GOLD
   ├─ Read Silver Parquet
   ├─ Build Dimensões
   ├─ Build Fato Município
   │  ├─ Join Dimensões
   │  ├─ Calcular YoY
   │  ├─ Calcular Shares
   │  └─ Calcular Rankings
   ├─ Agregar Fato UF
   ├─ Agregar Fato Brasil (com indicadores BACEN)
   ├─ Write Parquet
   └─ Quality Checks Gold
   
4. DELIVERY
   ├─ Exportar para Tabelas SQL
   │  ├─ dim_municipio
   │  ├─ dim_tempo
   │  ├─ fact_indicadores_municipio
   │  ├─ fact_indicadores_uf
   │  └─ fact_indicadores_brasil
   └─ Validação de Exportação
```

## ☁️ Arquitetura Azure

### Componentes Azure

#### 1. Azure Data Factory (ADF)
**Função:** Orquestração e agendamento

**Pipelines:**
- Pipeline Diário: Ingestão incremental
- Pipeline Mensal: Carga completa
- Pipeline de Qualidade: Validações pós-carga

#### 2. Azure Databricks
**Função:** Processamento PySpark

**Clusters:**
- **Bronze Cluster:** 
- **Silver/Gold Cluster:**

#### 3. Azure Data Lake Storage Gen2 (ADLS)
**Função:** Armazenamento Data Lake

**Containers:**
```
datalake/
├── bronze/
├── silver/
└── gold/
```

#### 4. Azure Synapse Analytics
**Função:** Query analytics e serveless SQL

#### 5. Camada Delivery (SQL Export)
**Função:** Exportação de indicadores para tabelas SQL

**Bancos de Dados Suportados:**
- SQL Server (local/Azure)
- PostgreSQL
- Azure SQL Database
- Azure Synapse Analytics (Dedicated Pool)
- MySQL

**Funcionalidades:**
- Exportação automática após Gold Layer
- Exportação manual via script
- Criação automática de tabelas
- Suporte a múltiplos schemas
- Modos: overwrite, append, ignore, error

**Tabelas Exportadas:**
- `dim_municipio`
- `dim_tempo`
- `fact_indicadores_municipio`
- `fact_indicadores_uf`
- `fact_indicadores_brasil`

**Integração:**
- Power BI 
- Análises SQL ad-hoc
- Relatórios em tempo real

#### 6. Azure Key Vault
**Função:** Gerenciamento de segredos

**Secrets:**
- Storage Account Keys
- Service Principal credentials
- Database credentials (SQL export)
- API tokens (se aplicável)

#### 7. Azure Monitor
**Função:** Monitoramento e alertas

**Métricas:**
- Pipeline execution duration
- Data quality scores
- Error rates
- Resource utilization

**Alertas:**
- Pipeline failure
- Quality checks failed
- High error rate
- Resource threshold

### Segurança

**Authentication & Authorization:**
- Service Principal com RBAC
- Managed Identity para Databricks
- Azure AD Integration

**Network Security:**
- VNet Integration
- Private Endpoints para Storage
- NSG Rules

**Data Protection:**
- Encryption at rest (Azure Storage)
- Encryption in transit (TLS 1.2+)
- Column-level security (Synapse)

## 🔧 Tecnologias Utilizadas

### Bibliotecas Utilizadas
- **Python 3.9+**: Linguagem principal
- **PySpark 3.5**: Processamento distribuído
- **Parquet**: Formato columnar
- **Pandas**: Manipulação de dados

### Cloud & Infrastructure
- **Azure Data Factory**: Orquestração
- **Azure Databricks**: Processamento
- **ADLS Gen2**: Storage
- **Azure Synapse**: Analytics

### Data Quality & Monitoring
- **Great Expectations**: Validações
- **Python Logging**: Logs estruturados
- **Azure Monitor**: Monitoramento

### BI & Visualization
- **Power BI**: Dashboards

### Otimizações

1. **Particionamento inteligente:** Por ano e fonte
2. **Compressão eficiente:** Snappy para Parquet
3. **Broadcast joins:** Para tabelas dimensão pequenas
4. **Adaptive Query Execution:** Habilitado no Spark
5. **Cache de dados:** Para análises iterativas

### KPIs de Monitoramento
- Pipeline success rate
- Data freshness (lag time)
- Quality check pass rate
- Query performance (P95)
- Cost per GB processed

---

## 📦 Camada Delivery (SQL Export)

### Objetivo

Exportar indicadores da camada Gold para tabelas SQL, permitindo:
- Consultas SQL diretas
- Integração com ferramentas de BI (Power BI, Tableau)
- Análises ad-hoc
- Relatórios em tempo real

### Arquitetura

```
Gold Layer (Parquet)
        ↓
   SQLExporter
        ↓
   JDBC Connection
        ↓
   SQL Database
   ├── dim_municipio
   ├── dim_tempo
   ├── fact_indicadores_municipio
   ├── fact_indicadores_uf
   └── fact_indicadores_brasil
```

### Configuração

**Modos de Exportação:**
- `overwrite`: Substitui dados existentes
- `append`: Adiciona novos dados
- `ignore`: Ignora se tabela existe
- `error`: Falha se tabela existe

### Integração no Pipeline

A exportação SQL é executada automaticamente após a construção da camada Gold:

```python
# No pipeline completo
pipeline.run_full_pipeline()
# → Bronze → Silver → Gold → SQL Export (se habilitado)
```

### Uso Manual

```bash
# Exportar todas as tabelas
python scripts/export_to_sql.py

# Exportar tabela específica
python scripts/export_to_sql.py --table fact_indicadores_municipio

# Criar tabelas primeiro
python scripts/export_to_sql.py --create-tables
```

---

