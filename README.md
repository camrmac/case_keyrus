# Case Engenharia de Dados - IBGE Population & GDP

[![Python](https://img.shields.io/badge/Python-3.9%2B-blue)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/PySpark-3.5-orange)](https://spark.apache.org/)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)

## 📋 Visão Geral

Solução completa de Engenharia de Dados que consome dados de **População (2024)** e **PIB Municipal (2021)** do IBGE, **6 indicadores econômicos do Banco Central** (IPCA, PIB, Desemprego, IGP-M, IGP-DI), realiza ETL com PySpark, implementa arquitetura Medallion (Bronze/Silver/Gold) e expõe indicadores via Power BI.

### 🎯 Objetivo

Implementar um data lake com ingestão via API (IBGE + Banco Central), modelagem dimensional e entrega de indicadores analíticos nos níveis: Município, UF e Brasil, enriquecidos com dados macroeconômicos nacionais.

### ✨ O Que Este Projeto Faz (Em 1 Minuto)

```
┌─────────────────┐
│  APIs Governo   │
│  IBGE + BACEN   │  ───┐
└─────────────────┘     │
                        ▼
            ┌───────────────────┐
            │  PIPELINE ETL     │
            │  BRONZE → SILVER  │
            │  → GOLD           │
            └───────────────────┘
                        ▼
            ┌───────────────────┐
            │  Azure Storage    │
            │  ou Local         │
            │  Parquet Files    │
            └───────────────────┘
                        ▼
            ┌───────────────────┐
            │  Power BI          │
            │  Dashboards        │
            └───────────────────┘
```

**Fluxo Simplificado:**
- **ENTRADA:** IBGE (População + PIB anual) + Banco Central (6 indicadores mensais)
- **PROCESSAMENTO:** Bronze (raw) → Silver (limpo) → Gold (Star Schema)
- **SAÍDA:** Dashboards Power BI + Tabelas SQL (opcional)

## 🏗️ Arquitetura

### Arquitetura Azure (Produção)

```
┌─────────────────────────────────────────────────────────────────┐
│                      AZURE CLOUD ARCHITECTURE                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────┐        ┌─────────────────────────────────┐  │
│  │  IBGE APIs   │───────▶│   Azure Data Factory (ADF)      │  │
│  │ - POP 6579   │        │   - Orchestration               │  │
│  │ - PIB 5938   │        │   - Scheduling                  │  │
│  └──────────────┘        │   - Monitoring                  │  │
│                          └────────────┬────────────────────┘  │
│  ┌──────────────┐        ┌────────────▼────────────────────┐  │
│  │ Banco Central│───────▶│   Azure Data Factory (ADF)      │  │
│  │ - IPCA       │        │   - Orchestration               │  │
│  │ - PIB Mensal │        │   - Scheduling                  │  │
│  │ - Desemprego │        │   - Monitoring                  │  │
│  │ - IGP-M/DI   │        │                                  │  │
│  └──────────────┘        └────────────┬────────────────────┘  │
│                          └────────────┬────────────────────┘  │
│                                       │                        │
│                          ┌────────────▼────────────────────┐  │
│                          │   Azure Databricks              │  │
│                          │   - PySpark Processing          │  │
│                          │   - Delta Lake Format           │  │
│                          └────────────┬────────────────────┘  │
│                                       │                        │
│         ┌─────────────────────────────┼────────────────────┐  │
│         │     Azure Data Lake Gen2 (ADLS)                   │  │
│         ├────────────────┬────────────┬────────────────────┤  │
│         │  BRONZE Layer  │ SILVER Layer│   GOLD Layer       │  │
│         │  (Raw Data)    │ (Cleaned)   │  (Analytics)       │  │
│         │  - Parquet     │ - Parquet   │  - Parquet         │  │
│         └────────────────┴────────────┴────────────────────┘  │
│                                       │                        │
│                          ┌────────────▼────────────────────┐  │
│                          │   Azure Synapse Analytics       │  │
│                          │   - SQL Serverless Pool         │  │
│                          │   - External Tables             │  │
│                          └────────────┬────────────────────┘  │
│                                       │                        │
│                          ┌────────────▼────────────────────┐  │
│                          │      Power BI Service           │  │
│                          │   - Dashboards                  │  │
│                          │   - Reports                     │  │
│                          └─────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

### Arquitetura Local (Desenvolvimento)

```
┌──────────────┐      ┌───────────────────────────────────────┐
│  IBGE APIs   │─────▶│   Python ETL Pipeline                 │
│ - POP 6579   │      │   - API Ingestion (requests)          │
│ - PIB 5938   │      │   - PySpark Transformations           │
└──────────────┘      │   - Data Quality Checks               │
                      └──────────────┬────────────────────────┘
                                     │
              ┌──────────────────────┼──────────────────────┐
              │         Local Data Lake (Parquet)            │
              ├──────────────┬───────────────┬───────────────┤
              │ bronze/      │  silver/      │   gold/       │
              │ - raw_pop    │  - clean_pop  │ - dim_*       │
              │ - raw_pib    │  - clean_pib  │ - fact_*      │
              └──────────────┴───────────────┴───────────────┘
                                     │
              ┌──────────────────────▼────────────────────────┐
              │          Power BI Desktop                      │
              │   - Import Parquet (Gold Layer)                │
              │   - DAX Measures                               │
              └────────────────────────────────────────────────┘
```

### Medallion Architecture

1. **🥉 Bronze (Raw)**: Dados brutos da API em formato original
2. **🥈 Silver (Cleaned)**: Dados limpos, padronizados e enriquecidos
3. **🥇 Gold (Analytics)**: Modelo dimensional otimizado para analytics

## 📊 Modelagem de Dados

### Modelo Dimensional (Star Schema)

```
                    ┌─────────────────────┐
                    │   dim_municipio     │
                    ├─────────────────────┤
                    │ sk_municipio (PK)   │
                    │ codigo_municipio    │
                    │ nome_municipio      │
                    │ codigo_uf           │
                    │ nome_uf             │
                    │ sigla_uf            │
                    │ regiao              │
                    └──────────┬──────────┘
                               │
                    ┌──────────┴──────────┐
                    │                     │
      ┌─────────────▼────────┐   ┌───────▼──────────────┐
      │   fact_indicadores   │   │   dim_tempo          │
      ├──────────────────────┤   ├──────────────────────┤
      │ sk_municipio (FK)    │   │ sk_tempo (PK)        │
      │ sk_tempo (FK)        │◀──│ ano                  │
      │ populacao            │   │ trimestre            │
      │ pib                  │   │ semestre             │
      │ pib_per_capita       │   └──────────────────────┘
      │ pib_yoy              │
      │ pop_yoy              │
      │ share_uf_pib         │
      │ share_brasil_pib     │
      │ rank_uf_pib          │
      │ rank_brasil_pib      │
      └──────────────────────┘
```

**Justificativa da Modelagem:**

- ✅ **Star Schema**: Performance otimizada para queries analíticas
- ✅ **Surrogate Keys**: Independência de sistemas de origem
- ✅ **Slowly Changing Dimensions**: Histórico de mudanças em municípios
- ✅ **Grain**: Município-Ano (permite agregações para UF e Brasil)
- ✅ **Métricas Calculadas**: YoY, Shares e Rankings pré-calculados

## 🚀 Começando do Zero

> **⚡ Início Rápido:** Veja [`COMECE_AQUI.md`](../COMECE_AQUI.md) para começar em 3 passos!

---

## 🚀 Instalação e Configuração

### Pré-requisitos

```bash
- Python 3.9+
- Java 8 ou 11 (para PySpark)
- Power BI Desktop (para visualizações - opcional)
```

### Instalação

1. **Clone o repositório**:
```bash
git clone https://github.com/seu-usuario/case-engenharia-dados-ibge.git
cd case-engenharia-dados-ibge
```

2. **Crie e ative ambiente virtual**:
```bash
python -m venv venv
# Windows
venv\Scripts\activate
# Linux/Mac
source venv/bin/activate
```

3. **Instale dependências**:
```bash
pip install -r requirements.txt
```

4. **Configure variáveis de ambiente**:
```bash
cp .env.example .env
# Edite .env conforme necessário
```

## 📁 Estrutura do Projeto

```
case-engenharia-dados-ibge/
├── config/                      # Configurações
│   ├── config.yaml             # Config principal
│   └── logging_config.yaml     # Config de logs
├── data/                        # Data Lake Local
│   ├── bronze/                 # Raw data
│   ├── silver/                 # Cleaned data
│   └── gold/                   # Analytics data
├── docs/                        # Documentação
│   ├── architecture.md         # Arquitetura detalhada
│   ├── data_dictionary.md      # Dicionário de dados
│   └── azure_deployment.md     # Deploy Azure
├── notebooks/                   # Jupyter Notebooks
│   ├── 01_exploratory_analysis.ipynb
│   └── 02_data_quality.ipynb
├── powerbi/                     # Power BI assets
│   ├── dashboard.pbix          # Dashboard
│   ├── dax_measures.txt        # Medidas DAX
│   └── queries.txt             # M queries
├── src/                         # Código fonte
│   ├── ingestion/              # Módulo de ingestão
│   │   ├── __init__.py
│   │   ├── api_client.py       # Cliente APIs IBGE
│   │   └── bronze_loader.py    # Carga Bronze
│   ├── transformation/          # Transformações
│   │   ├── __init__.py
│   │   ├── silver_processor.py # Limpeza Silver
│   │   └── gold_builder.py     # Modelagem Gold
│   ├── quality/                 # Qualidade de dados
│   │   ├── __init__.py
│   │   ├── validators.py       # Validações
│   │   └── quality_checks.py   # Checks
│   ├── utils/                   # Utilitários
│   │   ├── __init__.py
│   │   ├── spark_utils.py      # Helpers Spark
│   │   └── logger.py           # Logger
│   └── pipeline.py              # Pipeline principal
├── sql/                         # Scripts SQL
│   └── synapse_external_tables.sql
├── tests/                       # Testes
│   ├── test_ingestion.py
│   ├── test_transformation.py
│   └── test_quality.py
├── .env.example                 # Exemplo env vars
├── .gitignore                   # Git ignore
├── requirements.txt             # Dependências Python
├── setup.py                     # Setup do pacote
└── run_pipeline.py             # Script principal
```

## ▶️ Execução

### Pipeline Completo

```bash
# Executar pipeline completo (Bronze → Silver → Gold)
python run_pipeline.py --full

# Executar apenas ingestão (Bronze)
python run_pipeline.py --stage bronze

# Executar transformações (Silver)
python run_pipeline.py --stage silver

# Executar modelagem (Gold)
python run_pipeline.py --stage gold
```

### Notebooks Exploratórios

```bash
jupyter notebook
# Abra: notebooks/01_exploratory_analysis.ipynb
```

## 📊 Indicadores Entregues

### 1. Município-Ano (IBGE)
- População total
- PIB total
- PIB per capita
- Variação YoY (ano sobre ano)
- Share PIB na UF (%)
- Share PIB no Brasil (%)
- Ranking PIB na UF
- Ranking PIB no Brasil

### 2. UF-Ano (IBGE)
- População agregada
- PIB agregado
- PIB per capita médio
- Variação YoY
- Share PIB no Brasil
- Ranking PIB nacional

### 3. Brasil-Ano (IBGE + Banco Central)
- População total (IBGE)
- PIB total (IBGE)
- PIB per capita nacional (IBGE)
- Variação YoY (IBGE)
- **IPCA Mensal (Banco Central)** ⭐ NOVO
- **IPCA 12 Meses (Banco Central)** ⭐ NOVO
- **PIB Mensal (Banco Central)** ⭐ NOVO
- **Taxa de Desemprego (Banco Central)** ⭐ NOVO
- **IGP-M (Banco Central)** ⭐ NOVO
- **IGP-DI (Banco Central)** ⭐ NOVO

## 🧪 Testes e Qualidade

### Executar Testes

```bash
# Todos os testes
pytest tests/

# Testes específicos
pytest tests/test_ingestion.py -v

# Com coverage
pytest --cov=src tests/
```

### Data Quality Checks

- ✅ Validação de schemas
- ✅ Verificação de nulos críticos
- ✅ Validação de ranges numéricos
- ✅ Consistência temporal
- ✅ Integridade referencial
- ✅ Duplicatas

## 📈 Dashboard Power BI

### Principais Visualizações

1. **Overview Nacional**
   - Cards: População, PIB, PIB per capita
   - Linha temporal: Evolução histórica
   - Mapa: Distribuição geográfica

2. **Análise por UF**
   - Ranking top 10 UFs
   - Treemap: Share PIB por UF
   - Tabela detalhada

3. **Análise Municipal**
   - Top 20 municípios
   - Filtros: UF, Região, Ano
   - Drill-down capacidade

### Conexão Power BI

```m
// M Query - Conectar ao Gold Layer
let
    Source = Folder.Files("C:\path\to\data\gold"),
    FilterParquet = Table.SelectRows(Source, 
        each Text.EndsWith([Name], ".parquet")),
    CombineFiles = Table.Combine(
        FilterParquet[Content]
    )
in
    CombineFiles
```

## 🔄 CI/CD e Deploy Azure

### Azure Deployment

Veja documentação completa: [docs/azure_deployment.md](docs/azure_deployment.md)

**Resumo:**
1. Criar recursos Azure (ADF, Databricks, ADLS, Synapse)
2. Configurar Service Principal
3. Deploy via Azure DevOps ou GitHub Actions
4. Agendar pipeline ADF

## 📚 Documentação Adicional

### Documentação Técnica
- [Arquitetura Detalhada](docs/architecture.md) - Arquitetura completa da solução
- [Modelagem de Dados](docs/MODELAGEM_DADOS.md) - Definição e justificativa do modelo Star Schema
- [Dicionário de Dados](docs/data_dictionary.md) - Estrutura completa de todas as tabelas
- [Explicação Completa do Projeto](docs/EXPLICACAO_COMPLETA_PROJETO.md) - Documentação detalhada

### Guias de Deploy
- [Deploy Azure](docs/azure_deployment.md) - Deploy completo no Azure
- [Início Rápido Azure](docs/START_HERE_AZURE.md) - Guia rápido para Azure
- [Comandos Azure](docs/COMANDOS_AZURE.md) - Referência de comandos

### Funcionalidades
- [Exportação SQL](docs/COMO_SALVAR_EM_TABELAS_SQL.md) - Como salvar indicadores em tabelas SQL
- [Troubleshooting Azure](docs/TROUBLESHOOTING_AZURE.md) - Solução de problemas

## 🛠️ Tecnologias Utilizadas

- **Python 3.9+**: Linguagem principal
- **PySpark 3.5**: Processamento distribuído
- **Pandas**: Manipulação de dados
- **Requests**: Consumo APIs
- **PyArrow**: Formato Parquet
- **Pytest**: Testes automatizados
- **Power BI**: Visualização de dados
- **Azure**: Cloud platform (produção)

## 📝 Boas Práticas Implementadas

- ✅ Arquitetura Medallion (Bronze/Silver/Gold)
- ✅ Modelo Dimensional (Star Schema)
- ✅ Código modular e reutilizável
- ✅ Logging estruturado
- ✅ Tratamento robusto de erros
- ✅ Validações de qualidade de dados
- ✅ Testes automatizados
- ✅ Documentação completa
- ✅ Versionamento de dados (Parquet particionado)
- ✅ Configuração centralizada
- ✅ Type hints
- ✅ Docstrings
- ✅ PEP 8 compliance

## 👥 Autor

**Seu Nome**
- GitHub: [@seu-usuario](https://github.com/seu-usuario)
- LinkedIn: [seu-perfil](https://linkedin.com/in/seu-perfil)

## 📄 Licença

Este projeto está sob a licença MIT - veja [LICENSE](LICENSE) para detalhes.

## 🙏 Agradecimentos

- IBGE pela disponibilização das APIs públicas
- Comunidade PySpark
- Documentação oficial Azure

---

**Desenvolvido com ❤️ para o Case de Engenharia de Dados**

# case_keyrus
