# Pipeline na Azure

---

## ⚡ Quick Start (3 passos)

### 1️⃣ Configure Credenciais (2 minutos)

**Windows:**
```cmd
setup_azure.bat
```

**Linux/Mac:**
```bash
chmod +x setup_azure.sh
./setup_azure.sh
```

**Ou manualmente:**
1. Crie o arquivo `.env` com suas credenciais do Azure
3. Obtenha as credenciais no [Portal Azure](https://portal.azure.com) → Storage Account → Access keys

### 2️⃣ Teste Conexão (30 segundos)

```bash
python run_pipeline_azure.py --test-connection
```

✅ **Sucesso?** Vá para o passo 3!  
❌ **Erro?** Verifique as credenciais no `.env`

### 3️⃣ Execute Pipeline (3-5 minutos)

```bash
python run_pipeline_azure.py --stage full
```

**Pronto! 🎉** Seus dados estão no Azure!

---

## 📁 O Que Foi Criado?

```
Seu Azure Storage
│
└── datalake/                           ← Container
    ├── bronze/                         ← Dados brutos das APIs IBGE
    │   ├── populacao_2024/
    │   └── pib_2021/
    │
    ├── silver/                         ← Dados limpos e padronizados
    │   ├── populacao_clean/
    │   └── pib_clean/
    │
    └── gold/                           ← Modelo Star Schema
        ├── dim_municipio/              ← Dimensão: Municípios
        ├── dim_tempo/                  ← Dimensão: Tempo
        ├── fact_indicadores_municipio/ ← Fato: Indicadores detalhados
        ├── agg_indicadores_uf/         ← Agregado: Por Estado
        └── agg_indicadores_brasil/     ← Agregado: Nacional
```

---

## 💡 Casos de Uso

### Desenvolvimento Local + Storage Azure
```bash
# Desenvolva e teste localmente, mas salve direto no Azure
python run_pipeline_azure.py --stage full
```

### Atualização Diária Automatizada
```bash
# Configure um cron job ou Task Scheduler
python run_pipeline_azure.py --stage bronze  # Só atualiza dados novos
```

### Reprocessamento
```bash
# Reprocessar apenas transformações (Bronze já existe)
python run_pipeline_azure.py --stage silver
python run_pipeline_azure.py --stage gold
```

### Anos Específicos
```bash
python run_pipeline_azure.py --stage full --ano-populacao 2023 --ano-pib 2020
```

---

## 🆘 Problemas?

### ❌ Erro de Autenticação
**Solução:** Verifique o arquivo `.env`
```bash
# Deve conter (exemplo):
AZURE_STORAGE_ACCOUNT_NAME=keyruspipeline
AZURE_STORAGE_ACCOUNT_KEY=abc123...==
AZURE_CONTAINER_NAME=datalake
```

### ❌ Container não encontrado
**Solução:** Crie no Portal Azure
```
Storage Account → Containers → + Container → Nome: datalake
```

### ❌ Módulo não encontrado
**Solução:** Instale dependências
```bash
pip install -r requirements.txt
```

### ❌ Java heap space
**Solução:** Aumente memória no `config/config_azure.yaml`
```yaml
spark:
  config:
    spark.driver.memory: "8g"
```

---

**Tabelas criadas:**
- `dim_municipio` - Dimensão de municípios
- `dim_tempo` - Dimensão de tempo
- `fact_indicadores_municipio` - Indicadores municipais
- `fact_indicadores_uf` - Indicadores por UF
- `fact_indicadores_brasil` - Indicadores nacionais

---

## 🎓 Exemplo Passo a Passo

### Cenário: Você tem um Storage Account chamado `keyruspipeline`

**Passo 1: Obter credenciais**
1. Portal Azure → stibgedatalake → Access keys
2. Copiar **Nome**: `keyruspipeline`
3. Copiar **Key1**: `abc123def456...==`

**Passo 2: Criar .env**
```bash
notepad .env
```

**Passo 3: Preencher .env**
```env
AZURE_STORAGE_ACCOUNT_NAME=keyruspipeline
AZURE_STORAGE_ACCOUNT_KEY=abc123def456...==
AZURE_CONTAINER_NAME=datalake
```

**Passo 4: Testar**
```bash
python run_pipeline_azure.py --test-connection
```

**Passo 5: Criar estrutura**
```bash
python run_pipeline_azure.py --create-structure
```

**Passo 6: Rodar pipeline**
```bash
python run_pipeline_azure.py --stage full
```

**Passo 7: Verificar**
- Portal Azure → keyruspipeline → Containers → datalake
- Você verá: `bronze/`, `silver/`, `gold/`

### Comandos Essenciais:

```bash
# Testar conexão
python run_pipeline_azure.py --test-connection

# Criar estrutura (primeira vez)
python run_pipeline_azure.py --create-structure

# Executar pipeline completa
python run_pipeline_azure.py --stage full

# Ver logs
cat logs/pipeline_azure.log
```
