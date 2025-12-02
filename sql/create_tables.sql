-- Script SQL para criar tabelas de indicadores

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'indicadores')
BEGIN
    EXEC('CREATE SCHEMA indicadores')
END
GO

-- Tabela: dim_municipio
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_municipio' AND schema_id = SCHEMA_ID('indicadores'))
BEGIN
    CREATE TABLE indicadores.dim_municipio (
        sk_municipio BIGINT PRIMARY KEY,
        codigo_municipio VARCHAR(7) NOT NULL,
        nome_municipio VARCHAR(200) NOT NULL,
        codigo_uf VARCHAR(2),
        sigla_uf VARCHAR(2),
        nome_uf VARCHAR(100),
        codigo_regiao INT,
        nome_regiao VARCHAR(100),
        sigla_regiao VARCHAR(2),
        INDEX idx_codigo_municipio (codigo_municipio),
        INDEX idx_codigo_uf (codigo_uf)
    )
END
GO

-- Tabela: dim_tempo
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_tempo' AND schema_id = SCHEMA_ID('indicadores'))
BEGIN
    CREATE TABLE indicadores.dim_tempo (
        sk_tempo INT PRIMARY KEY,
        ano INT NOT NULL,
        semestre INT,
        trimestre INT,
        INDEX idx_ano (ano)
    )
END
GO

-- Tabela: fact_indicadores_municipio
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'fact_indicadores_municipio' AND schema_id = SCHEMA_ID('indicadores'))
BEGIN
    CREATE TABLE indicadores.fact_indicadores_municipio (
        sk_municipio BIGINT NOT NULL,
        sk_tempo INT NOT NULL,
        codigo_municipio VARCHAR(7) NOT NULL,
        ano INT NOT NULL,
        populacao_total BIGINT,
        pib_total DECIMAL(18,2),
        pib_per_capita DECIMAL(18,4),
        yoy_pib DECIMAL(10,4),
        yoy_populacao DECIMAL(10,4),
        share_pib_uf DECIMAL(10,4),
        share_pib_brasil DECIMAL(10,4),
        rank_pib_uf INT,
        rank_pib_brasil INT,
        PRIMARY KEY (sk_municipio, sk_tempo),
        FOREIGN KEY (sk_municipio) REFERENCES indicadores.dim_municipio(sk_municipio),
        FOREIGN KEY (sk_tempo) REFERENCES indicadores.dim_tempo(sk_tempo),
        INDEX idx_ano (ano),
        INDEX idx_codigo_municipio (codigo_municipio)
    )
END
GO

-- Tabela: fact_indicadores_uf
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'fact_indicadores_uf' AND schema_id = SCHEMA_ID('indicadores'))
BEGIN
    CREATE TABLE indicadores.fact_indicadores_uf (
        codigo_uf VARCHAR(2) NOT NULL,
        sigla_uf VARCHAR(2) NOT NULL,
        nome_uf VARCHAR(100) NOT NULL,
        ano INT NOT NULL,
        qtd_municipios INT,
        populacao_total BIGINT,
        pib_total DECIMAL(18,2),
        pib_per_capita_medio DECIMAL(18,4),
        yoy_pib DECIMAL(10,4),
        yoy_populacao DECIMAL(10,4),
        share_pib_brasil DECIMAL(10,4),
        rank_pib_brasil INT,
        PRIMARY KEY (codigo_uf, ano),
        INDEX idx_ano (ano),
        INDEX idx_sigla_uf (sigla_uf)
    )
END
GO

-- Tabela: fact_indicadores_brasil
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'fact_indicadores_brasil' AND schema_id = SCHEMA_ID('indicadores'))
BEGIN
    CREATE TABLE indicadores.fact_indicadores_brasil (
        ano INT PRIMARY KEY,
        qtd_ufs INT,
        populacao_total BIGINT,
        pib_total DECIMAL(18,2),
        pib_per_capita DECIMAL(18,4),
        yoy_pib DECIMAL(10,4),
        yoy_populacao DECIMAL(10,4),
        -- Indicadores Banco Central (se disponíveis)
        ipca_mensal_medio DECIMAL(10,4),
        ipca_12m_medio DECIMAL(10,4),
        pib_mensal_medio DECIMAL(18,2),
        desemprego_medio DECIMAL(10,4),
        igpm_medio DECIMAL(10,4),
        igpdi_medio DECIMAL(10,4),
        INDEX idx_ano (ano)
    )
END
GO
