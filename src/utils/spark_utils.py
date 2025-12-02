"""
Utilitários e funções auxiliares do Spark.
"""
from typing import Dict, Optional, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pathlib import Path
import yaml


def create_spark_session(
    app_name: str = "IBGE_DataLake_Pipeline",
    config: Optional[Dict] = None,
    enable_azure: bool = False,
    enable_jdbc: bool = False
) -> SparkSession:
    """
    Cria e configura uma sessão Spark.
    
    Args:
        app_name: Nome da aplicação Spark
        config: Configurações adicionais do Spark
        enable_azure: Se True, adiciona bibliotecas do Azure Storage
        enable_jdbc: Se True, adiciona drivers JDBC para SQL Server
        
    Returns:
        SparkSession configurada
    """
    builder = SparkSession.builder.appName(app_name)
    
    packages = []
    
    # Adicionar bibliotecas Azure se necessário
    if enable_azure:
        # Bibliotecas Hadoop do Azure Storage para suporte ADLS Gen2
        azure_packages = [
            "org.apache.hadoop:hadoop-azure:3.3.4",
            "com.microsoft.azure:azure-storage:8.6.6",
        ]
        packages.extend(azure_packages)
    
    # Adicionar driver JDBC do SQL Server se necessário
    if enable_jdbc:
        # Driver JDBC do Microsoft SQL Server (funciona para SQL Server, Azure SQL e Synapse)
        # Usar versão mais recente e estável
        jdbc_package = "com.microsoft.sqlserver:mssql-jdbc:12.4.2.jre8"
        packages.append(jdbc_package)
    
    # Configurar packages se houver algum
    if packages:
        existing_packages = config.get('spark.jars.packages', '') if config else ''
        if existing_packages:
            all_packages = f"{existing_packages},{','.join(packages)}"
        else:
            all_packages = ",".join(packages)
        builder = builder.config("spark.jars.packages", all_packages)
    
    # Configurações padrão
    default_config = {
        "spark.sql.shuffle.partitions": "200",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.parquet.compression.codec": "snappy",
        "spark.sql.sources.partitionOverwriteMode": "dynamic",
        "spark.driver.memory": "4g",
        "spark.executor.memory": "4g",
        # Configuração de rede para compatibilidade com Windows
        "spark.driver.host": "127.0.0.1",
        "spark.driver.bindAddress": "127.0.0.1",
    }
    
    # Mesclar com configuração customizada
    if config:
        default_config.update(config)
    
    # Definir configurações
    for key, value in default_config.items():
        builder = builder.config(key, value)
    
    spark = builder.getOrCreate()
    
    # Definir nível de log
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


def load_config(config_path: str = "config/config.yaml") -> Dict:
    """
    Carrega configuração de arquivo YAML.
    
    Args:
        config_path: Caminho para o arquivo de configuração
        
    Returns:
        Dicionário de configuração
    """
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def normalize_path(path: str) -> str:
    """
    Normaliza caminho para armazenamento em nuvem (sempre usar barras normais).
    
    Args:
        path: String de caminho (pode conter barras invertidas no Windows)
        
    Returns:
        Caminho normalizado com barras normais
        
    Note:
        Preserva formato URI de armazenamento em nuvem (ex: abfss://, s3://)
    """
    # Normalizar barras invertidas para barras normais
    normalized = path.replace('\\', '/')
    
    # Corrigir problemas comuns com URIs de armazenamento em nuvem
    # Garantir que abfss:// tenha duas barras (não abfss:/)
    if normalized.startswith('abfss:/') and not normalized.startswith('abfss://'):
        normalized = normalized.replace('abfss:/', 'abfss://', 1)
    if normalized.startswith('abfs:/') and not normalized.startswith('abfs://'):
        normalized = normalized.replace('abfs:/', 'abfs://', 1)
    if normalized.startswith('s3:/') and not normalized.startswith('s3://'):
        normalized = normalized.replace('s3:/', 's3://', 1)
    if normalized.startswith('s3a:/') and not normalized.startswith('s3a://'):
        normalized = normalized.replace('s3a:/', 's3a://', 1)
    if normalized.startswith('gs:/') and not normalized.startswith('gs://'):
        normalized = normalized.replace('gs:/', 'gs://', 1)
    if normalized.startswith('wasb:/') and not normalized.startswith('wasb://'):
        normalized = normalized.replace('wasb:/', 'wasb://', 1)
    if normalized.startswith('wasbs:/') and not normalized.startswith('wasbs://'):
        normalized = normalized.replace('wasbs:/', 'wasbs://', 1)
    if normalized.startswith('hdfs:/') and not normalized.startswith('hdfs://'):
        normalized = normalized.replace('hdfs:/', 'hdfs://', 1)
    
    return normalized


def is_cloud_storage_path(path: str) -> bool:
    """
    Verifica se o caminho é um caminho de armazenamento em nuvem.
    
    Args:
        path: String de caminho
        
    Returns:
        True se o caminho é armazenamento em nuvem, False caso contrário
    """
    normalized = normalize_path(path)
    cloud_prefixes = ('abfss://', 'abfs://', 's3://', 's3a://', 'gs://', 'wasb://', 'wasbs://', 'hdfs://')
    return any(normalized.startswith(prefix) for prefix in cloud_prefixes)


def read_parquet(
    spark: SparkSession,
    path: str,
    schema: Optional[str] = None
) -> DataFrame:
    """
    Lê arquivo(s) Parquet em DataFrame.
    
    Args:
        spark: SparkSession
        path: Caminho para arquivo(s) parquet
        schema: Schema opcional para forçar
        
    Returns:
        DataFrame
    """
    # Normalizar caminho para compatibilidade com armazenamento em nuvem
    normalized_path = normalize_path(path)
    
    reader = spark.read.format("parquet")
    
    if schema:
        reader = reader.schema(schema)
    
    return reader.load(normalized_path)


def write_parquet(
    df: DataFrame,
    path: str,
    mode: str = "overwrite",
    partition_by: Optional[List[str]] = None,
    compression: str = "snappy"
) -> None:
    """
    Escreve DataFrame em formato Parquet.
    
    Args:
        df: DataFrame para escrever
        path: Caminho de saída
        mode: Modo de escrita (overwrite, append, etc.)
        partition_by: Colunas para particionar
        compression: Codec de compressão
    """
    # Normalizar caminho para compatibilidade com armazenamento em nuvem
    normalized_path = normalize_path(path)
    
    writer = df.write.format("parquet").mode(mode)
    
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    
    writer.option("compression", compression).save(normalized_path)


def get_data_stats(df: DataFrame) -> Dict:
    """
    Calcula estatísticas básicas para um DataFrame.
    
    Args:
        df: DataFrame de entrada
        
    Returns:
        Dicionário com estatísticas
    """
    return {
        "row_count": df.count(),
        "column_count": len(df.columns),
        "columns": df.columns,
        "null_counts": {
            col: df.filter(F.col(col).isNull()).count()
            for col in df.columns
        }
    }


def validate_dataframe(
    df: DataFrame,
    required_columns: List[str]
) -> bool:
    """
    Valida que o DataFrame contém as colunas obrigatórias.
    
    Args:
        df: DataFrame para validar
        required_columns: Lista de nomes de colunas obrigatórias
        
    Returns:
        True se válido, levanta ValueError caso contrário
    """
    missing_columns = set(required_columns) - set(df.columns)
    
    if missing_columns:
        raise ValueError(f"Colunas obrigatórias ausentes: {missing_columns}")
    
    return True


def add_audit_columns(df: DataFrame) -> DataFrame:
    """
    Adiciona colunas de auditoria ao DataFrame (timestamp de processamento, etc).
    
    Args:
        df: DataFrame de entrada
        
    Returns:
        DataFrame com colunas de auditoria
    """
    return df.withColumn(
        "dt_processamento",
        F.current_timestamp()
    ).withColumn(
        "dt_carga",
        F.current_date()
    )


def remove_duplicates(
    df: DataFrame,
    key_columns: List[str],
    order_column: Optional[str] = None
) -> DataFrame:
    """
    Remove linhas duplicadas baseado em colunas-chave.
    
    Args:
        df: DataFrame de entrada
        key_columns: Colunas para usar na deduplicação
        order_column: Coluna para ordenar (mantém o mais recente)
        
    Returns:
        DataFrame deduplicado
    """
    if order_column:
        from pyspark.sql.window import Window
        
        window = Window.partitionBy(key_columns).orderBy(
            F.col(order_column).desc()
        )
        
        return df.withColumn("row_num", F.row_number().over(window)) \
                 .filter(F.col("row_num") == 1) \
                 .drop("row_num")
    else:
        return df.dropDuplicates(key_columns)


def create_directory_structure(base_path: str) -> None:
    """
    Cria estrutura de diretórios do data lake.
    
    Args:
        base_path: Caminho base para o data lake
    
    Note:
        Para caminhos Azure (abfss://), esta função não faz nada pois diretórios
        são criados automaticamente quando arquivos são escritos.
    """
    # Normalizar caminho para verificar armazenamento em nuvem (lidar com barras invertidas do Windows)
    normalized_path = base_path.replace('\\', '/')
    
    # Pular criação de diretório para caminhos de armazenamento em nuvem
    cloud_prefixes = ('abfss://', 'abfs://', 's3://', 's3a://', 'gs://', 'wasb://', 'wasbs://', 'hdfs://')
    if any(normalized_path.startswith(prefix) for prefix in cloud_prefixes):
        # Armazenamento em nuvem cria diretórios implicitamente ao escrever arquivos
        print(f"Pulando criação de diretório para armazenamento em nuvem: {base_path}")
        return
    
    # Criar diretórios apenas para sistemas de arquivos locais
    layers = ["bronze", "silver", "gold"]
    
    for layer in layers:
        path = Path(base_path) / layer
        path.mkdir(parents=True, exist_ok=True)
        
        # Criar .gitkeep para preservar estrutura de diretórios
        gitkeep = path / ".gitkeep"
        gitkeep.touch(exist_ok=True)

