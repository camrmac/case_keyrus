"""
Processador da camada Silver - Limpeza e padronização de dados.
"""
from typing import Dict
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, DoubleType

from src.utils.logger import PipelineLogger
from src.utils.spark_utils import (
    read_parquet, write_parquet, remove_duplicates, add_audit_columns,
    is_cloud_storage_path, normalize_path
)


class SilverProcessor:
    """
    Processador para a camada Silver - limpa e padroniza dados Bronze.
    
    Aplica regras de negócio, correções de qualidade de dados e padronização.
    """
    
    def __init__(
        self,
        spark: SparkSession,
        config: Dict,
        bronze_path: str,
        silver_path: str
    ):
        """
        Inicializa o processador Silver.
        
        Args:
            spark: SparkSession
            config: Dicionário de configuração
            bronze_path: Caminho para a camada Bronze
            silver_path: Caminho para a camada Silver
        """
        self.spark = spark
        self.config = config
        self.bronze_path = Path(bronze_path)
        self.silver_path = Path(silver_path)
        self.logger = PipelineLogger("transformation")
    
    def process_populacao(self) -> DataFrame:
        """
        Processa dados de população da camada Bronze para Silver.
        
        Returns:
            DataFrame de população limpo
        """
        self.logger.log_stage_start("process_populacao_silver")
        
        try:
            # Ler da camada Bronze
            df = read_parquet(
                self.spark,
                str(self.bronze_path / "populacao")
            )
            
            # Aplicar transformações
            df_clean = (
                df
                # Remover nulos em colunas críticas
                .filter(F.col("codigo_municipio").isNotNull())
                .filter(F.col("ano").isNotNull())
                
                # Converter para tipos apropriados
                .withColumn("codigo_municipio", F.col("codigo_municipio").cast(StringType()))
                .withColumn("ano", F.col("ano").cast(IntegerType()))
                .withColumn("populacao", F.col("valor").cast(IntegerType()))
                
                # Padronizar nomes de municípios
                .withColumn(
                    "nome_municipio",
                    F.trim(F.upper(F.col("nome_municipio")))
                )
                
                # Adicionar colunas calculadas
                .withColumn(
                    "codigo_uf",
                    F.substring(F.col("codigo_municipio"), 1, 2)
                )
                
                # Selecionar e renomear colunas
                .select(
                    F.col("codigo_municipio"),
                    F.col("nome_municipio"),
                    F.col("codigo_uf"),
                    F.col("ano"),
                    F.col("populacao"),
                    F.col("dt_processamento"),
                    F.col("dt_carga")
                )
                
                # Remover duplicatas
                .dropDuplicates(["codigo_municipio", "ano"])
                
                # Filtrar valores inválidos
                .filter(F.col("populacao") > 0)
                .filter(F.col("populacao") < 20000000)  # Verificação de sanidade
            )
            
            # Escrever na camada Silver
            output_path = self.silver_path / "populacao"
            write_parquet(
                df_clean,
                str(output_path),
                mode="overwrite",
                partition_by=["ano"]
            )
            
            stats = {
                "records_in": df.count(),
                "records_out": df_clean.count(),
                "output_path": str(output_path)
            }
            self.logger.log_stage_end("process_populacao_silver", stats)
            
            return df_clean
            
        except Exception as e:
            self.logger.log_error("process_populacao_silver", e)
            raise
    
    def process_pib(self) -> DataFrame:
        """
        Processa dados de PIB da camada Bronze para Silver.
        
        Returns:
            DataFrame de PIB limpo
        """
        self.logger.log_stage_start("process_pib_silver")
        
        try:
            # Ler da camada Bronze
            df = read_parquet(
                self.spark,
                str(self.bronze_path / "pib")
            )
            
            # Aplicar transformações
            df_clean = (
                df
                # Remover nulos em colunas críticas
                .filter(F.col("codigo_municipio").isNotNull())
                .filter(F.col("ano").isNotNull())
                .filter(F.col("valor").isNotNull())
                
                # Converter para tipos apropriados
                .withColumn("codigo_municipio", F.col("codigo_municipio").cast(StringType()))
                .withColumn("ano", F.col("ano").cast(IntegerType()))
                .withColumn("pib_per_capita", F.col("valor").cast(DoubleType()))
                
                # Padronizar nomes de municípios
                .withColumn(
                    "nome_municipio",
                    F.trim(F.upper(F.col("nome_municipio")))
                )
                
                # Adicionar colunas calculadas
                .withColumn(
                    "codigo_uf",
                    F.substring(F.col("codigo_municipio"), 1, 2)
                )
                
                # Selecionar e renomear colunas
                .select(
                    F.col("codigo_municipio"),
                    F.col("nome_municipio"),
                    F.col("codigo_uf"),
                    F.col("ano"),
                    F.col("pib_per_capita"),
                    F.col("dt_processamento"),
                    F.col("dt_carga")
                )
                
                # Remover duplicatas
                .dropDuplicates(["codigo_municipio", "ano"])
                
                # Filtrar valores inválidos
                .filter(F.col("pib_per_capita") > 0)
                .filter(F.col("pib_per_capita") < 1000000)  # Verificação de sanidade (R$ 1M)
            )
            
            # Escrever na camada Silver
            output_path = self.silver_path / "pib"
            write_parquet(
                df_clean,
                str(output_path),
                mode="overwrite",
                partition_by=["ano"]
            )
            
            stats = {
                "records_in": df.count(),
                "records_out": df_clean.count(),
                "output_path": str(output_path)
            }
            self.logger.log_stage_end("process_pib_silver", stats)
            
            return df_clean
            
        except Exception as e:
            self.logger.log_error("process_pib_silver", e)
            raise
    
    def process_localidades(self) -> DataFrame:
        """
        Processa dados de referência de localidades da camada Bronze para Silver.
        
        Returns:
            DataFrame de localidades limpo
        """
        self.logger.log_stage_start("process_localidades_silver")
        
        try:
            # Ler da camada Bronze
            df = read_parquet(
                self.spark,
                str(self.bronze_path / "localidades")
            )
            
            # Aplicar transformações
            df_clean = (
                df
                # Remover nulos
                .filter(F.col("codigo_municipio").isNotNull())
                
                # Converter tipos
                .withColumn("codigo_municipio", F.col("codigo_municipio").cast(StringType()))
                .withColumn("codigo_uf", F.col("codigo_uf").cast(StringType()))
                
                # Padronizar nomes
                .withColumn("nome_municipio", F.trim(F.upper(F.col("nome_municipio"))))
                .withColumn("nome_uf", F.trim(F.upper(F.col("nome_uf"))))
                .withColumn("sigla_uf", F.trim(F.upper(F.col("sigla_uf"))))
                .withColumn("nome_regiao", F.trim(F.upper(F.col("nome_regiao"))))
                
                # Selecionar colunas
                .select(
                    "codigo_municipio",
                    "nome_municipio",
                    "codigo_uf",
                    "sigla_uf",
                    "nome_uf",
                    "codigo_regiao",
                    "nome_regiao",
                    "sigla_regiao",
                    "dt_processamento",
                    "dt_carga"
                )
                
                # Remover duplicatas
                .dropDuplicates(["codigo_municipio"])
            )
            
            # Escrever na camada Silver
            output_path = self.silver_path / "localidades"
            write_parquet(
                df_clean,
                str(output_path),
                mode="overwrite"
            )
            
            stats = {
                "records_in": df.count(),
                "records_out": df_clean.count(),
                "output_path": str(output_path)
            }
            self.logger.log_stage_end("process_localidades_silver", stats)
            
            return df_clean
            
        except Exception as e:
            self.logger.log_error("process_localidades_silver", e)
            raise
    
    def process_bacen_indicadores(self) -> Dict[str, DataFrame]:
        """
        Processa indicadores econômicos do Banco Central da camada Bronze para Silver.
        
        Lê as séries configuradas em config['api']['banco_central']['series']
        
        Returns:
            Dicionário com DataFrames limpos do Banco Central
        """
        self.logger.log_stage_start("process_bacen_indicadores_silver")
        
        # Ler séries da configuração
        bacen_config = self.config.get('api', {}).get('banco_central', {})
        series_config = bacen_config.get('series', {})
        series = list(series_config.keys()) if series_config else []
        
        if not series:
            self.logger.logger.warning("Nenhuma série do Banco Central configurada")
            return {}
        
        self.logger.logger.info(f"Processando {len(series)} séries do Banco Central: {series}")
        
        dataframes = {}
        
        for serie in series:
            try:
                bronze_path = self.bronze_path / f"bacen_{serie}"
                bronze_path_str = normalize_path(str(bronze_path))
                
                # Verificar se existe (apenas para caminhos locais)
                # Para armazenamento em nuvem, tentamos ler e tratamos o erro se não existir
                if not is_cloud_storage_path(bronze_path_str):
                    if not bronze_path.exists():
                        self.logger.logger.warning(f"Caminho Bronze não encontrado: {bronze_path}, pulando {serie}")
                        continue
                
                # Ler da camada Bronze
                df = read_parquet(self.spark, bronze_path_str)
                
                if df.count() == 0:
                    self.logger.logger.warning(f"Sem dados para {serie}, pulando")
                    continue
                
                # Limpeza e padronização
                df_clean = (
                    df
                    # Converter data
                    .withColumn(
                        "data_parsed",
                        F.coalesce(
                            F.col("data_parsed"),
                            F.to_date(F.col("data"), "dd/MM/yyyy")
                        )
                    )
                    # Extrair ano e mês
                    .withColumn("ano", F.year(F.col("data_parsed")))
                    .withColumn("mes", F.month(F.col("data_parsed")))
                    # Converter valor
                    .withColumn(
                        "valor_decimal",
                        F.coalesce(
                            F.col("valor_decimal"),
                            F.col("valor").cast("decimal(18,4)")
                        )
                    )
                    # Remover nulos críticos
                    .filter(
                        F.col("data_parsed").isNotNull() &
                        F.col("valor_decimal").isNotNull()
                    )
                    # Remover duplicatas
                    .dropDuplicates(["serie_codigo", "data_parsed"])
                    # Selecionar colunas
                    .select(
                        "serie_codigo",
                        "serie_nome",
                        "data",
                        "data_parsed",
                        "ano",
                        "mes",
                        "valor",
                        "valor_decimal",
                        "fonte",
                        "dt_processamento",
                        "dt_carga"
                    )
                )
                
                # Salvar na camada Silver
                output_path = self.silver_path / f"bacen_{serie}_clean"
                output_path_str = normalize_path(str(output_path))
                write_parquet(
                    df_clean,
                    output_path_str,
                    mode="overwrite",
                    partition_by=["ano"]
                )
                
                stats = {
                    "records_in": df.count(),
                    "records_out": df_clean.count(),
                    "output_path": str(output_path)
                }
                self.logger.log_stage_end(f"process_bacen_{serie}_silver", stats)
                
                dataframes[serie] = df_clean
                
            except Exception as e:
                self.logger.log_error(f"process_bacen_{serie}_silver", e)
                # Continue com outras séries mesmo se uma falhar
                continue
        
        self.logger.log_stage_end("process_bacen_indicadores_silver", {
            "datasets": list(dataframes.keys()),
            "total_records": sum(df.count() for df in dataframes.values())
        })
        
        return dataframes
    
    def process_all(self) -> Dict[str, DataFrame]:
        """
        Processa todos os datasets da camada Bronze para Silver incluindo IBGE e Banco Central.
        
        Returns:
            Dicionário com todos os DataFrames processados
        """
        self.logger.log_stage_start("process_all_silver")
        
        dataframes = {}
        
        # Verificar se APIs do IBGE estão habilitadas
        ibge_enabled = self.config.get('api', {}).get('ibge', {}).get('enabled', True)
        
        try:
            # Processar dados de referência primeiro - apenas se IBGE estiver habilitado
            if ibge_enabled:
                try:
                    dataframes['localidades'] = self.process_localidades()
                except Exception as e:
                    self.logger.logger.warning(f"Falha ao processar localidades: {str(e)}")
            else:
                self.logger.logger.info("APIs do IBGE desabilitadas na configuração, pulando processamento de localidades")
            
            # Processar tabelas de fato do IBGE - apenas se IBGE estiver habilitado
            if ibge_enabled:
                try:
                    dataframes['populacao'] = self.process_populacao()
                except Exception as e:
                    self.logger.logger.warning(f"Falha ao processar população: {str(e)}")
            else:
                self.logger.logger.info("APIs do IBGE desabilitadas na configuração, pulando processamento de população")
            
            if ibge_enabled:
                try:
                    dataframes['pib'] = self.process_pib()
                except Exception as e:
                    self.logger.logger.warning(f"Falha ao processar PIB: {str(e)}")
            else:
                self.logger.logger.info("APIs do IBGE desabilitadas na configuração, pulando processamento de PIB")
            
            # Processar indicadores do Banco Central (sempre habilitado)
            try:
                bacen_dfs = self.process_bacen_indicadores()
                dataframes.update(bacen_dfs)
                self.logger.logger.info(f"✓ Indicadores do Banco Central processados: {list(bacen_dfs.keys())}")
            except Exception as e:
                self.logger.logger.warning(f"Falha ao processar indicadores do Banco Central: {str(e)}")
            
            self.logger.log_stage_end("process_all_silver", {
                "datasets": list(dataframes.keys()),
                "total_records": sum(df.count() for df in dataframes.values())
            })
            
            return dataframes
            
        except Exception as e:
            self.logger.log_error("process_all_silver", e)
            raise

