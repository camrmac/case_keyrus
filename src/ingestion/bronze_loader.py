"""
Ingestão de dados brutos.
"""
from typing import Dict, List, Optional
from pathlib import Path
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from src.ingestion.api_client import IBGEAPIClient
from src.utils.logger import PipelineLogger
from src.utils.spark_utils import write_parquet, add_audit_columns


class BronzeLoader:
    """
    Carrega para a camada Bronze (dados brutos das APIs).
    
    Gerencia a ingestão das APIs do IBGE e armazenamento em formato Parquet.
    """
    
    def __init__(
        self,
        spark: SparkSession,
        config: Dict,
        output_path: str
    ):
        """
        Inicializa o carregador Bronze.
        
        Args:
            spark: SparkSession
            config: Dicionário de configuração
            output_path: Caminho base para a camada Bronze
        """
        self.spark = spark
        self.config = config
        self.output_path = Path(output_path)
        self.logger = PipelineLogger("ingestion")
        
        # Inicializar cliente de API
        api_config = config.get('api', {})
        # Suporta tanto a estrutura antiga (api.base_url) quanto a nova (api.ibge.base_url)
        ibge_config = api_config.get('ibge', {})
        base_url = ibge_config.get('base_url') or api_config.get('base_url')
        
        self.api_client = IBGEAPIClient(
            base_url=base_url or "https://servicodados.ibge.gov.br/api/v3",
            timeout=api_config.get('timeout', 30),
            retry_attempts=api_config.get('retry', {}).get('attempts', 3)
        )
    
    def ingest_populacao(self, ano: Optional[str] = "2024") -> DataFrame:
        """
        Ingere dados de população da API do IBGE.
        
        Args:
            ano: Ano para ingerir (padrão: 2024)
            
        Returns:
            DataFrame com dados de população
        """
        self.logger.log_stage_start("ingest_populacao", {"ano": ano})
        
        try:
            # Obter dados da API
            data = self.api_client.get_populacao_municipios(ano)
            
            self.logger.log_data_stats(
                "populacao_api",
                {"records_fetched": len(data)}
            )
            
            # Converter para DataFrame
            pandas_df = pd.DataFrame(data)
            df = self.spark.createDataFrame(pandas_df)
            
            # Adicionar colunas de auditoria
            df = add_audit_columns(df)
            
            # Escrever na camada Bronze
            output_file = self.output_path / "populacao"
            write_parquet(
                df,
                str(output_file),
                mode="overwrite",
                partition_by=["ano", "fonte"]
            )
            
            stats = {
                "records": df.count(),
                "output_path": str(output_file)
            }
            self.logger.log_stage_end("ingest_populacao", stats)
            
            return df
            
        except Exception as e:
            self.logger.log_error("ingest_populacao", e, {"ano": ano})
            raise
    
    def ingest_pib(self, ano: Optional[str] = "2021") -> DataFrame:
        """
        Ingere dados de PIB da API do IBGE.
        
        Args:
            ano: Ano para ingerir (padrão: 2021)
            
        Returns:
            DataFrame com dados de PIB
        """
        self.logger.log_stage_start("ingest_pib", {"ano": ano})
        
        try:
            # Obter dados da API
            data = self.api_client.get_pib_municipios(ano)
            
            self.logger.log_data_stats(
                "pib_api",
                {"records_fetched": len(data)}
            )
            
            # Converter para DataFrame
            pandas_df = pd.DataFrame(data)
            df = self.spark.createDataFrame(pandas_df)
            
            # Adicionar colunas de auditoria
            df = add_audit_columns(df)
            
            # Escrever na camada Bronze
            output_file = self.output_path / "pib"
            write_parquet(
                df,
                str(output_file),
                mode="overwrite",
                partition_by=["ano", "fonte"]
            )
            
            stats = {
                "records": df.count(),
                "output_path": str(output_file)
            }
            self.logger.log_stage_end("ingest_pib", stats)
            
            return df
            
        except Exception as e:
            self.logger.log_error("ingest_pib", e, {"ano": ano})
            raise
    
    def ingest_localidades(self) -> DataFrame:
        """
        Ingere dados de referência de localidades (municípios, estados).
        
        Returns:
            DataFrame com informações de localidades
        """
        self.logger.log_stage_start("ingest_localidades")
        
        try:
            # Obter municípios (nível 6)
            municipios = self.api_client.get_localidades_info(nivel="6")
            
            # Extrair campos relevantes e padronizar
            processed_data = []
            for mun in municipios:
                record = {
                    'codigo_municipio': mun.get('id'),
                    'nome_municipio': mun.get('nome'),
                    'codigo_uf': mun.get('microrregiao', {}).get('mesorregiao', {}).get('UF', {}).get('id'),
                    'sigla_uf': mun.get('microrregiao', {}).get('mesorregiao', {}).get('UF', {}).get('sigla'),
                    'nome_uf': mun.get('microrregiao', {}).get('mesorregiao', {}).get('UF', {}).get('nome'),
                    'codigo_regiao': mun.get('microrregiao', {}).get('mesorregiao', {}).get('UF', {}).get('regiao', {}).get('id'),
                    'nome_regiao': mun.get('microrregiao', {}).get('mesorregiao', {}).get('UF', {}).get('regiao', {}).get('nome'),
                    'sigla_regiao': mun.get('microrregiao', {}).get('mesorregiao', {}).get('UF', {}).get('regiao', {}).get('sigla')
                }
                processed_data.append(record)
            
            # Converter para DataFrame
            pandas_df = pd.DataFrame(processed_data)
            df = self.spark.createDataFrame(pandas_df)
            
            # Adicionar colunas de auditoria
            df = add_audit_columns(df)
            
            # Escrever na camada Bronze
            output_file = self.output_path / "localidades"
            write_parquet(
                df,
                str(output_file),
                mode="overwrite"
            )
            
            stats = {
                "records": df.count(),
                "output_path": str(output_file)
            }
            self.logger.log_stage_end("ingest_localidades", stats)
            
            return df
            
        except Exception as e:
            self.logger.log_error("ingest_localidades", e)
            raise
    
    def ingest_bacen_indicadores(self) -> Dict[str, DataFrame]:
        """
        Ingere indicadores econômicos do Banco Central do Brasil.
        
        Lê as séries configuradas em config['api']['banco_central']['series']
        
        Returns:
            Dicionário com DataFrames de cada indicador
        """
        self.logger.log_stage_start("ingest_bacen_indicadores")
        
        # Ler séries da configuração
        bacen_config = self.config.get('api', {}).get('banco_central', {})
        series_config = bacen_config.get('series', {})
        
        if not series_config:
            self.logger.logger.warning("Nenhuma série do Banco Central configurada")
            return {}
        
        # Criar dicionário nome -> codigo
        series = {
            nome: serie_info.get('codigo')
            for nome, serie_info in series_config.items()
            if 'codigo' in serie_info
        }
        
        if not series:
            self.logger.logger.warning("Nenhuma série válida encontrada na configuração")
            return {}
        
        self.logger.logger.info(f"Ingerindo {len(series)} séries do Banco Central: {list(series.keys())}")
        
        dataframes = {}
        
        for nome, codigo in series.items():
            try:
                self.logger.log_stage_start(f"ingest_bacen_{nome}", {"codigo": codigo})
                
                # Buscar dados da API - apenas últimos 5 anos
                dados = self.api_client.get_bacen_serie(codigo, anos=5)
                
                if not dados:
                    self.logger.logger.warning(f"Nenhum dado retornado para {nome} (série {codigo})")
                    continue
                
                self.logger.log_data_stats(
                    f"bacen_{nome}_api",
                    {"records_fetched": len(dados)}
                )
                
                # Converter para DataFrame
                pandas_df = pd.DataFrame(dados)
                df = self.spark.createDataFrame(pandas_df)
                
                # Adicionar metadados
                df = df.withColumn("serie_codigo", F.lit(codigo)) \
                       .withColumn("serie_nome", F.lit(nome)) \
                       .withColumn("fonte", F.lit("banco_central")) \
                       .withColumn("dt_carga", F.current_timestamp())
                
                # Converter data e valor
                df = df.withColumn(
                    "data_parsed",
                    F.to_date(F.col("data"), "dd/MM/yyyy")
                ).withColumn(
                    "ano",
                    F.year(F.col("data_parsed"))
                ).withColumn(
                    "mes",
                    F.month(F.col("data_parsed"))
                ).withColumn(
                    "valor_decimal",
                    F.col("valor").cast("decimal(18,4)")
                )
                
                # Adicionar colunas de auditoria
                df = add_audit_columns(df)
                
                # Salvar na camada Bronze
                output_file = self.output_path / f"bacen_{nome}"
                write_parquet(
                    df,
                    str(output_file),
                    mode="overwrite",
                    partition_by=["ano", "fonte"]
                )
                
                stats = {
                    "records": df.count(),
                    "output_path": str(output_file)
                }
                self.logger.log_stage_end(f"ingest_bacen_{nome}", stats)
                
                dataframes[nome] = df
                
            except Exception as e:
                self.logger.log_error(f"ingest_bacen_{nome}", e, {"codigo": codigo})
                # Continue com outras séries mesmo se uma falhar
                continue
        
        self.logger.log_stage_end("ingest_bacen_indicadores", {
            "datasets": list(dataframes.keys()),
            "total_records": sum(df.count() for df in dataframes.values())
        })
        
        return dataframes
    
    def ingest_all(
        self,
        ano_populacao: str = "2024",
        ano_pib: str = "2021"
    ) -> Dict[str, DataFrame]:
        """
        Ingere todas as fontes de dados incluindo IBGE e Banco Central.
        
        Args:
            ano_populacao: Ano para dados de população
            ano_pib: Ano para dados de PIB
            
        Returns:
            Dicionário com todos os DataFrames ingeridos
        """
        self.logger.log_stage_start("ingest_all")
        
        dataframes = {}
        
        # Verificar se APIs do IBGE estão habilitadas
        ibge_enabled = self.config.get('api', {}).get('ibge', {}).get('enabled', True)
        
        try:
            # Ingerir localidades primeiro (dados de referência) - apenas se IBGE estiver habilitado
            if ibge_enabled:
                try:
                    dataframes['localidades'] = self.ingest_localidades()
                except Exception as e:
                    self.logger.logger.warning(f"Falha ao ingerir localidades: {str(e)}")
                    # Continuar mesmo se localidades falhar
            else:
                self.logger.logger.info("APIs do IBGE desabilitadas na configuração, pulando localidades")
            
            # Ingerir população - apenas se IBGE estiver habilitado
            if ibge_enabled:
                try:
                    dataframes['populacao'] = self.ingest_populacao(ano_populacao)
                except Exception as e:
                    self.logger.logger.warning(f"Falha ao ingerir população: {str(e)}")
                    # Continuar mesmo se população falhar
            else:
                self.logger.logger.info("APIs do IBGE desabilitadas na configuração, pulando população")
            
            # Ingerir PIB - apenas se IBGE estiver habilitado
            if ibge_enabled:
                try:
                    dataframes['pib'] = self.ingest_pib(ano_pib)
                except Exception as e:
                    self.logger.logger.warning(f"Falha ao ingerir PIB: {str(e)}")
                    # Continuar mesmo se PIB falhar
            else:
                self.logger.logger.info("APIs do IBGE desabilitadas na configuração, pulando PIB")
            
            # Ingerir indicadores do Banco Central (sempre habilitado)
            try:
                bacen_dfs = self.ingest_bacen_indicadores()
                dataframes.update(bacen_dfs)
                self.logger.logger.info(f"✓ Indicadores do Banco Central ingeridos: {list(bacen_dfs.keys())}")
            except Exception as e:
                self.logger.logger.warning(f"Falha ao ingerir indicadores do Banco Central: {str(e)}")
                # Continuar mesmo se Bacen falhar
            
            self.logger.log_stage_end("ingest_all", {
                "datasets": list(dataframes.keys()),
                "total_records": sum(df.count() for df in dataframes.values())
            })
            
            return dataframes
            
        except Exception as e:
            self.logger.log_error("ingest_all", e)
            raise
        finally:
            self.api_client.close()

