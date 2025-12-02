"""
Orquestrador principal do Pipeline ETL.
"""
from typing import Dict, Optional
from pathlib import Path
import time

from pyspark.sql import SparkSession

from src.ingestion.bronze_loader import BronzeLoader
from src.transformation.silver_processor import SilverProcessor
from src.transformation.gold_builder import GoldBuilder
from src.quality.quality_checks import QualityChecker
from src.utils.logger import PipelineLogger
from src.utils.spark_utils import create_spark_session, load_config, create_directory_structure


class DataPipeline:
    """
    Pipeline ETL principal para processamento de dados do IBGE.
    
    Orquestra o fluxo de dados através das camadas Bronze, Silver e Gold.
    """
    
    def __init__(
        self,
        config_path: str = "config/config.yaml",
        spark: Optional[SparkSession] = None
    ):
        """
        Inicializa o pipeline.
        
        Args:
            config_path: Caminho para o arquivo de configuração
            spark: SparkSession opcional (cria novo se não fornecido)
        """
        self.config = load_config(config_path)
        self.logger = PipelineLogger("pipeline")
        
        # Criar ou usar Spark session fornecido
        if spark:
            self.spark = spark
        else:
            self.spark = create_spark_session(
                app_name=self.config['spark']['app_name'],
                config=self.config['spark'].get('config', {})
            )
        
        # Configurar caminhos
        datalake_config = self.config['datalake']
        base_path_str = datalake_config['base_path']
        
        # Normalizar caminho e verificar se é armazenamento em nuvem
        from src.utils.spark_utils import normalize_path, is_cloud_storage_path
        base_path_normalized = normalize_path(base_path_str)
        is_cloud_storage = is_cloud_storage_path(base_path_normalized)
        
        # Para armazenamento em nuvem, construir caminhos como strings (Path() não funciona bem com URIs)
        if is_cloud_storage:
            # Garantir que base_path termine com / se ainda não terminar
            if not base_path_normalized.endswith('/'):
                base_path_normalized += '/'
            
            self.base_path = base_path_normalized
            self.bronze_path = base_path_normalized + datalake_config['layers']['bronze']['path']
            self.silver_path = base_path_normalized + datalake_config['layers']['silver']['path']
            self.gold_path = base_path_normalized + datalake_config['layers']['gold']['path']
            
            # Normalizar todos os caminhos
            self.bronze_path = normalize_path(self.bronze_path)
            self.silver_path = normalize_path(self.silver_path)
            self.gold_path = normalize_path(self.gold_path)
            
            self.logger.logger.info(f"Usando armazenamento em nuvem, pulando criação de diretório local: {base_path_str}")
        else:
            # Para caminhos locais, usar Path() para manipulação adequada de caminhos
            self.base_path = Path(base_path_str)
            self.bronze_path = self.base_path / datalake_config['layers']['bronze']['path']
            self.silver_path = self.base_path / datalake_config['layers']['silver']['path']
            self.gold_path = self.base_path / datalake_config['layers']['gold']['path']
            
            create_directory_structure(str(self.base_path))
        
        # Inicializar componentes (converter Path para string se necessário)
        self.bronze_loader = BronzeLoader(
            self.spark, self.config, str(self.bronze_path)
        )
        self.silver_processor = SilverProcessor(
            self.spark, self.config, str(self.bronze_path), str(self.silver_path)
        )
        self.gold_builder = GoldBuilder(
            self.spark, self.config, str(self.silver_path), str(self.gold_path)
        )
        self.quality_checker = QualityChecker(self.config)
        
        # Inicializar exportador SQL (se habilitado)
        self.sql_exporter = None
        if self.config.get('database', {}).get('enabled', False):
            try:
                from src.delivery.sql_exporter import SQLExporter
                self.sql_exporter = SQLExporter(
                    self.spark,
                    self.config,
                    str(self.gold_path)
                )
                self.logger.logger.info("Exportador SQL inicializado")
            except Exception as e:
                self.logger.logger.warning(f"Falha ao inicializar exportador SQL: {str(e)}")
        
        self.logger.logger.info(f"Pipeline inicializado - Caminho base: {self.base_path}")
    
    def run_bronze(
        self,
        ano_populacao: str = "2024",
        ano_pib: str = "2021"
    ) -> Dict:
        """
        Executa a ingestão da camada Bronze.
        
        Args:
            ano_populacao: Ano para dados de população
            ano_pib: Ano para dados de PIB
            
        Returns:
            Dicionário com DataFrames ingeridos e métricas
        """
        self.logger.log_stage_start("bronze_layer")
        start_time = time.time()
        
        try:
            # Ingerir dados
            dataframes = self.bronze_loader.ingest_all(ano_populacao, ano_pib)
            
            # Executar verificações de qualidade
            quality_results = self.quality_checker.run_bronze_checks(dataframes)
            
            duration = time.time() - start_time
            
            metrics = {
                "duration_seconds": round(duration, 2),
                "datasets": list(dataframes.keys()),
                "total_records": sum(df.count() for df in dataframes.values()),
                "quality_pass_rate": quality_results.get('summary', {}).get('pass_rate', 0)
            }
            
            self.logger.log_stage_end("bronze_layer", metrics)
            
            return {
                "dataframes": dataframes,
                "quality_results": quality_results,
                "metrics": metrics
            }
            
        except Exception as e:
            self.logger.log_error("bronze_layer", e)
            raise
    
    def run_silver(self) -> Dict:
        """
        Executa as transformações da camada Silver.
        
        Returns:
            Dicionário com DataFrames transformados e métricas
        """
        self.logger.log_stage_start("silver_layer")
        start_time = time.time()
        
        try:
            # Processar dados
            dataframes = self.silver_processor.process_all()
            
            # Executar verificações de qualidade
            quality_results = self.quality_checker.run_silver_checks(dataframes)
            
            duration = time.time() - start_time
            
            metrics = {
                "duration_seconds": round(duration, 2),
                "datasets": list(dataframes.keys()),
                "total_records": sum(df.count() for df in dataframes.values()),
                "quality_pass_rate": quality_results.get('summary', {}).get('pass_rate', 0)
            }
            
            self.logger.log_stage_end("silver_layer", metrics)
            
            return {
                "dataframes": dataframes,
                "quality_results": quality_results,
                "metrics": metrics
            }
            
        except Exception as e:
            self.logger.log_error("silver_layer", e)
            raise
    
    def run_gold(self) -> Dict:
        """
        Executa a modelagem da camada Gold.
        
        Returns:
            Dicionário com DataFrames modelados e métricas
        """
        self.logger.log_stage_start("gold_layer")
        start_time = time.time()
        
        try:
            # Construir modelo dimensional
            dataframes = self.gold_builder.build_all()
            
            # Executar verificações de qualidade
            quality_results = self.quality_checker.run_gold_checks(dataframes)
            
            duration = time.time() - start_time
            
            metrics = {
                "duration_seconds": round(duration, 2),
                "tables": list(dataframes.keys()),
                "total_records": sum(df.count() for df in dataframes.values()),
                "quality_pass_rate": quality_results.get('summary', {}).get('pass_rate', 0)
            }
            
            self.logger.log_stage_end("gold_layer", metrics)
            
            return {
                "dataframes": dataframes,
                "quality_results": quality_results,
                "metrics": metrics
            }
            
        except Exception as e:
            self.logger.log_error("gold_layer", e)
            raise
    
    def run_full_pipeline(
        self,
        ano_populacao: str = "2024",
        ano_pib: str = "2021"
    ) -> Dict:
        """
        Executa o pipeline completo (Bronze → Silver → Gold).
        
        Args:
            ano_populacao: Ano para dados de população
            ano_pib: Ano para dados de PIB
            
        Returns:
            Dicionário com todos os resultados e métricas
        """
        self.logger.log_stage_start("full_pipeline")
        pipeline_start = time.time()
        
        results = {
            "bronze": None,
            "silver": None,
            "gold": None,
            "overall_metrics": {}
        }
        
        try:
            # Executar Bronze
            results['bronze'] = self.run_bronze(ano_populacao, ano_pib)
            
            # Executar Silver
            results['silver'] = self.run_silver()
            
            # Executar Gold
            results['gold'] = self.run_gold()
            
            # Exportar para SQL (se habilitado)
            if self.sql_exporter:
                try:
                    self.logger.logger.info("Iniciando exportação para SQL...")
                    export_results = self.sql_exporter.export_all_indicators()
                    results['sql_export'] = {
                        "enabled": True,
                        "results": export_results,
                        "success": all(export_results.values())
                    }
                    self.logger.logger.info(f"✅ Exportação SQL concluída: {sum(export_results.values())}/{len(export_results)} tabelas")
                except Exception as e:
                    self.logger.logger.warning(f"Falha na exportação SQL: {str(e)}")
                    results['sql_export'] = {
                        "enabled": True,
                        "error": str(e),
                        "success": False
                    }
            else:
                results['sql_export'] = {"enabled": False}
            
            # Calcular métricas gerais
            pipeline_duration = time.time() - pipeline_start
            
            total_records = sum([
                results['bronze']['metrics']['total_records'],
                results['silver']['metrics']['total_records'],
                results['gold']['metrics']['total_records']
            ])
            
            avg_quality = sum([
                results['bronze']['metrics']['quality_pass_rate'],
                results['silver']['metrics']['quality_pass_rate'],
                results['gold']['metrics']['quality_pass_rate']
            ]) / 3
            
            results['overall_metrics'] = {
                "total_duration_seconds": round(pipeline_duration, 2),
                "total_records_processed": total_records,
                "average_quality_pass_rate": round(avg_quality, 2),
                "success": True
            }
            
            self.logger.log_stage_end("full_pipeline", results['overall_metrics'])
            
            return results
            
        except Exception as e:
            results['overall_metrics'] = {
                "success": False,
                "error": str(e)
            }
            self.logger.log_error("full_pipeline", e)
            raise
    
    def stop(self):
        """Para a sessão Spark."""
        if self.spark:
            self.spark.stop()
            self.logger.logger.info("Pipeline parado")

