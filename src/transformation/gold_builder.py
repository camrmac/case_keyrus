"""
Construtor da camada Gold - Modelagem dimensional e analytics.
"""
from typing import Dict
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.utils.logger import PipelineLogger
from src.utils.spark_utils import read_parquet, write_parquet


class GoldBuilder:
    """
    Construtor para a camada Gold - modelo dimensional para analytics.
    
    Cria esquema estrela com dimensões e tabelas de fato, calcula
    métricas de negócio e indicadores.
    """
    
    def __init__(
        self,
        spark: SparkSession,
        config: Dict,
        silver_path: str,
        gold_path: str
    ):
        """
        Inicializa o construtor Gold.
        
        Args:
            spark: SparkSession
            config: Dicionário de configuração
            silver_path: Caminho para a camada Silver
            gold_path: Caminho para a camada Gold
        """
        self.spark = spark
        self.config = config
        self.silver_path = Path(silver_path)
        self.gold_path = Path(gold_path)
        self.logger = PipelineLogger("transformation")
    
    def build_dim_municipio(self) -> DataFrame:
        """
        Constrói a tabela de dimensão dim_municipio.
        
        Returns:
            Tabela de dimensão para municípios
        """
        self.logger.log_stage_start("build_dim_municipio")
        
        try:
            df = read_parquet(
                self.spark,
                str(self.silver_path / "localidades")
            )
            
            # Criar chave substituta (surrogate key)
            df_dim = (
                df
                .withColumn(
                    "sk_municipio",
                    F.monotonically_increasing_id()
                )
                .select(
                    "sk_municipio",
                    "codigo_municipio",
                    "nome_municipio",
                    "codigo_uf",
                    "sigla_uf",
                    "nome_uf",
                    "codigo_regiao",
                    "nome_regiao",
                    "sigla_regiao"
                )
            )
            
            # Escrever dimensão
            output_path = self.gold_path / "dim_municipio"
            write_parquet(df_dim, str(output_path), mode="overwrite")
            
            self.logger.log_stage_end("build_dim_municipio", {
                "records": df_dim.count(),
                "path": str(output_path)
            })
            
            return df_dim
            
        except Exception as e:
            self.logger.log_error("build_dim_municipio", e)
            raise
    
    def build_dim_tempo(self, anos: list = None) -> DataFrame:
        """
        Constrói a tabela de dimensão dim_tempo.
        
        Args:
            anos: Lista de anos para incluir
            
        Returns:
            Tabela de dimensão para tempo
        """
        self.logger.log_stage_start("build_dim_tempo")
        
        try:
            if anos is None:
                # Verificar se IBGE está habilitado
                ibge_enabled = self.config.get('api', {}).get('ibge', {}).get('enabled', True)
                anos_list = []
                
                if ibge_enabled:
                    # Tentar obter anos dos dados do IBGE
                    try:
                        df_pop = read_parquet(self.spark, str(self.silver_path / "populacao"))
                        anos_list.extend(df_pop.select("ano").distinct().rdd.flatMap(lambda x: x).collect())
                    except Exception:
                        self.logger.logger.debug("Não foi possível ler dados de população")
                    
                    try:
                        df_pib = read_parquet(self.spark, str(self.silver_path / "pib"))
                        anos_list.extend(df_pib.select("ano").distinct().rdd.flatMap(lambda x: x).collect())
                    except Exception:
                        self.logger.logger.debug("Não foi possível ler dados de PIB")
                
                # Se não houver anos do IBGE, tentar obter dos dados do Banco Central
                if not anos_list:
                    try:
                        # Tentar ler qualquer série do Banco Central para obter os anos
                        from src.utils.spark_utils import normalize_path
                        bacen_config = self.config.get('api', {}).get('banco_central', {})
                        series_config = bacen_config.get('series', {})
                        series_list = list(series_config.keys()) if series_config else []
                        
                        # Tentar ler a primeira série disponível
                        for serie_nome in series_list:
                            try:
                                bacen_path = normalize_path(str(self.silver_path / f"bacen_{serie_nome}_clean"))
                                df_bacen = read_parquet(self.spark, bacen_path)
                                anos_list = df_bacen.select("ano").distinct().orderBy("ano").rdd.flatMap(lambda x: x).collect()
                                self.logger.logger.info(f"Obtidos {len(anos_list)} anos dos dados do Banco Central (série: {serie_nome})")
                                break
                            except Exception:
                                continue
                    except Exception as e:
                        self.logger.logger.warning(f"Não foi possível obter anos dos dados do Banco Central: {str(e)}")
                        # Usar anos padrão (últimos 5 anos)
                        from datetime import datetime
                        ano_atual = datetime.now().year
                        anos_list = list(range(ano_atual - 4, ano_atual + 1))
                        self.logger.logger.info(f"Usando anos padrão: {anos_list}")
                
                # Remover duplicatas e ordenar
                anos = sorted(list(set(anos_list)))
            
            # Criar dimensão de tempo
            data = [{
                "sk_tempo": i,
                "ano": ano,
                "semestre": 1 if ano % 2 == 0 else 2,
                "trimestre": (ano % 4) + 1
            } for i, ano in enumerate(anos, 1)]
            
            df_dim = self.spark.createDataFrame(data)
            
            # Escrever dimensão
            output_path = self.gold_path / "dim_tempo"
            write_parquet(df_dim, str(output_path), mode="overwrite")
            
            self.logger.log_stage_end("build_dim_tempo", {
                "records": df_dim.count(),
                "path": str(output_path)
            })
            
            return df_dim
            
        except Exception as e:
            self.logger.log_error("build_dim_tempo", e)
            raise
    
    def build_fact_indicadores_municipio(self) -> DataFrame:
        """
        Constrói fact_indicadores_municipio com todas as métricas no nível municipal.
        
        Returns:
            Tabela de fato com indicadores no nível municipal
        """
        self.logger.log_stage_start("build_fact_indicadores_municipio")
        
        try:
            # Ler dimensões e tabelas Silver
            dim_mun = read_parquet(self.spark, str(self.gold_path / "dim_municipio"))
            dim_tempo = read_parquet(self.spark, str(self.gold_path / "dim_tempo"))
            df_pop = read_parquet(self.spark, str(self.silver_path / "populacao"))
            df_pib = read_parquet(self.spark, str(self.silver_path / "pib"))
            
            # Juntar população e PIB
            df_base = (
                df_pop
                .select(
                    "codigo_municipio",
                    "ano",
                    F.col("populacao").alias("populacao_total")
                )
                .join(
                    df_pib.select("codigo_municipio", "ano", "pib_per_capita"),
                    on=["codigo_municipio", "ano"],
                    how="outer"
                )
            )
            
            # Calcular PIB total a partir de PIB per capita e população
            df_base = df_base.withColumn(
                "pib_total",
                F.when(
                    (F.col("pib_per_capita").isNotNull()) & 
                    (F.col("populacao_total").isNotNull()),
                    F.col("pib_per_capita") * F.col("populacao_total") / 1000  # PIB em milhares
                ).otherwise(None)
            )
            
            # Calcular métricas YoY (Year over Year)
            window_mun = Window.partitionBy("codigo_municipio").orderBy("ano")
            
            df_yoy = (
                df_base
                .withColumn("pib_total_anterior", F.lag("pib_total").over(window_mun))
                .withColumn("populacao_anterior", F.lag("populacao_total").over(window_mun))
                .withColumn(
                    "yoy_pib",
                    F.when(
                        F.col("pib_total_anterior").isNotNull(),
                        ((F.col("pib_total") - F.col("pib_total_anterior")) / 
                         F.col("pib_total_anterior") * 100)
                    ).otherwise(None)
                )
                .withColumn(
                    "yoy_populacao",
                    F.when(
                        F.col("populacao_anterior").isNotNull(),
                        ((F.col("populacao_total") - F.col("populacao_anterior")) / 
                         F.col("populacao_anterior") * 100)
                    ).otherwise(None)
                )
            )
            
            # Calcular participações e rankings por UF
            window_uf = Window.partitionBy("codigo_uf", "ano")
            window_uf_rank = Window.partitionBy("codigo_uf", "ano").orderBy(
                F.col("pib_total").desc()
            )
            
            df_with_uf = (
                df_yoy
                .join(
                    dim_mun.select("codigo_municipio", "codigo_uf"),
                    on="codigo_municipio",
                    how="left"
                )
                .withColumn("total_pib_uf", F.sum("pib_total").over(window_uf))
                .withColumn(
                    "share_pib_uf",
                    F.when(
                        F.col("total_pib_uf").isNotNull(),
                        (F.col("pib_total") / F.col("total_pib_uf") * 100)
                    ).otherwise(None)
                )
                .withColumn("rank_pib_uf", F.row_number().over(window_uf_rank))
            )
            
            # Calcular participações e rankings Brasil
            window_brasil = Window.partitionBy("ano")
            window_brasil_rank = Window.partitionBy("ano").orderBy(
                F.col("pib_total").desc()
            )
            
            df_with_brasil = (
                df_with_uf
                .withColumn("total_pib_brasil", F.sum("pib_total").over(window_brasil))
                .withColumn(
                    "share_pib_brasil",
                    F.when(
                        F.col("total_pib_brasil").isNotNull(),
                        (F.col("pib_total") / F.col("total_pib_brasil") * 100)
                    ).otherwise(None)
                )
                .withColumn("rank_pib_brasil", F.row_number().over(window_brasil_rank))
            )
            
            # Juntar com dimensões para obter chaves substitutas
            fact = (
                df_with_brasil
                .join(
                    dim_mun.select("sk_municipio", "codigo_municipio"),
                    on="codigo_municipio",
                    how="left"
                )
                .join(
                    dim_tempo.select("sk_tempo", "ano"),
                    on="ano",
                    how="left"
                )
                .select(
                    "sk_municipio",
                    "sk_tempo",
                    "codigo_municipio",
                    "ano",
                    "populacao_total",
                    "pib_total",
                    "pib_per_capita",
                    "yoy_pib",
                    "yoy_populacao",
                    "share_pib_uf",
                    "share_pib_brasil",
                    "rank_pib_uf",
                    "rank_pib_brasil"
                )
            )
            
            # Escrever tabela de fato
            output_path = self.gold_path / "fact_indicadores_municipio"
            write_parquet(fact, str(output_path), mode="overwrite")
            
            self.logger.log_stage_end("build_fact_indicadores_municipio", {
                "records": fact.count(),
                "path": str(output_path)
            })
            
            return fact
            
        except Exception as e:
            self.logger.log_error("build_fact_indicadores_municipio", e)
            raise
    
    def build_fact_indicadores_uf(self) -> DataFrame:
        """
        Constrói fact_indicadores_uf - agregações no nível UF.
        
        Returns:
            Tabela de fato com indicadores no nível UF
        """
        self.logger.log_stage_start("build_fact_indicadores_uf")
        
        try:
            fact_mun = read_parquet(
                self.spark,
                str(self.gold_path / "fact_indicadores_municipio")
            )
            dim_mun = read_parquet(
                self.spark,
                str(self.gold_path / "dim_municipio")
            )
            
            # Juntar para obter informações de UF
            df_with_uf = fact_mun.join(
                dim_mun.select("codigo_municipio", "codigo_uf", "sigla_uf", "nome_uf"),
                on="codigo_municipio",
                how="left"
            )
            
            # Agregar por UF e ano
            fact_uf = (
                df_with_uf
                .groupBy("codigo_uf", "sigla_uf", "nome_uf", "ano")
                .agg(
                    F.sum("populacao_total").alias("populacao_total"),
                    F.sum("pib_total").alias("pib_total"),
                    F.avg("pib_per_capita").alias("pib_per_capita_medio"),
                    F.count("codigo_municipio").alias("qtd_municipios")
                )
            )
            
            # Calcular YoY para UF
            window_uf = Window.partitionBy("codigo_uf").orderBy("ano")
            
            fact_uf = (
                fact_uf
                .withColumn("pib_total_anterior", F.lag("pib_total").over(window_uf))
                .withColumn("populacao_anterior", F.lag("populacao_total").over(window_uf))
                .withColumn(
                    "yoy_pib",
                    F.when(
                        F.col("pib_total_anterior").isNotNull(),
                        ((F.col("pib_total") - F.col("pib_total_anterior")) / 
                         F.col("pib_total_anterior") * 100)
                    ).otherwise(None)
                )
                .withColumn(
                    "yoy_populacao",
                    F.when(
                        F.col("populacao_anterior").isNotNull(),
                        ((F.col("populacao_total") - F.col("populacao_anterior")) / 
                         F.col("populacao_anterior") * 100)
                    ).otherwise(None)
                )
            )
            
            # Calcular participação Brasil e ranking
            window_brasil = Window.partitionBy("ano")
            window_rank = Window.partitionBy("ano").orderBy(F.col("pib_total").desc())
            
            fact_uf = (
                fact_uf
                .withColumn("total_pib_brasil", F.sum("pib_total").over(window_brasil))
                .withColumn(
                    "share_pib_brasil",
                    (F.col("pib_total") / F.col("total_pib_brasil") * 100)
                )
                .withColumn("rank_pib_brasil", F.row_number().over(window_rank))
                .select(
                    "codigo_uf",
                    "sigla_uf",
                    "nome_uf",
                    "ano",
                    "qtd_municipios",
                    "populacao_total",
                    "pib_total",
                    "pib_per_capita_medio",
                    "yoy_pib",
                    "yoy_populacao",
                    "share_pib_brasil",
                    "rank_pib_brasil"
                )
            )
            
            # Escrever tabela de fato
            output_path = self.gold_path / "fact_indicadores_uf"
            write_parquet(fact_uf, str(output_path), mode="overwrite")
            
            self.logger.log_stage_end("build_fact_indicadores_uf", {
                "records": fact_uf.count(),
                "path": str(output_path)
            })
            
            return fact_uf
            
        except Exception as e:
            self.logger.log_error("build_fact_indicadores_uf", e)
            raise
    
    def build_fact_indicadores_brasil(self) -> DataFrame:
        """
        Constrói fact_indicadores_brasil - agregações no nível Brasil.
        
        Returns:
            Tabela de fato com indicadores no nível Brasil
        """
        self.logger.log_stage_start("build_fact_indicadores_brasil")
        
        try:
            fact_uf = read_parquet(
                self.spark,
                str(self.gold_path / "fact_indicadores_uf")
            )
            
            # Agregar por ano
            fact_brasil = (
                fact_uf
                .groupBy("ano")
                .agg(
                    F.sum("populacao_total").alias("populacao_total"),
                    F.sum("pib_total").alias("pib_total"),
                    F.count("codigo_uf").alias("qtd_ufs")
                )
                .withColumn(
                    "pib_per_capita",
                    F.col("pib_total") / F.col("populacao_total")
                )
            )
            
            # Calcular YoY
            window_brasil = Window.orderBy("ano")
            
            fact_brasil = (
                fact_brasil
                .withColumn("pib_total_anterior", F.lag("pib_total").over(window_brasil))
                .withColumn("populacao_anterior", F.lag("populacao_total").over(window_brasil))
                .withColumn(
                    "yoy_pib",
                    F.when(
                        F.col("pib_total_anterior").isNotNull(),
                        ((F.col("pib_total") - F.col("pib_total_anterior")) / 
                         F.col("pib_total_anterior") * 100)
                    ).otherwise(None)
                )
                .withColumn(
                    "yoy_populacao",
                    F.when(
                        F.col("populacao_anterior").isNotNull(),
                        ((F.col("populacao_total") - F.col("populacao_anterior")) / 
                         F.col("populacao_anterior") * 100)
                    ).otherwise(None)
                )
                .select(
                    "ano",
                    "qtd_ufs",
                    "populacao_total",
                    "pib_total",
                    "pib_per_capita",
                    "yoy_pib",
                    "yoy_populacao"
                )
            )
            
            # Escrever tabela de fato
            output_path = self.gold_path / "fact_indicadores_brasil"
            write_parquet(fact_brasil, str(output_path), mode="overwrite")
            
            self.logger.log_stage_end("build_fact_indicadores_brasil", {
                "records": fact_brasil.count(),
                "path": str(output_path)
            })
            
            return fact_brasil
            
        except Exception as e:
            self.logger.log_error("build_fact_indicadores_brasil", e)
            raise
    
    def build_fact_indicadores_brasil_bacen_only(self) -> DataFrame:
        """
        Constrói fact_indicadores_brasil usando apenas dados do Banco Central (sem dados do IBGE).
        
        Returns:
            Tabela de fato com indicadores no nível Brasil do Banco Central
        """
        self.logger.log_stage_start("build_fact_indicadores_brasil_bacen_only")
        
        try:
            from src.utils.spark_utils import is_cloud_storage_path, normalize_path
            
            # Ler dim_tempo
            dim_tempo = read_parquet(self.spark, str(self.gold_path / "dim_tempo"))
            
            # Agregar dados do Banco Central por ano
            fact_brasil = dim_tempo.select("ano").distinct()
            
            # Ler séries configuradas do Banco Central
            bacen_config = self.config.get('api', {}).get('banco_central', {})
            series_config = bacen_config.get('series', {})
            bacen_series = list(series_config.keys()) if series_config else []
            
            if not bacen_series:
                self.logger.logger.warning("Nenhuma série do Banco Central configurada")
            else:
                self.logger.logger.info(f"Processando {len(bacen_series)} séries do Banco Central: {bacen_series}")
            
            # Adicionar dados do Banco Central (agregados por ano)
            for nome in bacen_series:
                try:
                    path = normalize_path(str(self.silver_path / f"bacen_{nome}_clean"))
                    df_bacen = read_parquet(self.spark, path)
                    df_agg = df_bacen.groupBy("ano").agg(
                        F.avg("valor_decimal").alias(f"{nome}_medio"),
                        F.max("valor_decimal").alias(f"{nome}_max"),
                        F.min("valor_decimal").alias(f"{nome}_min")
                    )
                    fact_brasil = fact_brasil.join(df_agg, on="ano", how="left")
                except Exception as e:
                    self.logger.logger.warning(f"Não foi possível carregar dados de {nome}: {str(e)}")
            
            # Escrever tabela de fato
            output_path = self.gold_path / "fact_indicadores_brasil"
            write_parquet(fact_brasil, str(output_path), mode="overwrite")
            
            self.logger.log_stage_end("build_fact_indicadores_brasil_bacen_only", {
                "records": fact_brasil.count(),
                "path": str(output_path)
            })
            
            return fact_brasil
            
        except Exception as e:
            self.logger.log_error("build_fact_indicadores_brasil_bacen_only", e)
            raise
    
    def build_all(self) -> Dict[str, DataFrame]:
        """
        Constrói todas as tabelas da camada Gold.
        
        Returns:
            Dicionário com todos os DataFrames Gold
        """
        self.logger.log_stage_start("build_all_gold")
        
        dataframes = {}
        
        # Verificar se APIs do IBGE estão habilitadas
        ibge_enabled = self.config.get('api', {}).get('ibge', {}).get('enabled', True)
        
        try:
            # Construir dimensões primeiro
            # dim_municipio depende de localidades (IBGE) - apenas se IBGE estiver habilitado
            if ibge_enabled:
                try:
                    dataframes['dim_municipio'] = self.build_dim_municipio()
                except Exception as e:
                    self.logger.logger.warning(f"Falha ao construir dim_municipio: {str(e)}")
            else:
                self.logger.logger.info("APIs do IBGE desabilitadas na configuração, pulando dim_municipio")
            
            # dim_tempo pode ser construído independentemente
            dataframes['dim_tempo'] = self.build_dim_tempo()
            
            # Construir tabelas de fato (em ordem de dependência)
            # fact_indicadores_municipio depende de populacao e pib (IBGE)
            if ibge_enabled:
                try:
                    dataframes['fact_indicadores_municipio'] = self.build_fact_indicadores_municipio()
                except Exception as e:
                    self.logger.logger.warning(f"Falha ao construir fact_indicadores_municipio: {str(e)}")
            else:
                self.logger.logger.info("APIs do IBGE desabilitadas na configuração, pulando fact_indicadores_municipio")
            
            # fact_indicadores_uf depende de fact_indicadores_municipio (IBGE)
            if ibge_enabled and 'fact_indicadores_municipio' in dataframes:
                try:
                    dataframes['fact_indicadores_uf'] = self.build_fact_indicadores_uf()
                except Exception as e:
                    self.logger.logger.warning(f"Falha ao construir fact_indicadores_uf: {str(e)}")
            else:
                self.logger.logger.info("APIs do IBGE desabilitadas ou fact_indicadores_municipio ausente, pulando fact_indicadores_uf")
            
            # fact_indicadores_brasil pode usar dados do Banco Central mesmo sem IBGE
            try:
                if ibge_enabled and 'fact_indicadores_uf' in dataframes:
                    # Versão completa com dados do IBGE
                    dataframes['fact_indicadores_brasil'] = self.build_fact_indicadores_brasil()
                elif not ibge_enabled:
                    # Versão apenas com dados do Banco Central (sem dados do IBGE)
                    self.logger.logger.info("Construindo fact_indicadores_brasil apenas com dados do Banco Central")
                    dataframes['fact_indicadores_brasil'] = self.build_fact_indicadores_brasil_bacen_only()
                else:
                    self.logger.logger.warning("fact_indicadores_uf ausente, pulando fact_indicadores_brasil")
            except Exception as e:
                self.logger.logger.warning(f"Falha ao construir fact_indicadores_brasil: {str(e)}")
            
            self.logger.log_stage_end("build_all_gold", {
                "tables": list(dataframes.keys()),
                "total_records": sum(df.count() for df in dataframes.values())
            })
            
            return dataframes
            
        except Exception as e:
            self.logger.log_error("build_all_gold", e)
            raise

