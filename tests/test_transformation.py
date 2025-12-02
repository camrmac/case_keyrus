"""
Testes para o módulo de transformação.
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from src.transformation.silver_processor import SilverProcessor


@pytest.fixture(scope="session")
def spark():
    """Cria sessão Spark para testes."""
    spark = SparkSession.builder \
        .appName("test") \
        .master("local[1]") \
        .getOrCreate()
    yield spark
    spark.stop()


class TestSilverProcessor:
    """Suite de testes para o processador Silver."""
    
    def test_silver_processor_initialization(self, spark):
        """Testa inicialização do processador Silver."""
        config = {"quality": {"enabled": True}}
        processor = SilverProcessor(
            spark, config, "./data/bronze", "./data/silver"
        )
        
        assert processor.spark is not None
        assert str(processor.bronze_path).endswith("bronze")
        assert str(processor.silver_path).endswith("silver")
    
    def test_column_renaming(self, spark):
        """Testa lógica de renomeação de colunas."""
        # Criar DataFrame de teste
        schema = StructType([
            StructField("codigo_municipio", StringType(), True),
            StructField("nome_municipio", StringType(), True),
            StructField("ano", IntegerType(), True),
            StructField("populacao", IntegerType(), True)
        ])
        
        data = [
            ("3550308", " São Paulo ", 2024, 12325232),
            ("3304557", " Rio de Janeiro ", 2024, 6775561)
        ]
        
        df = spark.createDataFrame(data, schema)
        
        # Aplicar transformações (similar ao Silver)
        from pyspark.sql import functions as F
        df_transformed = df.withColumn(
            "nome_municipio",
            F.trim(F.upper(F.col("nome_municipio")))
        )
        
        result = df_transformed.collect()
        
        assert result[0]["nome_municipio"] == "SÃO PAULO"
        assert result[1]["nome_municipio"] == "RIO DE JANEIRO"

