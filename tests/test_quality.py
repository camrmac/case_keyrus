"""
Testes para o módulo de qualidade de dados.
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from src.quality.validators import DataValidator


@pytest.fixture(scope="session")
def spark():
    """Cria sessão Spark para testes."""
    spark = SparkSession.builder \
        .appName("test") \
        .master("local[1]") \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def sample_df(spark):
    """Cria DataFrame de exemplo para testes."""
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("value", IntegerType(), True)
    ])
    
    data = [
        (1, "A", 100),
        (2, "B", 200),
        (3, None, 300),
        (4, "D", None)
    ]
    
    return spark.createDataFrame(data, schema)


class TestDataValidator:
    """Suite de testes para o Validador de Dados."""
    
    def test_check_schema_pass(self, sample_df):
        """Testa validação de schema - caso de sucesso."""
        validator = DataValidator()
        passed, details = validator.check_schema(
            sample_df,
            required_columns=["id", "name", "value"]
        )
        
        assert passed is True
        assert len(details["missing_columns"]) == 0
    
    def test_check_schema_fail(self, sample_df):
        """Testa validação de schema - caso de falha."""
        validator = DataValidator()
        passed, details = validator.check_schema(
            sample_df,
            required_columns=["id", "name", "value", "extra_column"]
        )
        
        assert passed is False
        assert "extra_column" in details["missing_columns"]
    
    def test_check_nulls(self, sample_df):
        """Testa validação de verificação de nulos."""
        validator = DataValidator()
        
        # Verificar coluna com nulos
        passed, details = validator.check_nulls(
            sample_df,
            critical_columns=["name"],
            threshold=0.0
        )
        
        assert passed is False
        assert details["null_counts"]["name"]["null_count"] > 0
    
    def test_check_duplicates(self, spark):
        """Testa validação de verificação de duplicatas."""
        validator = DataValidator()
        
        # Criar DataFrame com duplicatas
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True)
        ])
        
        data = [
            (1, "A"),
            (1, "A"),  # Duplicata
            (2, "B")
        ]
        
        df = spark.createDataFrame(data, schema)
        
        passed, details = validator.check_duplicates(
            df,
            key_columns=["id", "name"]
        )
        
        assert passed is False
        assert details["duplicate_count"] == 1
    
    def test_check_range(self, sample_df):
        """Testa validação de intervalo."""
        validator = DataValidator()
        
        # Verificar intervalo válido
        passed, details = validator.check_range(
            sample_df,
            column="value",
            min_value=0,
            max_value=1000
        )
        
        assert passed is True
        
        # Verificar intervalo inválido
        passed, details = validator.check_range(
            sample_df,
            column="value",
            min_value=0,
            max_value=150  # Valor máximo nos dados é 300
        )
        
        assert passed is False
        assert len(details["failures"]) > 0
    
    def test_check_completeness(self, sample_df):
        """Testa verificação de completude."""
        validator = DataValidator()
        
        passed, details = validator.check_completeness(
            sample_df,
            threshold=0.90  # 90% de completude necessária
        )
        
        # Com 2 nulos de 12 células = 83.3% de completude
        assert details["overall_completeness"] < 90

