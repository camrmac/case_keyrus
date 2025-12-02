"""
Orquestrador de verificações de qualidade de dados.
"""
from typing import Dict, List
from pyspark.sql import DataFrame

from src.quality.validators import DataValidator
from src.utils.logger import PipelineLogger


class QualityChecker:
    """
    Orquestrador para verificações de qualidade de dados.
    """
    
    def __init__(self, config: Dict):
        """
        Inicializa o verificador de qualidade.
        
        Args:
            config: Dicionário de configuração
        """
        self.config = config.get('quality', {})
        self.validator = DataValidator()
        self.logger = PipelineLogger("quality")
        self.enabled = self.config.get('enabled', True)
    
    def run_bronze_checks(self, dataframes: Dict[str, DataFrame]) -> Dict:
        """
        Executa verificações de qualidade na camada Bronze.
        
        Args:
            dataframes: Dicionário de DataFrames para verificar
            
        Returns:
            Dicionário com resultados das verificações
        """
        if not self.enabled:
            self.logger.logger.info("Verificações de qualidade desabilitadas")
            return {"enabled": False}
        
        self.logger.log_stage_start("quality_checks_bronze")
        
        results = {
            "layer": "bronze",
            "checks": {}
        }
        
        # Verificar população
        if 'populacao' in dataframes:
            df = dataframes['populacao']
            results['checks']['populacao'] = self._check_dataset(
                df,
                required_columns=['codigo_municipio', 'ano', 'valor'],
                critical_nulls=['codigo_municipio', 'ano'],
                key_columns=['codigo_municipio', 'ano']
            )
        
        # Verificar PIB
        if 'pib' in dataframes:
            df = dataframes['pib']
            results['checks']['pib'] = self._check_dataset(
                df,
                required_columns=['codigo_municipio', 'ano', 'valor'],
                critical_nulls=['codigo_municipio', 'ano'],
                key_columns=['codigo_municipio', 'ano']
            )
        
        # Verificar localidades
        if 'localidades' in dataframes:
            df = dataframes['localidades']
            results['checks']['localidades'] = self._check_dataset(
                df,
                required_columns=['codigo_municipio', 'nome_municipio'],
                critical_nulls=['codigo_municipio'],
                key_columns=['codigo_municipio']
            )
        
        # Calcular taxa de aprovação geral
        results['summary'] = self._calculate_summary(results['checks'])
        
        self.logger.log_stage_end("quality_checks_bronze", results['summary'])
        
        return results
    
    def run_silver_checks(self, dataframes: Dict[str, DataFrame]) -> Dict:
        """
        Executa verificações de qualidade na camada Silver.
        
        Args:
            dataframes: Dicionário de DataFrames para verificar
            
        Returns:
            Dicionário com resultados das verificações
        """
        if not self.enabled:
            return {"enabled": False}
        
        self.logger.log_stage_start("quality_checks_silver")
        
        results = {
            "layer": "silver",
            "checks": {}
        }
        
        # Verificar população
        if 'populacao' in dataframes:
            df = dataframes['populacao']
            checks = self._check_dataset(
                df,
                required_columns=['codigo_municipio', 'ano', 'populacao'],
                critical_nulls=['codigo_municipio', 'ano', 'populacao'],
                key_columns=['codigo_municipio', 'ano']
            )
            
            # Verificação de intervalo para população
            passed, details = self.validator.check_range(
                df, 'populacao', min_value=0, max_value=20000000
            )
            checks['range_populacao'] = {"passed": passed, "details": details}
            self.logger.log_quality_check("range_populacao", passed, details)
            
            results['checks']['populacao'] = checks
        
        # Verificar PIB
        if 'pib' in dataframes:
            df = dataframes['pib']
            checks = self._check_dataset(
                df,
                required_columns=['codigo_municipio', 'ano', 'pib_per_capita'],
                critical_nulls=['codigo_municipio', 'ano'],
                key_columns=['codigo_municipio', 'ano']
            )
            
            # Verificação de intervalo para PIB per capita
            passed, details = self.validator.check_range(
                df, 'pib_per_capita', min_value=0, max_value=1000000
            )
            checks['range_pib_per_capita'] = {"passed": passed, "details": details}
            self.logger.log_quality_check("range_pib_per_capita", passed, details)
            
            results['checks']['pib'] = checks
        
        results['summary'] = self._calculate_summary(results['checks'])
        
        self.logger.log_stage_end("quality_checks_silver", results['summary'])
        
        return results
    
    def run_gold_checks(self, dataframes: Dict[str, DataFrame]) -> Dict:
        """
        Executa verificações de qualidade na camada Gold.
        
        Args:
            dataframes: Dicionário de DataFrames para verificar
            
        Returns:
            Dicionário com resultados das verificações
        """
        if not self.enabled:
            return {"enabled": False}
        
        self.logger.log_stage_start("quality_checks_gold")
        
        results = {
            "layer": "gold",
            "checks": {}
        }
        
        # Verificar dimensões
        if 'dim_municipio' in dataframes:
            df = dataframes['dim_municipio']
            results['checks']['dim_municipio'] = self._check_dataset(
                df,
                required_columns=['sk_municipio', 'codigo_municipio'],
                critical_nulls=['sk_municipio', 'codigo_municipio'],
                key_columns=['sk_municipio']
            )
        
        # Verificar tabelas de fato
        if 'fact_indicadores_municipio' in dataframes:
            df = dataframes['fact_indicadores_municipio']
            checks = self._check_dataset(
                df,
                required_columns=['sk_municipio', 'sk_tempo', 'pib_total'],
                critical_nulls=['sk_municipio', 'sk_tempo'],
                key_columns=['sk_municipio', 'sk_tempo']
            )
            results['checks']['fact_indicadores_municipio'] = checks
        
        # Verificações de integridade referencial
        if 'fact_indicadores_municipio' in dataframes and 'dim_municipio' in dataframes:
            passed, details = self.validator.check_referential_integrity(
                dataframes['fact_indicadores_municipio'],
                dataframes['dim_municipio'],
                'sk_municipio',
                'sk_municipio'
            )
            results['checks']['referential_integrity'] = {
                "passed": passed,
                "details": details
            }
            self.logger.log_quality_check("referential_integrity", passed, details)
        
        results['summary'] = self._calculate_summary(results['checks'])
        
        self.logger.log_stage_end("quality_checks_gold", results['summary'])
        
        return results
    
    def _check_dataset(
        self,
        df: DataFrame,
        required_columns: List[str],
        critical_nulls: List[str],
        key_columns: List[str]
    ) -> Dict:
        """
        Executa verificações padrão em um dataset.
        
        Args:
            df: DataFrame para verificar
            required_columns: Colunas obrigatórias
            critical_nulls: Colunas que não devem ser nulas
            key_columns: Colunas-chave para verificação de duplicatas
            
        Returns:
            Dicionário com resultados das verificações
        """
        checks = {}
        
        # Verificação de schema
        passed, details = self.validator.check_schema(df, required_columns)
        checks['schema'] = {"passed": passed, "details": details}
        self.logger.log_quality_check("schema", passed, details)
        
        # Verificação de nulos
        passed, details = self.validator.check_nulls(df, critical_nulls, threshold=0.0)
        checks['nulls'] = {"passed": passed, "details": details}
        self.logger.log_quality_check("nulls", passed, details)
        
        # Verificação de duplicatas
        passed, details = self.validator.check_duplicates(df, key_columns)
        checks['duplicates'] = {"passed": passed, "details": details}
        self.logger.log_quality_check("duplicates", passed, details)
        
        # Verificação de completude
        threshold = self.config.get('threshold', {}).get('completeness', 0.95)
        passed, details = self.validator.check_completeness(df, threshold)
        checks['completeness'] = {"passed": passed, "details": details}
        self.logger.log_quality_check("completeness", passed, details)
        
        return checks
    
    def _calculate_summary(self, checks: Dict) -> Dict:
        """
        Calcula estatísticas resumidas das verificações.
        
        Args:
            checks: Dicionário de resultados das verificações
            
        Returns:
            Dicionário resumo
        """
        total_checks = 0
        passed_checks = 0
        
        for dataset, dataset_checks in checks.items():
            for check_name, check_result in dataset_checks.items():
                if isinstance(check_result, dict) and 'passed' in check_result:
                    total_checks += 1
                    if check_result['passed']:
                        passed_checks += 1
        
        pass_rate = (passed_checks / total_checks * 100) if total_checks > 0 else 0
        
        return {
            "total_checks": total_checks,
            "passed_checks": passed_checks,
            "failed_checks": total_checks - passed_checks,
            "pass_rate": round(pass_rate, 2)
        }

