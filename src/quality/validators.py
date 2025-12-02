"""
Funções de validação de dados.
"""
from typing import Dict, List, Tuple
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class DataValidator:
    """
    Classe de validação de dados para verificações de qualidade.
    """
    
    @staticmethod
    def check_schema(
        df: DataFrame,
        required_columns: List[str]
    ) -> Tuple[bool, Dict]:
        """
        Valida o schema do DataFrame.
        
        Args:
            df: DataFrame para validar
            required_columns: Lista de nomes de colunas obrigatórias
            
        Returns:
            Tupla de (passou, detalhes)
        """
        actual_columns = set(df.columns)
        required_set = set(required_columns)
        missing = required_set - actual_columns
        
        passed = len(missing) == 0
        
        details = {
            "required_columns": list(required_set),
            "actual_columns": list(actual_columns),
            "missing_columns": list(missing)
        }
        
        return passed, details
    
    @staticmethod
    def check_nulls(
        df: DataFrame,
        critical_columns: List[str],
        threshold: float = 0.0
    ) -> Tuple[bool, Dict]:
        """
        Verifica valores nulos em colunas críticas.
        
        Args:
            df: DataFrame para verificar
            critical_columns: Colunas que não devem ter nulos
            threshold: Percentual máximo permitido de nulos (0-1)
            
        Returns:
            Tupla de (passou, detalhes)
        """
        total_rows = df.count()
        null_counts = {}
        failures = []
        
        for col in critical_columns:
            if col in df.columns:
                null_count = df.filter(F.col(col).isNull()).count()
                null_pct = null_count / total_rows if total_rows > 0 else 0
                
                null_counts[col] = {
                    "null_count": null_count,
                    "null_percentage": round(null_pct * 100, 2)
                }
                
                if null_pct > threshold:
                    failures.append(col)
        
        passed = len(failures) == 0
        
        details = {
            "total_rows": total_rows,
            "null_counts": null_counts,
            "failed_columns": failures,
            "threshold_pct": threshold * 100
        }
        
        return passed, details
    
    @staticmethod
    def check_duplicates(
        df: DataFrame,
        key_columns: List[str]
    ) -> Tuple[bool, Dict]:
        """
        Verifica registros duplicados.
        
        Args:
            df: DataFrame para verificar
            key_columns: Colunas que definem unicidade
            
        Returns:
            Tupla de (passou, detalhes)
        """
        total_rows = df.count()
        unique_rows = df.select(key_columns).distinct().count()
        duplicate_count = total_rows - unique_rows
        
        passed = duplicate_count == 0
        
        details = {
            "total_rows": total_rows,
            "unique_rows": unique_rows,
            "duplicate_count": duplicate_count,
            "key_columns": key_columns
        }
        
        return passed, details
    
    @staticmethod
    def check_range(
        df: DataFrame,
        column: str,
        min_value: float = None,
        max_value: float = None
    ) -> Tuple[bool, Dict]:
        """
        Verifica se os valores estão dentro do intervalo esperado.
        
        Args:
            df: DataFrame para verificar
            column: Coluna para validar
            min_value: Valor mínimo permitido
            max_value: Valor máximo permitido
            
        Returns:
            Tupla de (passou, detalhes)
        """
        if column not in df.columns:
            return False, {"error": f"Coluna {column} não encontrada"}
        
        stats = df.select(
            F.min(column).alias("min"),
            F.max(column).alias("max"),
            F.avg(column).alias("avg"),
            F.count(column).alias("count")
        ).collect()[0]
        
        failures = []
        
        if min_value is not None and stats["min"] < min_value:
            failures.append(f"valor mínimo {stats['min']} < {min_value}")
        
        if max_value is not None and stats["max"] > max_value:
            failures.append(f"valor máximo {stats['max']} > {max_value}")
        
        passed = len(failures) == 0
        
        details = {
            "column": column,
            "min": float(stats["min"]) if stats["min"] else None,
            "max": float(stats["max"]) if stats["max"] else None,
            "avg": float(stats["avg"]) if stats["avg"] else None,
            "count": stats["count"],
            "expected_min": min_value,
            "expected_max": max_value,
            "failures": failures
        }
        
        return passed, details
    
    @staticmethod
    def check_completeness(
        df: DataFrame,
        threshold: float = 0.95
    ) -> Tuple[bool, Dict]:
        """
        Verifica completude dos dados (percentual de não-nulos).
        
        Args:
            df: DataFrame para verificar
            threshold: Completude mínima necessária (0-1)
            
        Returns:
            Tupla de (passou, detalhes)
        """
        total_rows = df.count()
        total_cells = total_rows * len(df.columns)
        
        # Contar valores não-nulos por coluna
        completeness_by_column = {}
        total_non_nulls = 0
        
        for col in df.columns:
            non_null_count = df.filter(F.col(col).isNotNull()).count()
            completeness = non_null_count / total_rows if total_rows > 0 else 0
            
            completeness_by_column[col] = {
                "non_null_count": non_null_count,
                "completeness": round(completeness * 100, 2)
            }
            
            total_non_nulls += non_null_count
        
        overall_completeness = total_non_nulls / total_cells if total_cells > 0 else 0
        passed = overall_completeness >= threshold
        
        details = {
            "total_rows": total_rows,
            "total_columns": len(df.columns),
            "total_cells": total_cells,
            "total_non_nulls": total_non_nulls,
            "overall_completeness": round(overall_completeness * 100, 2),
            "threshold_pct": threshold * 100,
            "by_column": completeness_by_column
        }
        
        return passed, details
    
    @staticmethod
    def check_referential_integrity(
        df_child: DataFrame,
        df_parent: DataFrame,
        child_key: str,
        parent_key: str
    ) -> Tuple[bool, Dict]:
        """
        Verifica integridade referencial entre tabelas.
        
        Args:
            df_child: DataFrame filho
            df_parent: DataFrame pai
            child_key: Coluna de chave estrangeira no filho
            parent_key: Coluna de chave primária no pai
            
        Returns:
            Tupla de (passou, detalhes)
        """
        # Encontrar registros órfãos (chaves filhas não presentes no pai)
        orphans = (
            df_child
            .select(child_key)
            .distinct()
            .join(
                df_parent.select(parent_key),
                df_child[child_key] == df_parent[parent_key],
                "left_anti"
            )
        )
        
        orphan_count = orphans.count()
        total_child_keys = df_child.select(child_key).distinct().count()
        
        passed = orphan_count == 0
        
        details = {
            "child_table": "child",
            "parent_table": "parent",
            "child_key": child_key,
            "parent_key": parent_key,
            "total_child_keys": total_child_keys,
            "orphan_count": orphan_count,
            "orphan_sample": [
                row[child_key] for row in orphans.limit(10).collect()
            ]
        }
        
        return passed, details

