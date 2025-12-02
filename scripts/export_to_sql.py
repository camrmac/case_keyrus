#!/usr/bin/env python
"""
Script para exportar indicadores da camada Gold para tabelas SQL.
"""
import argparse
import sys
from pathlib import Path

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.delivery.sql_exporter import SQLExporter
from src.utils.logger import setup_logger
from src.utils.spark_utils import create_spark_session, load_config


def main():
    """Função principal de exportação SQL."""
    parser = argparse.ArgumentParser(
        description='Exportar Indicadores Gold para Tabelas SQL'
    )
    
    parser.add_argument(
        '--config',
        default='config/config.yaml',
        help='Caminho para o arquivo de configuração'
    )
    
    parser.add_argument(
        '--table',
        default=None,
        help='Exportar apenas uma tabela específica (ex: fact_indicadores_municipio)'
    )
    
    parser.add_argument(
        '--create-tables',
        action='store_true',
        help='Criar tabelas se não existirem'
    )
    
    args = parser.parse_args()
    
    # Configurar logger
    logger = setup_logger('sql_export')
    
    logger.info("=" * 80)
    logger.info("Exportação de Indicadores para SQL")
    logger.info("=" * 80)
    
    try:
        # Carregar configuração
        config = load_config(args.config)
        
        # Verificar se exportação SQL está habilitada
        if not config.get('database', {}).get('enabled', False):
            logger.error("Exportação SQL não está habilitada!")
            logger.info("Configure database.enabled: true no arquivo de configuração")
            return 1
        
        # Criar sessão Spark
        logger.info("Criando sessão Spark...")
        spark = create_spark_session(
            app_name="SQL_Export",
            config=config['spark'].get('config', {})
        )
        
        # Inicializar exportador
        gold_path = config['datalake']['base_path'] + "/" + config['datalake']['layers']['gold']['path']
        exporter = SQLExporter(spark, config, gold_path)
        
        # Criar tabelas se solicitado
        if args.create_tables:
            logger.info("Criando tabelas SQL...")
            exporter.create_tables_if_not_exists()
        
        # Exportar
        if args.table:
            # Exportar tabela específica
            from src.utils.spark_utils import read_parquet
            
            logger.info(f"Exportando tabela: {args.table}")
            df = read_parquet(spark, f"{gold_path}/{args.table}")
            
            schema = config.get('database', {}).get('schema', 'dbo')
            success = exporter.export_table(
                table_name=args.table,
                df=df,
                schema=schema
            )
            
            if success:
                logger.info(f"✅ Tabela {args.table} exportada com sucesso!")
            else:
                logger.error(f"❌ Falha ao exportar {args.table}")
                return 1
        else:
            # Exportar todas as tabelas
            logger.info("Exportando todas as tabelas...")
            results = exporter.export_all_indicators()
            
            # Resumo
            successful = sum(1 for v in results.values() if v)
            total = len(results)
            
            logger.info("=" * 80)
            logger.info(f"✅ Exportação concluída: {successful}/{total} tabelas")
            logger.info("=" * 80)
            
            for table, success in results.items():
                status = "✅" if success else "❌"
                logger.info(f"{status} {table}")
        
        # Parar Spark
        spark.stop()
        
        logger.info("=" * 80)
        logger.info("Exportação finalizada!")
        logger.info("=" * 80)
        
        return 0
        
    except Exception as e:
        logger.error(f"Erro na exportação: {str(e)}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())

