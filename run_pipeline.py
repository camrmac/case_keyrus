#!/usr/bin/env python
"""
Script principal para executar o pipeline de dados.
"""
import argparse
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from src.pipeline import DataPipeline
from src.utils.logger import setup_logger


def main():
    """Função principal de execução."""
    parser = argparse.ArgumentParser(
        description='Pipeline de Dados IBGE - ETL Bronze/Silver/Gold'
    )
    
    parser.add_argument(
        '--stage',
        choices=['bronze', 'silver', 'gold', 'full'],
        default='full',
        help='Etapa do pipeline para executar (padrão: full)'
    )
    
    parser.add_argument(
        '--ano-populacao',
        default='2024',
        help='Ano para dados de população (padrão: 2024)'
    )
    
    parser.add_argument(
        '--ano-pib',
        default='2021',
        help='Ano para dados de PIB (padrão: 2021)'
    )
    
    parser.add_argument(
        '--config',
        default='config/config.yaml',
        help='Caminho para o arquivo de configuração'
    )
    
    args = parser.parse_args()
    
    # Configurar logger
    logger = setup_logger('main')
    
    logger.info("=" * 80)
    logger.info("Pipeline de Dados IBGE - Iniciando")
    logger.info("=" * 80)
    logger.info(f"Etapa: {args.stage}")
    logger.info(f"Ano População: {args.ano_populacao}")
    logger.info(f"Ano PIB: {args.ano_pib}")
    logger.info("=" * 80)
    
    try:
        # Inicializar pipeline
        pipeline = DataPipeline(config_path=args.config)
        
        # Executar etapa solicitada
        if args.stage == 'bronze':
            results = pipeline.run_bronze(args.ano_populacao, args.ano_pib)
            logger.info(f"Bronze completo: {results['metrics']}")
            
        elif args.stage == 'silver':
            results = pipeline.run_silver()
            logger.info(f"Silver completo: {results['metrics']}")
            
        elif args.stage == 'gold':
            results = pipeline.run_gold()
            logger.info(f"Gold completo: {results['metrics']}")
            
        else:  # full
            results = pipeline.run_full_pipeline(args.ano_populacao, args.ano_pib)
            logger.info(f"Pipeline completo finalizado: {results['overall_metrics']}")
        
        logger.info("=" * 80)
        logger.info("Pipeline finalizado com sucesso!")
        logger.info("=" * 80)
        
        # Parar pipeline
        pipeline.stop()
        
        return 0
        
    except Exception as e:
        logger.error(f"Pipeline falhou: {str(e)}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())

