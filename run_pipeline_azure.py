#!/usr/bin/env python
"""
Script para executar o pipeline de dados no Azure.
Configura conexão Azure ADLS Gen2 e executa o pipeline.
"""
import argparse
import sys
import os
from pathlib import Path
from dotenv import load_dotenv

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from src.pipeline import DataPipeline
from src.utils.logger import setup_logger
from src.utils.spark_utils import create_spark_session, load_config
from src.utils.azure_utils import (
    configure_azure_storage,
    test_azure_connection,
    create_azure_directory_structure,
    substitute_env_vars
)


def main():
    """Função principal de execução para pipeline Azure."""
    
    # Carregar variáveis de ambiente
    load_dotenv()
    
    parser = argparse.ArgumentParser(
        description='Pipeline de Dados IBGE - Deploy Azure ADLS Gen2'
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
        default='config/config_azure.yaml',
        help='Caminho para o arquivo de configuração Azure'
    )
    
    parser.add_argument(
        '--test-connection',
        action='store_true',
        help='Testar conexão Azure e sair'
    )
    
    parser.add_argument(
        '--create-structure',
        action='store_true',
        help='Criar estrutura de diretórios no Azure e sair'
    )
    
    args = parser.parse_args()
    
    # Configurar logger
    logger = setup_logger('azure_pipeline')
    
    logger.info("=" * 80)
    logger.info("Pipeline de Dados Keyrus - Deploy Azure")
    logger.info("=" * 80)
    
    try:
        # Carregar e processar configuração
        logger.info("Carregando configuração...")
        config = load_config(args.config)
        config = substitute_env_vars(config)
        
        # Obter configuração Azure
        azure_config = config.get('azure', {})
        
        if not azure_config.get('enabled', False):
            logger.warning("Azure não está habilitado na configuração!")
            logger.info("Defina azure.enabled: true no arquivo de configuração")
            return 1
        
        storage_account = azure_config['storage']['account_name']
        container = azure_config['storage']['container']
        auth_method = azure_config['storage']['auth_method']
        
        logger.info(f"Storage Account: {storage_account}")
        logger.info(f"Container: {container}")
        logger.info(f"Método de Autenticação: {auth_method}")
        
        # Verificar se exportação SQL está habilitada
        db_enabled = config.get('database', {}).get('enabled', False)
        
        # Criar sessão Spark com suporte Azure e JDBC (se necessário)
        logger.info("Criando sessão Spark com bibliotecas Azure...")
        if db_enabled:
            logger.info("Exportação SQL habilitada - incluindo drivers JDBC...")
        logger.info("Isso pode levar alguns minutos na primeira execução (baixando bibliotecas)...")
        spark = create_spark_session(
            app_name=config['spark']['app_name'],
            config=config['spark'].get('config', {}),
            enable_azure=True,  # Habilitar bibliotecas Azure
            enable_jdbc=db_enabled  # Habilitar drivers JDBC se exportação SQL estiver ativa
        )
        
        # Configurar acesso Azure Storage
        logger.info("Configurando acesso ao Azure Storage...")
        
        auth_kwargs = {}
        if auth_method == "account_key":
            auth_kwargs['account_key'] = os.getenv('AZURE_STORAGE_ACCOUNT_KEY')
        elif auth_method == "service_principal":
            auth_kwargs['client_id'] = os.getenv('AZURE_CLIENT_ID')
            auth_kwargs['client_secret'] = os.getenv('AZURE_CLIENT_SECRET')
            auth_kwargs['tenant_id'] = os.getenv('AZURE_TENANT_ID')
        elif auth_method == "sas":
            auth_kwargs['sas_token'] = os.getenv('AZURE_STORAGE_SAS_TOKEN')
        
        configure_azure_storage(
            spark,
            storage_account,
            auth_method,
            container,
            **auth_kwargs
        )
        
        # Testar conexão
        logger.info("Testando conexão Azure...")
        if not test_azure_connection(spark, storage_account, container):
            logger.error("Falha ao conectar ao Azure Storage!")
            logger.error("Por favor, verifique suas credenciais e configuração.")
            return 1
        
        # Se apenas testando conexão, sair aqui
        if args.test_connection:
            logger.info("Teste de conexão bem-sucedido!")
            spark.stop()
            return 0
        
        # Criar estrutura de diretórios se solicitado
        if args.create_structure:
            logger.info("Criando estrutura de diretórios...")
            create_azure_directory_structure(spark, storage_account, container)
            logger.info("Estrutura de diretórios criada!")
            spark.stop()
            return 0
        
        # Inicializar pipeline
        logger.info("=" * 80)
        logger.info("Iniciando Execução do Pipeline")
        logger.info("=" * 80)
        logger.info(f"Etapa: {args.stage}")
        logger.info(f"Ano População: {args.ano_populacao}")
        logger.info(f"Ano PIB: {args.ano_pib}")
        logger.info("=" * 80)
        
        # Salvar configuração modificada temporariamente
        import tempfile
        import yaml
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config, f)
            temp_config_path = f.name
        
        try:
            # Inicializar pipeline com sessão Spark pré-configurada
            pipeline = DataPipeline(config_path=temp_config_path, spark=spark)
            
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
            
        finally:
            # Limpar configuração temporária
            os.unlink(temp_config_path)
        
        return 0
        
    except Exception as e:
        logger.error(f"Pipeline falhou: {str(e)}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())

