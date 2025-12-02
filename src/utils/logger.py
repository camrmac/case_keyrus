"""
Configuração e utilitários de logging.
"""
import logging
import logging.config
from pathlib import Path
from typing import Optional
import yaml


def setup_logger(
    name: str,
    config_path: Optional[str] = None,
    level: str = "INFO"
) -> logging.Logger:
    """
    Configura e retorna uma instância de logger.
    
    Args:
        name: Nome do logger
        config_path: Caminho para o arquivo YAML de configuração de logging
        level: Nível de logging padrão
        
    Returns:
        Instância de logger configurada
    """
    if config_path and Path(config_path).exists():
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
            
        # Criar diretório de logs se não existir
        logs_dir = Path("logs")
        logs_dir.mkdir(exist_ok=True)
        
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(
            level=getattr(logging, level.upper()),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    
    return logging.getLogger(name)


class PipelineLogger:
    """
    Classe wrapper para logging estruturado no pipeline.
    """
    
    def __init__(self, name: str):
        """
        Inicializa o logger do pipeline.
        
        Args:
            name: Nome do logger
        """
        self.logger = setup_logger(name)
    
    def log_stage_start(self, stage: str, details: Optional[dict] = None) -> None:
        """Registra o início de uma etapa do pipeline."""
        msg = f"Iniciando etapa: {stage}"
        if details:
            msg += f" | Detalhes: {details}"
        self.logger.info(msg)
    
    def log_stage_end(self, stage: str, metrics: Optional[dict] = None) -> None:
        """Registra a conclusão de uma etapa do pipeline."""
        msg = f"Etapa concluída: {stage}"
        if metrics:
            msg += f" | Métricas: {metrics}"
        self.logger.info(msg)
    
    def log_error(self, stage: str, error: Exception, context: Optional[dict] = None) -> None:
        """Registra um erro com contexto."""
        msg = f"Erro na etapa '{stage}': {str(error)}"
        if context:
            msg += f" | Contexto: {context}"
        self.logger.error(msg, exc_info=True)
    
    def log_quality_check(self, check_name: str, passed: bool, details: dict) -> None:
        """Registra resultados de verificação de qualidade de dados."""
        status = "PASSOU" if passed else "FALHOU"
        msg = f"Verificação de Qualidade [{check_name}]: {status} | {details}"
        
        if passed:
            self.logger.info(msg)
        else:
            self.logger.warning(msg)
    
    def log_data_stats(self, stage: str, stats: dict) -> None:
        """Registra estatísticas de dados."""
        msg = f"Estatísticas de Dados [{stage}]: {stats}"
        self.logger.info(msg)

