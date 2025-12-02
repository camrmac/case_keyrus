"""
Salva indicadores da camada Gold em tabelas SQL.
"""
from typing import Dict, Optional, List
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from src.utils.logger import PipelineLogger
from src.utils.spark_utils import read_parquet, is_cloud_storage_path, normalize_path


class SQLExporter:
    """
    Exporta para salvar indicadores da camada Gold em tabelas SQL.
    
    Suporta múltiplos bancos de dados:
    - SQL Server
    - PostgreSQL
    - Azure SQL Database
    - Azure Synapse Analytics
    - MySQL
    """
    
    def __init__(
        self,
        spark: SparkSession,
        config: Dict,
        gold_path: str
    ):
        """
        Inicializa o exportador SQL.
        
        Args:
            spark: SparkSession
            config: Dicionário de configuração
            gold_path: Caminho para a camada Gold
        """
        self.spark = spark
        self.config = config
        # Normalizar gold_path - pode ser caminho local ou Azure (abfss://)
        if isinstance(gold_path, str):
            # Se for caminho de nuvem (Azure), manter como string
            if is_cloud_storage_path(gold_path):
                self.gold_path = normalize_path(gold_path)
            else:
                # Se for caminho local, converter para Path
                self.gold_path = Path(gold_path)
        else:
            self.gold_path = gold_path
        self.logger = PipelineLogger("delivery")
        
        # Configuração de banco de dados
        self.db_config = config.get('database', {})
        self.enabled = self.db_config.get('enabled', False)
        
        if not self.enabled:
            self.logger.logger.info("Exportação SQL desabilitada na configuração")
            return
        
        # Validar configuração antes de continuar
        if not self._validate_config():
            self.logger.logger.error("Configuração de banco de dados inválida. Exportação SQL desabilitada.")
            self.enabled = False
            return
        
        # Configurar conexão JDBC
        try:
            self._configure_jdbc()
        except Exception as e:
            self.logger.logger.error(f"Falha ao configurar conexão JDBC: {str(e)}")
            self.enabled = False
            raise
    
    def _configure_jdbc(self):
        """Configura conexão JDBC baseado no tipo de banco."""
        db_type = self.db_config.get('type', 'sqlserver').lower()
        
        if db_type == 'sqlserver':
            self.jdbc_url = self._build_sqlserver_url()
            self.driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        elif db_type == 'postgresql':
            self.jdbc_url = self._build_postgresql_url()
            self.driver = "org.postgresql.Driver"
        elif db_type == 'azuresql':
            self.jdbc_url = self._build_azuresql_url()
            self.driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        elif db_type == 'synapse':
            self.jdbc_url = self._build_synapse_url()
            self.driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        elif db_type == 'mysql':
            self.jdbc_url = self._build_mysql_url()
            self.driver = "com.mysql.cj.jdbc.Driver"
        else:
            raise ValueError(f"Tipo de banco não suportado: {db_type}")
        
        # Propriedades de conexão
        user = self.db_config.get('user', '').strip()
        password = self.db_config.get('password', '').strip()
        
        if not user or not password:
            raise ValueError("Usuário e senha do banco de dados são obrigatórios")
        
        self.connection_properties = {
            "driver": self.driver,
            "user": user,
            "password": password
        }
        
        # Nota: Não podemos modificar spark.jars após a sessão ser criada
        # Os drivers JDBC devem ser configurados na criação da sessão Spark
        # ou estar disponíveis no classpath. O Spark tentará carregar o driver automaticamente.
    
    def _build_sqlserver_url(self) -> str:
        """Constrói URL JDBC para SQL Server."""
        server = self.db_config.get('server')
        database = self.db_config.get('database')
        port = self.db_config.get('port', 1433)
        
        return f"jdbc:sqlserver://{server}:{port};databaseName={database};encrypt=true;trustServerCertificate=true"
    
    def _build_postgresql_url(self) -> str:
        """Constrói URL JDBC para PostgreSQL."""
        server = self.db_config.get('server')
        database = self.db_config.get('database')
        port = self.db_config.get('port', 5432)
        
        return f"jdbc:postgresql://{server}:{port}/{database}"
    
    def _build_azuresql_url(self) -> str:
        """Constrói URL JDBC para Azure SQL Database."""
        server = self.db_config.get('server', '').strip()
        database = self.db_config.get('database', '').strip()
        
        if not server:
            raise ValueError("Campo 'server' é obrigatório para Azure SQL Database. Exemplo: seu-servidor.database.windows.net")
        if not database:
            raise ValueError("Campo 'database' é obrigatório")
        
        # Se o servidor já contém .database.windows.net, usar como está
        # Caso contrário, adicionar o sufixo
        if '.database.windows.net' not in server:
            server = f"{server}.database.windows.net"
        
        return f"jdbc:sqlserver://{server}:1433;database={database};encrypt=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30"
    
    def _build_synapse_url(self) -> str:
        """Constrói URL JDBC para Azure Synapse Analytics."""
        workspace = self.db_config.get('workspace', '').strip()
        sql_pool = self.db_config.get('sql_pool', '').strip()
        
        if not workspace:
            raise ValueError("Campo 'workspace' é obrigatório para Azure Synapse. Exemplo: seu-workspace")
        if not sql_pool:
            raise ValueError("Campo 'sql_pool' é obrigatório para Azure Synapse")
        
        return f"jdbc:sqlserver://{workspace}.sql.azuresynapse.net:1433;database={sql_pool};encrypt=true;hostNameInCertificate=*.sql.azuresynapse.net;loginTimeout=30"
    
    def _build_mysql_url(self) -> str:
        """Constrói URL JDBC para MySQL."""
        server = self.db_config.get('server')
        database = self.db_config.get('database')
        port = self.db_config.get('port', 3306)
        
        return f"jdbc:mysql://{server}:{port}/{database}?useSSL=true"
    
    def _get_jdbc_jars(self) -> str:
        """Retorna caminho para JARs JDBC necessários."""
        db_type = self.db_config.get('type', 'sqlserver').lower()
        
        # Caminhos padrão para JARs (podem ser configurados)
        jars = {
            'sqlserver': self.db_config.get('jdbc_jar', 'sqljdbc42.jar'),
            'azuresql': self.db_config.get('jdbc_jar', 'mssql-jdbc-12.4.2.jre8.jar'),
            'synapse': self.db_config.get('jdbc_jar', 'mssql-jdbc-12.4.2.jre8.jar'),
            'postgresql': self.db_config.get('jdbc_jar', 'postgresql-42.7.1.jar'),
            'mysql': self.db_config.get('jdbc_jar', 'mysql-connector-java-8.0.33.jar')
        }
        
        return jars.get(db_type, '')
    
    def export_table(
        self,
        table_name: str,
        df: DataFrame,
        mode: str = "overwrite",
        schema: Optional[str] = None
    ) -> bool:
        """
        Exporta um DataFrame para uma tabela SQL.
        
        Args:
            table_name: Nome da tabela de destino
            df: DataFrame para exportar
            mode: Modo de escrita (overwrite, append, ignore, error)
            schema: Schema opcional (se None, usa schema padrão do banco)
            
        Returns:
            True se exportação foi bem-sucedida
        """
        if not self.enabled:
            self.logger.logger.warning("Exportação SQL desabilitada, pulando exportação")
            return False
        
        self.logger.log_stage_start(f"export_sql_{table_name}")
        
        try:
            # Preparar nome da tabela com schema
            full_table_name = f"{schema}.{table_name}" if schema else table_name
            
            # Contar registros antes
            record_count = df.count()
            
            # Escrever no banco
            df.write \
                .format("jdbc") \
                .option("url", self.jdbc_url) \
                .option("dbtable", full_table_name) \
                .option("driver", self.driver) \
                .option("user", self.connection_properties["user"]) \
                .option("password", self.connection_properties["password"]) \
                .mode(mode) \
                .save()
            
            self.logger.log_stage_end(f"export_sql_{table_name}", {
                "records": record_count,
                "table": full_table_name,
                "mode": mode
            })
            
            return True
            
        except Exception as e:
            error_msg = str(e)
            error_type = type(e).__name__
            
            # Log detalhado do erro com mensagens amigáveis
            self.logger.logger.error(f"❌ Erro ao exportar {table_name} para SQL")
            self.logger.logger.error(f"   Tipo: {error_type}")
            self.logger.logger.error(f"   Mensagem: {error_msg}")
            
            # Mensagens de erro mais amigáveis e específicas
            if "No suitable driver" in error_msg or "Driver" in error_msg or "java.lang.ClassNotFoundException" in error_msg:
                self.logger.logger.error("   → PROBLEMA: Driver JDBC não encontrado")
                self.logger.logger.error("   → SOLUÇÃO: O Spark precisa do driver JDBC do SQL Server")
                self.logger.logger.error("   → Tente adicionar o driver na criação da sessão Spark ou use spark.jars.packages")
            elif "Login failed" in error_msg or "authentication" in error_msg.lower() or "Invalid credentials" in error_msg:
                self.logger.logger.error("   → PROBLEMA: Erro de autenticação")
                self.logger.logger.error("   → SOLUÇÃO: Verifique usuário e senha no config ou variáveis de ambiente")
            elif "Connection refused" in error_msg or "timeout" in error_msg.lower() or "Network" in error_msg:
                self.logger.logger.error("   → PROBLEMA: Erro de conexão com o servidor")
                self.logger.logger.error("   → SOLUÇÃO: Verifique se o servidor está acessível e o firewall permite conexões")
            elif "Table" in error_msg and "already exists" in error_msg:
                self.logger.logger.error("   → PROBLEMA: Tabela já existe")
                self.logger.logger.error("   → SOLUÇÃO: Use write_mode: 'append' ou 'overwrite' no config")
            elif "PATH_NOT_FOUND" in error_msg or "does not exist" in error_msg.lower():
                self.logger.logger.error("   → PROBLEMA: Arquivo/tabela não encontrado na camada Gold")
                self.logger.logger.error("   → SOLUÇÃO: Execute o pipeline completo primeiro para gerar os dados")
            else:
                self.logger.logger.error("   → Verifique os logs detalhados acima para mais informações")
            
            self.logger.log_error(f"export_sql_{table_name}", e)
            raise
    
    def export_all_indicators(self) -> Dict[str, bool]:
        """
        Exporta todos os indicadores da camada Gold para tabelas SQL.
        
        Returns:
            Dicionário com status de exportação de cada tabela
        """
        if not self.enabled:
            self.logger.logger.warning("Exportação SQL desabilitada")
            return {}
        
        self.logger.log_stage_start("export_all_indicators_sql")
        
        results = {}
        schema = self.db_config.get('schema', 'dbo')
        
        try:
            # Mapeamento de tabelas Gold para nomes SQL
            # Tabelas obrigatórias (sempre devem existir)
            required_tables = {
                'dim_tempo': 'dim_tempo',
                'fact_indicadores_brasil': 'fact_indicadores_brasil'
            }
            
            # Tabelas opcionais (podem não existir se IBGE estiver desabilitado)
            optional_tables = {
                'dim_municipio': 'dim_municipio',
                'fact_indicadores_municipio': 'fact_indicadores_municipio',
                'fact_indicadores_uf': 'fact_indicadores_uf'
            }
            
            # Combinar todos os mapeamentos
            tables_mapping = {**required_tables, **optional_tables}
            
            # Exportar cada tabela
            for gold_table, sql_table in tables_mapping.items():
                try:
                    # Construir caminho - suporta tanto Path quanto string (Azure)
                    if isinstance(self.gold_path, Path):
                        table_path = str(self.gold_path / gold_table)
                    else:
                        # Caminho Azure ou string - usar concatenação
                        base_path = self.gold_path.rstrip('/')
                        table_path = f"{base_path}/{gold_table}"
                    
                    # Verificar se a tabela existe antes de tentar ler
                    is_required = gold_table in required_tables
                    self.logger.logger.info(f"Tentando ler tabela Gold: {table_path} {'(OBRIGATÓRIA)' if is_required else '(OPCIONAL)'}")
                    
                    # Ler da camada Gold
                    try:
                        df = read_parquet(self.spark, table_path)
                        record_count = df.count()
                        self.logger.logger.info(f"✓ Tabela {gold_table} encontrada com {record_count} registros")
                    except Exception as read_error:
                        # Tabela pode não existir (ex: se IBGE estiver desabilitado)
                        error_msg = str(read_error)
                        if "PATH_NOT_FOUND" in error_msg or "does not exist" in error_msg.lower():
                            if is_required:
                                # Tabela obrigatória não encontrada - erro crítico
                                self.logger.logger.error(f"❌ ERRO CRÍTICO: Tabela obrigatória {gold_table} não encontrada na camada Gold!")
                                self.logger.logger.error(f"Caminho esperado: {table_path}")
                                raise Exception(f"Tabela obrigatória {gold_table} não encontrada. Verifique se a camada Gold foi construída corretamente.")
                            else:
                                # Tabela opcional não encontrada - apenas avisar
                                self.logger.logger.warning(f"Tabela opcional {gold_table} não encontrada na camada Gold (pode ser normal se IBGE estiver desabilitado). Pulando exportação.")
                                results[sql_table] = False
                                continue
                        else:
                            raise  # Re-raise se for outro tipo de erro
                    
                    # Exportar para SQL
                    self.logger.logger.info(f"Exportando {sql_table} para SQL...")
                    success = self.export_table(
                        table_name=sql_table,
                        df=df,
                        mode=self.db_config.get('write_mode', 'overwrite'),
                        schema=schema
                    )
                    
                    results[sql_table] = success
                    
                except Exception as e:
                    error_msg = str(e)
                    is_required = gold_table in required_tables
                    if is_required:
                        # Erro em tabela obrigatória - logar como erro crítico
                        self.logger.logger.error(f"❌ ERRO CRÍTICO ao exportar tabela obrigatória {gold_table} → {sql_table}: {error_msg}")
                    else:
                        self.logger.logger.error(f"❌ Falha ao exportar {gold_table} → {sql_table}: {error_msg}")
                    # Log detalhado do erro para diagnóstico
                    import traceback
                    self.logger.logger.debug(f"Traceback completo: {traceback.format_exc()}")
                    results[sql_table] = False
                    # Se for tabela obrigatória, não continuar (ou continuar mas marcar como falha crítica)
                    if is_required:
                        self.logger.logger.error(f"Exportação de {gold_table} falhou, mas continuando com outras tabelas...")
            
            # Resumo
            successful = sum(1 for v in results.values() if v)
            total = len(results)
            
            self.logger.log_stage_end("export_all_indicators_sql", {
                "total_tables": total,
                "successful": successful,
                "failed": total - successful
            })
            
            return results
            
        except Exception as e:
            self.logger.log_error("export_all_indicators_sql", e)
            raise
    
    def _validate_config(self) -> bool:
        """
        Valida se a configuração do banco de dados está completa.
        
        Returns:
            True se configuração é válida, False caso contrário
        """
        db_type = self.db_config.get('type', '').lower()
        
        # Validar tipo
        valid_types = ['sqlserver', 'postgresql', 'azuresql', 'synapse', 'mysql']
        if db_type not in valid_types:
            self.logger.logger.error(f"Tipo de banco inválido: {db_type}. Tipos válidos: {valid_types}")
            return False
        
        # Validar campos obrigatórios comuns
        database = str(self.db_config.get('database', '')).strip()
        user = str(self.db_config.get('user', '')).strip()
        password = str(self.db_config.get('password', '')).strip()
        
        # Verificar se ainda contém variáveis de ambiente não substituídas
        def is_env_var(value):
            """Verifica se o valor ainda é uma variável de ambiente não substituída."""
            return isinstance(value, str) and value.startswith('${') and value.endswith('}')
        
        if not database or is_env_var(database):
            self.logger.logger.error("Campo 'database' é obrigatório e deve estar preenchido")
            if is_env_var(database):
                self.logger.logger.error(f"  Variável de ambiente não encontrada: {database}")
            return False
        
        if not user or is_env_var(user):
            self.logger.logger.error("Campo 'user' é obrigatório e deve estar preenchido")
            if is_env_var(user):
                self.logger.logger.error(f"  Variável de ambiente não encontrada: {user}")
                self.logger.logger.error("  Defina a variável de ambiente no arquivo .env ou preencha diretamente no config")
            return False
        
        if not password or is_env_var(password):
            self.logger.logger.error("Campo 'password' é obrigatório e deve estar preenchido")
            if is_env_var(password):
                self.logger.logger.error(f"  Variável de ambiente não encontrada: {password}")
                self.logger.logger.error("  Defina a variável de ambiente no arquivo .env ou preencha diretamente no config")
            return False
        
        # Validar campos específicos por tipo
        if db_type == 'azuresql':
            server = str(self.db_config.get('server', '')).strip()
            if not server or is_env_var(server):
                self.logger.logger.error("Campo 'server' é obrigatório para Azure SQL Database")
                if is_env_var(server):
                    self.logger.logger.error(f"  Variável de ambiente não encontrada: {server}")
                return False
        elif db_type == 'synapse':
            workspace = str(self.db_config.get('workspace', '')).strip()
            sql_pool = str(self.db_config.get('sql_pool', '')).strip()
            if not workspace or is_env_var(workspace):
                self.logger.logger.error("Campo 'workspace' é obrigatório para Azure Synapse")
                if is_env_var(workspace):
                    self.logger.logger.error(f"  Variável de ambiente não encontrada: {workspace}")
                return False
            if not sql_pool or is_env_var(sql_pool):
                self.logger.logger.error("Campo 'sql_pool' é obrigatório para Azure Synapse")
                if is_env_var(sql_pool):
                    self.logger.logger.error(f"  Variável de ambiente não encontrada: {sql_pool}")
                return False
        elif db_type in ['sqlserver', 'postgresql', 'mysql']:
            server = str(self.db_config.get('server', '')).strip()
            if not server or is_env_var(server):
                self.logger.logger.error(f"Campo 'server' é obrigatório para {db_type}")
                if is_env_var(server):
                    self.logger.logger.error(f"  Variável de ambiente não encontrada: {server}")
                return False
        
        return True
    
    def create_tables_if_not_exists(self):
        """
        Cria tabelas SQL se não existirem.
        
        Usa schema inferido dos DataFrames Gold.
        """
        if not self.enabled:
            return
        
        self.logger.log_stage_start("create_sql_tables")
        
        schema = self.db_config.get('schema', 'dbo')
        
        # Tabelas obrigatórias (sempre devem existir)
        required_tables = {
            'dim_tempo': 'dim_tempo',
            'fact_indicadores_brasil': 'fact_indicadores_brasil'
        }
        
        # Tabelas opcionais (podem não existir se IBGE estiver desabilitado)
        optional_tables = {
            'dim_municipio': 'dim_municipio',
            'fact_indicadores_municipio': 'fact_indicadores_municipio',
            'fact_indicadores_uf': 'fact_indicadores_uf'
        }
        
        # Combinar todos os mapeamentos
        tables_mapping = {**required_tables, **optional_tables}
        
        try:
            for gold_table, sql_table in tables_mapping.items():
                try:
                    # Construir caminho - suporta tanto Path quanto string (Azure)
                    if isinstance(self.gold_path, Path):
                        table_path = str(self.gold_path / gold_table)
                    else:
                        # Caminho Azure ou string - usar concatenação
                        base_path = self.gold_path.rstrip('/')
                        table_path = f"{base_path}/{gold_table}"
                    
                    is_required = gold_table in required_tables
                    self.logger.logger.info(f"Criando tabela SQL {sql_table} a partir de {gold_table} {'(OBRIGATÓRIA)' if is_required else '(OPCIONAL)'}")
                    
                    # Ler schema do DataFrame
                    try:
                        df = read_parquet(self.spark, table_path)
                    except Exception as read_error:
                        error_msg = str(read_error)
                        if "PATH_NOT_FOUND" in error_msg or "does not exist" in error_msg.lower():
                            if is_required:
                                self.logger.logger.error(f"❌ ERRO CRÍTICO: Tabela obrigatória {gold_table} não encontrada!")
                                raise Exception(f"Tabela obrigatória {gold_table} não encontrada. Verifique se a camada Gold foi construída corretamente.")
                            else:
                                self.logger.logger.warning(f"Tabela opcional {gold_table} não encontrada. Pulando criação da tabela SQL {sql_table}.")
                                continue
                        else:
                            raise
                    
                    # Criar tabela usando Spark (se não existir)
                    full_table_name = f"{schema}.{sql_table}" if schema else sql_table
                    
                    # Usar modo 'error' para não sobrescrever se já existir
                    df.write \
                        .format("jdbc") \
                        .option("url", self.jdbc_url) \
                        .option("dbtable", full_table_name) \
                        .option("driver", self.driver) \
                        .option("user", self.connection_properties["user"]) \
                        .option("password", self.connection_properties["password"]) \
                        .mode("error") \
                        .save()
                    
                    self.logger.logger.info(f"Tabela {full_table_name} criada/verificada")
                    
                except Exception as e:
                    # Se erro for "table already exists", está OK
                    if "already exists" in str(e).lower() or "já existe" in str(e).lower():
                        self.logger.logger.info(f"Tabela {sql_table} já existe, pulando criação")
                    else:
                        self.logger.logger.warning(f"Erro ao criar tabela {sql_table}: {str(e)}")
            
            self.logger.log_stage_end("create_sql_tables", {
                "tables_processed": len(tables_mapping)
            })
            
        except Exception as e:
            self.logger.log_error("create_sql_tables", e)
            raise

