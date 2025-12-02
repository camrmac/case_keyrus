"""
Utilitários Azure para integração ADLS Gen2.
"""
import os
from typing import Dict, Optional
from pyspark.sql import SparkSession
from pathlib import Path


def configure_azure_storage(
    spark: SparkSession,
    storage_account: str,
    auth_method: str = "account_key",
    container: str = "datalake",
    **kwargs
) -> SparkSession:
    """
    Configura Spark para acessar Azure Data Lake Storage Gen2.
    
    Args:
        spark: SparkSession para configurar
        storage_account: Nome da Conta de Armazenamento Azure
        auth_method: Método de autenticação (account_key, service_principal, sas, managed_identity)
        container: Nome do container
        **kwargs: Parâmetros adicionais de autenticação
        
    Returns:
        SparkSession configurada
    """
    
    if auth_method == "account_key":
        # Método 1: Account Key (mais simples)
        account_key = kwargs.get('account_key') or os.getenv('AZURE_STORAGE_ACCOUNT_KEY')
        
        if not account_key:
            raise ValueError(
                "Chave de conta não fornecida. Defina a variável de ambiente AZURE_STORAGE_ACCOUNT_KEY "
                "ou passe o parâmetro account_key."
            )
        
        # Definir configuração Hadoop para Azure ADLS Gen2
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        hadoop_conf.set(
            f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
            account_key
        )
        
        # Também definir para Spark conf (abordagem de segurança dupla)
        spark.conf.set(
            f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
            account_key
        )
        
        # Definir a classe de implementação para abfss (importante!)
        hadoop_conf.set("fs.abfss.impl", "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem")
        hadoop_conf.set("fs.AbstractFileSystem.abfss.impl", "org.apache.hadoop.fs.azurebfs.Abfs")
        
        print(f"✓ Azure Storage configurado com Account Key para: {storage_account}")
    
    elif auth_method == "service_principal":
        # Método 2: Service Principal (OAuth)
        client_id = kwargs.get('client_id') or os.getenv('AZURE_CLIENT_ID')
        client_secret = kwargs.get('client_secret') or os.getenv('AZURE_CLIENT_SECRET')
        tenant_id = kwargs.get('tenant_id') or os.getenv('AZURE_TENANT_ID')
        
        if not all([client_id, client_secret, tenant_id]):
            raise ValueError(
                "Credenciais do Service Principal ausentes. Defina as variáveis de ambiente AZURE_CLIENT_ID, "
                "AZURE_CLIENT_SECRET e AZURE_TENANT_ID."
            )
        
        spark.conf.set(
            f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net",
            "OAuth"
        )
        spark.conf.set(
            f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
        )
        spark.conf.set(
            f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net",
            client_id
        )
        spark.conf.set(
            f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net",
            client_secret
        )
        spark.conf.set(
            f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net",
            f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
        )
        
        print(f"✓ Azure Storage configurado com Service Principal para: {storage_account}")
    
    elif auth_method == "sas":
        # Método 3: Token SAS
        sas_token = kwargs.get('sas_token') or os.getenv('AZURE_STORAGE_SAS_TOKEN')
        
        if not sas_token:
            raise ValueError(
                "Token SAS não fornecido. Defina a variável de ambiente AZURE_STORAGE_SAS_TOKEN "
                "ou passe o parâmetro sas_token."
            )
        
        spark.conf.set(
            f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net",
            "SAS"
        )
        spark.conf.set(
            f"fs.azure.sas.token.provider.type.{storage_account}.dfs.core.windows.net",
            "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider"
        )
        spark.conf.set(
            f"fs.azure.sas.fixed.token.{storage_account}.dfs.core.windows.net",
            sas_token
        )
        
        print(f"✓ Azure Storage configurado com Token SAS para: {storage_account}")
    
    elif auth_method == "managed_identity":
        # Método 4: Managed Identity (para Databricks/Azure VMs)
        spark.conf.set(
            f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net",
            "OAuth"
        )
        spark.conf.set(
            f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
            "org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider"
        )
        
        print(f"✓ Azure Storage configurado com Managed Identity para: {storage_account}")
    
    else:
        raise ValueError(
            f"Método de autenticação desconhecido: {auth_method}. "
            "Use: account_key, service_principal, sas ou managed_identity"
        )
    
    return spark


def get_azure_path(
    storage_account: str,
    container: str,
    path: str = ""
) -> str:
    """
    Constrói caminho Azure ABFS.
    
    Args:
        storage_account: Nome da conta de armazenamento
        container: Nome do container
        path: Caminho dentro do container
        
    Returns:
        Caminho ABFS completo
    """
    base = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
    
    if path:
        path = path.lstrip("/")
        return f"{base}/{path}"
    
    return base


def test_azure_connection(
    spark: SparkSession,
    storage_account: str,
    container: str
) -> bool:
    """
    Testa conexão com Azure Storage.
    
    Args:
        spark: SparkSession configurada
        storage_account: Nome da conta de armazenamento
        container: Nome do container
        
    Returns:
        True se a conexão foi bem-sucedida
    """
    try:
        test_path = get_azure_path(storage_account, container)
        
        # Tentar criar um DataFrame simples e escrever no Azure (melhor forma de testar)
        # Criar um DataFrame de teste
        test_df = spark.createDataFrame([("test",)], ["value"])
        
        # Tentar escrever no Azure (isso validará a conexão completa)
        test_file_path = f"{test_path}/.test_connection"
        test_df.coalesce(1).write.mode("overwrite").text(test_file_path)
        
        # Se a escrita funcionou, tentar ler de volta
        spark.read.text(test_file_path)
        
        # Limpar arquivo de teste
        try:
            hadoop_conf = spark._jsc.hadoopConfiguration()
            hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(test_file_path)
            fs = hadoop_path.getFileSystem(hadoop_conf)
            fs.delete(hadoop_path, True)
        except:
            pass  # Limpeza é opcional
        
        print(f"✓ Conexão com Azure Storage bem-sucedida: {test_path}")
        return True
        
    except Exception as e:
        error_msg = str(e)
        # Fornecer mensagens de erro mais úteis
        if "Wrong FS" in error_msg or "expected: file:///" in error_msg:
            print(f"✗ Falha na conexão com Azure Storage: FileSystem não configurado corretamente")
            print(f"  Erro: {error_msg}")
            print(f"  Dica: Isso geralmente significa que o Spark não conseguiu carregar as bibliotecas Azure")
        elif "AuthenticationFailed" in error_msg or "403" in error_msg:
            print(f"✗ Falha na conexão com Azure Storage: Erro de autenticação")
            print(f"  Verifique sua AZURE_STORAGE_ACCOUNT_KEY no arquivo .env")
        elif "ContainerNotFound" in error_msg or "404" in error_msg:
            print(f"✗ Falha na conexão com Azure Storage: Container '{container}' não encontrado")
            print(f"  Crie o container no Portal Azure ou usando: --create-structure")
        else:
            print(f"✗ Falha na conexão com Azure Storage: {error_msg}")
        return False


def create_azure_directory_structure(
    spark: SparkSession,
    storage_account: str,
    container: str
) -> None:
    """
    Cria estrutura de diretórios do data lake no Azure.
    
    Args:
        spark: SparkSession configurada
        storage_account: Nome da conta de armazenamento
        container: Nome do container
    """
    layers = ["bronze", "silver", "gold"]
    
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jsc.hadoopConfiguration()
    )
    
    for layer in layers:
        layer_path = get_azure_path(storage_account, container, layer)
        hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(layer_path)
        
        if not fs.exists(hadoop_path):
            fs.mkdirs(hadoop_path)
            print(f"✓ Diretório criado: {layer_path}")
        else:
            print(f"✓ Diretório existe: {layer_path}")


def substitute_env_vars(config: Dict, prefix: str = "${", suffix: str = "}") -> Dict:
    """
    Substitui variáveis de ambiente na configuração.
    
    Args:
        config: Dicionário de configuração
        prefix: Prefixo da variável (padrão: "${")
        suffix: Sufixo da variável (padrão: "}")
        
    Returns:
        Configuração com valores substituídos
    """
    import re
    
    def _substitute(value):
        if isinstance(value, str):
            # Encontrar todos os padrões ${VAR_NAME}
            pattern = re.escape(prefix) + r'(\w+)' + re.escape(suffix)
            
            def replace_var(match):
                var_name = match.group(1)
                return os.getenv(var_name, match.group(0))
            
            return re.sub(pattern, replace_var, value)
        
        elif isinstance(value, dict):
            return {k: _substitute(v) for k, v in value.items()}
        
        elif isinstance(value, list):
            return [_substitute(item) for item in value]
        
        return value
    
    return _substitute(config)

