"""
Cliente de API para ingestão de dados.
"""
import requests
import time
from typing import Dict, List, Optional
from urllib.parse import urljoin
import json


class IBGEAPIClient:
    """
    Cliente para interagir com as APIs do IBGE.
    
    Gerencia requisições de API, tentativas de retry e tratamento de erros
    para endpoints de dados estatísticos do IBGE.
    """
    
    def __init__(
        self,
        base_url: str = "https://servicodados.ibge.gov.br/api/v3",
        timeout: int = 30,
        retry_attempts: int = 3,
        backoff_factor: float = 2.0
    ):
        """
        Inicializa o cliente de API do IBGE.
        
        Args:
            base_url: URL base da API do IBGE
            timeout: Timeout da requisição em segundos
            retry_attempts: Número de tentativas de retry
            backoff_factor: Fator de backoff exponencial
        """
        self.base_url = base_url
        self.timeout = timeout
        self.retry_attempts = retry_attempts
        self.backoff_factor = backoff_factor
        self.session = requests.Session()
        
    def _make_request(
        self,
        endpoint: str,
        params: Optional[Dict] = None
    ) -> Dict:
        """
        Faz requisição HTTP com lógica de retry.
        
        Args:
            endpoint: Endpoint da API
            params: Parâmetros de query
            
        Returns:
            Resposta JSON como dicionário
            
        Raises:
            requests.exceptions.RequestException: Se todas as tentativas falharem
        """
        url = urljoin(self.base_url, endpoint)
        last_exception = None
        
        for attempt in range(self.retry_attempts):
            try:
                response = self.session.get(
                    url,
                    params=params,
                    timeout=self.timeout
                )
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.RequestException as e:
                last_exception = e
                
                if attempt < self.retry_attempts - 1:
                    wait_time = self.backoff_factor ** attempt
                    time.sleep(wait_time)
                    continue
                    
        raise last_exception
    
    def get_agregado_metadata(self, agregado_id: str) -> Dict:
        """
        Obtém metadados para um agregado específico.
        
        Args:
            agregado_id: ID do agregado (ex: '6579' para população)
            
        Returns:
            Dicionário com metadados
        """
        endpoint = f"/agregados/{agregado_id}/metadados"
        return self._make_request(endpoint)
    
    def get_agregado_periodos(self, agregado_id: str) -> List[str]:
        """
        Obtém períodos disponíveis para um agregado.
        
        Args:
            agregado_id: ID do agregado
            
        Returns:
            Lista de períodos disponíveis
        """
        endpoint = f"/agregados/{agregado_id}/periodos"
        return self._make_request(endpoint)
    
    def get_populacao_municipios(
        self,
        ano: Optional[str] = None
    ) -> List[Dict]:
        """
        Obtém dados de população para todos os municípios.
        
        Args:
            ano: Ano (ex: '2024'). Se None, retorna todos os anos disponíveis.
            
        Returns:
            Lista de registros com dados de população
        """
        agregado_id = "6579"
        endpoint = f"/agregados/{agregado_id}/periodos"
        
        if ano:
            endpoint += f"/{ano}"
        else:
            endpoint += "/202201|202210|2024"  # Obter períodos disponíveis
        
        # Obter dados por municípios (nível 6)
        endpoint += "/variaveis/9324"  # Variável: População residente
        params = {
            "localidades": "N6[all]"  # Todos os municípios (nível 6)
        }
        
        response = self._make_request(endpoint, params)
        return self._parse_ibge_response(response, "populacao")
    
    def get_pib_municipios(
        self,
        ano: Optional[str] = None
    ) -> List[Dict]:
        """
        Obtém dados de PIB para todos os municípios.
        
        Args:
            ano: Ano (ex: '2021'). Se None, retorna todos os anos disponíveis.
            
        Returns:
            Lista de registros com dados de PIB
        """
        agregado_id = "5938"
        endpoint = f"/agregados/{agregado_id}/periodos"
        
        if ano:
            endpoint += f"/{ano}"
        else:
            endpoint += "/2010|2011|2012|2013|2014|2015|2016|2017|2018|2019|2020|2021"
        
        # Obter dados de PIB per capita
        endpoint += "/variaveis/37"  # Variável: PIB per capita
        params = {
            "localidades": "N6[all]"  # Todos os municípios
        }
        
        response = self._make_request(endpoint, params)
        return self._parse_ibge_response(response, "pib")
    
    def _parse_ibge_response(
        self,
        response: List[Dict],
        source: str
    ) -> List[Dict]:
        """
        Analisa a resposta da API do IBGE em formato padronizado.
        
        Args:
            response: Resposta bruta da API
            source: Fonte de dados ('populacao' ou 'pib')
            
        Returns:
            Lista de registros analisados
        """
        records = []
        
        for item in response:
            periodo = item.get('periodo', {})
            variavel = item.get('id', '')
            
            for resultado in item.get('resultados', []):
                classificacoes = resultado.get('classificacoes', [])
                series = resultado.get('series', [])
                
                for serie in series:
                    localidade = serie.get('localidade', {})
                    
                    # Obter o valor - lidar com diferentes estruturas de resposta
                    serie_data = serie.get('serie', {})
                    
                    for ano, valor_dict in serie_data.items():
                        # valor_dict pode ser string ou dict
                        if isinstance(valor_dict, dict):
                            valor = valor_dict.get('valor')
                        else:
                            valor = valor_dict
                        
                        record = {
                            'codigo_municipio': localidade.get('id'),
                            'nome_municipio': localidade.get('nome'),
                            'ano': int(ano) if ano.isdigit() else ano,
                            'valor': float(valor) if valor and valor != '...' else None,
                            'variavel': variavel,
                            'fonte': source,
                            'nivel_territorial': localidade.get('nivel', {}).get('id')
                        }
                        records.append(record)
        
        return records
    
    def get_localidades_info(self, nivel: str = "6") -> List[Dict]:
        """
        Obtém informações sobre localidades (municípios, estados, etc).
        
        Args:
            nivel: Nível territorial
                   '1' - Brasil
                   '2' - Grande Região
                   '3' - Unidade da Federação (UF)
                   '6' - Município
                   
        Returns:
            Lista de informações de localidades
        """
        endpoint = f"/localidades/niveis/N{nivel}"
        return self._make_request(endpoint)
    
    def get_bacen_serie(
        self,
        codigo_serie: int,
        data_inicial: Optional[str] = None,
        data_final: Optional[str] = None,
        anos: Optional[int] = None
    ) -> List[Dict]:
        """
        Busca série temporal do Banco Central do Brasil.
        
        Args:
            codigo_serie: Código da série (ex: 433 para IPCA, 4380 para PIB)
            data_inicial: Data início no formato DD/MM/YYYY (opcional)
            data_final: Data fim no formato DD/MM/YYYY (opcional)
            anos: Número de anos para buscar a partir de hoje (opcional, ex: 5 para últimos 5 anos)
            
        Returns:
            Lista com dados da série no formato [{"data": "01/01/2020", "valor": "0.5"}, ...]
            
        Raises:
            requests.exceptions.RequestException: Se a requisição falhar
        """
        from datetime import datetime, timedelta
        
        url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo_serie}/dados"
        params = {"formato": "json"}
        
        # Se anos foi especificado, calcular data_inicial automaticamente
        if anos is not None:
            data_fim = datetime.now()
            data_ini = data_fim - timedelta(days=anos * 365)
            data_inicial = data_ini.strftime("%d/%m/%Y")
            data_final = data_fim.strftime("%d/%m/%Y")
        
        if data_inicial and data_final:
            params["dataInicial"] = data_inicial
            params["dataFinal"] = data_final
        
        # Headers necessários para evitar erro 406
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json'
        }
        
        try:
            response = self.session.get(url, params=params, headers=headers, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            # Lógica de retry similar a _make_request
            last_exception = e
            for attempt in range(self.retry_attempts):
                if attempt > 0:
                    wait_time = self.backoff_factor ** (attempt - 1)
                    time.sleep(wait_time)
                
                try:
                    response = self.session.get(url, params=params, headers=headers, timeout=self.timeout)
                    response.raise_for_status()
                    return response.json()
                except requests.exceptions.RequestException as retry_e:
                    last_exception = retry_e
                    if attempt == self.retry_attempts - 1:
                        raise last_exception
    
    def close(self):
        """Fecha a sessão HTTP."""
        self.session.close()

