"""
Testes para o módulo de ingestão de dados.
"""
import pytest
from unittest.mock import Mock, patch
from src.ingestion.api_client import IBGEAPIClient


class TestIBGEAPIClient:
    """Suite de testes para o Cliente de API do IBGE."""
    
    def test_client_initialization(self):
        """Testa inicialização do cliente com parâmetros padrão."""
        client = IBGEAPIClient()
        
        assert client.base_url == "https://servicodados.ibge.gov.br/api/v3"
        assert client.timeout == 30
        assert client.retry_attempts == 3
    
    def test_client_initialization_custom(self):
        """Testa inicialização do cliente com parâmetros customizados."""
        client = IBGEAPIClient(
            base_url="https://custom.api.com",
            timeout=60,
            retry_attempts=5
        )
        
        assert client.base_url == "https://custom.api.com"
        assert client.timeout == 60
        assert client.retry_attempts == 5
    
    @patch('src.ingestion.api_client.requests.Session.get')
    def test_make_request_success(self, mock_get):
        """Testa requisição de API bem-sucedida."""
        # Mock de resposta
        mock_response = Mock()
        mock_response.json.return_value = {"data": "test"}
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        
        client = IBGEAPIClient()
        result = client._make_request("/test")
        
        assert result == {"data": "test"}
        assert mock_get.called
    
    @patch('src.ingestion.api_client.requests.Session.get')
    def test_make_request_retry(self, mock_get):
        """Testa requisição com lógica de retry."""
        # Primeira chamada falha, segunda tem sucesso
        mock_response_fail = Mock()
        mock_response_fail.raise_for_status.side_effect = Exception("Erro")
        
        mock_response_success = Mock()
        mock_response_success.json.return_value = {"data": "test"}
        mock_response_success.raise_for_status = Mock()
        
        mock_get.side_effect = [mock_response_fail, mock_response_success]
        
        client = IBGEAPIClient(retry_attempts=2)
        result = client._make_request("/test")
        
        assert result == {"data": "test"}
        assert mock_get.call_count == 2
    
    def test_parse_ibge_response(self):
        """Testa análise da resposta da API do IBGE."""
        client = IBGEAPIClient()
        
        raw_response = [
            {
                "periodo": {"id": "2024"},
                "id": "9324",
                "resultados": [
                    {
                        "series": [
                            {
                                "localidade": {
                                    "id": "3550308",
                                    "nome": "São Paulo"
                                },
                                "serie": {
                                    "2024": "12325232"
                                }
                            }
                        ]
                    }
                ]
            }
        ]
        
        result = client._parse_ibge_response(raw_response, "populacao")
        
        assert len(result) == 1
        assert result[0]["codigo_municipio"] == "3550308"
        assert result[0]["nome_municipio"] == "São Paulo"
        assert result[0]["fonte"] == "populacao"

