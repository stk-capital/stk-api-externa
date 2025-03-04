import pytest
import json
import base64
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, patch, MagicMock
from bson import ObjectId

# Importando a aplicação FastAPI e a função do endpoint diretamente
from main import app, execute_graph_ws

# Cliente de teste para FastAPI
client = TestClient(app)

# ID válido para o MongoDB (24 caracteres hexadecimais)
VALID_OBJECT_ID = "123456789012345678901234"

# Mock para o cliente WebSocket
class MockWebSocket:
    def __init__(self):
        self.sent_messages = []
        self.closed = False
        self.accepted = False
    
    async def accept(self):
        self.accepted = True
    
    async def receive_text(self):
        # Simula o recebimento de uma mensagem JSON
        return json.dumps({
            "initial_message": "Teste de mensagem inicial",
            "files": []
        })
    
    async def send_text(self, message):
        self.sent_messages.append(message)
    
    async def send_json(self, data):
        self.sent_messages.append(json.dumps(data))
    
    async def close(self):
        self.closed = True

# Mock para o banco de dados
class MockDB:
    def __init__(self, graph_data=None):
        self.graph_data = graph_data
    
    async def find_one(self, query):
        # Simula a busca por um grafo no banco de dados
        if "_id" in query and isinstance(query["_id"], ObjectId) and str(query["_id"]) == VALID_OBJECT_ID:
            return self.graph_data
        return None

# Mock para a função execute_graph
async def mock_execute_graph(graph, initial_message, file_paths=None):
    # Simula a execução do grafo e retorna resultados
    yield json.dumps({"step": {"message": "Passo 1 do grafo"}})
    yield json.dumps({"step": {"message": "Passo 2 do grafo"}})
    yield json.dumps({"status": "completed"})

@pytest.mark.asyncio
async def test_execute_graph_ws_success():
    """
    Testa o caso de sucesso do endpoint WebSocket para execução de grafo.
    """
    # Mock para o grafo que será retornado pelo banco de dados
    mock_graph = {
        "_id": ObjectId(VALID_OBJECT_ID),
        "name": "Grafo de Teste",
        "nodes": [],
        "edges": []
    }
    
    # Cria o WebSocket mock
    mock_websocket = MockWebSocket()
    
    # Configura os mocks para o banco de dados e a função execute_graph
    with patch("main.db", MockDB(mock_graph)), \
         patch("main.execute_graph", mock_execute_graph):
        
        # Executa a função do endpoint diretamente
        await execute_graph_ws(mock_websocket, VALID_OBJECT_ID)
    
    # Verifica se o WebSocket foi aceito
    assert mock_websocket.accepted == True
    
    # Verifica se as mensagens foram enviadas corretamente
    assert len(mock_websocket.sent_messages) == 3
    assert json.loads(mock_websocket.sent_messages[0])["step"]["message"] == "Passo 1 do grafo"
    assert json.loads(mock_websocket.sent_messages[1])["step"]["message"] == "Passo 2 do grafo"
    assert json.loads(mock_websocket.sent_messages[2])["status"] == "completed"
    
    # Verifica se o WebSocket foi fechado
    assert mock_websocket.closed == True

@pytest.mark.asyncio
async def test_execute_graph_ws_graph_not_found():
    """
    Testa o caso em que o grafo não é encontrado no banco de dados.
    """
    # Cria o WebSocket mock
    mock_websocket = MockWebSocket()
    
    # Configura o mock para o banco de dados que retornará None (grafo não encontrado)
    with patch("main.db", MockDB(None)):
        
        # Executa a função do endpoint diretamente
        await execute_graph_ws(mock_websocket, "grafo_inexistente")
    
    # Verifica se o WebSocket foi aceito
    assert mock_websocket.accepted == True
    
    # Verifica se a mensagem de erro foi enviada
    assert len(mock_websocket.sent_messages) == 1
    assert json.loads(mock_websocket.sent_messages[0])["error"] == "Graph not found"
    
    # Verifica se o WebSocket foi fechado
    assert mock_websocket.closed == True

@pytest.mark.asyncio
async def test_execute_graph_ws_invalid_json():
    """
    Testa o caso em que o JSON recebido é inválido.
    """
    # Cria o WebSocket mock com sobrescrita do método receive_text
    mock_websocket = MockWebSocket()
    mock_websocket.receive_text = AsyncMock(return_value="dados inválidos")
    
    # Executa a função do endpoint
    with patch("main.db", MockDB(None)):
        await execute_graph_ws(mock_websocket, VALID_OBJECT_ID)
    
    # Verifica se o WebSocket foi aceito
    assert mock_websocket.accepted == True
    
    # Verifica se a mensagem de erro foi enviada
    assert len(mock_websocket.sent_messages) == 1
    assert json.loads(mock_websocket.sent_messages[0])["error"] == "Invalid JSON format"
    
    # Verifica se o WebSocket foi fechado
    assert mock_websocket.closed == True

@pytest.mark.asyncio
async def test_execute_graph_ws_with_files():
    """
    Testa o caso de sucesso com envio de arquivos.
    """
    # Mock para o grafo que será retornado pelo banco de dados
    mock_graph = {
        "_id": ObjectId(VALID_OBJECT_ID),
        "name": "Grafo de Teste com Arquivo",
        "nodes": [],
        "edges": []
    }
    
    # Cria o WebSocket mock com sobrescrita para retornar JSON com arquivos
    mock_websocket = MockWebSocket()
    mock_websocket.receive_text = AsyncMock(return_value=json.dumps({
        "initial_message": "Teste com arquivos",
        "files": [
            {
                "name": "arquivo_teste.txt",
                "content": base64.b64encode(b"Conteudo do arquivo de teste").decode()
            }
        ]
    }))
    
    # Mock para a função open que seria usada para salvar os arquivos
    mock_open = MagicMock()
    mock_open.return_value.__enter__.return_value = MagicMock()
    
    # Configura os mocks
    with patch("main.db", MockDB(mock_graph)), \
         patch("main.execute_graph", mock_execute_graph), \
         patch("builtins.open", mock_open):
        
        # Executa a função do endpoint diretamente
        await execute_graph_ws(mock_websocket, VALID_OBJECT_ID)
    
    # Verifica se o arquivo foi "salvo"
    mock_open.assert_called_with("/tmp/arquivo_teste.txt", "wb")
    
    # Verifica se as mensagens foram enviadas corretamente
    assert len(mock_websocket.sent_messages) == 3
    assert json.loads(mock_websocket.sent_messages[2])["status"] == "completed"
    
    # Verifica se o WebSocket foi fechado
    assert mock_websocket.closed == True

@pytest.mark.asyncio
async def test_execute_graph_ws_exception():
    """
    Testa o caso em que ocorre uma exceção durante a execução do grafo.
    """
    # Mock para o grafo que será retornado pelo banco de dados
    mock_graph = {
        "_id": ObjectId(VALID_OBJECT_ID),
        "name": "Grafo de Teste com Exceção",
        "nodes": [],
        "edges": []
    }
    
    # Cria o WebSocket mock
    mock_websocket = MockWebSocket()
    
    # Mock para a função execute_graph que lança uma exceção
    async def mock_execute_graph_exception(*args, **kwargs):
        raise Exception("Erro de teste na execução do grafo")
    
    # Configura os mocks
    with patch("main.db", MockDB(mock_graph)), \
         patch("main.execute_graph", mock_execute_graph_exception):
        
        # Executa a função do endpoint diretamente
        await execute_graph_ws(mock_websocket, VALID_OBJECT_ID)
    
    # Verifica se a mensagem de erro foi enviada
    assert len(mock_websocket.sent_messages) == 1
    assert json.loads(mock_websocket.sent_messages[0])["error"] == "Erro de teste na execução do grafo"
    
    # Verifica se o WebSocket foi fechado
    assert mock_websocket.closed == True

if __name__ == "__main__":
    pytest.main(["-xvs", "test_websocket_endpoint.py"]) 