# Testes da API Externa

Este documento descreve os testes unitários implementados para a API externa e como executá-los.

## Requisitos

Antes de executar os testes, certifique-se de ter instalado as dependências necessárias:

```bash
pip install -r requirements.txt
```

## Estrutura dos Testes

Os testes estão organizados da seguinte forma:

- `test_websocket_endpoint.py`: Testes para o endpoint WebSocket de execução de grafos.

## Executando os Testes

Para executar todos os testes:

```bash
pytest
```

Para executar testes específicos com saída detalhada:

```bash
pytest test_websocket_endpoint.py -v
```

Para executar um teste específico:

```bash
pytest test_websocket_endpoint.py::test_execute_graph_ws_success -v
```

## Detalhes dos Testes WebSocket

Os testes para o endpoint WebSocket `/api/graphs/{graph_id}/execute` incluem:

1. **Caso de sucesso**: Testa o fluxo normal onde um grafo é encontrado e executado com sucesso.
2. **Grafo não encontrado**: Testa o comportamento quando o ID do grafo não existe.
3. **JSON inválido**: Testa o comportamento quando o cliente envia um JSON inválido.
4. **Execução com arquivos**: Testa o envio de arquivos junto com a mensagem inicial.
5. **Tratamento de exceções**: Testa o comportamento quando ocorre uma exceção durante a execução.

## Mocks Utilizados

Os testes utilizam vários mocks para simular o comportamento dos componentes externos:

- **MockWebSocket**: Simula uma conexão WebSocket.
- **MockDB**: Simula o banco de dados MongoDB.
- **mock_execute_graph**: Simula a execução de um grafo.

## Notas Adicionais

- Os testes são assíncronos e utilizam `pytest-asyncio` para gerenciar a execução.
- Os mocks garantem que os testes sejam isolados e não dependam de recursos externos.
- Para depurar os testes, use a flag `-v` ou `-xvs` para obter saídas mais detalhadas. 