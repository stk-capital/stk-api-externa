#!/usr/bin/env python3
"""
Exemplo de cliente WebSocket para se conectar ao endpoint de execução de grafos.
Este script demonstra como um cliente pode se conectar e interagir com o 
endpoint WebSocket `/api/graphs/{graph_id}/execute`.
"""

import asyncio
import json
import base64
import sys
import websockets
import logging

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def connect_to_graph_execution(
    graph_id: str, 
    initial_message: str, 
    files=None, 
    server_url="ws://localhost:8000"
):
    """
    Conecta ao endpoint WebSocket e executa um grafo.

    Args:
        graph_id: ID do grafo a ser executado.
        initial_message: Mensagem inicial para o grafo.
        files: Lista de caminhos de arquivos para enviar (opcional).
        server_url: URL base do servidor.

    Returns:
        Uma lista com todas as respostas do servidor.
    """
    uri = f"{server_url}/api/graphs/{graph_id}/execute"
    files_data = []

    # Preparar os arquivos se houver
    if files:
        for file_path in files:
            try:
                with open(file_path, "rb") as f:
                    file_content = f.read()
                    file_name = file_path.split("/")[-1]
                    files_data.append({
                        "name": file_name,
                        "content": base64.b64encode(file_content).decode()
                    })
            except Exception as e:
                logger.error(f"Erro ao ler o arquivo {file_path}: {e}")

    # Preparar o payload
    payload = {
        "initial_message": initial_message,
        "files": files_data
    }

    responses = []
    try:
        logger.info(f"Conectando ao WebSocket em {uri}")
        async with websockets.connect(uri) as websocket:
            # Enviar o payload
            logger.info(f"Enviando payload: {json.dumps(payload)[:100]}...")
            await websocket.send(json.dumps(payload))

            # Receber as respostas
            while True:
                try:
                    response = await websocket.recv()
                    response_data = json.loads(response)
                    responses.append(response_data)
                    
                    # Processar a resposta
                    if "step" in response_data:
                        logger.info(f"Passo da execução recebido: {response_data}")
                    elif "status" in response_data and response_data["status"] == "completed":
                        logger.info("Execução do grafo concluída com sucesso!")
                        break
                    elif "error" in response_data:
                        logger.error(f"Erro durante a execução: {response_data['error']}")
                        break
                except websockets.exceptions.ConnectionClosed:
                    logger.warning("Conexão WebSocket fechada.")
                    break
    except Exception as e:
        logger.error(f"Erro na conexão WebSocket: {e}")

    return responses

async def main():
    """
    Função principal que executa o cliente WebSocket.
    """
    if len(sys.argv) < 3:
        print("Uso: python websocket_client_example.py <graph_id> <mensagem_inicial> [arquivos...]")
        return

    graph_id = sys.argv[1]
    initial_message = sys.argv[2]
    files = sys.argv[3:] if len(sys.argv) > 3 else None

    responses = await connect_to_graph_execution(graph_id, initial_message, files)
    
    # Imprimir um resumo das respostas
    print("\nResumo da execução:")
    for i, resp in enumerate(responses):
        print(f"Resposta {i+1}: {json.dumps(resp)[:100]}...")

if __name__ == "__main__":
    asyncio.run(main()) 