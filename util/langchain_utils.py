
import asyncio
import json
import logging
from typing import Any, Dict, List
import websockets


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def connect_to_graph_execution(
    graph_id: str, initial_message: str, timeout_seconds: float = 30,
    retry_attempts: int = 5
) -> List[Dict[str, Any]]:
    uri = f"ws://localhost:8000/api/graphs/{graph_id}/execute"
    attempt = 0
    while attempt < retry_attempts:
        try:
            responses = []
            async with websockets.connect(uri, ping_interval=None) as websocket:
                request_payload = json.dumps({"initial_message": initial_message})
                logger.info(f"Sending graph API request: {request_payload}")
                await websocket.send(request_payload)
                while True:
                    response = await asyncio.wait_for(websocket.recv(), timeout=timeout_seconds)
                    logger.info(f"Received: {response}")
                    response_dict = json.loads(response)
                    responses.append(response_dict)
                    if response_dict.get("status") == "completed":
                        break
                return responses
        except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed) as e:
            attempt += 1
            logger.error(f"Graph execution attempt {attempt} failed with error: {e}")
            if attempt >= retry_attempts:
                raise
            await asyncio.sleep(2)
        except Exception as e:
            attempt += 1
            logger.error(f"Graph execution attempt {attempt} failed with error: {e}")
            if attempt >= retry_attempts:
                raise
            await asyncio.sleep(1)