
import env
import logging
from typing import List

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_embedding(text: str, timeout_seconds: float = 20, retry_attempts: int = 3) -> List[float]:
    import time
    from openai import OpenAI
    client = OpenAI(api_key=env.OPENAI_API_KEY)
    for attempt in range(retry_attempts):
        try:
            response = client.embeddings.create(
                input=text,
                model="text-embedding-3-small",
                timeout=timeout_seconds
            )
            return response.data[0].embedding
        except Exception as e:
            logger.error(f"OpenAI embedding call failed on attempt {attempt+1} with error: {e}")
            if attempt == retry_attempts - 1:
                raise
            time.sleep(1)