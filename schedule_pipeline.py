import requests
import schedule
import time
import logging
import os
from datetime import datetime

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("pipeline_scheduler.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("pipeline_scheduler")

# URL do endpoint
BASE_URL = os.environ.get("API_URL", "http://localhost:8000")
PIPELINE_ENDPOINT = f"{BASE_URL}/api/pipeline"

def execute_pipeline():
    """Executa o endpoint de pipeline"""
    try:
        logger.info(f"Iniciando execução agendada do pipeline em {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        response = requests.post(
            PIPELINE_ENDPOINT, 
            json={"process_emails_count": 50}
        )
        if response.status_code == 200:
            result = response.json()
            logger.info(f"Pipeline executado com sucesso: {result}")
        else:
            logger.error(f"Erro ao executar pipeline. Status: {response.status_code}, Resposta: {response.text}")
    except Exception as e:
        logger.error(f"Exceção ao executar pipeline: {str(e)}")

def main():
    logger.info("Iniciando agendador de pipeline...")
    
    # Agenda a execução a cada 5 minutos
    schedule.every(5).minutes.do(execute_pipeline)
    
    # Executa imediatamente na primeira vez
    execute_pipeline()
    
    logger.info("Agendamento configurado. Pipeline será executado a cada 5 minutos.")
    
    # Loop principal
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main() 