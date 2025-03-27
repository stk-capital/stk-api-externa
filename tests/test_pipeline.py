import logging
from email_processor import process_full_pipeline

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_process_pipeline():
    try:
        logger.info("Iniciando teste da função process_full_pipeline")
        # Chamando a função com um valor pequeno para o parâmetro n
        result = process_full_pipeline(n=3)
        logger.info(f"Função executada com sucesso. Resultado: {result}")
        return result
    except Exception as e:
        logger.error(f"Erro ao executar process_full_pipeline: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    test_process_pipeline() 