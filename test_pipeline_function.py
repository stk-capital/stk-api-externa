import sys
import logging
from email_processor import process_full_pipeline

# Configurando o logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger(__name__)

def main():
    try:
        logger.info("Iniciando teste da função process_full_pipeline")
        
        # Chamando a função com apenas 1 email para ser mais rápido
        result = process_full_pipeline(process_emails_count=1)
        
        logger.info(f"Resultado do pipeline: {result}")
        logger.info("Teste concluído com sucesso")
        
    except Exception as e:
        logger.error(f"Erro ao executar process_full_pipeline: {str(e)}", exc_info=True)
        sys.exit(1)
        
    sys.exit(0)

if __name__ == "__main__":
    main() 