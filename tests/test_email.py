import os
from dotenv import load_dotenv
load_dotenv()

from email_processor import send_notification_email
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("Iniciando teste de envio de email...")
    
    # Criando dados de teste para posts simulados
    mock_posts = [
        {
            "_id": "post1",
            "title": "Notícias do Mercado Financeiro",
            "content": "O mercado financeiro teve um dia positivo com alta de 2% no índice principal. Analistas recomendam atenção às ações do setor de tecnologia que continuam em tendência de alta.",
            "source": "Newsletter Financeira"
        },
        {
            "_id": "post2",
            "title": "Novas Regulamentações no Setor Bancário",
            "content": "O Banco Central anunciou novas medidas para regular o setor bancário, com foco em segurança cibernética e prevenção à lavagem de dinheiro.",
            "source": "Boletim Regulatório"
        },
        {
            "_id": "post3",
            "title": "Impacto da Inteligência Artificial nos Investimentos",
            "content": "Empresas que investem em IA estão vendo retornos significativos. Um estudo recente mostrou que a adoção de tecnologias de IA pode aumentar a eficiência operacional em até 30%.",
            "source": "Tech Insights"
        }
    ]
    
    # Testando a função de envio de email
    resultado = send_notification_email(mock_posts)
    
    if resultado:
        logger.info("✅ Email enviado com sucesso!")
    else:
        logger.error("❌ Falha no envio do email.")

if __name__ == "__main__":
    main() 