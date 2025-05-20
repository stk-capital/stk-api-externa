import dotenv
import os
import msal
import requests
import json
from bs4 import BeautifulSoup  # Adicionado para processamento de HTML
dotenv.load_dotenv()
import env as env
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



OUTLOOK_CLIENT_ID  = env.OUTLOOK_CLIENT_ID
OUTLOOK_CLIENT_SECRET = env.OUTLOOK_CLIENT_SECRET
OUTLOOK_TENANT_ID = env.OUTLOOK_TENANT_ID
OUTLOOK_AUTHORITY = f"https://login.microsoftonline.com/{OUTLOOK_TENANT_ID}"
SCOPE = [env.OUTLOOK_SCOPE]
USER_EMAIL = env.USER_EMAIL



def get_access_token(client_id: str=OUTLOOK_CLIENT_ID, authority: str=OUTLOOK_AUTHORITY, client_secret: str=OUTLOOK_CLIENT_SECRET, scopes: list[str]=SCOPE) -> str:
    """
    Obtém o token de acesso usando a biblioteca MSAL.
    :param client_id: ID do cliente registrado no Azure.
    :param authority: URL da autoridade de autenticação.
    :param client_secret: Segredo do cliente.
    :param scopes: Escopos de permissão solicitados.
    :return: Token de acesso.
    """
    app = msal.ConfidentialClientApplication(
        client_id=client_id,
        authority=authority,
        client_credential=client_secret
    )
    token_response = app.acquire_token_for_client(scopes=scopes)
    if "access_token" in token_response:
        return token_response["access_token"]
    else:
        error = token_response.get("error")
        error_description = token_response.get("error_description")
        raise Exception(f"Erro ao obter o token de acesso: {error} - {error_description}")     

def extract_text_from_html(html_content):
    """
    Extrai texto puro de conteúdo HTML.
    :param html_content: String contendo conteúdo HTML.
    :return: String com o texto extraído.
    """
    soup = BeautifulSoup(html_content, "html.parser")
    return soup.get_text(separator='\n').strip()

def get_recent_emails(user_email=USER_EMAIL, top_n=10):
    """
    Recupera os e-mails mais recentes da caixa de entrada do usuário usando a API Microsoft Graph.
    :param user_email: Email do usuário.
    :param access_token: Token de acesso para autenticação.
    :param top_n: Número máximo de e-mails a serem recuperados.
    :return: Lista dos e-mails mais recentes com texto extraído.
    """
    access_token = get_access_token(OUTLOOK_CLIENT_ID, OUTLOOK_AUTHORITY, OUTLOOK_CLIENT_SECRET, SCOPE)
    emails = []
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    params = {
        "$select": "subject,receivedDateTime,from,body",
        "$filter": "isRead eq false",
        "$orderby": "receivedDateTime desc",
        "$top": f"{top_n}"
    }
    url = f"https://graph.microsoft.com/v1.0/users/{user_email}/messages"

    while url and len(emails) < top_n:
        try:
            response = requests.get(
                url,
                headers=headers,
                params=params
            )
            response.raise_for_status()
            data = response.json()
            for message in data.get("value", []):
                # Extrai texto do corpo do e-mail
                if message.get("body") and message["body"].get("content"):
                    plain_text = extract_text_from_html(message["body"]["content"])
                else:
                    plain_text = ""
                
                # Adiciona o e-mail com texto extraído à lista
                emails.append({
                    "id": message.get("id"),
                    "receivedDateTime": message.get("receivedDateTime"),
                    "subject": message.get("subject"),
                    "from": message.get("from", {}).get("emailAddress", {}).get("address"),
                    "body": plain_text
                })
                
                if len(emails) >= top_n:
                    break

            if "@odata.nextLink" in data and len(emails) < top_n:
                url = data["@odata.nextLink"]
                params = {}  # Limpa os parâmetros após a primeira requisição
            else:
                break
        except requests.exceptions.HTTPError as http_err:
            print(f"Erro HTTP ocorreu: {http_err} - {response.text}")
            break
        except Exception as err:
            print(f"Erro ao recuperar e-mails: {err}")
            break
    # Limita a lista aos top_n e-mails mais recentes
    return emails[:top_n]

def send_notification_email(posts_created, destination_email="ruhany.aragao@gmail.com"):
    """
    Envia um email de notificação quando novos posts forem criados pelo pipeline.
    
    Args:
        posts_created: Lista de posts criados
        destination_email: Email destinatário
    """
    try:
        smtp_server = "smtp.gmail.com"
        smtp_port = 587
        smtp_username = os.getenv("GMAIL_USER")
        smtp_password = os.getenv("GMAIL_PASSWORD")
        
        if not smtp_username or not smtp_password:
            logger.error("Configurações de Gmail ausentes. Não é possível enviar email.")
            return False
            
        # Criando a mensagem
        msg = MIMEMultipart()
        msg['From'] = smtp_username
        msg['To'] = destination_email
        msg['Subject'] = f"STK API: {len(posts_created)} novos posts criados pelo pipeline"
        
        # Corpo do email
        body = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; line-height: 1.6; }}
                .post {{ border: 1px solid #ccc; padding: 10px; margin-bottom: 15px; }}
                .title {{ font-weight: bold; color: #333; }}
                .content {{ color: #555; }}
                .source {{ color: #888; font-style: italic; }}
            </style>
        </head>
        <body>
            <h2>Novos Posts Criados</h2>
            <p>O pipeline de processamento de emails acabou de criar {len(posts_created)} novos posts:</p>
        """
        
        # Adiciona até 5 posts na mensagem
        for i, post in enumerate(posts_created[:5]):
            body += f"""
            <div class="post">
                <div class="title">{post.get('title', 'Sem título')}</div>
                <div class="content">{post.get('content', 'Sem conteúdo')[:150]}...</div>
                <div class="source">Fonte: {post.get('source', 'Desconhecida')}</div>
            </div>
            """
        
        if len(posts_created) > 5:
            body += f"<p>E mais {len(posts_created) - 5} posts foram criados...</p>"
        
        body += """
            <p>Este é um email automatizado enviado pelo sistema de pipeline da STK API.</p>
        </body>
        </html>
        """
        
        msg.attach(MIMEText(body, 'html'))
        
        # Conectando e enviando o email
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(smtp_username, smtp_password)
            server.send_message(msg)
            
        logger.info(f"Email de notificação enviado para {destination_email}")
        return True
            
    except Exception as e:
        logger.error(f"Erro ao enviar email de notificação: {str(e)}")
        return False
# def main():
#     try:
#         access_token = get_access_token(OUTLOOK_CLIENT_ID, OUTLOOK_AUTHORITY, OUTLOOK_CLIENT_SECRET, SCOPE)
#         emails = get_recent_emails(USER_EMAIL, access_token, top_n=10)
#         print(json.dumps(emails, indent=4, ensure_ascii=False))
#     except Exception as e:
#         print(f"Falha no processamento: {e}")

# if __name__ == "__main__":
#     main()

def get_email_by_id(email_id: str):
    #email_id = "AAMkAGM2ZWMyZjEyLThkMjQtNGQzMS04ZGM2LTRhMmVhZWZhYTYyYgBGAAAAAADiynN7aLLDTINlhXpDfBwLBwA7bKgXB0usSYAR9-d72BLIAAAAAAEMAAA7bKgXB0usSYAR9-d72BLIAAIDlP_cAAA="
    access_token = get_access_token(OUTLOOK_CLIENT_ID, OUTLOOK_AUTHORITY, OUTLOOK_CLIENT_SECRET, SCOPE)
    url = f"https://graph.microsoft.com/v1.0/users/{USER_EMAIL}/messages/{email_id}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    response = requests.get(url, headers=headers)
    #format the response as a json grabbing the relevant fields
    message = response.json()
   
  
    if message.get("body") and message["body"].get("content"):
        plain_text = extract_text_from_html(message["body"]["content"])
    else:
        plain_text = ""
    
    # Adiciona o e-mail com texto extraído à lista
    email={
        "id": message.get("id"),
        "receivedDateTime": message.get("receivedDateTime"),
        "subject": message.get("subject"),
        "from": message.get("from", {}).get("emailAddress", {}).get("address"),
        "body": plain_text
    }

    return email


    