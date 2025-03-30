def format_followers(count: int) -> str:
    """Format follower count for display"""
    if count >= 1_000_000:
        return f"{count/1_000_000:.1f}M"
    elif count >= 1000:
        return f"{count/1000:.1f}K"
    return str(count)

import requests
import os
import time
import logging
import re
from pathlib import Path
from typing import Optional, Dict, Any, Tuple

logger = logging.getLogger(__name__)

# Configuração para diretório de logos
LOGO_DIR = "/tmp/company_logos"
LOGO_BASE_URL = "/api/images/tmp/company_logos"

def get_company_logo(company_name: str, company_id: str) -> str:
    """
    Obtém o logo de uma empresa através da API Clearbit.
    
    Args:
        company_name: Nome da empresa
        company_id: ID da empresa para nomear o arquivo
        
    Returns:
        URL relativa para o logo ou URL do placeholder se não encontrado
    """
    # Verifica se o diretório existe, se não, cria
    Path(LOGO_DIR).mkdir(parents=True, exist_ok=True)
    
    # Remove caracteres especiais para busca e arquivo
    safe_name = re.sub(r'[^\w\s]', '', company_name).strip().lower()
    file_name = f"{company_id}.png"
    file_path = os.path.join(LOGO_DIR, file_name)
    
    # Se o arquivo já existe, retorna o caminho
    if os.path.exists(file_path):
        logger.info(f"Logo para {company_name} já existe: {file_path}")
        return f"{LOGO_BASE_URL}/{file_name}"
    
    try:
        # Consulta a API Clearbit para sugestões
        suggest_url = f"https://autocomplete.clearbit.com/v1/companies/suggest?query={safe_name}"
        suggest_response = requests.get(suggest_url, timeout=5)
        
        if suggest_response.status_code != 200:
            logger.warning(f"Falha ao consultar Clearbit para {company_name}: Status {suggest_response.status_code}")
            return "/placeholder.svg?height=400&width=400"
            
        companies = suggest_response.json()
        
        # Se não encontrou nenhuma empresa
        if not companies:
            logger.info(f"Nenhuma empresa encontrada para {company_name}")
            return "/placeholder.svg?height=400&width=400"
            
        # Tenta encontrar correspondência exata ou próxima
        best_match = None
        for company in companies:
            if company['name'].lower() == company_name.lower():
                best_match = company
                break
                
        # Se não encontrou correspondência exata, usa a primeira sugestão
        if not best_match and companies:
            best_match = companies[0]
            
        if best_match and 'logo' in best_match:
            # Baixa o logo
            logo_url = best_match['logo']
            logo_response = requests.get(logo_url, timeout=5)
            
            if logo_response.status_code == 200:
                # Salva o logo localmente
                with open(file_path, 'wb') as f:
                    f.write(logo_response.content)
                
                logger.info(f"Logo para {company_name} baixado com sucesso: {file_path}")
                return f"{LOGO_BASE_URL}/{file_name}"
    
    except Exception as e:
        logger.error(f"Erro ao obter logo para {company_name}: {str(e)}")
    
    # Em caso de falha, retorna o placeholder
    return "/placeholder.svg?height=400&width=400"

def calculate_priority_score(followers: int, post_count: int, last_activity_days: int) -> float:
    """
    Calcula o score de prioridade para uma empresa.
    
    Args:
        followers: Número de seguidores
        post_count: Número de posts
        last_activity_days: Dias desde a última atividade
        
    Returns:
        Score de prioridade (maior = mais prioritário)
    """
    # Normalização simples (ajustar conforme necessário)
    norm_followers = min(followers / 10000, 1.0)  # Normaliza para max 10k seguidores
    norm_post_count = min(post_count / 100, 1.0)  # Normaliza para max 100 posts
    
    # Fator de recência (maior = mais recente)
    recency_factor = 1.0 / (1.0 + (last_activity_days / 30))  # Decai com o tempo
    
    # Cálculo do score (pesos conforme definido no plano)
    score = (
        (norm_followers * 0.4) + 
        (norm_post_count * 0.3) + 
        (recency_factor * 0.3)
    )
    
    return score
