"""
Script para atualizar avatares de empresas existentes usando a API Clearbit.
Foca nas 400 empresas mais importantes com base em score de prioridade.
"""

import logging
import os
import sys
import time
from datetime import datetime, timedelta
from pymongo import MongoClient
from bson import ObjectId
import json

# Adicionar o diretório raiz ao path para importação de módulos
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from util.users_utils import get_company_logo, calculate_priority_score
from util.mongodb_utils import get_mongo_collection
from env import db_name_alphasync, db_name_stkfeed

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("company_avatars_update.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("avatar_updater")

def get_priority_companies(limit=400):
    """
    Obtém as empresas prioritárias com base no score calculado.
    
    Args:
        limit: Número máximo de empresas para processar
        
    Returns:
        Lista das empresas prioritárias com seus IDs e nomes
    """
    logger.info(f"Buscando top {limit} empresas prioritárias...")
    
    # Obter coleções
    companies_coll = get_mongo_collection(db_name=db_name_alphasync, collection_name="companies")
    users_coll = get_mongo_collection(db_name=db_name_stkfeed, collection_name="users")
    posts_coll = get_mongo_collection(db_name=db_name_stkfeed, collection_name="posts")
    
    # Lista para armazenar empresas com seus scores
    company_scores = []
    
    # Obter todas as empresas que já têm usuários
    users = list(users_coll.find(
        {"avatar": "/placeholder.svg?height=400&width=400"},
        {"_id": 1, "companyId": 1, "name": 1, "followers": 1}
    ))
    
    now = datetime.now()
    
    for user in users:
        try:
            company_id = user.get("companyId")
            if not company_id:
                continue
                
            # Converter follower string para número
            followers_str = user.get("followers", "0")
            followers = 0
            
            if followers_str.endswith("M"):
                followers = float(followers_str.rstrip("M")) * 1_000_000
            elif followers_str.endswith("K"):
                followers = float(followers_str.rstrip("K")) * 1_000
            else:
                try:
                    followers = int(followers_str)
                except (ValueError, TypeError):
                    followers = 0
            
            # Contar posts
            post_count = posts_coll.count_documents({"userId": str(user["_id"])})
            
            # Obter data do post mais recente
            last_post = posts_coll.find_one(
                {"userId": str(user["_id"])}, 
                {"created_at": 1},
                sort=[("created_at", -1)]
            )
            
            last_activity_days = 365  # Padrão para empresas sem posts
            if last_post and "created_at" in last_post:
                last_date = last_post["created_at"]
                if isinstance(last_date, str):
                    last_date = datetime.fromisoformat(last_date.replace("Z", "+00:00"))
                last_activity_days = (now - last_date).days
            
            # Calcular score
            score = calculate_priority_score(followers, post_count, last_activity_days)
            
            company_scores.append({
                "user_id": str(user["_id"]),
                "company_id": company_id,
                "name": user.get("name", "Unknown"),
                "score": score,
                "followers": followers,
                "post_count": post_count,
                "last_activity_days": last_activity_days
            })
            
        except Exception as e:
            logger.error(f"Erro ao processar usuário {user.get('_id')}: {str(e)}")
    
    # Ordenar por score e pegar os top N
    company_scores.sort(key=lambda x: x["score"], reverse=True)
    top_companies = company_scores[:limit]
    
    logger.info(f"Encontradas {len(top_companies)} empresas prioritárias de {len(company_scores)} totais")
    
    # Salvar lista em arquivo para referência
    with open("priority_companies.json", "w") as f:
        json.dump(top_companies, f, indent=2)
    
    return top_companies

def update_company_avatars(companies, batch_size=50, sleep_between_batches=30):
    """
    Atualiza os avatares das empresas em lotes.
    
    Args:
        companies: Lista de dicionários com informações das empresas
        batch_size: Tamanho do lote para processamento
        sleep_between_batches: Tempo de espera entre lotes (segundos)
    """
    logger.info(f"Iniciando atualização de avatares para {len(companies)} empresas em lotes de {batch_size}")
    
    users_coll = get_mongo_collection(db_name=db_name_stkfeed, collection_name="users")
    
    # Estatísticas
    stats = {
        "total": len(companies),
        "processed": 0,
        "success": 0,
        "failed": 0,
        "start_time": datetime.now()
    }
    
    # Dividir em lotes
    batches = [companies[i:i+batch_size] for i in range(0, len(companies), batch_size)]
    
    for batch_num, batch in enumerate(batches):
        logger.info(f"Processando lote {batch_num+1}/{len(batches)} ({len(batch)} empresas)")
        
        for company in batch:
            try:
                user_id = company["user_id"]
                company_id = company["company_id"]
                company_name = company["name"]
                
                logger.info(f"Atualizando avatar para {company_name} (ID: {company_id})")
                
                # Obter novo avatar
                new_avatar = get_company_logo(company_name, company_id)
                
                # Atualizar no banco de dados
                if new_avatar != "/placeholder.svg?height=400&width=400":
                    result = users_coll.update_one(
                        {"_id": ObjectId(user_id)},
                        {"$set": {"avatar": new_avatar}}
                    )
                    
                    if result.modified_count > 0:
                        logger.info(f"Avatar atualizado com sucesso para {company_name}: {new_avatar}")
                        stats["success"] += 1
                    else:
                        logger.warning(f"Avatar não foi atualizado para {company_name}")
                        stats["failed"] += 1
                else:
                    logger.info(f"Nenhum logo encontrado para {company_name}, mantendo placeholder")
                    stats["failed"] += 1
                
                stats["processed"] += 1
                
                # Breve pausa entre requisições
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Erro ao atualizar avatar para {company.get('name')}: {str(e)}")
                stats["failed"] += 1
                stats["processed"] += 1
        
        # Relatório de progresso do lote
        elapsed = (datetime.now() - stats["start_time"]).total_seconds() / 60
        logger.info(
            f"Progresso: {stats['processed']}/{stats['total']} ({stats['processed']/stats['total']*100:.1f}%) "
            f"| Sucesso: {stats['success']} | Falhas: {stats['failed']} | "
            f"Tempo decorrido: {elapsed:.1f} minutos"
        )
        
        # Pausa entre lotes
        if batch_num < len(batches) - 1:
            logger.info(f"Aguardando {sleep_between_batches} segundos até o próximo lote...")
            time.sleep(sleep_between_batches)
    
    # Relatório final
    total_time = (datetime.now() - stats["start_time"]).total_seconds() / 60
    logger.info(
        f"Atualização de avatares concluída em {total_time:.1f} minutos | "
        f"Total: {stats['total']} | Sucesso: {stats['success']} | Falhas: {stats['failed']} | "
        f"Taxa de sucesso: {stats['success']/stats['total']*100:.1f}%"
    )
    
    return stats

if __name__ == "__main__":
    try:
        # Parâmetros configuráveis via linha de comando
        import argparse
        parser = argparse.ArgumentParser(description='Atualiza avatares de empresas usando Clearbit')
        parser.add_argument('--limit', type=int, default=400, help='Número de empresas para processar')
        parser.add_argument('--batch', type=int, default=50, help='Tamanho do lote')
        parser.add_argument('--sleep', type=int, default=30, help='Tempo de espera entre lotes (segundos)')
        args = parser.parse_args()
        
        # Obter empresas prioritárias
        priority_companies = get_priority_companies(limit=args.limit)
        
        # Atualizar avatares
        stats = update_company_avatars(
            priority_companies, 
            batch_size=args.batch, 
            sleep_between_batches=args.sleep
        )
        
        sys.exit(0)
    except Exception as e:
        logger.critical(f"Erro fatal na execução do script: {str(e)}", exc_info=True)
        sys.exit(1) 