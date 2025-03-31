import logging

logger = logging.getLogger(__name__)

def deduplicate_posts(posts):
    """
    Remove posts duplicados (mesmo título e conteúdo).
    
    Args:
        posts: Lista de posts
        
    Returns:
        Lista de posts únicos (mantendo o primeiro de cada grupo de posts idênticos)
    """
    # Dicionário para mapear a combinação title+content para o primeiro post com essa combinação
    unique_posts_map = {}
    
    # Estatísticas para logging
    total_posts = len(posts)
    duplicates_found = 0
    
    for post in posts:
        # Criar uma chave baseada no título e conteúdo
        title = post.get('title', '')
        content = post.get('content', '')
        
        # Se não tiver título, usa apenas o conteúdo
        if not title:
            key = content
        else:
            key = f"{title}:{content}"
        
        # Se já existe um post com esta combinação, é uma duplicata
        if key in unique_posts_map:
            duplicates_found += 1
            # Podemos registrar informações sobre duplicatas encontradas
            logger.debug(f"Post duplicado encontrado: {post.get('_id')} (original: {unique_posts_map[key].get('_id')})")
        else:
            # Se é o primeiro post com esta combinação, adiciona ao mapa
            unique_posts_map[key] = post
    
    # Converter o mapa de volta para uma lista
    unique_posts = list(unique_posts_map.values())
    
    # Mostrar estatísticas
    if duplicates_found > 0:
        logger.info(f"Encontrados {duplicates_found} posts duplicados de um total de {total_posts}.")
    
    return unique_posts

