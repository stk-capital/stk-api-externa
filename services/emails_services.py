from util.mongodb_utils import get_mongo_collection
from env import db_name_alphasync, db_name_stkfeed
from util.langchain_utils import connect_to_graph_execution
from util.parsing_utils import extract_brace_arguments, extract_json_from_content
import logging
import asyncio
from pymongo import errors
from util.emails_utils import get_unprocessed_emails
from models.chunks import Chunk
from util.embedding_utils import get_embedding
from datetime import datetime
import json
from typing import List, Dict
from models.emails import Email
from util.outlook_utils import get_recent_emails, get_email_by_id
from util.llm_services import execute_llm_with_threads
from concurrent.futures import ThreadPoolExecutor
from util.companies_utils import intruments_to_companies_ids_parallel
from util.sources_utils import intruments_to_sources_ids_parallel
from util.infos_utils import find_similar_info_vector_search
from util.infos_utils import upsert_infos_for_chunks
from models.infos import Info
from util.dates_utils import relative_time
from models.posts import Post
from services.users_services import get_or_create_users_by_companies
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
import pymongo
from collections import defaultdict



def get_last_n_emails(n: int = 10) -> List[Email]:
    """
    Retrieve the last n emails via Outlook and insert new ones into MongoDB.
    """
    try:
        
        emails_data = get_recent_emails(top_n=n)
        collection = get_mongo_collection(db_name=db_name_alphasync, collection_name="emails")
        email_objects = []

        for email_data in emails_data:
            email_obj = Email(
                message_id=email_data["id"],
                from_address=email_data["from"],
                subject=email_data["subject"],
                body=email_data["body"],
                received_at=email_data["receivedDateTime"],
                attachments=[],
            )
            email_objects.append(email_obj)
            if not collection.find_one({"message_id": email_obj.message_id}):
                try:
                    collection.insert_one(email_obj.to_formatted_dict(by_alias=True))
                    logger.info(f"Inserted email with Message-ID: {email_obj.id}")
                except errors.PyMongoError as e:
                    logger.error(f"Failed to insert email into MongoDB: {e}")
            else:
                logger.info(f"Email with Message-ID: {email_obj.id} already exists")
                
        return email_objects
    except Exception as e:
        logger.error(f"Failed to retrieve emails: {e}")
        raise


def filter_emails():
    """
    Process unprocessed emails by running them through a graph execution,
    then update the email document in MongoDB.
    """
    emails_list = get_unprocessed_emails()
    collection = get_mongo_collection(db_name=db_name_alphasync, collection_name="emails")
    for email_obj in emails_list:
        #email_obj = list(emails_list)[0]
        #print(email_obj.get_document_pretty())
        #limit the email to 130 lines
        email_obj.body = "\n".join(email_obj.body.split("\n")[:130])
        try:
            response = asyncio.run(
                connect_to_graph_execution(
                    "66e88c9c7d27c163b1c128f2", initial_message=email_obj.get_document_pretty()
                )
            )
            content = response[0]["step"]["Email Filter"][-1]["content"]
            email_obj.relevant = extract_brace_arguments(content).get("relevant", "false") == "true"
        except (IndexError, KeyError) as e:
            logger.error(f"Failed to extract relevant data: {e}")
        email_obj.was_processed = False
        try:
            collection.update_one(
                {"_id": email_obj.id},
                {"$set": {"was_processed": email_obj.was_processed, "relevant": email_obj.relevant}},
            )
            logger.info(f"Updated email with ID: {email_obj.id}")
        except errors.PyMongoError as e:
            logger.error(f"MongoDB update error: {e}")

def chunkenize_emails():
    """
    Process relevant unprocessed emails to generate and store chunks.
    """
    emails_list = get_unprocessed_emails()
    emails_collection = get_mongo_collection(db_name=db_name_alphasync, collection_name="emails")
    chunks_collection = get_mongo_collection(db_name=db_name_alphasync, collection_name="chunks")

    # Delete existing chunks for these emails
    for email_obj in emails_list:
        chunks_collection.delete_many({"document_id": email_obj.id})

    graph_id = "66e9bc0d68d9def3e3bd49b6"
    for email_obj in emails_list:
        #email_obj = list(emails_list)[0]
        #print(email_obj.get_document_pretty())
        #limit the email to 130 lines
        email_obj.body = "\n".join(email_obj.body.split("\n")[:130])
        try:
            response = asyncio.run(
                connect_to_graph_execution(
                    graph_id, initial_message=email_obj.get_document_pretty()
                )
            )
            json_string = response[0]["step"]["Email Chunckenizer"][-1]["content"]
            content = extract_json_from_content(json_string)
            chunk_data = json.loads(content)["chunks"]
        except (IndexError, KeyError, ValueError, json.JSONDecodeError) as e:
            logger.error(f"Chunk extraction failed: {e}")
            continue

        email_lines = email_obj.get_lines_pretty(numbered=False)
        previous_end = -1
        for chunk_index, chunk in enumerate(chunk_data):
            current_end = chunk["end"]
            start_index = max(previous_end + 1, 0)
            end_index = min(current_end + 1, len(email_lines))
            chunk_text = "\n".join(email_lines[start_index:end_index])
            previous_end = current_end

            try:
                embedding = get_embedding(chunk_text)
            except Exception as e:
                logger.error(f"Embedding generation failed: {e}")
                embedding = []

            chunk_obj = Chunk(
                content=chunk_text,
                summary=chunk["summary"],
                subject=chunk["subject"],
                source=chunk["source"],
                instrument_ids=chunk.get("companies", []),
                embedding=embedding,
                include=chunk["relevant"],
                has_events=chunk["has_events"],
                index=chunk_index,
                document_id=email_obj.id,
                document_collection="emails",
                published_at=email_obj.received_at,
                created_at=datetime.now(),
            )
            email_obj.was_processed = True
            try:
                emails_collection.update_one(
                    {"_id": email_obj.id},
                    {"$set": {"was_processed": email_obj.was_processed, "relevant": email_obj.relevant}},
                )
                logger.info(f"Email updated with ID: {email_obj.id}")
            except errors.PyMongoError as e:
                logger.error(f"Error updating email: {e}")

            try:
                chunks_collection.insert_one(chunk_obj.model_dump(by_alias=True))
                logger.info(f"Inserted chunk with ID: {chunk_obj.id}")
            except errors.PyMongoError as e:
                logger.error(f"Error inserting chunk: {e}")

def _process_emails(n: int = 10):
    """
    End-to-end email processing: retrieve emails, filter, chunk, and prepare for info association.
    """
    get_last_n_emails(n=n)
    filter_emails()
    chunkenize_emails()

def check_relevant_email(email_obj: Email):
    """
    Check if an email is relevant by running it through a graph execution.
    """
    with open("prompts/email_filter.md", "r") as file:
        prompt = file.read()
    formatted_prompt = prompt.format(email_data=email_obj.get_document_pretty())
    
    try:
        response = execute_llm_with_threads(
            formatted_prompt,
            model_name="gemini-2.0-flash",
            max_tokens=1000,
            timeout=30.0,
            temperature=0.7
        )
        
        is_relevant = json.loads(extract_json_from_content(response)).get("relevant", "false") == True
    except Exception as e:
        logger.error(f"Failed to check relevant email: {e}")
    

    return is_relevant


def chunkenize_single_email(email_obj: Email):
    """
    Chunk a single email.
    """
    logger.info(f"Iniciando chunkenização do email {email_obj.id}")
    
    try:
        number_of_lines = 300

        # Carregar prompt
        try:
            prompt = open("prompts/email_chunkenizer.md", "r").read()
            logger.debug(f"Prompt de chunkenização carregado com sucesso")
        except Exception as e:
            logger.error(f"Erro ao carregar prompt de chunkenização: {e}")
            return []
        
        # Obter linhas do email
        try:
            email_lines = email_obj.get_lines_pretty(numbered=False)
            logger.debug(f"Email {email_obj.id} tem {len(email_lines)} linhas")
        except Exception as e:
            logger.error(f"Erro ao obter linhas do email {email_obj.id}: {e}")
            return []
        
        if not email_lines:
            logger.warning(f"Email {email_obj.id} não tem conteúdo para chunkenizar")
            return []
        
        # Dividir em partes
        parts = [email_lines[i:i+number_of_lines] for i in range(0, len(email_lines), number_of_lines)]
        logger.info(f"Email dividido em {len(parts)} partes")

        # Preparar prompts para cada parte
        prompt_parts = []
        for part in parts:
            try:
                if not part:
                    logger.warning("Parte vazia encontrada, ignorando")
                    continue
                    
                number_start = email_lines.index(part[0])
                number_end = email_lines.index(part[-1])
                
                prompt_part = prompt.format(
                    email_data=email_obj.get_document_pretty(),
                    start_line=number_start,
                    end_line=number_end
                )
                prompt_parts.append(prompt_part)
            except Exception as e:
                logger.error(f"Erro ao preparar prompt para parte do email: {e}")
        
        if not prompt_parts:
            logger.warning(f"Nenhum prompt válido para chunkenização do email {email_obj.id}")
            return []
            
        logger.info(f"Preparados {len(prompt_parts)} prompts para processamento LLM")
        
        # Processar com LLM
        try:
            results = execute_llm_with_threads(
                prompt_parts,
                model_name="gemini-2.0-flash",
                max_tokens=100000,
                timeout=120.0,
                temperature=1,
                max_workers=10
            )
            logger.info(f"Recebidas {len(results)} respostas do LLM")
        except Exception as e:
            logger.error(f"Erro durante execução do LLM para chunkenização: {e}")
            return []

        # Extrair chunks de cada resposta
        chunk_data = []
        success_count = 0
        error_count = 0
        
        for i, result in enumerate(results):
            try:
                if not result:
                    logger.warning(f"Resposta vazia para parte {i+1}, ignorando")
                    continue
                    
                content = extract_json_from_content(result)
                if not content:
                    logger.warning(f"Nenhum conteúdo JSON encontrado na resposta para parte {i+1}")
                    continue
                    
                chunk_json = json.loads(content)
                chunks_in_part = chunk_json.get("chunks", [])
                
                if not chunks_in_part:
                    logger.warning(f"Nenhum chunk encontrado na parte {i+1}")
                    continue
                    
                chunk_data.extend(chunks_in_part)
                success_count += 1
                logger.debug(f"Extraídos {len(chunks_in_part)} chunks da parte {i+1}")
                
            except json.JSONDecodeError as e:
                logger.error(f"Erro ao decodificar JSON da parte {i+1}: {e}")
                error_count += 1
            except KeyError as e:
                logger.error(f"Chave não encontrada na resposta da parte {i+1}: {e}")
                error_count += 1
            except Exception as e:
                logger.error(f"Erro inesperado ao processar parte {i+1}: {e}")
                error_count += 1

        # Verificar resultado final
        logger.info(f"Partes processadas com sucesso: {success_count}, Erros: {error_count}")
        logger.info(f"Total de chunks extraídos: {len(chunk_data)}")

        # Se não conseguiu extrair nenhum chunk válido
        if not chunk_data:
            logger.warning(f"Nenhum chunk válido encontrado para o email {email_obj.id}")
            try:
                collection = get_mongo_collection(db_name=db_name_alphasync, collection_name="emails")
                collection.update_one(
                    {"_id": email_obj.id},
                    {"$set": {"was_processed": False}}
                )
                logger.info(f"Email {email_obj.id} marcado como não processado")
            except Exception as e:
                logger.error(f"Erro ao marcar email como não processado: {e}")
            return []

        # Salvar dados de chunk para log
        try:
            with open("logs/chunk_data.json", "w") as f:
                f.write(json.dumps(chunk_data, indent=4))
            with open("logs/chunk_data.json", "a") as f:
                f.write(email_obj.get_document_pretty())
            logger.debug("Dados de chunk salvos em logs/chunk_data.json")
        except Exception as e:
            logger.error(f"Erro ao salvar dados de chunk em log: {e}")
            
        logger.info(f"Chunkenização do email {email_obj.id} concluída com sucesso")
        return chunk_data
        
    except Exception as e:
        logger.error(f"Falha crítica na chunkenização do email {email_obj.id}: {e}")
        try:
            collection = get_mongo_collection(db_name=db_name_alphasync, collection_name="emails")
            collection.update_one(
                {"_id": email_obj.id},
                {"$set": {"was_processed": False}}
            )
            logger.info(f"Email {email_obj.id} marcado como não processado após erro")
        except Exception as sub_e:
            logger.error(f"Erro ao marcar email como não processado: {sub_e}")
        return []


def create_chunks_object(chunk_data, email_obj: Email):
    """
    Create a Chunk object from the chunk data.
    """
    logger.info(f"Criando objetos Chunk para email {email_obj.id} ({len(chunk_data)} chunks)")
    
    chunk_objects_list = []
    errors = 0
    
    for chunk_index, chunk in enumerate(chunk_data):
        try:
            if not isinstance(chunk, dict):
                logger.warning(f"Chunk {chunk_index} não é um dicionário, ignorando")
                continue
                
            # Verificar campos obrigatórios
            required_fields = ["start", "end", "summary", "subject", "source", "relevant", "has_events"]
            missing_fields = [field for field in required_fields if field not in chunk]
            
            if missing_fields:
                logger.warning(f"Chunk {chunk_index} faltando campos: {missing_fields}, ignorando")
                continue
            
            # Obter o conteúdo do chunk a partir das linhas do email
            try:
                email_lines = email_obj.get_lines_pretty(numbered=False)
                start_idx = max(0, min(chunk["start"], len(email_lines)-1))
                end_idx = max(start_idx, min(chunk["end"], len(email_lines)))
                content = '\n'.join(email_lines[start_idx:end_idx])
            except Exception as e:
                logger.error(f"Erro ao extrair conteúdo para chunk {chunk_index}: {e}")
                errors += 1
                continue
                
            if not content:
                logger.warning(f"Conteúdo vazio para chunk {chunk_index}, ignorando")
                continue
                
            # Criar objeto Chunk
            chunk_obj = Chunk(
                content=content,
                summary=chunk["summary"],
                subject=chunk["subject"],
                source=chunk["source"],
                instrument_ids=chunk.get("companies", []),
                embedding=[],
                include=chunk["relevant"],
                has_events=chunk["has_events"],
                index=chunk_index,
                document_id=email_obj.id,
                document_collection="emails",
                published_at=email_obj.received_at,
                created_at=datetime.now(),
                was_processed=False,
            )
            chunk_objects_list.append(chunk_obj)
            logger.debug(f"Objeto Chunk {chunk_index} criado com sucesso")
            
        except Exception as e:
            logger.error(f"Erro ao criar objeto para chunk {chunk_index}: {e}")
            errors += 1
    
    logger.info(f"Criados {len(chunk_objects_list)} objetos Chunk com {errors} erros")
    
    if not chunk_objects_list:
        logger.warning(f"Nenhum objeto Chunk criado para o email {email_obj.id}")
        return []
    
    # Adicionar embeddings em paralelo
    logger.info("Gerando embeddings para os chunks em paralelo")
    def add_embedding(chunk_obj):
        try:
            chunk_obj.embedding = get_embedding(chunk_obj.content)
            return chunk_obj
        except Exception as e:
            logger.error(f"Erro ao gerar embedding para chunk {chunk_obj.index}: {e}")
            # Retornar o objeto sem embedding em caso de erro
            return chunk_obj

    try:
        with ThreadPoolExecutor(max_workers=10) as executor:
            processed_chunks = list(executor.map(add_embedding, chunk_objects_list))
            
        # Contar quantos chunks têm embeddings
        with_embedding = sum(1 for chunk in processed_chunks if chunk.embedding)
        logger.info(f"Gerados embeddings para {with_embedding} de {len(processed_chunks)} chunks")
        
        return processed_chunks
    except Exception as e:
        logger.error(f"Erro crítico ao processar embeddings: {e}")
        # Em caso de falha, retornar os chunks sem embeddings
        return chunk_objects_list
    
def create_posts_from_infos(infos: List[Info], users_by_company: Dict[str, List[str]]) -> int:
    """Cria posts para uma lista de *Info* de forma **batch**.

    • Consulta chunks, sources e usuários em lote.
    • Gera todos os posts em memória e faz **um único** `insert_many`.
    • Conta com índice único (infoId, userId) para descartar duplicatas.
    Retorna o nº efetivo de posts inseridos.
    infos = new_infos
    """

    if not infos:
        return 0

    # --- Coleções ---
    posts_coll   = get_mongo_collection(db_name=db_name_stkfeed,  collection_name="posts")
    users_coll   = get_mongo_collection(db_name=db_name_stkfeed,  collection_name="users")
    chunks_coll  = get_mongo_collection(db_name=db_name_alphasync, collection_name="chunks")
    sources_coll = get_mongo_collection(db_name=db_name_alphasync, collection_name="sources")

    # Garante índice único
    try:
        posts_coll.create_index([("infoId", 1), ("userId", 1)], unique=True)
    except Exception:
        pass

    # --- Pré-processamento em lote ------------------------------------------------
    infos = [i for i in infos if i.companiesId]
    if not infos:
        return 0

    # 1) Chunks --------------------------------------------------------------
    first_chunk_ids = [i.chunk_ids[0] for i in infos if i.chunk_ids]
    chunk_docs = {c["_id"]: c for c in chunks_coll.find({"_id": {"$in": first_chunk_ids}}, {"summary": 1, "content": 1, "subject": 1})}

    # 2) Sources -------------------------------------------------------------
    first_source_ids = [i.sourcesId[0] for i in infos if i.sourcesId]
    source_docs = {s["_id"]: s for s in sources_coll.find({"_id": {"$in": first_source_ids}}, {"name": 1})}

    # 3) Users ---------------------------------------------------------------
    # if users_by_company is None:
    #     all_company_ids = list({cid for info in infos for cid in info.companiesId})
    #     user_cursor = users_coll.find({"companyId": {"$in": all_company_ids}}, {"_id": 1, "companyId": 1})
    #     users_by_company = defaultdict(list)
    #     for u in user_cursor:
    #         users_by_company[str(u["companyId"])].append(str(u["_id"]))
    # else:
    #     # ensure default dict type for consistency
    #     tmp = defaultdict(list)
    #     for k, v in users_by_company.items():
    #         tmp[k] = v
    #     users_by_company = tmp

    # --- Construir posts -----------------------------------------------------------
    posts_to_insert = []

    for info in infos:
        # dados de chunk
        summary, content, subject = "", "", "Industry Update"
        if info.chunk_ids:
            ch = chunk_docs.get(info.chunk_ids[0])
            if ch:
                summary = (ch.get("summary") or "").lstrip()
                content = (ch.get("content") or "").lstrip()
                subject = (ch.get("subject") or subject).lstrip()

        # source
        source_name = "Unknown"
        if info.sourcesId:
            src = source_docs.get(info.sourcesId[0])
            if src and src.get("name"):
                source_name = src["name"]

        # usuários
        user_ids = []
        for cid in info.companiesId:
            user_ids.extend(users_by_company.get(cid, []))
        user_ids = list(set(user_ids))  # remove duplicados
        if not user_ids:
            continue

        # construir posts para cada usuário – reaproveita embedding existente
        emb = info.embedding if getattr(info, "embedding", None) else get_embedding(summary)
        for uid in user_ids:
            post_obj = Post(
                infoId   = str(info.id),
                userId   = uid,
                source   = source_name,
                title    = subject or "Industry Update",
                embedding= emb,
                content  = f"{summary}:\n\n ```{content}```" if content else summary or "Industry update",
                timestamp= relative_time(info.created_at),
                created_at = info.created_at,
            )
            posts_to_insert.append(post_obj.model_dump(by_alias=True))

    if not posts_to_insert:
        return 0

    # --- Inserção única -----------------------------------------------------------
    try:
        result = posts_coll.insert_many(posts_to_insert, ordered=False)
        return len(result.inserted_ids)
    except errors.BulkWriteError as bwe:
        success = len(posts_to_insert) - len(bwe.details.get("writeErrors", []))
        return success
    except Exception as e:
        logger.error(f"[POSTS] Erro ao inserir posts em batch: {e}")
        return 0

def _process_single_email(email_id: str = ''):
    """
    db_name_alphasync = "alphasync_db_dev"
    db_name_stkfeed = "STKFeed_dev"
    Process a single email by retrieving it from Outlook and inserting it into MongoDB.
    """
    if email_id == '':
        email_id = get_recent_emails(top_n=1)[0]["id"]
    email = get_email_by_id(email_id)
    
    try:
        email_obj = Email(
            message_id=email["id"],from_address=email["from"],subject=email["subject"],body=email["body"],received_at=email["receivedDateTime"],
        )
    except Exception as e:
        logger.error(f"Erro ao criar objeto Email: {e}")
        return
    
    #insert email into mongodb check if the email is already in the database
    collection = get_mongo_collection(db_name=db_name_alphasync, collection_name="emails")
    if not collection.find_one({"message_id": email_obj.message_id}):
        collection.insert_one(email_obj.to_formatted_dict(by_alias=True))
    
    #check if relevant is true
    try:
        is_relevant = check_relevant_email(email_obj)
        if is_relevant:
            email_obj.relevant = is_relevant
    except Exception as e:
        logger.error(f"Erro ao verificar relevância do email: {e}")
        return  

    #update email in mongodb
    collection.update_one(
        {"_id": email_obj.id},
        {"$set": {"relevant": email_obj.relevant}},
    )

    #chunkenize email
    chunk_data = chunkenize_single_email(email_obj)
    chunk_objects = create_chunks_object(chunk_data, email_obj)

    #bulk insert chunks into mongodb
    if chunk_objects:
        chunks_collection = get_mongo_collection(db_name=db_name_alphasync, collection_name="chunks")
        chunks_collection.insert_many([chunk.model_dump(by_alias=True) for chunk in chunk_objects])

    else:
        logger.warning(f"Nenhum chunk criado para o email {email_obj.id}")
    
    #grab relevant chunks from chunk_objects
    relevant_chunks = [chunk for chunk in chunk_objects if chunk.include]
    
    # users_by_company: Dict[str, List[str]] | None = None
    users_by_company={}
    
    ## Process companies
    companies_ids: dict = {}

    #grab companies ids from relevant chunks
    instruments_ids = []
    for chunk in relevant_chunks:
        if chunk.instrument_ids:
            instruments_ids.extend(chunk.instrument_ids)
    instruments_ids = list(set(instruments_ids))
    
    #grab companies from companies_ids
    if instruments_ids:
        companies_collection = get_mongo_collection(db_name=db_name_alphasync, collection_name="companies")
        companies_ids, _new_company_ids = intruments_to_companies_ids_parallel(
            instruments_ids, companies_collection, return_new_company_ids=True
        )

        # garantir usuários para todas as empresas envolvidas
        all_company_ids = list(set(companies_ids.values()))
        users_by_company = get_or_create_users_by_companies(all_company_ids)
    #log companies names and ids created
    logger.info(f"Companies names and ids created: {companies_ids}")
    ## Process sources
    sources_ids: dict = {}
    sources_list = []
    for chunk in relevant_chunks:
        if chunk.source:
            sources_list.append(chunk.source)
    sources_list = list(set(sources_list))    

    if sources_list:
        #grab sources from sources_list
        sources_collection = get_mongo_collection(db_name=db_name_alphasync, collection_name="sources")
        sources_ids = intruments_to_sources_ids_parallel(sources_list, sources_collection)
    
    ## Process infos
    logger.info("Buscando correspondências existentes no banco de dados...")
    
    #use upsert_infos_for_chunks to create infos
    if relevant_chunks:
        mapping, new_infos = upsert_infos_for_chunks(
            relevant_chunks,
            companies_ids,
            sources_ids,
            db_name=db_name_alphasync,
            return_new_infos=True,
        )

        if users_by_company:
            total_created_posts = create_posts_from_infos(new_infos, users_by_company)
        else:
            total_created_posts = 0
        #log posts ids created
        logger.info(f"[POSTS] Posts ids created: {total_created_posts}")

        logger.info(f"[POSTS] Total de posts criados a partir deste email: {total_created_posts}")
    
    return


