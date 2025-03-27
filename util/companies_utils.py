
from util.mongodb_utils import get_mongo_collection
from util.embedding_utils import get_embedding
from models.companies import Companies
from typing import List, Dict, Any, Optional
from datetime import datetime
from pymongo import errors
import logging
import json
import asyncio
from util.langchain_utils import connect_to_graph_execution


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from util.parsing_utils import extract_brace_arguments, extract_json_from_content
import re


def parse_companies(content: str) -> List[Dict[str, Any]]:
    cleaned = extract_json_from_content(content).strip()
    cleaned = re.sub(r",\s*([}\]])", r"\1", cleaned)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError as e:
        logger.error(f"Company parsing error: {e}")
        return []

def get_candidate_companies(company: str, companies_collection, similarity_threshold: float = 0.7) -> List[Dict[str, Any]]:
    """
    Return a list of candidate companies from companies_collection with similarity > threshold.
    Each candidate is a dict with fields: name, ticker, public, parent_company, description, sector.
    """
    embedding = get_embedding(company)
    results = companies_collection.aggregate([
        {
            "$vectorSearch": {
                "index": "vector_index",  # assuming same index
                "path": "embedding",
                "queryVector": embedding,
                "numCandidates": 3,
                "limit": 3,
            }
        },
        {
            "$project": {
                "similarityScore": {"$meta": "vectorSearchScore"},
                "document": "$$ROOT",
            }
        },
    ])
    candidate_list = []
    for result in results:
        score = result.get("similarityScore", 0)
        if score > similarity_threshold:
            doc = result["document"]
            candidate = {
                "id": doc.get("_id", ""),
                "name": doc.get("name", ""),
                "ticker": doc.get("ticker", ""),
                "public": doc.get("public", False),
                "parent_company": doc.get("parent_company", ""),
                "description": doc.get("description", ""),
                "sector": doc.get("sector", "")  # Added sector here
            }
            candidate_list.append(candidate)
    return candidate_list


def grab_tickers_company(target: str, candidates: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Call the ticker-guesser graph with input containing:
      - ticker: target (string)
      - candidates: list of candidate companies (each with fields: name, ticker, public, parent_company, description)
    Returns a dict with fields: name, ticker, public, parent_company, description, already_exists.
    """
    #target="Adecoagro"
    #candidates=[]
    input_payload = json.dumps({
        "ticker": target,
        "candidates": candidates
    })
    response = asyncio.run(
        connect_to_graph_execution("66ec6713966174697a68ed23", initial_message=input_payload)
    )
    content = response[0]["step"]["Ticker Guesser"][-1]["content"]
    # Use parse_companies to parse the JSON output (which should be a list)
    result_list = parse_companies(content)
    return result_list[0] if result_list else {}




def find_similar_company(embedding: List[float], companies_collection, similarity_threshold: float = 0.9) -> Optional[Companies]:
    results = companies_collection.aggregate([
        {
            "$vectorSearch": {
                "index": "vector_index",  # adjust index name if necessary
                "path": "embedding",
                "queryVector": embedding,
                "numCandidates": 10,
                "limit": 10,
            }
        },
        {
            "$project": {
                "similarityScore": {"$meta": "vectorSearchScore"},
                "document": "$$ROOT",
            }
        },
    ])
    results_list = list(results)
    if results_list:
        most_similar = results_list[0]
        if most_similar["similarityScore"] >= similarity_threshold:
            return Companies(**most_similar["document"])
    return None


def intruments_to_companies_ids(instruments: List[str], companies_collection) -> List[str]:
    """
    Convert a list of instrument names to a list of company IDs.
    """
    #instruments = ["Bradesco Banco Brasileiro S.A.", "XP Inc."]
    #companies_collection = get_mongo_collection(collection_name="companies")
    companies_ids = []
        
    if instruments != []:
        
        for company in instruments:
            #company =instruments[1]
            
            try:
                company_embedding = get_embedding(company)
            except Exception as e:
                logger.error(f"Error generating embedding for company '{company}': {e}")
                continue
            existing_company = find_similar_company(company_embedding, companies_collection)
            if existing_company:
                companies_ids.append(existing_company.id)
            else:
                # Use ticker-guesser graph to determine company info
                candidates = get_candidate_companies(company, companies_collection)
                
                ticker_data = grab_tickers_company(company, candidates)
                
                new_company = Companies(
                    name=ticker_data.get("name", company),
                    ticker=ticker_data.get("ticker", ""),
                    public=ticker_data.get("public", False),
                    parent_company=ticker_data.get("parent_company", ""),
                    description=ticker_data.get("description", ""),
                    sector=ticker_data.get("sector", ""),  # Include sector value from ticker_data
                    embedding=company_embedding,
                    created_at=datetime.now(),
                )
                
                try:
                    if ticker_data.get("already_exists", False):
                        new_company.id = ticker_data.get("id", "")
                        result = companies_collection.update_one(
                            {"id": ticker_data.get("id", "")},
                            {"$set": new_company.model_dump(by_alias=True)}
                        )
                        logger.info(f"Updated existing company '{company}' with ID: {new_company.id}")
                    else:
                        result = companies_collection.insert_one(new_company.model_dump(by_alias=True))
                        new_company.id = result.inserted_id if result.inserted_id else new_company.id
                        logger.info(f"Inserted new company '{company}' with ID: {new_company.id}")
                    
                    companies_ids.append(new_company.id)
                    
                except errors.PyMongoError as e:
                    logger.error(f"MongoDB error inserting company '{company}': {e}")
    return companies_ids