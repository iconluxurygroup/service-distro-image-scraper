import logging
import ray
import pandas as pd
from database import process_search_row, get_endpoint
from typing import List, Dict, Optional

@ray.remote
def process_db_row(entry_id: int, search_string: str, search_type: str, endpoint: str, logger: Optional[logging.Logger] = None) -> Dict:
    """Process a single database row with a specific search type using Ray."""
    logger = logger or logging.getLogger(__name__)
    try:
        if not search_string or not isinstance(search_string, str) or search_string.strip() == "":
            logger.warning(f"Invalid search string for EntryID {entry_id}, SearchType {search_type}: {search_string}")
            return {
                "entry_id": entry_id,
                "search_type": search_type,
                "status": "skipped",
                "error": "Empty or invalid search string",
                "result": None
            }

        logger.info(f"Processing {search_type} search for EntryID {entry_id}: {search_string}")
        result = process_search_row(search_string, endpoint, entry_id, logger=logger)

        if isinstance(result, pd.DataFrame) and not result.empty:
            logger.info(f"🟢 ✅ Successfully processed {search_type} search for EntryID {entry_id} with {len(result)} images")
            return {
                "entry_id": entry_id,
                "search_type": search_type,
                "status": "success",
                "result_count": len(result),
                "result": result  # Include the DataFrame
            }
        else:
            logger.warning(f"No valid results for {search_type} search for EntryID {entry_id}")
            return {
                "entry_id": entry_id,
                "search_type": search_type,
                "status": "failed",
                "result_count": 0,
                "result": None
            }
    except Exception as e:
        logger.error(f"🔴 Error processing {search_type} search for EntryID {entry_id}: {e}", exc_info=True)
        return {
            "entry_id": entry_id,
            "search_type": search_type,
            "status": "failed",
            "error": str(e),
            "result": None
        }
@ray.remote
def process_batch(batch: List[Dict], logger: Optional[logging.Logger] = None) -> List[Dict]:
    """Process a batch of database rows with dual searches in parallel."""
    logger = logger or logging.getLogger(__name__)
    try:
        if not batch:
            logger.warning("Empty batch received")
            return []

        endpoint = get_endpoint(logger=logger)
        if not endpoint:
            logger.error("No healthy endpoint found")
            return [{"entry_id": row['EntryID'], "search_type": row['SearchType'], "status": "failed", "error": "No endpoint", "result": None} for row in batch]

        logger.info(f"⚙️ Processing batch of {len(batch)} search tasks with endpoint {endpoint}")

        futures = [
            process_db_row.remote(row['EntryID'], row['SearchString'], row['SearchType'], endpoint, logger=logger)
            for row in batch if row.get('SearchString')
        ]
        results = ray.get(futures)

        success_count = sum(1 for r in results if r['status'] == 'success')
        logger.info(f"Batch completed: {success_count}/{len(results)} successful")
        return results
    except Exception as e:
        logger.error(f"🔴 Error processing batch: {e}", exc_info=True)
        return [{"entry_id": "unknown", "search_type": "unknown", "status": "failed", "error": str(e), "result": None}]