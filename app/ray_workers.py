# ray_workers.py
import logging
import ray
from database import process_search_row, get_endpoint

@ray.remote
def process_db_row(entry_id, search_string, search_type, endpoint, logger=None):
    """Process a single database row with a specific search type using Ray."""
    logger = logger or logging.getLogger(__name__)
    try:
        if not search_string or not isinstance(search_string, str) or search_string.strip() == "":
            logger.warning(f"Invalid search string for EntryID {entry_id}, SearchType {search_type}: {search_string}")
            return {
                "entry_id": entry_id,
                "search_type": search_type,
                "status": "skipped",
                "error": "Empty or invalid search string"
            }
        
        logger.info(f"Processing {search_type} search for EntryID {entry_id}: {search_string}")
        result = process_search_row(search_string, endpoint, entry_id, logger=logger)
        return {
            "entry_id": entry_id,
            "search_type": search_type,
            "status": "success" if result else "failed",
            "result_count": len(result) if result and isinstance(result, pd.DataFrame) else 0
        }
    except Exception as e:
        logger.error(f"Error processing {search_type} search for EntryID {entry_id}: {e}")
        return {
            "entry_id": entry_id,
            "search_type": search_type,
            "status": "failed",
            "error": str(e)
        }

@ray.remote
def process_batch(batch, logger=None):
    """
    Process a batch of database rows with dual searches in parallel.
    
    Args:
        batch (list): List of dicts with 'EntryID', 'SearchString', and 'SearchType'
        logger: Logger instance
    
    Returns:
        list: Results of all search tasks
    """
    logger = logger or logging.getLogger(__name__)
    try:
        if not batch:
            logger.warning("Empty batch received")
            return []
        
        endpoint = get_endpoint(logger=logger)
        logger.info(f"Processing batch of {len(batch)} search tasks with endpoint {endpoint}")
        
        # Launch dual searches in parallel
        futures = [
            process_db_row.remote(row['EntryID'], row['SearchString'], row['SearchType'], endpoint, logger=logger)
            for row in batch
            if row.get('SearchString')  # Skip rows with missing SearchString
        ]
        results = ray.get(futures)
        
        # Log batch summary
        success_count = sum(1 for r in results if r['status'] == 'success')
        skipped_count = sum(1 for r in results if r['status'] == 'skipped')
        logger.info(f"Batch completed: {success_count}/{len(results)} successful, {skipped_count} skipped")
        return results
    except Exception as e:
        logger.error(f"Error processing batch: {e}")
        return [{"entry_id": "unknown", "search_type": "unknown", "status": "failed", "error": str(e)}]