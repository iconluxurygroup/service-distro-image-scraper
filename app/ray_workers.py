import ray
import logging
from database import process_search_row, get_endpoint

logging.getLogger(__name__)
@ray.remote
def process_db_row(entry_id, search_string, search_type, endpoint):
    """Process a single database row with a specific search type using Ray."""
    try:
        if not search_string or not isinstance(search_string, str) or search_string.strip() == "":
            logging.warning(f"Invalid search string for EntryID {entry_id}, SearchType {search_type}: {search_string}")
            return {
                "entry_id": entry_id,
                "search_type": search_type,
                "status": "skipped",
                "error": "Empty or invalid search string"
            }
        
        logging.info(f"Processing {search_type} search for EntryID {entry_id}: {search_string}")
        result = process_search_row(search_string, endpoint, entry_id)
        return {
            "entry_id": entry_id,
            "search_type": search_type,
            "status": "success" if result else "failed",
            "result_count": result if isinstance(result, int) else None  # Assuming process_search_row returns count
        }
    except Exception as e:
        logging.error(f"Error processing {search_type} search for EntryID {entry_id}: {e}")
        return {
            "entry_id": entry_id,
            "search_type": search_type,
            "status": "failed",
            "error": str(e)
        }
@ray.remote
def process_batch(batch):
    """
    Process a batch of database rows with dual searches in parallel.
    
    Args:
        batch (list): List of dicts with 'EntryID', 'SearchString', and 'SearchType'
    
    Returns:
        list: Results of all search tasks
    """
    try:
        if not batch:
            logging.warning("Empty batch received")
            return []
        
        endpoint = get_endpoint()  # Get endpoint once per batch
        logging.info(f"Processing batch of {len(batch)} search tasks with endpoint {endpoint}")
        
        # Launch dual searches in parallel
        futures = [
            process_db_row.remote(row['EntryID'], row['SearchString'], row['SearchType'], endpoint)
            for row in batch
            if row.get('SearchString')  # Skip rows with missing SearchString
        ]
        results = ray.get(futures)
        
        # Log batch summary
        success_count = sum(1 for r in results if r['status'] == 'success')
        skipped_count = sum(1 for r in results if r['status'] == 'skipped')
        logging.info(f"Batch completed: {success_count}/{len(results)} successful, {skipped_count} skipped")
        return results
    except Exception as e:
        logging.error(f"Error processing batch: {e}")
        return [{"entry_id": "unknown", "search_type": "unknown", "status": "failed", "error": str(e)}]