import re
import logging
import ray
import pandas as pd
from database import process_search_row, get_endpoint
from typing import List, Dict, Optional

@ray.remote
def process_db_row(entry_id: int, search_string: str, search_type: str, endpoint: str, logger: Optional[logging.Logger] = None) -> Dict:
    """Process a single database row, manipulating the search_string based on search_type."""
    logger = logger or logging.getLogger(__name__)
    try:
        # Validate the search string
        if not search_string or not isinstance(search_string, str) or search_string.strip() == "":
            logger.warning(f"Invalid search string for EntryID {entry_id}, SearchType {search_type}: {search_string}")
            return {
                "entry_id": entry_id,
                "search_type": search_type,
                "status": "skipped",
                "error": "Empty or invalid search string",
                "result": None
            }

        # Manipulate the search_string based on search_type
        original_search_string = search_string  # Keep the original for logging

        if search_type == "default":
            # For default search: lowercase and remove special characters
            search_string = search_string.lower()
            # search_string = re.sub(r'[^a-z0-9 ]', '', search_string)
            variations = [search_string]
        elif search_type == "retry_with_alternative":
            # Use special characters as delimiters (spaces, hyphens, underscores)
            delimiters = r'[\s\-_ ]+'
            parts = re.split(delimiters, search_string)
            # Generate variations by removing one part at a time from right to left
            variations = [' '.join(parts[:i]) for i in range(len(parts), 0, -1)]
        else:
            # For unrecognized search_types, use the string as is
            logger.info(f"No specific manipulation for SearchType {search_type}")
            variations = [search_string]

        logger.info(f"Manipulated search string for EntryID {entry_id} (SearchType: {search_type}): {variations} original: {original_search_string}")

        # Try each variation in sequence
        for idx, variation in enumerate(variations):
            logger.info(f"Attempting variation {idx+1} for EntryID {entry_id}: {variation}")
            result = process_search_row(variation, endpoint, entry_id, logger=logger, search_type=search_type)
            if isinstance(result, pd.DataFrame) and not result.empty:
                logger.info(f"Successfully processed {search_type} search for EntryID {entry_id} with {len(result)} images using variation: {variation}")
                return {
                    "entry_id": entry_id,
                    "search_type": search_type,
                    "status": "success",
                    "result_count": len(result),
                    "result": result,
                    "used_variation": variation
                }

        # If all variations fail
        logger.warning(f"All variations failed for EntryID {entry_id}")
        return {
            "entry_id": entry_id,
            "search_type": search_type,
            "status": "failed",
            "result_count": 0,
            "result": None,
            "error": "All search variations failed"
        }

    except Exception as e:
        logger.error(f"Error processing {search_type} search for EntryID {entry_id}: {e}", exc_info=True)
        return {
            "entry_id": entry_id,
            "search_type": search_type,
            "status": "failed",
            "error": str(e),
            "result": None
        }
import pyodbc
from image_process import batch_process_images  
import asyncio
from database import insert_search_results, update_sort_order_based_on_match_score, default_logger
from config import conn_str, engine
async def process_file_with_retries(file_id, max_retries=2, logger=None):

    """
    Process all entries for a given file_id, retrying with different search types if necessary.

    Args:
        file_id (str): The ID of the file to process.
        max_retries (int): Maximum number of retry attempts for entries with invalid results.
        logger (Optional[logging.Logger]): Logger instance for logging messages.

    Returns:
        None
    """
    logger = logger or default_logger
    try:
        # Step 1: Get all entry IDs for the file_id
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT EntryID FROM utb_ImageScraperRecords WHERE FileID = ?", (file_id,))
            all_entry_ids = [row[0] for row in cursor.fetchall()]
        
        # Initialize entries to process and search types
        entries_to_process = set(all_entry_ids)
        search_types = ["default", "retry_with_alternative"]
        
        # Step 2: Process entries with retries
        for retry in range(max_retries):
            if not entries_to_process:
                logger.info("All entries have valid results")
                break
            search_type = search_types[retry % len(search_types)]
            logger.info(f"Retry {retry+1}: Processing {len(entries_to_process)} entries with search_type '{search_type}'")
            
            # Step 3: Prepare batch for processing
            batch = []
            for entry_id in entries_to_process:
                cursor.execute("SELECT ProductModel FROM utb_ImageScraperRecords WHERE EntryID = ?", (entry_id,))
                search_string = cursor.fetchone()[0]
                batch.append({"EntryID": entry_id, "SearchString": search_string, "SearchType": search_type})
            
            # Step 4: Run search and sort
            results_ref = process_batch.remote(batch, logger=logger)
            results = ray.get(results_ref)
            
            # Step 5: Insert search results into the database
            for res in results:
                if res["status"] == "success":
                    insert_search_results(res["result"], logger=logger)
            
            # Step 6: Run AI analysis
            await batch_process_images(file_id, logger=logger)
            
            # Step 7: Run sort by match and SSIM
            update_sort_order_based_on_match_score(file_id, logger=logger)
            
            # Step 8: Check for entries with all results -2 or -1
            cursor.execute("""
                SELECT EntryID
                FROM utb_ImageScraperRecords
                WHERE FileID = ?
                AND EntryID NOT IN (
                    SELECT EntryID
                    FROM utb_ImageScraperResult
                    WHERE EntryID IN (SELECT EntryID FROM utb_ImageScraperRecords WHERE FileID = ?)
                    AND SortOrder >= 0
                )
            """, (file_id, file_id))
            remaining_entries = [row[0] for row in cursor.fetchall()]
            entries_to_process = set(remaining_entries)
            logger.info(f"After retry {retry+1}, {len(entries_to_process)} entries still need processing")
        
        # Step 9: Final status check
        if entries_to_process:
            logger.warning(f"After {max_retries} retries, {len(entries_to_process)} entries still have no valid results")
        else:
            logger.info("All entries have valid results")
    except Exception as e:
        logger.error(f"Error in process_file_with_retries: {e}", exc_info=True)


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