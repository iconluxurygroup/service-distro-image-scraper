import pyodbc
import pandas as pd
import logging
import requests
import zlib
import urllib
import base64
from icon_image_lib.google_parser import process_search_result
from config import conn_str, engine
from urllib3.util.retry import Retry
from typing import List, Optional
import urllib.parse
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException, Timeout, ConnectionError
import json
import re
import pyodbc
import pandas as pd
import logging
import requests
from skimage import io as skio
from skimage.metrics import structural_similarity as ssim
from skimage.transform import resize
import numpy as np
from io import BytesIO
from typing import Optional
from io import BytesIO
import skimage.io as skio
from PIL import Image
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging
import urllib.parse
import json
import pandas as pd
# Fallback logger for standalone calls
default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

async def update_log_url_in_db(file_id, log_url, logger=None):
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            # Add LogFileUrl column if it doesn't exist
            cursor.execute("""
                IF NOT EXISTS (
                    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_NAME = 'utb_ImageScraperFiles' 
                    AND COLUMN_NAME = 'LogFileUrl'
                )
                BEGIN
                    ALTER TABLE utb_ImageScraperFiles 
                    ADD LogFileUrl NVARCHAR(MAX)
                END
            """)
            # Update the log file URL
            update_query = """
                UPDATE utb_ImageScraperFiles 
                SET LogFileUrl = ? 
                WHERE ID = ?
            """
            cursor.execute(update_query, (log_url, file_id))
            conn.commit()
            logger.info(f"✅ Successfully updated log URL '{log_url}' for FileID {file_id}")
            return True
    except Exception as e:
        logger.error(f"🔴 Error updating log URL for FileID {file_id}: {e}", exc_info=True)
        return False

def unpack_content(encoded_content, logger=None):
    logger = logger or default_logger
    try:
        if encoded_content:
            compressed_content = base64.b64decode(encoded_content)
            original_content = zlib.decompress(compressed_content)
            return original_content
        return None
    except Exception as e:
        logger.error(f"Error unpacking content: {e}")
        return None

def get_records_to_search(file_id, logger=None):
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        sql_query = """
            SELECT EntryID, ProductModel AS SearchString, 'model_only' AS SearchType, FileID
            FROM utb_ImageScraperRecords 
            WHERE FileID = ? AND Step1 IS NULL
            ORDER BY EntryID, SearchType
        """
        with pyodbc.connect(conn_str) as connection:
            df = pd.read_sql_query(sql_query, connection, params=[file_id])
        if not df.empty:
            invalid_rows = df[df['FileID'] != file_id]
            if not invalid_rows.empty:
                logger.error(f"Found {len(invalid_rows)} rows with incorrect FileID for requested FileID {file_id}: {invalid_rows[['EntryID', 'FileID']].to_dict()}")
                df = df[df['FileID'] == file_id]
        logger.info(f"Got {len(df)} search records for FileID: {file_id}")
        return df[['EntryID', 'SearchString', 'SearchType']]
    except Exception as e:
        logger.error(f"Error getting records to search for FileID {file_id}: {e}")
        return pd.DataFrame()

def check_endpoint_health(endpoint: str, timeout: int = 5) -> bool:
    """Check if the endpoint can reach Google using /health/google."""
    health_url = f"{endpoint}/health/google"
    try:
        response = requests.get(health_url, timeout=timeout)
        if response.status_code != 200:
            return False
        status = response.json().get("status", "")
        return "Google is reachable" in status
    except (requests.RequestException, json.JSONDecodeError):
        return False

def update_sort_order_based_on_match_score(file_id, logger=None):
    """
    Update the SortOrder in utb_ImageScraperResult based on match scores from aijson.
    For entries with the same top match score per EntryID, use SSIM against an optimal image to break ties.
    
    Args:
        file_id (int): The FileID to filter records.
        logger (logging.Logger, optional): Logger instance. Defaults to default_logger.
    """
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        with pyodbc.connect(conn_str) as connection:
            connection.timeout = 300
            cursor = connection.cursor()
            logger.info(f"🔄 Updating SortOrder based on match_score for FileID: {file_id}")

            # Define category keywords for mapping
            category_keywords = {
                "shoe": ["shoe", "shoes", "sneaker", "sneakers", "boot", "boots", "sandal", "sandals"],
                "pant": ["pant", "pants", "trouser", "trousers", "jean", "jeans", "short", "shorts"],
                "t-shirt": ["t-shirt", "tshirt", "tee", "shirt","sweatshirt"],
                "jacket": ["jacket", "jackets", "coat", "coats", "blazer"],
                "hat": ["hat", "hats", "cap", "beanie"]
            }

            def map_category(input_str: str) -> str:
                """Map raw category to standardized category based on keywords."""
                normalized = input_str.lower().replace(" ", "").replace("-", "").replace("—", "")
                max_pos = -1
                selected_category = None
                for category, keywords in category_keywords.items():
                    for keyword in keywords:
                        keyword_normalized = keyword.replace(" ", "").replace("-", "")
                        pos = normalized.rfind(keyword_normalized)
                        if pos > max_pos:
                            max_pos = pos
                            selected_category = category
                if selected_category:
                    return selected_category
                logger.warning(f"No category keyword found in: {input_str}")
                return input_str.lower()
            # Define load_image before using it
            def load_image(url_or_path: str) -> Optional[np.ndarray]:
                """Load an image as a grayscale numpy array."""
                try:
                    if url_or_path.startswith('http'):
                        response = requests.get(url_or_path, timeout=10)
                        response.raise_for_status()
                        image_data = BytesIO(response.content)
                        img = Image.open(image_data)
                        if img.mode == 'P':  # Palette image
                            img = img.convert('RGBA')
                        return skio.imread(BytesIO(response.content), as_gray=True)
                    else:
                        img = Image.open(url_or_path)
                        if img.mode == 'P':
                            img = img.convert('RGBA')
                        return skio.imread(url_or_path, as_gray=True)
                except Exception as e:
                    logger.error(f"Error loading image from {url_or_path}: {e}")
                    return None
            # Define optimal reference URLs (update with actual paths/URLs as needed)
            category_to_ref_url ={
            "shoe": "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcQ7MGfepTaFjpQhcNFyetjseybIRxLUe58eag&s",
            "clothing": "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcTyYe3Vgmztdh089e9IHLqdPPLuE2jUtV8IZg&s",  # Replace with actual path or URL
            "pant": "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcStaRmmSmbuIuozGgVJa6GHuR59RuW3W8_8jA&s",
            "jeans": "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcSjiKQ5mZWi6qnWCr6Yca5_AFinCDZXhXhiAg&s",
            "accessories": "path/to/optimal_accessories.jpg",  # Replace with actual path or URL
            "t-shirt": "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcTyYe3Vgmztdh089e9IHLqdPPLuE2jUtV8IZg&s",
            "jacket": "path/to/optimal_jacket.jpg",  # Replace with actual path or URL
            "hat": "path/to/optimal_hat.jpg",  # Replace with actual path or URL
            }



            # Load optimal reference images
            optimal_references = {}
            for category, url in category_to_ref_url.items():
                image = load_image(url)
                if image is not None:
                    optimal_references[category] = image
                else:
                    logger.warning(f"Failed to load reference image for category: {category}")

            def download_image(url: str, thumbnail_url: Optional[str] = None, logger=None) -> Optional[bytes]:
                """Download image data from the main URL, falling back to the thumbnail URL if provided."""
                logger = logger or default_logger
                
                # Validate and try main URL first
                if url and is_valid_url(url):
                    try:
                        logger.info(f"Attempting to download image from main URL: {url}")
                        response = requests.get(url, timeout=10)
                        response.raise_for_status()  # Raises exception for 4xx/5xx errors
                        logger.info(f"Successfully downloaded image from main URL: {url}")
                        return response.content
                    except requests.RequestException as e:
                        logger.warning(f"Failed to download from main URL {url}: {e}")
                
                # Fallback to thumbnail URL if provided
                if thumbnail_url and is_valid_url(thumbnail_url):
                    try:
                        logger.info(f"Attempting to download image from thumbnail URL: {thumbnail_url}")
                        response = requests.get(thumbnail_url, timeout=10)
                        response.raise_for_status()
                        logger.info(f"Successfully downloaded image from thumbnail URL: {thumbnail_url}")
                        return response.content
                    except requests.RequestException as e:
                        logger.warning(f"Failed to download from thumbnail URL {thumbnail_url}: {e}")
                
                logger.error(f"All attempts failed for main URL {url} and thumbnail URL {thumbnail_url}")
                return None

            # Helper function to validate URLs
            def is_valid_url(url: str) -> bool:
                """Check if the URL is valid."""
                return bool(url and url.startswith("http") and re.match(r'^https?://[^\s/$.?#].[^\s]*$', url))

            def calculate_ssim(image_data: bytes, reference_image: np.ndarray) -> float:
                """Calculate SSIM between downloaded image and reference image."""
                try:
                    img = skio.imread(BytesIO(image_data), as_gray=True)
                    img_resized = resize(img, reference_image.shape, anti_aliasing=True)
                    score = ssim(img_resized, reference_image, data_range=img_resized.max() - img_resized.min())
                    return score
                except Exception as e:
                    logger.error(f"Error calculating SSIM: {e}")
                    return -1

            query = """
            SELECT 
                t.ResultID,
                t.EntryID,
                t.ImageURL,
                t.ImageURLThumbnail,  -- Added thumbnail URL
                r.ProductCategory,
                ISNULL(CAST(JSON_VALUE(t.aijson, '$.match_score') AS FLOAT), 0) AS match_score
            FROM 
                utb_ImageScraperResult t
            INNER JOIN 
                utb_ImageScraperRecords r ON r.EntryID = t.EntryID
            WHERE 
                r.FileID = ?
            """
            df = pd.read_sql_query(query, connection, params=[file_id])
            df['match_score'] = pd.to_numeric(df['match_score'], errors='coerce').fillna(0)

            # Process each EntryID group
            grouped = df.groupby('EntryID')
            for entry_id, group in grouped:
                group = group.sort_values('match_score', ascending=False)
                top_score = group['match_score'].iloc[0]
                top_group = group[group['match_score'] == top_score]

                if len(top_group) > 1:
                    raw_category = top_group['ProductCategory'].iloc[0]
                    category = map_category(raw_category)
                    reference_image = optimal_references.get(category)
                    if reference_image is None:
                        logger.warning(f"No reference image for category: {category}")
                        top_group = top_group.sort_values('ResultID')
                    else:
                        ssim_scores = []
                        for idx, row in top_group.iterrows():
                            image_url = row['ImageURL']
                            thumbnail_url = row['ImageURLThumbnail']  # Pass thumbnail URL
                            image_data = download_image(image_url, thumbnail_url, logger)  # Updated call
                            ssim_score = -1 if not image_data else calculate_ssim(image_data, reference_image)
                            ssim_scores.append(ssim_score)
                        top_group = top_group.assign(ssim_score=ssim_scores).sort_values('ssim_score', ascending=False)
                
                remaining_group = group[group['match_score'] < top_score].sort_values('match_score', ascending=False)
                sorted_group = pd.concat([top_group, remaining_group])
                sorted_group['new_sort_order'] = range(1, len(sorted_group) + 1)

                for idx, row in sorted_group.iterrows():
                    cursor.execute(
                        "UPDATE utb_ImageScraperResult SET SortOrder = ? WHERE ResultID = ?",
                        (row['new_sort_order'], row['ResultID'])
                    )

            connection.commit()
            logger.info(f"Successfully updated SortOrder for FileID: {file_id}")

    except Exception as e:
        logger.error(f"Error updating SortOrder for FileID {file_id}: {e}", exc_info=True)
        if 'connection' in locals():
            connection.rollback()
        raise
    finally:
        if 'connection' in locals():
            connection.close()

def get_healthy_endpoint(endpoints: List[str], logger) -> Optional[str]:
    """Return the first healthy endpoint where Google is reachable."""
    for endpoint in endpoints:
        logger.debug(f"Checking health of {endpoint}")
        if check_endpoint_health(endpoint):
            logger.info(f"Selected healthy endpoint: {endpoint}")
            return endpoint
    logger.error("No healthy endpoints found")
    return None

def process_search_row_gcloud(search_string: str, entry_id: int, logger=None):
    logger = logger or default_logger
    logger.debug(f"Entering process_search_row_gcloud with search_string={search_string}, entry_id={entry_id}")
    logger.info(f"🔎 Search started for {search_string}")
    logger.info(f"📓 Entry ID: {entry_id}")
    try:
        if not search_string or len(search_string.strip()) < 3:
            logger.warning(f"Invalid search string for EntryID {entry_id}: '{search_string}'")
            return False
        fetch_endpoints = [
            "https://southamerica-west1-image-scraper-451516.cloudfunctions.net/main",
            "https://us-central1-image-scraper-451516.cloudfunctions.net/main",
            "https://us-east1-image-scraper-451516.cloudfunctions.net/main",
            "https://us-east4-image-scraper-451516.cloudfunctions.net/main",
            "https://us-west1-image-scraper-451516.cloudfunctions.net/main",
            "https://europe-west4-image-scraper-451516.cloudfunctions.net/main",
            "https://us-west4-image-proxy-453319.cloudfunctions.net/main",
            "https://europe-west1-image-proxy-453319.cloudfunctions.net/main",
            "https://europe-north1-image-proxy-453319.cloudfunctions.net/main",
            "https://asia-east1-image-proxy-453319.cloudfunctions.net/main",
            "https://us-south1-gen-lang-client-0697423475.cloudfunctions.net/main",
            "https://us-west3-gen-lang-client-0697423475.cloudfunctions.net/main",
            "https://us-east5-gen-lang-client-0697423475.cloudfunctions.net/main",
            "https://asia-southeast1-gen-lang-client-0697423475.cloudfunctions.net/main",
            "https://us-west2-gen-lang-client-0697423475.cloudfunctions.net/main",
            "https://northamerica-northeast2-image-proxy2-453320.cloudfunctions.net/main",
            "https://southamerica-east1-image-proxy2-453320.cloudfunctions.net/main",
            "https://europe-west8-icon-image3.cloudfunctions.net/main",
            "https://europe-southwest1-icon-image3.cloudfunctions.net/main",
            "https://europe-west6-icon-image3.cloudfunctions.net/main",
            "https://europe-west3-icon-image3.cloudfunctions.net/main",
            "https://europe-west2-icon-image3.cloudfunctions.net/main",
            "https://europe-west9-image-proxy2-453320.cloudfunctions.net/main",
            "https://me-west1-image-proxy4.cloudfunctions.net/main",
            "https://me-central1-image-proxy4.cloudfunctions.net/main",
            "https://europe-west12-image-proxy4.cloudfunctions.net/main",
            "https://europe-west10-image-proxy4.cloudfunctions.net/main",
            "https://asia-northeast2-image-proxy4.cloudfunctions.net/main"
        ]
        available_endpoints = fetch_endpoints.copy()
        attempt_count = 0
        max_attempts = len(fetch_endpoints)
        while attempt_count < max_attempts:
            base_endpoint = get_healthy_endpoint(available_endpoints, logger)
            if not base_endpoint:
                logger.error("No healthy endpoints available after exhausting all options")
                return False
            fetch_endpoint = f"{base_endpoint}/fetch"
            search_url = f"https://www.google.com/search?q={urllib.parse.quote(search_string)}&tbm=isch"
            logger.info(f"Fetching URL via proxy: {search_url} using {fetch_endpoint}")
            session = requests.Session()
            retry_strategy = Retry(
                total=5,
                status_forcelist=[500, 502, 503, 504],
                allowed_methods=["POST"],
                backoff_factor=1,
                raise_on_status=False
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            session.mount("https://", adapter)
            try:
                response = session.post(fetch_endpoint, json={"url": search_url}, timeout=30)
                status_code = response.status_code
                logger.debug(f"Fetch response status: {status_code} from {fetch_endpoint}")
                if status_code in [500, 502, 503, 504]:
                    logger.warning(f"Fetch failed with status {status_code} from {fetch_endpoint}")
                    attempt_count += 1
                    available_endpoints.remove(base_endpoint)
                    continue
                elif status_code != 200 or not response.json().get("result"):
                    logger.warning(f"Fetch failed with status {status_code} or no result from {fetch_endpoint}")
                    return False
                break
            except Timeout:
                logger.error(f"Fetch timed out after 30s from {fetch_endpoint}")
                attempt_count += 1
                available_endpoints.remove(base_endpoint)
                continue
            except ConnectionError as e:
                logger.error(f"Connection error: {e} from {fetch_endpoint}")
                attempt_count += 1
                available_endpoints.remove(base_endpoint)
                continue
            except RequestException as e:
                logger.error(f"Request error: {e} from {fetch_endpoint}")
                attempt_count += 1
                available_endpoints.remove(base_endpoint)
                continue
        try:
            response_json = response.json()
            result = response_json.get("result", None)
        except json.JSONDecodeError:
            logger.warning(f"Invalid JSON response from {fetch_endpoint}")
            return False
        if not result:
            logger.warning(f"No 'result' in response from {fetch_endpoint}")
            return False
        results_html_bytes = result if isinstance(result, bytes) else result.encode('utf-8')
        df = process_search_result(
            results_html_bytes,
            results_html_bytes,
            entry_id,
            search_url,
            engine,
            conn_str,
            fetch_endpoint,
            logger
        )
        if df.empty:
            logger.warning(f"No valid parsed data for {search_url}")
            return False
        logger.info(f"Processed EntryID {entry_id} with {len(df)} images using {fetch_endpoint}")
        return df
    except Exception as e:
        logger.error(f"Error processing search row: {e}")
        return False



def process_search_row(search_string, endpoint, entry_id, logger=None):
    logger = logger or logging.getLogger(__name__)
    logger.debug(f"Entering process_search_row with search_string={search_string}, endpoint={endpoint}, entry_id={entry_id}")

    # Input validation
    if not search_string or len(search_string.strip()) < 1:
        logger.warning(f"Invalid search string for EntryID {entry_id}: '{search_string}'")
        return False
    if not endpoint:
        logger.warning(f"Invalid endpoint for EntryID {entry_id}")
        return False

    try:
        search_url = f"{endpoint}?query={urllib.parse.quote(search_string)}"
        logger.info(f"Searching URL: {search_url}")

        # Configure retries and timeouts for HTTP requests
        session = requests.Session()
        retry_strategy = Retry(
            total=3,  # Retry up to 3 times
            status_forcelist=[500, 502, 503, 504],  # Retry on server-side errors
            allowed_methods=["GET"],
            backoff_factor=1  # Wait 1s, 2s, 4s between retries
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)

        # Make the request with a timeout
        response = session.get(search_url, timeout=60)
        
        if response.status_code != 200:
            logger.warning(f"Non-200 status {response.status_code} for {search_url}")
            remove_endpoint(endpoint, logger=logger)
            return process_search_row_gcloud(search_string, entry_id, logger=logger)

        # Parse JSON response
        try:
            response_json = response.json()
            result = response_json.get('body', None)
        except json.JSONDecodeError:
            logger.warning(f"Invalid JSON response for {search_url}")
            remove_endpoint(endpoint, logger=logger)
            return process_search_row_gcloud(search_string, entry_id, logger=logger)

        # Check response content
        if not result:
            logger.warning(f"No body in response for {search_url}")
            remove_endpoint(endpoint, logger=logger)
            return process_search_row_gcloud(search_string, entry_id, logger=logger)

        # Unpack content
        unpacked_html = unpack_content(result, logger=logger)
        if not unpacked_html or len(unpacked_html) < 100:
            logger.warning(f"Invalid unpacked HTML for {search_url}")
            remove_endpoint(endpoint, logger=logger)
            return process_search_row_gcloud(search_string, entry_id, logger=logger)

        results_html_bytes = unpacked_html
        df = process_search_result(
            unpacked_html,
            results_html_bytes,
            entry_id,
            search_url,
            engine,  # Assume engine is defined elsewhere
            conn_str,  # Assume conn_str is defined elsewhere
            endpoint,
            logger
        )

        if df.empty:
            logger.warning(f"No valid parsed data for {search_url}")
            remove_endpoint(endpoint, logger=logger)
            return process_search_row_gcloud(search_string, entry_id, logger=logger)

        logger.info(f"Processed EntryID {entry_id} with {len(df)} images")
        return df

    except requests.RequestException as e:
        logger.error(f"Request error for {search_url}: {e}")
        remove_endpoint(endpoint, logger=logger)
        return process_search_row_gcloud(search_string, entry_id, logger=logger)
    except Exception as e:
        logger.error(f"Unexpected error processing search row for EntryID {entry_id}: {e}", exc_info=True)
        remove_endpoint(endpoint, logger=logger)
        return process_search_row_gcloud(search_string, entry_id, logger=logger)
    
import pyodbc
import pandas as pd
import logging

def fetch_images_by_file_id(file_id, logger=None):
    logger = logger or logging.getLogger(__name__)
    query = """
        SELECT rr.ResultID, rr.EntryID, rr.ImageURL, 
               r.ProductBrand, r.ProductCategory, r.ProductColor,
               rr.aijson
        FROM utb_ImageScraperRecords r
        INNER JOIN utb_ImageScraperResult rr ON r.EntryID = rr.EntryID
        WHERE r.FileID = ?
    """
    try:
        with pyodbc.connect(conn_str) as conn:  # Assume conn_str is defined
            df = pd.read_sql(query, conn, params=[file_id])
            logger.info(f"Fetched {len(df)} images for FileID {file_id}")
            return df
    except pyodbc.Error as e:
        logger.error(f"Database error fetching images for FileID {file_id}: {e}")
        return pd.DataFrame()  # Return empty DataFrame as fallback
    except Exception as e:
        logger.error(f"Unexpected error fetching images for FileID {file_id}: {e}", exc_info=True)
        return pd.DataFrame()

import pyodbc
import pandas as pd
import logging

def fetch_missing_images(file_id=None, limit=8, ai_analysis_only=True, exclude_excel_row_ids=[1], logger=None):
    logger = logger or logging.getLogger(__name__)

    # Input validation
    if limit is None or not isinstance(limit, int) or limit < 1:
        limit = 20000
    if not isinstance(exclude_excel_row_ids, list):
        logger.warning("exclude_excel_row_ids must be a list; defaulting to [1]")
        exclude_excel_row_ids = [1]

    logger.info(f"Fetching missing images with file_id={file_id}, limit={limit}, exclude_excel_row_ids={exclude_excel_row_ids}")

    try:
        connection = pyodbc.connect(conn_str)  # Assume conn_str is defined
        query_params = []
        exclude_condition = ""
        if exclude_excel_row_ids:
            placeholders = ",".join("?" * len(exclude_excel_row_ids))
            exclude_condition = f"AND r.ExcelRowID NOT IN ({placeholders})"

        if ai_analysis_only:
            query = f"""
                SELECT MIN(t.ResultID) AS ResultID, t.EntryID, t.ImageURL, t.ImageURLThumbnail,
                    r.ProductBrand, r.ProductCategory, r.ProductColor
                FROM utb_ImageScraperResult t
                INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID
                WHERE (
                    t.ImageURL IS NOT NULL 
                    AND t.ImageURL <> ''
                    AND (
                        ISJSON(t.aijson) = 0 
                        OR t.aijson IS NULL
                        OR JSON_VALUE(t.aijson, '$.similarity_score') IS NULL
                        OR JSON_VALUE(t.aijson, '$.similarity_score') IN ('NaN', 'null', 'undefined')
                        OR JSON_VALUE(t.aijson, '$.match_score') IS NULL
                        OR JSON_VALUE(t.aijson, '$.match_score') IN ('NaN', 'null', 'undefined')
                    )
                )
                AND (r.FileID = ? OR ? IS NULL)
                {exclude_condition}
                GROUP BY t.EntryID, t.ImageURL, t.ImageURLThumbnail, r.ProductBrand, r.ProductCategory, r.ProductColor
            """
            query_params = [file_id, file_id] + exclude_excel_row_ids
        else:
            query = f"""
                SELECT 
                    NULL as ResultID,
                    r.EntryID,
                    NULL as ImageURL,
                    NULL as ImageURLThumbnail,
                    r.ProductBrand,
                    r.ProductCategory,
                    r.ProductColor
                FROM utb_ImageScraperRecords r
                LEFT JOIN utb_ImageScraperResult t ON r.EntryID = t.EntryID
                WHERE t.EntryID IS NULL
                AND (r.FileID = ? OR ? IS NULL)
                {exclude_condition}
                UNION ALL
                SELECT 
                    t.ResultID,
                    t.EntryID,
                    t.ImageURL,
                    t.ImageURLThumbnail,
                    r.ProductBrand,
                    r.ProductCategory,
                    r.ProductColor
                FROM utb_ImageScraperResult t
                INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID
                WHERE (t.ImageURL IS NULL OR t.ImageURL = '')
                AND (r.FileID = ? OR ? IS NULL)
                {exclude_condition}
            """
            query_params = [file_id, file_id, file_id, file_id] + exclude_excel_row_ids * 2

        df = pd.read_sql(query, connection, params=query_params)
        logger.info(f"Fetched {len(df)} missing images")
        return df

    except pyodbc.Error as e:
        logger.error(f"Database error fetching missing images: {e}")
        return pd.DataFrame()  # Return empty DataFrame as fallback
    except Exception as e:
        logger.error(f"Unexpected error fetching missing images: {e}", exc_info=True)
        return pd.DataFrame()
    finally:
        if 'connection' in locals():
            connection.close()

def update_initial_sort_order(file_id, logger=None):
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        with pyodbc.connect(conn_str) as connection:
            connection.timeout = 300
            cursor = connection.cursor()
            logger.info(f"🔄 Setting initial SortOrder for FileID: {file_id}")
            cursor.execute("BEGIN TRANSACTION")
            cursor.execute("UPDATE utb_ImageScraperRecords SET Step1 = NULL WHERE FileID = ?", (file_id,))
            reset_step1_count = cursor.rowcount
            logger.info(f"Reset Step1 for {reset_step1_count} rows in utb_ImageScraperRecords")
            cursor.execute("UPDATE utb_ImageScraperResult SET SortOrder = NULL WHERE EntryID IN (SELECT EntryID FROM utb_ImageScraperRecords WHERE FileID = ?)", (file_id,))
            reset_sort_count = cursor.rowcount
            logger.info(f"Reset SortOrder for {reset_sort_count} rows in utb_ImageScraperResult")
            deduplicate_query = """
                WITH duplicates AS (
                    SELECT ResultID, EntryID,
                           ROW_NUMBER() OVER (PARTITION BY EntryID ORDER BY ResultID) AS row_num
                    FROM utb_ImageScraperResult
                    WHERE EntryID IN (SELECT EntryID FROM utb_ImageScraperRecords WHERE FileID = ?)
                )
                UPDATE utb_ImageScraperResult
                SET SortOrder = CASE WHEN d.row_num > 1 THEN -1 ELSE NULL END
                FROM utb_ImageScraperResult r
                INNER JOIN duplicates d ON r.ResultID = d.ResultID AND r.EntryID = d.EntryID;
            """
            cursor.execute(deduplicate_query, (file_id,))
            dedup_count = cursor.rowcount
            logger.info(f"Marked {dedup_count} rows as duplicates with SortOrder = -1")
            initial_sort_query = """
                WITH toupdate AS (
                    SELECT t.*, 
                           ROW_NUMBER() OVER (PARTITION BY t.EntryID ORDER BY t.ResultID) AS seqnum
                    FROM utb_ImageScraperResult t 
                    INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID 
                    WHERE r.FileID = ? AND t.SortOrder IS NULL
                ) 
                UPDATE toupdate 
                SET SortOrder = seqnum;
            """
            cursor.execute(initial_sort_query, (file_id,))
            update_count = cursor.rowcount
            logger.info(f"Set initial SortOrder for {update_count} unique rows")
            cursor.execute("COMMIT")
            verify_query = """
                SELECT t.ResultID, t.EntryID, t.SortOrder
                FROM utb_ImageScraperResult t
                INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID
                WHERE r.FileID = ?
                ORDER BY t.EntryID, t.SortOrder
            """
            cursor.execute(verify_query, (file_id,))
            results = cursor.fetchall()
            for record in results:
                logger.info(f"Initial - EntryID: {record[1]}, ResultID: {record[0]}, SortOrder: {record[2]}")
            count_query = "SELECT COUNT(*) FROM utb_ImageScraperResult WHERE EntryID IN (SELECT EntryID FROM utb_ImageScraperRecords WHERE FileID = ?)"
            cursor.execute(count_query, (file_id,))
            total_results = cursor.fetchone()[0]
            unique_count_query = """
                SELECT COUNT(*) 
                FROM utb_ImageScraperResult 
                WHERE EntryID IN (SELECT EntryID FROM utb_ImageScraperRecords WHERE FileID = ?) 
                AND SortOrder > 0
            """
            cursor.execute(unique_count_query, (file_id,))
            unique_results = cursor.fetchone()[0]
            logger.info(f"Total results: {total_results}, Unique results: {unique_results}")
            if unique_results < 16:
                logger.warning(f"Expected at least 16 unique results (8 per search type), got {unique_results}")
            return [{"ResultID": row[0], "EntryID": row[1], "SortOrder": row[2]} for row in results]
    except Exception as e:
        logger.error(f"Error setting initial SortOrder: {e}")
        if 'cursor' in locals():
            cursor.execute("ROLLBACK")
        return None
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()

def update_search_sort_order(file_id, logger=None):
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        with pyodbc.connect(conn_str) as connection:
            connection.timeout = 300
            cursor = connection.cursor()
            logger.info(f"🔄 Setting Search SortOrder for FileID: {file_id}")
            fetch_query = """
                SELECT 
                    t.ResultID, 
                    t.EntryID, 
                    t.ImageDesc, 
                    t.ImageURL, 
                    t.ImageSource, 
                    r.ProductBrand, 
                    r.ProductModel
                FROM utb_ImageScraperResult t
                INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID
                WHERE r.FileID = ?
            """
            df = pd.read_sql(fetch_query, connection, params=[file_id])
            def clean_string(text):
                if pd.isna(text):
                    return ''
                delimiters = r'[- _.,;:/]'
                return re.sub(delimiters, '', str(text).upper())
            df['ImageDesc_clean'] = df['ImageDesc'].apply(clean_string)
            df['ImageSource_clean'] = df['ImageSource'].apply(clean_string)
            df['ImageUrl_clean'] = df['ImageURL'].apply(clean_string)
            df['ProductBrand_clean'] = df['ProductBrand'].apply(clean_string)
            df['ProductModel_clean'] = df['ProductModel'].apply(clean_string)
            def calculate_priority(row):
                brand_match = (
                    (row['ProductBrand_clean'] in row['ImageDesc_clean'] if row['ProductBrand_clean'] else False) or
                    (row['ProductBrand_clean'] in row['ImageSource_clean'] if row['ProductBrand_clean'] else False) or
                    (row['ProductBrand_clean'] in row['ImageUrl_clean'] if row['ProductBrand_clean'] else False)
                )
                model_match = (
                    (row['ProductModel_clean'] in row['ImageDesc_clean'] if row['ProductModel_clean'] else False) or
                    (row['ProductModel_clean'] in row['ImageSource_clean'] if row['ProductModel_clean'] else False) or
                    (row['ProductModel_clean'] in row['ImageUrl_clean'] if row['ProductModel_clean'] else False)
                )
                if brand_match and model_match and pd.notna(row['ProductBrand']) and pd.notna(row['ProductModel']):
                    return 1
                elif model_match and pd.notna(row['ProductModel']):
                    return 2
                elif brand_match and pd.notna(row['ProductBrand']):
                    return 3
                else:
                    return 4
            df['priority'] = df.apply(calculate_priority, axis=1)
            df['new_sort_order'] = df.apply(lambda row: -2 if row['priority'] == 4 else None, axis=1)
            df.loc[df['priority'] != 4, 'new_sort_order'] = df[df['priority'] != 4].groupby('EntryID').cumcount() + 1
            cursor.execute("BEGIN TRANSACTION")
            update_query = "UPDATE utb_ImageScraperResult SET SortOrder = ? WHERE ResultID = ?"
            updates = df[['ResultID', 'new_sort_order']].dropna()
            for _, row in updates.iterrows():
                cursor.execute(update_query, (int(row['new_sort_order']), row['ResultID']))
            update_count = len(updates)
            logger.info(f"Set Search SortOrder for {update_count} rows")
            verify_query = """
                SELECT TOP 20 t.ResultID, t.EntryID, t.ImageDesc, t.SortOrder, r.ProductBrand, r.ProductModel
                FROM utb_ImageScraperResult t
                INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID
                WHERE r.FileID = ?
                ORDER BY t.EntryID, t.SortOrder
            """
            cursor.execute(verify_query, (file_id,))
            results = cursor.fetchall()
            for record in results:
                logger.info(f"Search - EntryID: {record[1]}, ResultID: {record[0]}, ImageDesc: {record[2]}, SortOrder: {record[3]}")
            cursor.execute("COMMIT")
            return [{"ResultID": row[0], "EntryID": row[1], "ImageDesc": row[2], "SortOrder": row[3], "ProductBrand": row[4], "ProductModel": row[5]} for row in results]
    except Exception as e:
        logger.error(f"Error setting Search SortOrder: {e}")
        if 'cursor' in locals():
            cursor.execute("ROLLBACK")
        return None
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()

def update_file_location_complete(file_id, file_location, logger=None):
    logger = logger or default_logger
    file_id = int(file_id)
    try:
        with pyodbc.connect(conn_str) as connection:
            cursor = connection.cursor()
            cursor.execute("BEGIN TRANSACTION")
            cursor.execute("UPDATE utb_ImageScraperFiles SET FileLocationURLComplete = ? WHERE ID = ?", (file_location, file_id))
            cursor.execute("COMMIT")
            logger.info(f"Updated file location URL for FileID: {file_id}")
    except Exception as e:
        logger.error(f"Error updating file location URL: {e}")
        if 'cursor' in locals():
            cursor.execute("ROLLBACK")
        raise

def update_file_generate_complete(file_id, logger=None):
    logger = logger or default_logger
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            query = "UPDATE utb_ImageScraperFiles SET CreateFileCompleteTime = GETDATE() WHERE ID = ?"
            cursor.execute(query, (file_id,))
            conn.commit()
            logger.info(f"Marked file generation as complete for FileID: {file_id}")
    except Exception as e:
        logger.error(f"Error updating file generation completion time: {e}")
        raise

def get_send_to_email(file_id, logger=None):
    logger = logger or default_logger
    try:
        with pyodbc.connect(conn_str, timeout=60) as conn:
            cursor = conn.cursor()
            query = "SELECT UserEmail FROM utb_ImageScraperFiles WHERE ID = ?"
            cursor.execute(query, (file_id,))
            send_to_email = cursor.fetchone()
            if send_to_email:
                logger.info(f"Got email address for FileID: {file_id}: {send_to_email[0]}")
                return send_to_email[0]
            logger.warning(f"No email address found for FileID: {file_id}")
            return "No Email Found"
    except Exception as e:
        logger.error(f"Error getting email address: {e}")
        return "nik@iconluxurygroup.com"

def get_images_excel_db(file_id, logger=None):
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        with pyodbc.connect(conn_str) as connection:
            cursor = connection.cursor()
            cursor.execute("UPDATE utb_ImageScraperFiles SET CreateFileStartTime = GETDATE() WHERE ID = ?", (file_id,))
            connection.commit()
            query = """
                SELECT s.ExcelRowID AS ExcelRowID, 
                        r.ImageURL AS ImageURL, 
                        r.ImageURLThumbnail AS ImageURLThumbnail 
                    FROM utb_ImageScraperFiles f
                    INNER JOIN utb_ImageScraperRecords s ON s.FileID = f.ID 
                    INNER JOIN utb_ImageScraperResult r ON r.EntryID = s.EntryID 
                    WHERE f.ID = ? AND r.SortOrder = 1
                    ORDER BY s.ExcelRowID
            """
            df = pd.read_sql_query(query, connection, params=[file_id])
            logger.info(f"Queried FileID: {file_id}, retrieved {len(df)} images for Excel export")
        return df
    except Exception as e:
        logger.error(f"Error getting images for Excel export: {e}")
        return pd.DataFrame()

def get_lm_products(file_id, logger=None):
    logger = logger or default_logger
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            query = f"EXEC usp_ImageScrapergetMatchFromRetail {file_id}"
            cursor.execute(query)
            conn.commit()
            logger.info(f"Executed stored procedure to match products for FileID: {file_id}")
    except Exception as e:
        logger.error(f"Error executing stored procedure to match products: {e}")

def get_endpoint(logger=None):
    logger = logger or default_logger
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            sql_query = "SELECT TOP 1 EndpointURL FROM utb_Endpoints WHERE EndpointIsBlocked = 0 ORDER BY NEWID()"
            cursor.execute(sql_query)
            endpoint_url = cursor.fetchone()
            if endpoint_url:
                endpoint = endpoint_url[0]
                logger.info(f"Got endpoint URL: {endpoint}")
                return endpoint
            else:
                logger.warning("No endpoint URL found")
                return "No EndpointURL"
    except Exception as e:
        logger.error(f"Error getting endpoint URL: {e}")
        return "No EndpointURL"

def remove_endpoint(endpoint, logger=None):
    logger = logger or default_logger
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            sql_query = f"UPDATE utb_Endpoints SET EndpointIsBlocked = 1 WHERE EndpointURL = ?"
            cursor.execute(sql_query, (endpoint,))
            conn.commit()
            logger.info(f"Marked endpoint as blocked: {endpoint}")
    except Exception as e:
        logger.error(f"Error marking endpoint as blocked: {e}")