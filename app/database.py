# database.py
# Wrapper for database utility functions, importing from db_utils.py

from db_utils import (
    get_endpoint,
    insert_search_results,
    update_sort_order,
    update_log_url_in_db,
    get_records_to_search,
    fetch_missing_images,
    remove_endpoint,
    update_search_sort_order,
    update_sort_no_image_entry,
    update_sort_order_per_entry,
    set_sort_order_negative_four_for_zero_match,
    get_images_excel_db,
    get_send_to_email,
    sync_update_search_sort_order,
    update_file_generate_complete,
    update_file_location_complete,
    update_initial_sort_order,
    call_fetch_missing_images,
    call_get_images_excel_db,
    call_get_send_to_email,
    call_update_file_generate_complete,
    call_update_file_location_complete
)

# Expose functions for use in other modules
__all__ = [
    'get_endpoint',
    'insert_search_results',
    'update_sort_order',
    'update_log_url_in_db',
    'get_records_to_search',
    'update_sort_no_image_entry',
    'sync_update_search_sort_order',
    'fetch_missing_images',
    'remove_endpoint',
    'update_search_sort_order',
    'update_sort_order_per_entry',
    'set_sort_order_negative_four_for_zero_match',
    'get_images_excel_db',
    'get_send_to_email',
    'update_file_generate_complete',
    'update_file_location_complete',
    'update_initial_sort_order',
    'call_fetch_missing_images',
    'call_get_images_excel_db',
    'call_get_send_to_email',
    'call_update_file_generate_complete',
    'call_update_file_location_complete'
]