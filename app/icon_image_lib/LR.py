import re
import logging
from charset_normalizer import detect
from bs4 import BeautifulSoup
import pandas as pd
from typing import Tuple, List, Optional

# Configure logging with detailed format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s - [File: %(filename)s, Line: %(lineno)d]'
)

class LR:
    """Class to extract substrings between two delimiters from a given string."""
    
    def __init__(self):
        """Initialize LR with empty state."""
        self.string = None
        self.str1 = None
        self.str2 = None
        self.results = []

    def escape_ansi(self, line: str) -> str:
        """Remove ANSI escape sequences from a string (unused in current implementation)."""
        ansi_escape = re.compile(r'(\x9B|\x1B\[)[0-?]*[ -\/]*[@-~]')
        return ansi_escape.sub('', line)

    def get_all_results(self) -> List[str]:
        """Extract all substrings between str1 and str2 from the stored string."""
        if not all([self.string, self.str1, self.str2]):
            return []
        
        results = []
        start_idx = 0
        str_length = len(self.string)
        
        while start_idx < str_length:
            # Find the start delimiter
            start_pos = self.string.find(self.str1, start_idx)
            if start_pos == -1:
                break
            start_pos += len(self.str1)
            
            # Find the end delimiter
            end_pos = self.string.find(self.str2, start_pos)
            if end_pos == -1:
                break
                
            # Extract the substring
            subsequence = self.string[start_pos:end_pos]
            results.append(subsequence)
            start_idx = end_pos + len(self.str2)
            
        return results

    # def get_result(self, string: str, sub1: str, sub2: str) -> str:
    #     """Extract a single substring between sub1 and sub2 (unused in current implementation)."""
    #     string = str(string[string.find(sub1) + len(sub1):string.rfind(sub2)])
    #     string = string.split(sub1)[1]
    #     string = string.split(sub2)[0]
    #     return string

    def get(self, string: str, sub1: str, sub2: str, logger: Optional[logging.Logger] = None) -> str:
        """Extract the first substring between sub1 and sub2, or empty string if none found."""
        logger = logger or logging.getLogger(__name__)
        
        try:
            # Input validation
            string = str(string)
            sub1 = str(sub1)
            sub2 = str(sub2)
            if not all([string, sub1, sub2]):
                logger.error("Empty string or delimiters provided")
                return ""
            if sub1 not in string or sub2 not in string:
                logger.debug(f"Delimiters not found: sub1='{sub1}', sub2='{sub2}'")
                return ""
                
            # Store state
            self.string = string
            self.str1 = sub1
            self.str2 = sub2
            
            # Get all results and return the first one
            results = self.get_all_results()
            if not results:
                logger.debug("No substrings found between delimiters")
                return ""
                
            logger.debug(f"Extracted {len(results)} substring(s); returning first")
            return results[0]
            
        except Exception as e:
            logger.error(f"Error in get: {e}", exc_info=True)
            return ""

