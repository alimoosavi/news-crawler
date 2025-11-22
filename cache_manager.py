import json
import os
from typing import Optional, Dict


class CacheManager:
    """
    Manages key-value string caching by persisting data to a local JSON file.
    This implementation replaces Redis with simple file I/O for lightweight caching.
    """

    def __init__(self, cache_file_path: str = "last_links_cache.json"):
        # The path where the JSON file will be stored
        self.cache_file_path: str = cache_file_path
        # The in-memory dictionary holding the cache data
        self.cache_data: Dict[str, str] = self._load_cache()

    def _load_cache(self) -> Dict[str, str]:
        """
        Loads cache data from the JSON file into memory.
        Handles missing files and corrupted JSON gracefully.
        """
        if not os.path.exists(self.cache_file_path):
            return {}

        try:
            with open(self.cache_file_path, 'r', encoding='utf-8') as f:
                # Read content first to check for empty file
                content = f.read()
                if not content:
                    return {}
                return json.loads(content)
        except json.JSONDecodeError:
            # Occurs if the JSON file is corrupted
            print(f"Warning: Cache file '{self.cache_file_path}' is corrupted. Starting with an empty cache.")
            return {}
        except Exception as e:
            print(f"Error loading cache file: {e}. Starting with an empty cache.")
            return {}

    def _save_cache(self):
        """
        Writes the current in-memory cache data back to the JSON file on disk.
        """
        try:
            # Use 'w' mode to overwrite the file content
            with open(self.cache_file_path, 'w', encoding='utf-8') as f:
                # Use indent=4 for human readability in the JSON file
                json.dump(self.cache_data, f, indent=4)
        except Exception as e:
            # This is critical, so log the error if persistence fails
            print(f"Critical Error: Failed to save cache file '{self.cache_file_path}': {e}")

    def get_last_link(self, source: str) -> Optional[str]:
        """
        Retrieve the last published link for a given source from the in-memory cache.
        Returns the link string or None if the source key is not found.
        """
        return self.cache_data.get(source)

    def update_last_link(self, source: str, last_link: str):
        """
        Update the last published link for a given source and immediately
        persists the change to the JSON file.
        """
        # Update the dictionary in memory
        self.cache_data[source] = last_link

        # Persist the changes to disk
        self._save_cache()
