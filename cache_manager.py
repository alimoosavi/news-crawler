import json
from pathlib import Path


class CacheManager:
    def __init__(self, cache_file="cache.json"):
        self.cache_file = Path(cache_file)
        self.cache = self.load_cache()

    def load_cache(self):
        if self.cache_file.exists():
            with open(self.cache_file, "r") as f:
                return json.load(f)
        return {}

    def get_last_link(self, source):
        if not source in self.cache:
            return None
        return self.cache.get(source)

    def update_last_link(self, source, link):
        self.cache[source] = link
        self.save_cache()

    def save_cache(self):
        with open(self.cache_file, "w") as f:
            json.dump(self.cache, f, indent=2)
