import logging
import requests
from bs4 import BeautifulSoup
from typing import Dict, List, Optional

TGJU_BASE_URL = "https://www.tgju.org"

# Mapping Farsi asset names to English normalized keys
ASSET_NAME_MAPPING = {
    "بورس": "bourse",
    "انس طلا": "gold_ounce",
    "مثقال طلا": "gold_mesghal",
    "طلا ۱۸": "gold_18k",
    "سکه": "coin",
    "دلار": "usd",
    "یورو": "eur",
    "پوند انگلیس": "gbp",
    "لیر ترکیه": "try",
    "فرانک سوئیس": "chf",
    "یوان چین": "cny",
    "ین ژاپن": "jpy",
    "وون کره جنوبی": "krw",
    "دلار کانادا": "cad",
    "دلار استرالیا": "aud",
    "روبل روسیه": "rub",
    "اتریوم": "ethereum",
    "بیت کوین": "bitcoin",
    "لایت کوین": "litecoin",
    "بیت کوین کش": "bitcoin_cash",
    "تتر": "tether",
    "ترون": "tron",
    "بایننس کوین": "binance_coin",
    "استلار": "stellar",
    "ریپل": "ripple",
    "دوج کوین": "dogecoin",
    "دش": "dash",
    "کاردانو": "cardano",
    "پولکادات": "polkadot",
    "سولانا": "solana",
    "آوالانچ": "avalanche",
    "شیبا اینو": "shiba_inu",
    "تون‌کوین": "toncoin",

    # Missing ones from your logs
    "نفت برنت": "brent_oil",
    "سکه امامی": "sekke_emami",
    "سکه بهار آزادی": "sekke_bahar_azadi",
    "نیم سکه": "half_coin",
    "ربع سکه": "quarter_coin",
    "سکه گرمی": "gram_coin",
}

class TGJUPriceCollector:
    """
    Collects all real-time asset prices from TGJU and optionally stores them in a cache manager.
    """

    INFO_BAR_SELECTOR = "ul.info-bar.mobile-hide"
    TABLE_IDS = ["tolerance_high", "last", "tolerance_low", "coin-table"]

    def __init__(self, fetch_timeout: int = 10, cache_manager: Optional[object] = None):
        self.fetch_timeout = fetch_timeout
        self.cache_manager = cache_manager
        self.logger = logging.getLogger(self.__class__.__name__)

    def _fetch_html(self, url: str) -> str:
        try:
            response = requests.get(
                url,
                timeout=self.fetch_timeout,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                                  "Chrome/121.0.0.0 Safari/537.36"
                }
            )
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            self.logger.error(f"Error fetching {url}: {e}")
            return ""

    def _parse_info_bar(self, html: str) -> List[Dict[str, str]]:
        soup = BeautifulSoup(html, "html.parser")
        info_bar = soup.select_one(self.INFO_BAR_SELECTOR)
        if not info_bar:
            self.logger.warning("Info bar not found on TGJU page")
            return []

        results = []
        for li in info_bar.find_all("li"):
            h3_tag = li.find("h3")
            price_tag = li.select_one(".info-price")
            change_tag = li.select_one(".info-change")

            if h3_tag and price_tag and change_tag:
                asset_name_fa = h3_tag.get_text(strip=True)
                asset_key = ASSET_NAME_MAPPING.get(asset_name_fa, asset_name_fa.lower().replace(" ", "_"))
                asset_data = {
                    "key": asset_key,
                    "name": asset_name_fa,
                    "current_price": price_tag.get_text(strip=True),
                    "change": change_tag.get_text(strip=True),
                }
                results.append(asset_data)
                if self.cache_manager:
                    self.cache_manager.set(asset_key, asset_data)
        return results

    def _parse_table_by_id(self, soup: BeautifulSoup, table_id: str) -> List[Dict[str, str]]:
        table = soup.find("table", id=table_id)
        if not table:
            return []

        rows_list = []
        tbody = table.find("tbody")
        if not tbody:
            return []

        for row in tbody.find_all("tr"):
            cells = row.find_all(["th", "td"])
            if len(cells) < 2:
                continue

            asset_name_fa = cells[0].get_text(strip=True)
            asset_key = ASSET_NAME_MAPPING.get(asset_name_fa, asset_name_fa.lower().replace(" ", "_"))

            if table_id == "coin-table":
                asset_data = {
                    "key": asset_key,
                    "name": asset_name_fa,
                    "current_price": cells[1].get_text(strip=True),
                    "change": cells[2].get_text(strip=True) if len(cells) > 2 else "",
                    "lowest": cells[3].get_text(strip=True) if len(cells) > 3 else "",
                    "highest": cells[4].get_text(strip=True) if len(cells) > 4 else "",
                    "time": cells[5].get_text(strip=True) if len(cells) > 5 else ""
                }
            else:
                asset_data = {
                    "key": asset_key,
                    "name": asset_name_fa,
                    "current_price": cells[1].get_text(strip=True),
                    "change": cells[2].get_text(strip=True)
                }

            rows_list.append(asset_data)
            if self.cache_manager:
                self.cache_manager.set(asset_key, asset_data)

        return rows_list

    def collect_prices(self) -> List[Dict[str, str]]:
        """
        Collects all price data and stores them in cache if available.
        """
        html = self._fetch_html(TGJU_BASE_URL)
        if not html:
            return []

        soup = BeautifulSoup(html, "html.parser")
        results = []

        # Info bar
        results.extend(self._parse_info_bar(html))

        # Tables by ID
        for table_id in self.TABLE_IDS:
            results.extend(self._parse_table_by_id(soup, table_id))

        return results
