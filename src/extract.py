# src/extract.py

import re
import os
import requests
import pandas as pd
from fake_useragent import UserAgent
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

try:
    from src.logger import get_logger
except ModuleNotFoundError:
    from logger import get_logger

logger = get_logger(__name__)

ua = UserAgent()


def get_headers() -> dict:
    return {"User-Agent": ua.random}


def get_build_id() -> str:
    logger.info("==============================")
    """
    Fetch Graana homepage and extract current Next.js BUILD_ID.
    This changes on every deployment so we fetch it fresh each run.
    """
    response = requests.get("https://www.graana.com", headers=get_headers(), timeout=30)
    match = re.search(r'"buildId":"([^"]+)"', response.text)
    if not match:
        raise ValueError("Could not find BUILD_ID on Graana homepage")
    build_id = match.group(1)
    logger.info(f"Fetched BUILD_ID: {build_id}")
    return build_id


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(min=2, max=10),
    retry=retry_if_exception_type(requests.exceptions.Timeout),
)
def extract_data(page: int, build_id: str) -> list:
    """
    Extract property listings from Graana API for a single page.
    build_id is passed explicitly — never read from global.
    """
    try:
        url = (
            f"https://www.graana.com/_next/data/{build_id}"
            f"/sale/residential-properties-sale-islamabad-1.json"
            f"?page={page}&pageSize=30&purpose=sale&id=residential-properties-sale-islamabad-1"
        )
        response = requests.get(url, headers=get_headers(), timeout=30)
        response.raise_for_status()

        data = response.json()
        raw_data = data.get("pageProps", {}).get("properties", [])

        listings = []
        for item in raw_data:
            listings.append(
                {
                    "id": item.get("id"),
                    "title": item.get("customTitle"),
                    "seller_name": item.get("name"),
                    "price": item.get("price"),
                    "size": item.get("size"),
                    "unit": item.get("sizeUnit"),
                    "area": item.get("area", {}).get("name"),
                    "city": item.get("city", {}).get("name"),
                    "bedrooms": item.get("bed"),
                    "bathrooms": item.get("bath"),
                    "property_type": item.get("type"),
                    "property_sub_type": item.get("subtype"),
                    "purpose": item.get("purpose"),
                    "created_at": item.get("createdAt"),
                }
            )

        logger.info(f"Page {page}: {len(listings)} properties extracted")
        return listings

    except requests.exceptions.Timeout:
        logger.warning(f"Timeout on page {page}, retrying...")
        raise
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error on page {page}: {e}")
        return []
    except (KeyError, ValueError) as e:
        logger.error(f"Parsing error on page {page}: {e}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error on page {page}: {e}")
        return []


def extract_all(pages: int = 5) -> list:
    """
    Extract all pages. Fetches BUILD_ID once then reuses it.
    """
    build_id = get_build_id()
    all_listings = []

    for page in range(1, pages + 1):
        listings = extract_data(page, build_id)
        all_listings.extend(listings)

    logger.info(f"Total extracted: {len(all_listings)} properties")
    return all_listings


if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)
    results = extract_all(pages=5)
    df = pd.DataFrame(results)
    df.to_csv(
        f"data/properties_raw_{pd.Timestamp.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv",
        index=False,
    )
    print(f"Extracted {len(results)} properties")
