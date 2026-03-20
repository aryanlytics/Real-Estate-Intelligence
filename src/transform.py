import pandas as pd
from datetime import date

try:
    from logger import get_logger
except ModuleNotFoundError:
    from src.logger import get_logger

logger = get_logger(__name__)


def to_sqft(size: float, unit: str) -> float:
    unit = unit.lower().strip()
    if unit == "marla":
        return size * 225
    elif unit == "kanal":
        return size * 4500
    elif unit == "sqft":
        return size
    elif unit == "sqm":
        return size * 10.764  # 1 square meter = 10.764 sqft
    elif unit == "sqyd":
        return size * 9  # 1 square yard = 9 sqft
    else:
        logger.warning(f"Unknown unit: {unit}, returning None")
        return None


def price_label(price: int) -> str:
    if price >= 10_000_000:
        return f"{price / 10_000_000:.2f} Cr"
    elif price >= 100_000:
        return f"{price / 100_000:.2f} Lac"
    else:
        return str(price)


def transform_data(listings: list) -> pd.DataFrame:
    if not listings:
        logger.warning("No data to transform")
        return pd.DataFrame()

    try:
        df = pd.DataFrame(listings)

        # clean strings
        df["title"] = df["title"].str.strip()
        df["seller_name"] = df["seller_name"].str.strip().str.title()
        df["area"] = df["area"].str.strip()
        df["city"] = df["city"].str.strip()
        df["property_type"] = df["property_type"].str.strip()
        df["property_sub_type"] = df["property_sub_type"].str.strip()
        df["purpose"] = df["purpose"].str.strip()
        df["price"] = pd.to_numeric(df["price"], errors="coerce").astype("Int64")

        # normalize size to sqft
        df["size_sqft"] = df.apply(
            lambda row: to_sqft(row["size"], row["unit"]), axis=1
        )

        # parse datetime to date only
        df["created_at"] = pd.to_datetime(df["created_at"]).dt.date

        # add scraped date
        df["scraped_date"] = date.today()

        # deduplicate by id
        df.drop_duplicates(subset=["id"], keep="first", inplace=True)

        # add price label
        df["price_label"] = df["price"].apply(price_label)

        logger.info(f"Duplicates removed: {len(listings) - len(df)}")
        logger.info(f"Transform complete: {len(df)} properties")
        return df

    except Exception as e:
        logger.error(f"Transform error: {e}")
        return pd.DataFrame()


if __name__ == "__main__":
    try:
        from extract import extract_all
    except ModuleNotFoundError:
        from src.extract import extract_all

    raw = extract_all(pages=5)
    df = transform_data(raw)
    df.to_csv(
        f"data/properties_clean_data{pd.Timestamp.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv",
        index=False,
    )
    print(f"Transformed {len(df)} properties")
