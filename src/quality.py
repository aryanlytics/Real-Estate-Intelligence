# src/quality.py

import pandas as pd

try:
    from logger import get_logger
except ModuleNotFoundError:
    from src.logger import get_logger

logger = get_logger(__name__)


def check_quality(df) -> dict:
    logger.info("================================================")
    logger.info(f"QUALITY CHECK START — {len(df)} properties")
    logger.info("================================================")

    report = {}

    if df.empty:
        logger.warning("DataFrame is empty")
        return {"empty": True, "total_issues": 1}

    # null critical fields
    nulls = df[["id", "price", "size_sqft"]].isnull().sum()
    report["null_critical_fields"] = nulls[nulls > 0].to_dict()
    if report["null_critical_fields"]:
        logger.warning(f"Null critical fields: {report['null_critical_fields']}")
    else:
        logger.info("Null critical fields: OK")

    # invalid price
    report["invalid_price"] = int((df["price"] <= 0).sum())
    logger.info("Prices: OK") if report["invalid_price"] == 0 else logger.warning(
        f"Invalid prices: {report['invalid_price']}"
    )

    # invalid size
    report["invalid_size_sqft"] = int((df["size_sqft"] <= 0).sum())
    logger.info("Sizes: OK") if report["invalid_size_sqft"] == 0 else logger.warning(
        f"Invalid sizes: {report['invalid_size_sqft']}"
    )

    # future dates
    report["future_dates"] = int((df["created_at"] > pd.Timestamp.today().date()).sum())
    logger.info("Dates: OK") if report["future_dates"] == 0 else logger.warning(
        f"Future dates: {report['future_dates']}"
    )

    # duplicates
    report["duplicate_ids"] = int(df["id"].duplicated().sum())
    logger.info("Duplicates: OK") if report["duplicate_ids"] == 0 else logger.warning(
        f"Duplicate IDs: {report['duplicate_ids']}"
    )

    # bedrooms
    report["invalid_bedrooms"] = int(
        ((df["bedrooms"] < 1) | (df["bedrooms"] > 20)).sum()
    )
    logger.info("Bedrooms: OK") if report["invalid_bedrooms"] == 0 else logger.warning(
        f"Invalid bedrooms: {report['invalid_bedrooms']}"
    )

    # bathrooms
    report["invalid_bathrooms"] = int(
        ((df["bathrooms"] < 1) | (df["bathrooms"] > 20)).sum()
    )
    logger.info("Bathrooms: OK") if report[
        "invalid_bathrooms"
    ] == 0 else logger.warning(f"Invalid bathrooms: {report['invalid_bathrooms']}")

    # price label
    report["null_price_label"] = int(df["price_label"].isnull().sum())
    logger.info("Price labels: OK") if report[
        "null_price_label"
    ] == 0 else logger.warning(f"Null price labels: {report['null_price_label']}")

    report["total_issues"] = sum(
        v if isinstance(v, int) else len(v)
        for k, v in report.items()
        if k != "total_issues"
    )
    report["total_rows"] = len(df)

    if report["total_issues"] == 0:
        logger.info("QUALITY RESULT: PASSED")
    else:
        logger.warning(f"QUALITY RESULT: {report['total_issues']} issues found")

    logger.info("================================================")
    return report


if __name__ == "__main__":
    try:
        from transform import transform_data
        from extract import extract_all
    except ModuleNotFoundError:
        from src.transform import transform_data
        from src.extract import extract_all

    raw = extract_all(pages=5)
    df = transform_data(raw)
    report = check_quality(df)
    print(f"\nTotal rows: {report['total_rows']}")
    print(f"Total issues: {report['total_issues']}")
