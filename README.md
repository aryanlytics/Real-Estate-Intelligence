# Real Estate Intelligence Pipeline

An automated data engineering pipeline that scrapes daily property listings from Graana.com, transforms and validates the data, and loads it into Google BigQuery for price trend analysis across Islamabad.

---

## Problem It Solves

Property prices in Pakistan are scattered across multiple platforms with no standardization. A buyer looking for a 10 Marla house in Bahria Enclave has no way to know if a listed price is fair, what the area average is, or how prices have changed over time.

This pipeline builds that dataset — clean, validated, historically tracked property data ready for analysis.

---

## Architecture

```
Graana.com API
      ↓
  extract.py — fetches listings, handles pagination, retries on failure
      ↓
  transform.py — normalizes units, parses prices, cleans strings
      ↓
  quality.py — validates data before loading
      ↓
  loader.py — loads to BigQuery partitioned by scraped_date
      ↓
Google BigQuery — historical price dataset grows daily
```

---

## What It Does

- Fetches residential property listings from Graana.com's internal API
- Dynamically fetches the Next.js `BUILD_ID` so the pipeline doesn't break on deployments
- Normalizes property sizes to sqft — handles Marla, Kanal, Sqm, Sqyd
- Adds human-readable price labels — `3.20 Cr`, `85.00 Lac`
- Runs 8 data quality checks before every load
- Loads clean data into BigQuery partitioned by date — building price history over time

---

## Tech Stack

| Tool | Purpose |
|---|---|
| Python 3.11 | Core pipeline language |
| requests + fake-useragent | HTTP requests with rotating user agents |
| pandas | Data transformation |
| Tenacity | Retry logic with exponential backoff |
| Google BigQuery | Cloud data warehouse |
| pandas-gbq | BigQuery loader |
| Docker + Airflow | Pipeline orchestration (coming soon) |

---

## Project Structure

```
Real-Estate-Intelligence/
├── src/
│   ├── extract.py      # Scrapes Graana API, handles pagination and retries
│   ├── transform.py    # Cleans, normalizes, and enriches raw data
│   ├── quality.py      # Validates data before loading
│   ├── loader.py       # Loads to BigQuery via pandas-gbq
│   └── logger.py       # Centralized logging to logs/pipeline.log
├── dag/
│   └── pipeline_dag.py # Airflow DAG (coming soon)
├── data/               # Local CSV snapshots (gitignored)
├── logs/               # Pipeline run logs (gitignored)
├── tests/              # pytest test suite (coming soon)
├── .env                # Environment variables (gitignored)
├── gcp-key.json        # GCP service account key (gitignored)
└── requirements.txt
```

---

## BigQuery Schema

Table: `real-estate-intel-491208.real_estate.properties`
Partitioned by: `scraped_date`

| Column | Type | Description |
|---|---|---|
| id | INTEGER | Unique Graana listing ID |
| title | STRING | Property listing title |
| seller_name | STRING | Name of seller or agency |
| price | INTEGER | Listed price in PKR |
| price_label | STRING | Human readable price (e.g. 3.20 Cr) |
| size | FLOAT | Original size value |
| unit | STRING | Original unit (marla, kanal, sqft) |
| size_sqft | FLOAT | Size normalized to square feet |
| area | STRING | Neighborhood or sector |
| city | STRING | City |
| bedrooms | INTEGER | Number of bedrooms |
| bathrooms | INTEGER | Number of bathrooms |
| property_type | STRING | residential / commercial |
| property_sub_type | STRING | house / flat / plot |
| purpose | STRING | buy / rent |
| created_at | DATE | Date listing was posted |
| scraped_date | DATE | Date pipeline ran (partition key) |

---

## Data Quality Checks

Before every load the pipeline validates:

- No null values in critical fields (`id`, `price`, `size_sqft`)
- All prices greater than 0
- All sizes greater than 0
- No future `created_at` dates
- No duplicate listing IDs
- Bedroom and bathroom counts between 1 and 20
- No null price labels

---

## Setup

### Prerequisites
- Python 3.11
- Google Cloud account with BigQuery enabled

### Installation

```bash
git clone https://github.com/aryanlytics/Real-Estate-Intelligence.git
cd Real-Estate-Intelligence
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### BigQuery Setup
1. Create a Google Cloud project
2. Enable the BigQuery API
3. Create a dataset named `real_estate`
4. Create a service account with `BigQuery Data Editor` and `BigQuery Job User` roles
5. Download the JSON key and save as `gcp-key.json` in the project root
6. Update `PROJECT_ID` in `src/loader.py` with your project ID

### Run the Pipeline

```bash
python src/loader.py
```

This runs the full pipeline — extract → transform → quality → load.

---

## Sample Analytics Queries

**Average price per area**
```sql
SELECT area, ROUND(AVG(price), 0) as avg_price, COUNT(*) as listings
FROM `real-estate-intel-491208.real_estate.properties`
WHERE scraped_date = CURRENT_DATE()
GROUP BY area
ORDER BY avg_price DESC
```

**Price per sqft by property type**
```sql
SELECT 
    property_sub_type,
    ROUND(AVG(price / size_sqft), 0) as avg_price_per_sqft,
    COUNT(*) as listings
FROM `real-estate-intel-491208.real_estate.properties`
WHERE scraped_date = CURRENT_DATE()
AND size_sqft > 0
GROUP BY property_sub_type
```

**Most active areas today**
```sql
SELECT area, COUNT(*) as new_listings
FROM `real-estate-intel-491208.real_estate.properties`
WHERE scraped_date = CURRENT_DATE()
GROUP BY area
ORDER BY new_listings DESC
LIMIT 10
```

---

## Author

Built by Muhammad Aryan — data engineering portfolio project.

GitHub: [aryanlytics](https://github.com/aryanlytics)
LinkedIn: [muhammadaryan008](https://www.linkedin.com/in/muhammadaryan008/)
