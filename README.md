# Telegram Health Data Pipeline

A data engineering solution to automate extraction, processing, and analysis of health-related content from public Ethiopian Telegram channels.

**Tech Stack:**  
- Python (scraping, orchestration scripts)  
- PostgreSQL (data warehouse)  
- dbt (transformation & analytics)  
- YOLOv8 (image enrichment)  
- FastAPI (analytical API)  
- Dagster (pipeline orchestration)

---

## Overview

This ELT pipeline extracts messages and media from Telegram, stores raw data in a partitioned Data Lake, loads it into PostgreSQL, transforms it via dbt into a star schema, enriches images using YOLOv8 object detection, and exposes insights through FastAPI. Dagster orchestrates and monitors the entire workflow.

---

## Features

- **Telegram Scraping:** Extracts text, metadata, and images using Telethon.
- **Data Lake:** Organizes raw JSON/messages and images by date/channel.
- **Robust Loading:** Python scripts sanitize and load data into PostgreSQL.
- **dbt Modeling:** Cleans, structures, and tests data in layered models (`raw`, `staging`, `marts`).
- **Image Enrichment:** YOLOv8 detects objects in images, linked to messages.
- **Analytical API:** FastAPI endpoints for querying insights.
- **Orchestration:** Dagster automates and schedules pipeline steps.

---

## Directory Structure

```
week7/
├── .env                  # Env variables
├── docker-compose.yml    # PostgreSQL & app containers
├── data/
│   ├── raw/              # Telegram messages/images
│   └── processed/        # YOLO detections/logs
├── scripts/              # Python scripts
├── api/                  # FastAPI app
├── orchestration/        # Dagster code
└── my_project/           # dbt project
```

---

## Quick Start

1. **Clone & Set Up Python:**
    ```bash
    git clone https://github.com/fevenissayas/Telegram-health-data-pipeline.git
    cd Telegram-health-data-pipeline
    python3 -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt
    ```
2. **Configure `.env`:**
    - Fill in Telegram API and PostgreSQL credentials.

3. **Start PostgreSQL with Docker:**
    ```bash
    docker-compose up -d --build
    ```

4. **Run Pipeline Steps:**
    - Scrape Telegram:  
      `python scripts/telegram_scraper.py`
    - Load to DB:  
      `python scripts/load_to_postgres.py`
    - YOLO detection:  
      `python scripts/yolo_detector.py`
    - Load YOLO results:  
      `python scripts/load_yolo_to_pg.py`
    - dbt transformations:  
      `cd my_project && dbt run --full-refresh`
    - API server:  
      `uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload`
    - Dagster UI:  
      `dagster dev -m orchestration.definitions`

---

## Data Model

- **Lake:** Partitioned `data/raw/` (messages, images), `data/processed/` (YOLO).
- **Warehouse:**  
  - Raw tables: `raw.raw_telegram_messages`, `raw.raw_yolo_detections`
  - Staging: `staging.stg_telegram_messages`, `staging.stg_yolo_detections`
  - Marts: `dim_channels`, `dim_dates`, `fct_messages`, `fct_image_detections`

---

## Troubleshooting

- Serialization errors: Custom encoders handle `datetime`, `bytes`, and `\u0000`.
- dbt errors: Check for file syntax, schema hooks, and `profiles.yml`.
- YOLO issues: Ensure internet for first run; delete logs to reprocess.
- Dagster errors: Run from project root; check op definitions and timezone.