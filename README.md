## Architecture

This project implements a three-layer Big Data pipeline using MongoDB Atlas:
Raw data is ingested from NYC Taxi parquet files, cleaned and validated using
Pydantic models, aggregated into a gold layer, and visualized through a dashboard.

📄 [Architecture Diagram](docs/architecture.md)

GOT IT — sorry about that 😭
You want **ONE clean Markdown block** you can **paste directly into the GitHub web README editor**, **without touching terminal/Bash**.

Below is **EXACTLY what to paste**, nothing extra.

---

## ✅ COPY-PASTE THIS DIRECTLY INTO `README.md` ON GITHUB (WEB)

```md
## Overview

This project implements an **end-to-end Big Data pipeline** using **MongoDB Atlas** and **Python** to process, aggregate, and visualize NYC Taxi trip data.  
The pipeline follows a **Medallion Architecture (Bronze → Silver → Gold)** and culminates in an interactive **Streamlit dashboard** backed directly by MongoDB (no flat files).

---

## Architecture

This project implements a three-layer Big Data pipeline using MongoDB Atlas:

- Raw NYC Taxi data is ingested from Parquet files
- Data is cleaned and validated using Pydantic models
- Business-level aggregates are stored in a Gold layer
- Insights are visualized through a Streamlit dashboard

 **Architecture Diagram:**  
`docs/architecture.png`

---

## Medallion Data Layers

### Bronze Layer — Raw Data
- Source: NYC Taxi Parquet files
- Collection: `raw_trips`
- Purpose: Store raw, unmodified data for traceability

### Silver Layer — Cleaned Data
- Collection: `clean_trips`
- Validation: Pydantic schemas
- Processing:
  - Type validation
  - Missing value handling
  - Invalid record removal

### Gold Layer — Aggregated Data
- Collection: `agg_trips`
- Aggregations:
  - Monthly trip count
  - Average fare
  - Average tip
  - Total revenue
- Grouped by:
  - `month`
  - `VendorID`

---

## Project Structure

```

bigdata-taxi-mongodb/
├── src/
│   ├── ingestion/
│   ├── cleaning/
│   ├── aggregation/
│   ├── dashboard/
│   ├── models/
│   └── utils/
├── docs/
│   └── architecture.png
├── pyproject.toml
├── uv.lock
└── README.md

```

---

## How to Run the Pipeline

All commands are executed from the project root.

### 1. Ingest Raw Data
```

uv run python -m src.ingestion.ingest_data

```

### 2. Clean and Validate
```

uv run python -m src.cleaning.clean_data

```

### 3. Build Gold Aggregates
```

uv run python -m src.aggregation.aggregate_data

```

---

## Dashboard (Visualization Layer)

The dashboard visualizes **Gold-layer data (`agg_trips`)** directly from MongoDB Atlas.

### Visualizations
- Trips by Month
- Revenue by Vendor
- Average Fare Trends

### Run Locally
```

uv run streamlit run src/dashboard/app.py

```

### Live Dashboard
*(Add your Streamlit Cloud URL here)*

---

## Key Insights

- Taxi demand shows strong month-to-month variation.
- Revenue is concentrated among a small number of vendors.
- Average fares remain relatively stable despite large trip volume changes.

---

## Technologies Used

- MongoDB Atlas (Dedicated Cluster)
- Python
- PyMongo
- Pydantic
- Streamlit
- Pandas
- Matplotlib
- uv

---

## Design Decisions

- No Tableau or Power BI used
- No flat-file dashboards
- MongoDB-backed analytics
- Modular, reproducible pipeline

---

## Status

✅ Ingestion Complete  
✅ Cleaning Complete  
✅ Aggregation Complete  
✅ Dashboard Deployed

```


