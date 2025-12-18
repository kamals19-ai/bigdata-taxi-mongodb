## System Architecture Diagram



```mermaid
flowchart LR
  A[Parquet Files] --> B[Raw Ingestion]
  B --> C[(MongoDB Atlas: raw_trips)]
  C --> D[Cleaning + Validation]
  D --> E[(MongoDB Atlas: clean_trips)]
  E --> F[Aggregation]
  F --> G[(MongoDB Atlas: agg_trips)]
  G --> H[Dashboard / Visualizations]

