\# System Architecture Diagram



```mermaid

flowchart LR

&nbsp; A\[Parquet Files] --> B\[Raw Ingestion]

&nbsp; B --> C\[(MongoDB Atlas: raw\_trips)]

&nbsp; C --> D\[Cleaning + Validation]

&nbsp; D --> E\[(MongoDB Atlas: clean\_trips)]

&nbsp; E --> F\[Aggregation]

&nbsp; F --> G\[(MongoDB Atlas: agg\_trips)]

&nbsp; G --> H\[Dashboard / Visualizations]



