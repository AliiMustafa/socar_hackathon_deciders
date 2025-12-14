# 🌊 Caspian Seismic Data Platform
**End-to-End Data Vault → Analytics → Dashboard Pipeline**

---

## 📌 Project Overview

This project implements a full data engineering pipeline for seismic survey data from the Caspian basin.
The objective is to recover corrupted data files, preserve full historical provenance, and transform the data
into analytics-ready structures for operational and strategic decision-making.

The solution follows **Data Vault 2.0** methodology and delivers:
- Incremental ingestion
- Full historical tracking
- Analytics marts
- Business dashboards

---

## 🧱 Architecture Overview

Raw Files (SGX / Parquet)  
→ Recovery & Decoding  
→ Raw Vault (Hubs / Links / Satellites)  
→ Incremental Ingest (Apache Airflow)  
→ Analytics Layer (Star Schema)  
→ Visualization (Metabase)

---

## 🛠️ Technology Stack

- **Database:** PostgreSQL  
- **Orchestration:** Apache Airflow  
- **Containers:** Docker, Docker Compose  
- **Modeling:** Data Vault 2.0  
- **Analytics:** Dimensional Modeling (Star Schema)  
- **Visualization:** Metabase  
- **Languages:** Python, SQL  
- **Environment:** Linux VM  

---

---

## 🧩 Data Vault Model

### Hubs
- hub_well
- hub_sensor
- hub_survey_type

Store immutable business keys.

### Links
- link_sensor_well
- link_sensor_survey_type
- link_survey_type_well

Represent relationships between business entities.

### Satellites
- sat_link_sensor_well_readings

Store all historical measurements and metadata:
- amplitude
- timestamp
- record_source
- checksum
- load_dts

No transformations or business rules are applied at this layer.

---

## 🔁 Incremental Ingestion (Apache Airflow)

DAG: **raw_vault_incremental_ingest**

Steps:
1. Scan parquet directories
2. Detect new or changed files (file_manifest)
3. Process valid data
4. Track missing files

Control tables:
- raw_vault.file_manifest
- raw_vault.rejected_records

---

## 📊 Analytics Layer

### Dimensions
- dim_well
- dim_sensor
- dim_time
- dim_source_format
- dim_survey_type

### Fact Table
- fact_sensor_readings

Only valid, fully mapped records are loaded into analytics.

---

## 📈 Analytics Marts

### mart_well_performance
- total_readings
- avg_amplitude
- data_quality_pct
- breakdown by source_format

### mart_sensor_analysis
- total_readings
- avg_amplitude
- data_quality_pct  
(sensor_id used as sensor type)

### mart_survey_summary
- wells_surveyed
- total_readings
- avg_amplitude
- time coverage
- breakdown by source_format

---

## 📊 Dashboards (Metabase)

Dashboards include:
- Wells geospatial map
- Well activity over time
- Amplitude distributions
- Sensor reliability analysis
- Survey coverage summaries
- Data quality indicators

---

## 🚀 How to Run

Start services:
```bash
docker compose up -d

