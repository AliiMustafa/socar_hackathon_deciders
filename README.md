# 🌊 Caspian Seismic Data Platform
**End-to-End Data Vault → Analytics → Dashboard Pipeline**

---

## 📌 Project Overview

This project implements a full data engineering pipeline for seismic survey data from the Caspian basin.

The objective is to:
- recover corrupted data files,
- preserve full historical provenance,
- transform raw data into analytics-ready structures  
for operational and strategic decision-making.

The solution follows **Data Vault 2.0** methodology and delivers:
- Incremental ingestion
- Full historical tracking
- Analytics marts
- Business dashboards

---

## 🧱 Architecture Overview

```
Raw Files (SGX / Parquet)
↓
Recovery & Decoding
↓
Raw Vault (Hubs / Links / Satellites)
↓
Incremental Ingest (Apache Airflow)
↓
Analytics Layer (Star Schema)
↓
Visualization (Metabase)
```

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

## 🎬 Episode 1 — Data Recovery & Decoding

Episode 1 focuses on **making the raw hackathon data readable and trustworthy**.

The dataset contains:
- corrupted Parquet files
- proprietary binary `.sgx` files

These must be analyzed, recovered, and decoded **before any modeling or analytics**.

All output directories are **created automatically** by the scripts.

---

## 🛠 Step 1 — Recover Corrupted Parquet Files

Some Parquet files are corrupted due to extra trailing bytes after the `PAR1` footer, making them unreadable.

### Recovery process:
- Locate the last valid `PAR1` marker
- Truncate invalid trailing bytes
- Restore a readable Parquet file

### Run
```bash
./solutions/corrupted_parquet.sh --data-dir ./caspian_hackathon_assets/track_1_forensics
```

**Output**

```
processed_data/parquet_recovered/
```

### Flag verification

```bash
./solutions/flag_parquet.sh --data-dir ./caspian_hackathon_assets/track_1_forensics
```

**Output**

```
processed_data/flags/
FLAG{DATA_ARCHAEOLOGIST_LVL_99}
```

---

## 🔓 Step 2 — Decoding `.sgx` Binary Files

The dataset contains proprietary `.sgx` binary files storing seismic survey readings.

These files are decoded into Parquet format **without modifying original measurements**.

---

### What is an `.sgx` File?

**Header (16 bytes)**

* Magic string: `CPETRO01` (8 bytes)
* `survey_type_id` (uint32)
* `trace_count` (uint32)

**Trace Records (repeated)**

* `well_id` (uint32)
* `depth_ft` (float32)
* `amplitude` (float32)
* `quality_flag` (uint8)

Each record represents a single seismic trace.

---

### Decoding Logic

1. Read binary file as raw bytes
2. Validate magic header (`CPETRO01`)
3. Extract metadata (`survey_type_id`, `trace_count`)
4. Parse fixed-length trace records
5. Convert into structured table
6. Write decoded output as Parquet

Invalid files are skipped with explicit errors.

---

### How to Decode SGX Files

```bash
./solutions/decode_sgx.sh --data-dir ./caspian_hackathon_assets/track_1_forensics
```

Decoded files are written to:

```
processed_data/sgx_parquet/
```

---

## ➕ Adding Metadata Columns to Decoded SGX Files

Decoded SGX parquet files lack metadata required for Data Vault ingestion.

### Added columns:

* `timestamp` — derived from year in filename (`YYYY-01-01`)
* `sensor_id` — preserved as-is (no inference)

**No existing data is modified.**

---

### Logic

1. Extract year from filename
2. Create timestamp (`YYYY-01-01`)
3. Add missing columns only if absent
4. Apply **no transformations or rules**

---

### Usage

```bash
python3 scripts/add_metadata_to_decoded_sgx.py
```

---

### Validation

```bash
python3 - << EOF
import pandas as pd
df = pd.read_parquet("processed_data/sgx_parquet/legacy_survey_1991_101_decoded.parquet")
print(df[["timestamp", "sensor_id"]].head())
EOF
```

---

### Guarantees

* No data loss
* No business logic
* Safe re-runs
* Data Vault compatible

---

## 🧩 Data Vault Model

### Hubs

* hub_well
* hub_sensor
* hub_survey_type

Store immutable business keys.

---

### Links

* link_sensor_well
* link_sensor_survey_type
* link_survey_type_well

Represent relationships between business entities.

---

### Satellites

* sat_link_sensor_well_readings

Store all historical measurements and metadata:

* amplitude
* timestamp
* record_source
* checksum
* load_dts

No transformations are applied at this layer.

---

## 🔁 Incremental Ingestion (Apache Airflow)

**DAG:** `raw_vault_incremental_ingest`

### Steps

1. Scan parquet directories
2. Detect new/changed files (`file_manifest`)
3. Process valid data
4. Track missing files

### Control tables

* raw_vault.file_manifest
* raw_vault.rejected_records

---

## 📊 Analytics Layer

### Dimensions

* dim_well
* dim_sensor
* dim_time
* dim_source_format
* dim_survey_type

### Fact Table

* fact_sensor_readings

Only fully mapped, valid records are loaded.

---

## 📈 Analytics Marts

### mart_well_performance

* total_readings
* avg_amplitude
* data_quality_pct
* breakdown by source_format

### mart_sensor_analysis

* total_readings
* avg_amplitude
* data_quality_pct
  (sensor_id used as sensor type)

### mart_survey_summary

* wells_surveyed
* total_readings
* avg_amplitude
* time coverage
* breakdown by source_format

---

## 📊 Dashboards (Metabase)

Dashboards include:

* Wells geospatial map
* Well activity over time
* Amplitude distributions
* Sensor reliability analysis
* Survey coverage summaries
* Data quality indicators

---

## 🚀 How to Run

Start services:

```bash
docker compose up -d
```

Trigger ingestion:

```bash
airflow dags trigger raw_vault_incremental_ingest
```

Access:

* Airflow UI: http://<VM_IP>:8080
* Metabase: http://<VM_IP>:3000

Airflow:
admin - user
admin - pass

Metabase:
email - aliimustafazada@gmail.com
pass - Deciders2025!!
---

## 🏁 Outcome

The platform delivers:

* Full data lineage
* Historical traceability
* Decision-ready analytics

**The data didn't just survive. It spoke.**
