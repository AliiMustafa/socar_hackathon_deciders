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
## Episode 1

Episode 1 focuses on **making the raw hackathon data readable and trustworthy**.  
The dataset contains **corrupted Parquet files** and **binary `.sgx` files**, which must be analyzed, recovered, and decoded before any data modeling or analytics.

This episode performs **forensic inspection**, **data recovery**, and **binary decoding**, while preserving original data and provenance.

All output directories are **created automatically** when scripts are executed.

---

## Step 1 – Recover Corrupted Parquet Files

Some Parquet files in the dataset are **corrupted** due to extra trailing bytes after the `PAR1` footer.  
These files cannot be read by standard Parquet readers.

The recovery process:
- locates the last valid `PAR1` marker
- truncates invalid trailing bytes
- restores a readable Parquet file

### Run

```bash
./solutions/corrupted_parquet.sh --data-dir ./caspian_hackathon_assets/track_1_forensics

Output

processed_data/parquet_recovered/

Run ./solutions/flag_parquet.sh --data-dir ./caspian_hackathon_assets/track_1_forensics

Output
processed_data/flags/
FLAG{DATA_ARCHAEOLOGIST_LVL_99}
---
## Decoding `.sgx` Binary Files

The dataset contains proprietary binary files with the `.sgx` extension.  
These files represent seismic survey readings stored in a **custom binary format** and cannot be read directly using standard tools.

The goal of this step is to **decode `.sgx` files into readable Parquet format** while preserving the original measurements.

---

### What is an `.sgx` File?

An `.sgx` file is a binary container with the following structure:

**Header (16 bytes)**
- Magic string: `CPETRO01` (8 bytes)
- `survey_type_id` (unsigned int, 4 bytes)
- `trace_count` (unsigned int, 4 bytes)

**Records (repeated `trace_count` times)**
- `well_id` (unsigned int, 4 bytes)
- `depth_ft` (float, 4 bytes)
- `amplitude` (float, 4 bytes)
- `quality_flag` (unsigned char, 1 byte)

Each record represents a single seismic trace measurement.

---

### Decoding Logic

The decoding process performs the following steps:

1. Read the binary file as raw bytes
2. Validate the magic header (`CPETRO01`)
3. Extract survey metadata (`survey_type_id`, `trace_count`)
4. Parse each fixed-length trace record
5. Convert decoded records into a structured table
6. Write the result as a Parquet file

If the binary structure does not match expectations, the file is skipped and an error is reported.

---

### How to Decode SGX Files

Run the following command from the repository root:

```bash
./solutions/decode_sgx.sh --data-dir ./caspian_hackathon_assets/track_1_forensics

Decoded SGX parquet files originally do not contain the required metadata columns
needed for Data Vault ingestion.

This step enriches decoded SGX parquet files by adding:

- timestamp (derived from year in filename)
- sensor_id (preserved as-is, no inference)

No existing data is modified.

---

## Input & Output

Input directory:
processed_data/sgx_parquet/

Output:
Files are overwritten in-place after adding missing columns.

---

## Logic

1. Extract year from filename
2. Create timestamp: YYYY-01-01
3. Add missing columns only if absent
4. No transformations or rules applied

---

## Usage

Run from project root:

python3 scripts/add_metadata_to_decoded_sgx.py

---

## Validation

python3 - << EOF2
import pandas as pd
df = pd.read_parquet("processed_data/sgx_parquet/legacy_survey_1991_101_decoded.parquet")
print(df[["timestamp", "sensor_id"]].head())
EOF2

---

## Guarantees

- No data loss
- No business logic
- Safe re-runs
- Data Vault compatible


## Repository Structure (Episode 1)


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

