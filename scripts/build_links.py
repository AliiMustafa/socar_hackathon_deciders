#!/usr/bin/env python3

import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
import glob

# Project root (important: makes paths robust)
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# INPUT FILES (masters)
SENSORS_CSV = "/home/hackathon/socar_hackathon_deciders/caspian_hackathon_assets/master_sensors.csv"
SURVEYS_CSV = "/home/hackathon/socar_hackathon_deciders/caspian_hackathon_assets/master_surveys.csv"
WELLS_CSV   = "/home/hackathon/socar_hackathon_deciders/caspian_hackathon_assets/master_wells.csv"

# Parquet sources (BOTH SGX + recovered)
SGX_DIR = PROJECT_ROOT / "processed_data/sgx_parquet"
RECOVERED_DIR = PROJECT_ROOT / "processed_data/parquet_recovered"

PARQUET_FILES = (
    list(SGX_DIR.glob("*.parquet")) +
    list(RECOVERED_DIR.glob("*.parquet"))
)

if not PARQUET_FILES:
    raise FileNotFoundError("No parquet files found in sgx_parquet or parquet_recovered")

print("Parquet files found:", [p.name for p in PARQUET_FILES])

# OUTPUT
LINK_DIR = PROJECT_ROOT / "processed_data/raw_vault/links"
LINK_DIR.mkdir(parents=True, exist_ok=True)

# LOAD MASTER HUB KEYS
df_sensors = pd.read_csv(SENSORS_CSV)
df_surveys = pd.read_csv(SURVEYS_CSV)
df_wells   = pd.read_csv(WELLS_CSV)

SENSOR_BK = "sensor_id"
SURVEY_BK = "survey_type_id"
WELL_BK   = "well_id"

valid_sensors = set(df_sensors[SENSOR_BK].astype("string").str.strip().dropna())
valid_surveys = set(df_surveys[SURVEY_BK].astype("string").str.strip().dropna())
valid_wells   = set(df_wells[WELL_BK].astype("string").str.strip().dropna())

# HELPERS
def first_existing_col(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None

def clean_series(s):
    return s.astype("string").str.strip()

def build_link_from_parquet(df, left_col, right_col, record_source):
    link = df[[left_col, right_col]].copy()
    link[left_col] = clean_series(link[left_col])
    link[right_col] = clean_series(link[right_col])

    link = link.dropna(subset=[left_col, right_col])
    link = link[(link[left_col] != "") & (link[right_col] != "")]
    link = link.drop_duplicates(subset=[left_col, right_col])

    link["load_dts"] = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    link["record_source"] = record_source
    return link

# COLUMN CANDIDATES
SENSOR_COL_CANDIDATES = ["sensor_id", "sensorid", "sensor", "sid"]
WELL_COL_CANDIDATES   = ["well_id", "wellid", "well", "wid"]
SURVEY_COL_CANDIDATES = ["survey_type_id", "survey_id", "surveyid", "survey", "survey_type"]

# BUILD LINKS
all_sensor_well = []
all_survey_well = []
all_sensor_survey = []

for fp in PARQUET_FILES:
    dfp = pd.read_parquet(fp)
    src = fp.name

    sensor_col = first_existing_col(dfp, SENSOR_COL_CANDIDATES)
    well_col   = first_existing_col(dfp, WELL_COL_CANDIDATES)
    survey_col = first_existing_col(dfp, SURVEY_COL_CANDIDATES)

    if sensor_col and well_col:
        l = build_link_from_parquet(dfp, sensor_col, well_col, src)
        l = l[l[sensor_col].isin(valid_sensors) & l[well_col].isin(valid_wells)]
        l = l.rename(columns={sensor_col: SENSOR_BK, well_col: WELL_BK})
        all_sensor_well.append(l)

    if survey_col and well_col:
        l = build_link_from_parquet(dfp, survey_col, well_col, src)
        l = l[l[survey_col].isin(valid_surveys) & l[well_col].isin(valid_wells)]
        l = l.rename(columns={survey_col: SURVEY_BK, well_col: WELL_BK})
        all_survey_well.append(l)

    if sensor_col and survey_col:
        l = build_link_from_parquet(dfp, sensor_col, survey_col, src)
        l = l[l[sensor_col].isin(valid_sensors) & l[survey_col].isin(valid_surveys)]
        l = l.rename(columns={sensor_col: SENSOR_BK, survey_col: SURVEY_BK})
        all_sensor_survey.append(l)

# SAVE LINKS
def save_link(all_parts, out_path):
    if not all_parts:
        print(f"Skipped {out_path.name} (no data)")
        return
    link = pd.concat(all_parts, ignore_index=True).drop_duplicates()
    link.to_csv(out_path, index=False)
    print(f"Saved {out_path.name}: {len(link)} rows")

save_link(all_sensor_well, LINK_DIR / "link_sensor_well.csv")
save_link(all_survey_well, LINK_DIR / "link_survey_type_well.csv")
save_link(all_sensor_survey, LINK_DIR / "link_sensor_survey_type.csv")

