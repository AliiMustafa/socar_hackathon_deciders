#!/usr/bin/env python3

import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

ASSETS_DIR = Path("/home/hackathon/socar_hackathon_deciders/caspian_hackathon_assets")
OUT_DIR = Path("/home/hackathon/socar_hackathon_deciders/processed_data/raw_vault/hubs")

def build_hub(csv_path: Path, bk_col: str) -> pd.DataFrame:
    if not csv_path.exists():
        raise FileNotFoundError(f"Missing file: {csv_path}")

    df = pd.read_csv(csv_path)

    if bk_col not in df.columns:
        raise ValueError(
            f"{csv_path.name}: missing business key '{bk_col}'. "
            f"Found columns: {list(df.columns)}"
        )

    hub = df[[bk_col]].copy()
    hub[bk_col] = hub[bk_col].astype("string").str.strip()
    hub = hub.dropna(subset=[bk_col])
    hub = hub[hub[bk_col] != ""]
    hub = hub.drop_duplicates(subset=[bk_col])

    hub["load_dts"] = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    hub["record_source"] = csv_path.name
    return hub

def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    specs = [
        (ASSETS_DIR / "master_sensors.csv", "sensor_id", "hub_sensor.csv"),
        (ASSETS_DIR / "master_surveys.csv", "survey_type_id", "hub_survey_type.csv"),
        (ASSETS_DIR / "master_wells.csv", "well_id", "hub_well.csv"),
    ]

    for src, bk, out_name in specs:
        print(f"Building {out_name} from {src}")
        hub = build_hub(src, bk)
        out_path = OUT_DIR / out_name
        hub.to_csv(out_path, index=False)
        print(f"  → rows: {len(hub)}")

    print("Hubs created in:", OUT_DIR)

if __name__ == "__main__":
    main()

