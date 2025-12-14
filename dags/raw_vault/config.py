from pathlib import Path

PROJECT_ROOT = Path("/opt/project/socar_hackathon_deciders")

PARQUET_DIRS = [
    ("sgx_parquet", PROJECT_ROOT / "processed_data/sgx_parquet"),
    ("parquet_recovered", PROJECT_ROOT / "processed_data/parquet_recovered"),
]

MANIFEST_TABLE = "raw_vault.file_manifest"
REJECT_TABLE = "raw_vault.rejected_records"

POSTGRES_CONN_ID = "RAWVAULT_PG"


