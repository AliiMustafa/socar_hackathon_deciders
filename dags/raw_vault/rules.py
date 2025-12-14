import json
import pandas as pd
from typing import Tuple, List, Dict

from .db import get_hook
from .utils import utc_now_iso

def rule_well_must_exist(df: pd.DataFrame, record_source: str) -> Tuple[pd.DataFrame, List[Dict]]:
    """
    Reject rows whose well_id does not exist in raw_vault.hub_well.
    Returns: (valid_df, rejected_rows_as_dicts)
    """
    if "well_id" not in df.columns:
        return df, []

    hook = get_hook()
    ref = pd.read_sql("SELECT well_id FROM raw_vault.hub_well;", hook.get_conn())
    valid_wells = set(ref["well_id"].astype("string").str.strip().dropna())

    well_series = df["well_id"].astype("string").str.strip()
    bad_mask = (~well_series.isin(valid_wells)) & (~well_series.isna()) & (well_series != "")

    bad = df[bad_mask]
    good = df[~bad_mask]

    rejected = []
    for _, row in bad.iterrows():
        rejected.append({
            "rejected_dts": utc_now_iso(),
            "rule_name": "well_must_exist",
            "reason": "well_id not found in raw_vault.hub_well",
            "record_source": record_source,
            "payload": row.to_dict(),
        })

    return good, rejected


def apply_all_rules(df: pd.DataFrame, record_source: str) -> Tuple[pd.DataFrame, List[Dict]]:
    """
    Add new rules here in order.
    """
    all_rejected: List[Dict] = []

    df, rejected = rule_well_must_exist(df, record_source)
    all_rejected.extend(rejected)

    return df, all_rejected

