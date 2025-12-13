#!/usr/bin/env python3
import argparse
import re
from pathlib import Path

PRINTABLE_RE = re.compile(rb"[ -~]{6,}") 


def extract_printable_chunks(blob: bytes) -> list[str]:
    chunks = []
    for m in PRINTABLE_RE.findall(blob):
        s = m.decode("utf-8", errors="ignore").strip()
        if s:
            chunks.append(s)
    return chunks


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", required=True)
    ap.add_argument("--out-dir", default="processed_data/flags")
    args = ap.parse_args()

    data_dir = Path(args.data_dir).resolve()
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    parquet_files = list(data_dir.rglob("*.parquet"))
    found = []

    for p in parquet_files:
        data = p.read_bytes()
        last_par1 = data.rfind(b"PAR1")
        if last_par1 == -1:
            continue

        junk = data[last_par1 + 4 :]
        if not junk:
            continue

        chunks = extract_printable_chunks(junk)
        for c in chunks:
            if "{" in c and "}" in c and len(c) <= 300:
                found.append(c)

    found = sorted(set(found))

    if found:
        for s in found:
            print(s)

    (out_dir / "extracted_flags.txt").write_text("\n".join(found) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()

