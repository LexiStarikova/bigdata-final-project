"""
Build team PostgreSQL project database (Stage I).
Loads NYC Yellow Taxi Parquet from ./data into table yellow_taxi_trips.

Connection matches course materials:
  host=hadoop-04.uni.innopolis.ru, dbname={USER}_projectdb, user={USER}
Password: secrets/.psql.pass (see IU Hadoop Cluster manual).
"""

import glob
import io
import os
import sys
from pathlib import Path
from typing import List

import pandas as pd
import psycopg2 as psql
import pyarrow.parquet as pq

# Columns loaded into PostgreSQL (trip_id is SERIAL, not in COPY)
COPY_COLUMNS = [
    "vendorid",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "ratecodeid",
    "store_and_fwd_flag",
    "pulocationid",
    "dolocationid",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
]

COPY_SQL = (
    "COPY yellow_taxi_trips ("
    + ", ".join(COPY_COLUMNS)
    + ") FROM STDIN WITH (FORMAT csv, NULL '\\N')"
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _load_password(root: Path) -> str:
    """Password: file secrets/.psql.pass (course), or env PGPASSWORD."""
    secret = root / "secrets" / ".psql.pass"
    alt = root / "secrets" / "psql.pass"
    raw = os.environ.get("PGPASSWORD", "").strip()
    if raw:
        return raw
    for path in (secret, alt):
        if path.is_file():
            raw = path.read_text(encoding="utf-8").strip()
            if raw:
                return raw
    print(
        "No database password: set PGPASSWORD, or create a non-empty "
        "secrets/.psql.pass (see secrets/README.txt).",
        file=sys.stderr,
    )
    sys.exit(1)


def _connect(root: Path):
    user = os.environ.get("PGUSER") or os.environ.get("USER") or os.environ.get("USERNAME")
    if not user:
        print("USER / USERNAME not set; cannot derive database name.", file=sys.stderr)
        sys.exit(1)
    dbname = f"{user}_projectdb"
    password = _load_password(root)
    if not password:
        print(
            "Password is empty after reading secrets/.psql.pass. "
            "Put the cluster DB password on one line, no spaces around it.",
            file=sys.stderr,
        )
        sys.exit(1)
    host = os.environ.get("PGHOST", "hadoop-04.uni.innopolis.ru")
    port = int(os.environ.get("PGPORT", "5432"))
    # Keyword args avoid breakage if the password contains special characters.
    return psql.connect(
        host=host,
        port=port,
        user=user,
        dbname=dbname,
        password=password,
    )


def _normalize_frame(raw: pd.DataFrame) -> pd.DataFrame:
    """Map TLC Parquet column names (mixed case) to lowercase SQL columns."""
    col_map = {c.lower(): c for c in raw.columns}
    out = pd.DataFrame()
    for target in COPY_COLUMNS:
        src = col_map.get(target)
        if src is None and target == "airport_fee":
            src = col_map.get("airport_fee") or col_map.get("airportfee")
        if src is None:
            out[target] = pd.NA
        else:
            out[target] = raw[src]
    for ts in ("tpep_pickup_datetime", "tpep_dropoff_datetime"):
        if ts in out.columns:
            out[ts] = pd.to_datetime(out[ts], errors="coerce").dt.tz_localize(None)
    for col in out.columns:
        if col not in ("tpep_pickup_datetime", "tpep_dropoff_datetime", "store_and_fwd_flag"):
            out[col] = pd.to_numeric(out[col], errors="coerce")
    flag = out["store_and_fwd_flag"]
    out["store_and_fwd_flag"] = (
        flag.map(lambda x: "" if pd.isna(x) else str(x)[:8])
    )
    return out


def _parquet_paths(root: Path) -> List[Path]:
    pattern = str(root / "data" / "yellow_tripdata_2025-*.parquet")
    paths = sorted(Path(p) for p in glob.glob(pattern))
    if not paths:
        print(
            "No Parquet files matching data/yellow_tripdata_2025-*.parquet. "
            "Run: bash scripts/preprocess.sh",
            file=sys.stderr,
        )
        sys.exit(1)
    return paths


def load_parquet_to_postgres(conn, root: Path) -> None:
    """Stream Parquet batches into PostgreSQL with COPY."""
    max_rows = os.environ.get("STAGE1_MAX_ROWS")
    max_total = int(max_rows) if max_rows else None
    batch_rows = int(os.environ.get("STAGE1_BATCH_ROWS", "50000"))
    total = 0
    cur = conn.cursor()
    for path in _parquet_paths(root):
        print(f"Loading {path.name} …")
        pf_file = pq.ParquetFile(path)
        for batch in pf_file.iter_batches(batch_size=batch_rows):
            chunk = batch.to_pandas()
            frame = _normalize_frame(chunk)
            if max_total is not None:
                remain = max_total - total
                if remain <= 0:
                    break
                if len(frame) > remain:
                    frame = frame.iloc[:remain].copy()
            buf = io.StringIO()
            frame.to_csv(
                buf,
                index=False,
                header=False,
                na_rep="\\N",
                date_format="%Y-%m-%d %H:%M:%S",
                columns=COPY_COLUMNS,
            )
            buf.seek(0)
            cur.copy_expert(COPY_SQL, buf)
            total += len(frame)
            print(f"  … {total:,} rows committed so far")
        if max_total is not None and total >= max_total:
            break
    conn.commit()
    cur.close()
    print(f"Done loading {total:,} rows into yellow_taxi_trips.")


def main() -> None:
    """Create tables, load Parquet data into PostgreSQL, and verify."""
    root = _repo_root()
    os.chdir(root)
    sql_dir = root / "sql"
    with _connect(root) as conn:
        cur = conn.cursor()
        with open(sql_dir / "create_tables.sql", encoding="utf-8") as fh:
            cur.execute(fh.read())
        conn.commit()
        cur.close()
        load_parquet_to_postgres(conn, root)
        cur = conn.cursor()
        with open(sql_dir / "test_database.sql", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line or line.startswith("--"):
                    continue
                cur.execute(line)
                if cur.description:
                    rows = cur.fetchall()
                    print(rows)
        conn.commit()
        cur.close()


if __name__ == "__main__":
    main()
