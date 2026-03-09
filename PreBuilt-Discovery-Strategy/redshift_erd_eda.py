"""
Redshift ERD + EDA Generator  (v3 – Maximum Optimization)
==========================================================
Optimization history
--------------------
v1 (original) : 2×N queries per table  (one per column, twice)
v2            : 3 queries per table     (batched UNION ALL + percentile split)
v3            : parallelised across tables + system-table row counts
                + skip top-values entirely when all cols are high-cardinality
v4 (this file): exponential backoff on retries + atomic JSON writes
                + elapsed-time logging + failed-table summary
                + improved measure/dimension heuristic
                + removed svv_table_info dependency; row count from Pass 1
                + boolean columns cast safely in top-values query

v3 optimizations over v2
------------------------
1. PARALLEL TABLE PROFILING
   Tables are profiled concurrently using a thread pool (default 4 workers,
   configurable via "parallel_workers" in the config).  Wall-clock time drops
   by up to N× on multi-table schemas because Redshift handles concurrent
   queries well.

2. ROW COUNTS FROM SYSTEM TABLES (zero scan cost)
   svv_table_info already stores accurate row counts maintained internally.
   We fetch all counts in ONE query before profiling starts.

3. SKIP TOP-VALUES QUERY WHEN UNNECESSARY
   If Pass 1 reveals that every column exceeds the cardinality threshold
   (distinct > top_values_limit), the entire Pass 3 round-trip is skipped.

4. CURSOR-PER-THREAD
   Each worker thread opens its own cursor so threads never block each other.

All EDA output fields are fully preserved.

Requirements:
    pip install redshift-connector   (or psycopg2-binary as a drop-in)

Usage:
    1) Edit redshift_erd_eda_config.json
    2) python redshift_erd_eda.py [--config path/to/config.json]

Config keys:
    host, db, user, password  (required)
    port                      (default 5439)
    schema                    (default "public")
    tables                    list of names; omit/[] = all tables
    output_dir                where to write erd.json / eda.json (default: cwd)
    parallel_workers          concurrent table workers (default 4)
    top_values_limit          cardinality threshold for top-values (default 1000)
    max_retries               retries on SSL/connection errors (default 3)
    retry_wait_secs           base seconds between retries, doubles each attempt (default 3)
"""

import argparse
import json
import math
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date, timezone

# ── connectors ────────────────────────────────────────────────────────────────
try:
    import redshift_connector as _driver
    def _connect(host, port, db, user, password):
        conn = _driver.connect(
            host=host, port=port, database=db, user=user, password=password)
        conn.autocommit = True
        return conn
except ImportError:
    try:
        import psycopg2 as _driver
        def _connect(host, port, db, user, password):
            conn = _driver.connect(
                host=host, port=port, dbname=db, user=user, password=password)
            conn.autocommit = True
            return conn
    except ImportError:
        sys.exit("Install redshift-connector or psycopg2-binary.")

# ── type sets ─────────────────────────────────────────────────────────────────
NUMERIC_TYPES = {
    "integer", "bigint", "smallint", "decimal", "numeric",
    "real", "double precision", "float", "float4", "float8",
}
DATE_TYPES = {
    "date", "timestamp", "timestamp without time zone",
    "timestamp with time zone", "timestamptz",
}
STRING_TYPES = {
    "character varying", "varchar", "char", "character", "text", "bpchar",
}

DEFAULT_CONFIG_FILE = "config.json"
DEFAULT_WORKERS     = 4
DEFAULT_TOP_LIMIT   = 1000
DEFAULT_RETRIES     = 3    # max retries per table on connection errors
RETRY_WAIT          = 3    # seconds to wait between retries

# SSL / connection error signatures to catch and retry
_RETRYABLE = (
    "bad length", "ssl", "broken pipe", "connection",
    "eof", "tls", "reset", "closed",
)


def _is_retryable(exc: Exception) -> bool:
    """Return True if the exception looks like a transient connection drop."""
    return any(k in str(exc).lower() for k in _RETRYABLE)


# ─────────────────────────────────────────────────────────────────────────────
# Utilities
# ─────────────────────────────────────────────────────────────────────────────

def load_config(path=None):
    cfg_path = path or DEFAULT_CONFIG_FILE
    if not os.path.isabs(cfg_path):
        cfg_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), cfg_path)
    if not os.path.exists(cfg_path):
        raise FileNotFoundError(f"Config file not found: {cfg_path}")
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    missing = [k for k in ("host", "db", "user", "password") if not cfg.get(k)]
    if missing:
        raise ValueError(f"Missing config keys: {', '.join(missing)}")
    return cfg


def _safe(val):
    if val is None:
        return None
    if isinstance(val, (datetime, date)):
        return str(val)
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
        return None
    if hasattr(val, "item"):
        return val.item()
    return val


def run_query(cursor, sql, params=None):
    cursor.execute(sql, params or ())
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def new_cursor(conn):
    return conn.cursor()



# ─────────────────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
# Dynamic worker calculation
# ─────────────────────────────────────────────────────────────────────────────

def fetch_wlm_slots(cursor):
    """
    Query Redshift WLM to find how many concurrent query slots are available.
    Falls back to 15 if the view is inaccessible.
    """
    try:
        rows = run_query(cursor, """
            SELECT SUM(num_query_tasks) AS total_slots
            FROM   stv_wlm_service_class_config
            WHERE  service_class > 4
        """)
        slots = int((rows[0]["total_slots"] or 0)) if rows else 0
        return slots if slots > 0 else 15
    except Exception:
        return 15   # conservative fallback if user lacks access


def calc_optimal_workers(n_tables, wlm_slots, max_workers=None):
    """
    Derive optimal worker count using the formula:
        W = min( sqrt(N), WLM_slots, max_workers )

    sqrt(N) is the empirical sweet spot — beyond it, SSL/connection overhead
    costs more than the parallelism gains.
    WLM_slots caps us at Redshift's actual concurrency limit.
    max_workers is an optional hard ceiling from config.
    """
    import math
    w = math.isqrt(n_tables)           # e.g. sqrt(121) = 11
    w = min(w, wlm_slots)              # never exceed Redshift's slot count
    w = max(w, 1)                      # always at least 1
    if max_workers is not None:
        w = min(w, int(max_workers))   # respect explicit config ceiling
    return w


# ERD
# ─────────────────────────────────────────────────────────────────────────────

ERD_SQL = """
SELECT
    c.table_name,
    c.column_name,
    c.data_type,
    c.character_maximum_length,
    c.numeric_precision,
    c.numeric_scale,
    c.is_nullable,
    c.ordinal_position,
    tc.constraint_type,
    ccu.table_name  AS referenced_table,
    ccu.column_name AS referenced_column
FROM information_schema.columns c
LEFT JOIN information_schema.key_column_usage kcu
       ON kcu.table_schema = c.table_schema
      AND kcu.table_name   = c.table_name
      AND kcu.column_name  = c.column_name
LEFT JOIN information_schema.table_constraints tc
       ON tc.constraint_name = kcu.constraint_name
      AND tc.table_schema    = c.table_schema
LEFT JOIN information_schema.constraint_column_usage ccu
       ON ccu.constraint_name = kcu.constraint_name
      AND tc.constraint_type  = 'FOREIGN KEY'
WHERE c.table_schema = %s
  AND c.table_name   IN ({placeholders})
ORDER BY c.table_name, c.ordinal_position;
"""


def build_erd(cursor, schema, tables):
    if not tables:
        return []
    sql  = ERD_SQL.format(placeholders=", ".join(["%s"] * len(tables)))
    rows = run_query(cursor, sql, (schema, *tables))

    erd_tables = {}
    for r in rows:
        tname = r["table_name"]
        if tname not in erd_tables:
            erd_tables[tname] = {"name": tname, "schema": schema,
                                  "columns": [], "relationships": []}
        col = {
            "name":     r["column_name"],
            "type":     r["data_type"].upper(),
            "nullable": r["is_nullable"] == "YES",
        }
        if r["character_maximum_length"]:
            col["max_length"] = r["character_maximum_length"]
        if r["numeric_precision"]:
            col["precision"] = r["numeric_precision"]
            col["scale"]     = r["numeric_scale"]

        ctype = r["constraint_type"]
        if ctype == "PRIMARY KEY":
            col["primary_key"] = True
        elif ctype == "FOREIGN KEY":
            col["foreign_key"] = {
                "references_table":  r["referenced_table"],
                "references_column": r["referenced_column"],
            }
        erd_tables[tname]["columns"].append(col)

        if ctype == "FOREIGN KEY":
            rel = {"type": "many-to-one", "to_table": r["referenced_table"],
                   "on_column": r["column_name"]}
            if rel not in erd_tables[tname]["relationships"]:
                erd_tables[tname]["relationships"].append(rel)

    return list(erd_tables.values())


# ─────────────────────────────────────────────────────────────────────────────
# EDA – per-column SQL builders
# ─────────────────────────────────────────────────────────────────────────────

def _base_expr(col_name, dtype, schema, table):
    """Pass 1: no PERCENTILE_CONT → safe with COUNT(DISTINCT)."""
    q   = f'"{schema}"."{table}"'
    c   = f'"{col_name}"'
    lit = col_name.replace("'", "''")

    common = f"""
        '{lit}'               AS col_name,
        COUNT(*)              AS total_rows,
        COUNT({c})            AS non_null_count,
        COUNT(*) - COUNT({c}) AS null_count,
        COUNT(DISTINCT {c})   AS distinct_count"""

    if dtype in NUMERIC_TYPES:
        return f"""
        SELECT {common},
            MIN({c})::FLOAT             AS min_val,
            MAX({c})::FLOAT             AS max_val,
            AVG({c})::FLOAT             AS mean_val,
            STDDEV({c})::FLOAT          AS stddev_val,
            NULL::VARCHAR AS min_date,  NULL::VARCHAR AS max_date,
            NULL::INT     AS date_range_days,
            NULL::INT     AS min_length, NULL::INT   AS max_length,
            NULL::FLOAT   AS avg_length
        FROM {q}"""

    elif dtype in DATE_TYPES:
        return f"""
        SELECT {common},
            NULL::FLOAT AS min_val,  NULL::FLOAT AS max_val,
            NULL::FLOAT AS mean_val, NULL::FLOAT AS stddev_val,
            MIN({c})::VARCHAR           AS min_date,
            MAX({c})::VARCHAR           AS max_date,
            DATEDIFF('day', MIN({c}), MAX({c})) AS date_range_days,
            NULL::INT   AS min_length, NULL::INT   AS max_length,
            NULL::FLOAT AS avg_length
        FROM {q}"""

    elif dtype in STRING_TYPES:
        return f"""
        SELECT {common},
            NULL::FLOAT AS min_val,  NULL::FLOAT AS max_val,
            NULL::FLOAT AS mean_val, NULL::FLOAT AS stddev_val,
            NULL::VARCHAR AS min_date, NULL::VARCHAR AS max_date,
            NULL::INT     AS date_range_days,
            MIN(LEN({c}))               AS min_length,
            MAX(LEN({c}))               AS max_length,
            AVG(LEN({c})::FLOAT)::FLOAT AS avg_length
        FROM {q}"""

    else:
        return f"""
        SELECT {common},
            NULL::FLOAT AS min_val,  NULL::FLOAT AS max_val,
            NULL::FLOAT AS mean_val, NULL::FLOAT AS stddev_val,
            NULL::VARCHAR AS min_date, NULL::VARCHAR AS max_date,
            NULL::INT     AS date_range_days,
            NULL::INT     AS min_length, NULL::INT   AS max_length,
            NULL::FLOAT   AS avg_length
        FROM {q}"""


def _pct_expr(col_name, schema, table):
    """Pass 2: PERCENTILE_CONT only – no COUNT(DISTINCT)."""
    q   = f'"{schema}"."{table}"'
    c   = f'"{col_name}"'
    lit = col_name.replace("'", "''")
    return f"""
        SELECT
            '{lit}' AS col_name,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {c})::FLOAT AS p25,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY {c})::FLOAT AS median,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {c})::FLOAT AS p75
        FROM {q}"""


def fetch_table_stats(cursor, schema, table, dtypes):
    """2 round-trips per table regardless of column count."""
    if not dtypes:
        return {}

    base_rows = run_query(
        cursor,
        "\nUNION ALL\n".join(
            _base_expr(col, dtype, schema, table)
            for col, dtype in dtypes.items()
        )
    )
    result = {r["col_name"]: dict(r) for r in base_rows}

    num_cols = [col for col, dtype in dtypes.items() if dtype in NUMERIC_TYPES]
    if num_cols:
        pct_rows = run_query(
            cursor,
            "\nUNION ALL\n".join(_pct_expr(col, schema, table) for col in num_cols)
        )
        for r in pct_rows:
            if r["col_name"] in result:
                result[r["col_name"]]["p25"]    = r["p25"]
                result[r["col_name"]]["median"] = r["median"]
                result[r["col_name"]]["p75"]    = r["p75"]

    return result


def _to_varchar(col_expr, dtype):
    """
    Return a SQL expression that safely casts any Redshift type to VARCHAR.
    Redshift rejects BOOLEAN::VARCHAR directly; all others cast fine.
    """
    if dtype == "boolean":
        return f"CASE WHEN {col_expr} THEN 'true' ELSE 'false' END"
    return f"{col_expr}::VARCHAR"


def fetch_top_values_batch(cursor, schema, table, col_dtypes):
    """
    1 round-trip for all qualifying columns via UNION ALL + ROW_NUMBER.
    col_dtypes: dict of {col_name: dtype} for safe per-type casting.
    """
    if not col_dtypes:
        return {}

    q     = f'"{schema}"."{table}"'
    parts = []
    for col_name, dtype in col_dtypes.items():
        c    = f'"{col_name}"'
        lit  = col_name.replace("'", "''")
        val  = _to_varchar(c, dtype)
        parts.append(f"""
        SELECT '{lit}' AS col_name,
               {val} AS value,
               COUNT(*) AS frequency
        FROM {q}
        WHERE {c} IS NOT NULL
        GROUP BY {c}""")

    inner = "\nUNION ALL\n".join(parts)
    sql   = f"""
    SELECT col_name, value, frequency
    FROM (
        SELECT col_name, value, frequency,
               ROW_NUMBER() OVER (PARTITION BY col_name ORDER BY frequency DESC) AS rn
        FROM ({inner}) src
    ) ranked
    WHERE rn <= 10
    ORDER BY col_name, frequency DESC
    """
    rows   = run_query(cursor, sql)
    result = {}
    for r in rows:
        result.setdefault(r["col_name"], []).append(
            {"value": _safe(r["value"]), "frequency": _safe(r["frequency"])}
        )
    return result


# ─────────────────────────────────────────────────────────────────────────────
# OPTIMIZATION 1 – single-table worker (runs in a thread)
# ─────────────────────────────────────────────────────────────────────────────

def profile_table(conn_factory, schema, tname, dtypes, erd_by_name,
                  prefetched_row_count, top_limit):
    """
    Profile one table with automatic retry + reconnect on SSL/connection drops.
    conn_factory is a zero-arg callable that returns a fresh connection.
    """
    conn   = conn_factory()
    cursor = new_cursor(conn)

    # ── Pass 1 + 2: stats with retry ─────────────────────────────────────
    stats_by_col = {}
    for attempt in range(1, DEFAULT_RETRIES + 1):
        try:
            stats_by_col = fetch_table_stats(cursor, schema, tname, dtypes)
            break
        except Exception as e:
            if _is_retryable(e) and attempt < DEFAULT_RETRIES:
                print(f"  ⚠️  Stats failed [{tname}] (attempt {attempt}/{DEFAULT_RETRIES}): {e} — retrying in {RETRY_WAIT}s", flush=True)
                try: cursor.close()
                except Exception: pass
                try: conn.close()
                except Exception: pass
                wait = RETRY_WAIT * (2 ** (attempt - 1))  # exponential backoff
                print(f"     ↳ waiting {wait}s before retry...", flush=True)
                time.sleep(wait)
                conn   = conn_factory()
                cursor = new_cursor(conn)
            else:
                print(f"  ⚠️  Stats failed [{tname}] (gave up after {attempt} attempt(s)): {e}", flush=True)
                stats_by_col = {}
                break

    # ── Pass 3: top values with retry ────────────────────────────────────
    # OPTIMIZATION 3 – skip entirely if all columns are high-cardinality
    low_card_cols = {
        col: dtypes[col]
        for col, s in stats_by_col.items()
        if (s.get("distinct_count") or 0) <= top_limit and col in dtypes
    }
    top_values_by_col = {}
    if low_card_cols:
        for attempt in range(1, DEFAULT_RETRIES + 1):
            try:
                top_values_by_col = fetch_top_values_batch(
                    cursor, schema, tname, low_card_cols)
                break
            except Exception as e:
                if _is_retryable(e) and attempt < DEFAULT_RETRIES:
                    print(f"  ⚠️  Top-values failed [{tname}] (attempt {attempt}/{DEFAULT_RETRIES}): {e} — retrying in {RETRY_WAIT}s", flush=True)
                    try: cursor.close()
                    except Exception: pass
                    try: conn.close()
                    except Exception: pass
                    wait = RETRY_WAIT * (2 ** (attempt - 1))  # exponential backoff
                    print(f"     ↳ waiting {wait}s before retry...", flush=True)
                    time.sleep(wait)
                    conn   = conn_factory()
                    cursor = new_cursor(conn)
                else:
                    print(f"  ⚠️  Top-values failed [{tname}] (gave up after {attempt} attempt(s)): {e}", flush=True)
                    top_values_by_col = {}
                    break

    try: cursor.close()
    except Exception: pass
    try: conn.close()
    except Exception: pass

    col_profiles   = []
    measure_cols   = []
    dimension_cols = []

    # Derive row count from Pass 1: every column's COUNT(*) is identical
    # since it scans the full table — just take the first one available.
    table_row_count = prefetched_row_count  # always None now, kept for API compat
    if table_row_count is None and stats_by_col:
        first_stats = next(iter(stats_by_col.values()))
        table_row_count = first_stats.get("total_rows") or 0

    for col_name, dtype in dtypes.items():
        stats = stats_by_col.get(col_name)
        if stats is None:
            col_profiles.append({"name": col_name, "error": "stats missing"})
            continue

        total = table_row_count or 1

        profile = {
            "name":           col_name,
            "data_type":      dtype.upper(),
            "total_rows":     _safe(table_row_count),
            "non_null_count": _safe(stats.get("non_null_count")),
            "null_count":     _safe(stats.get("null_count")),
            "null_pct":       round((_safe(stats.get("null_count")) or 0) / total * 100, 2),
            "distinct_count": _safe(stats.get("distinct_count")),
        }

        if dtype in NUMERIC_TYPES:
            for k in ("min_val", "max_val", "mean_val", "stddev_val",
                      "p25", "median", "p75"):
                if stats.get(k) is not None:
                    profile[k] = _safe(stats[k])
        elif dtype in DATE_TYPES:
            profile["min_date"]        = _safe(stats.get("min_date"))
            profile["max_date"]        = _safe(stats.get("max_date"))
            profile["date_range_days"] = _safe(stats.get("date_range_days"))
        elif dtype in STRING_TYPES:
            profile["min_length"] = _safe(stats.get("min_length"))
            profile["max_length"] = _safe(stats.get("max_length"))
            profile["avg_length"] = _safe(stats.get("avg_length"))

        profile["top_values"] = top_values_by_col.get(col_name, [])

        lowered = col_name.lower()
        _dim_suffixes = ("_id", "_code", "_flag", "_seq", "_order",
                         "_year", "_month", "_day", "_num", "_no")
        _is_dim_numeric = (
            lowered == "id"
            or any(lowered.endswith(s) for s in _dim_suffixes)
        )
        if dtype in NUMERIC_TYPES and not _is_dim_numeric:
            measure_cols.append(col_name)
        else:
            dimension_cols.append(col_name)

        col_profiles.append(profile)

    null_pcts = [
        c["null_pct"] for c in col_profiles
        if isinstance(c.get("null_pct"), (int, float))
    ]
    quality = {
        "avg_null_pct":      round(sum(null_pcts) / len(null_pcts), 2) if null_pcts else None,
        "max_null_pct":      max(null_pcts) if null_pcts else None,
        "high_null_columns": [
            c["name"] for c in col_profiles
            if isinstance(c.get("null_pct"), (int, float)) and c["null_pct"] > 50
        ],
    }

    erd_cols = erd_by_name.get(tname, {}).get("columns", [])
    pk_cols  = [c["name"] for c in erd_cols if c.get("primary_key")]
    has_pk   = bool(pk_cols)

    modeling_readiness = (
        "high"   if has_pk and quality["avg_null_pct"] is not None and quality["avg_null_pct"] < 10  else
        "medium" if has_pk and quality["avg_null_pct"] is not None and quality["avg_null_pct"] < 30  else
        "low"
    )

    return {
        "name":               tname,
        "schema":             schema,
        "row_count":          table_row_count,
        "structure":          {"column_count": len(col_profiles)},
        "quality":            quality,
        "keys":               {"primary_keys": pk_cols},
        "grain": (
            "unique by primary key columns " + ", ".join(pk_cols)
            if has_pk else "no explicit primary key; grain unclear"
        ),
        "measures":           measure_cols,
        "dimensions":         dimension_cols,
        "business_meaning":   None,
        "modeling_readiness": modeling_readiness,
        "columns":            col_profiles,
    }


# ─────────────────────────────────────────────────────────────────────────────
# EDA – parallel orchestrator
# ─────────────────────────────────────────────────────────────────────────────

def build_eda(conn, conn_factory, schema, tables, erd_tables,
              workers=DEFAULT_WORKERS, top_limit=DEFAULT_TOP_LIMIT):
    dtype_map   = {}
    erd_by_name = {}
    for t in erd_tables:
        erd_by_name[t["name"]] = t
        dtype_map[t["name"]]   = {c["name"]: c["type"].lower() for c in t["columns"]}

    total   = len(tables)
    results = {}

    print(f"  🚀  {total} tables × {workers} parallel workers\n", flush=True)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(
                profile_table, conn_factory, schema, tname,
                dtype_map.get(tname, {}), erd_by_name,
                None, top_limit,   # row count derived from Pass 1 COUNT(*)
            ): tname
            for tname in tables
        }
        done = 0
        t_start = time.time()
        for future in as_completed(futures):
            tname = futures[future]
            done += 1
            elapsed = time.time() - t_start
            try:
                results[tname] = future.result()
                print(f"  ✅  [{done:>3}/{total}] {tname} ({elapsed:.1f}s elapsed)", flush=True)
            except Exception as e:
                print(f"  ❌  [{done:>3}/{total}] {tname} – {e}", flush=True)
                results[tname] = {"name": tname, "schema": schema, "error": str(e)}

    failed = [t for t in tables if isinstance(results.get(t), dict) and "error" in results.get(t, {})]
    if failed:
        print(f"\n  ⚠️  {len(failed)} table(s) failed and will have partial/no stats:", flush=True)
        for t in failed:
            print(f"      - {t}: {results[t].get('error', 'unknown')}", flush=True)
    return [results[t] for t in tables if t in results]


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def get_all_tables(cursor, schema):
    rows = run_query(cursor, """
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = %s AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """, (schema,))
    return [r["table_name"] for r in rows]


def main():
    parser = argparse.ArgumentParser(
        description="Redshift ERD + EDA → JSON (v3 optimized)")
    parser.add_argument("--config", default=DEFAULT_CONFIG_FILE)
    args = parser.parse_args()

    cfg       = load_config(args.config)
    host      = cfg["host"]
    port      = int(cfg.get("port", 5439))
    db        = cfg["db"]
    user      = cfg["user"]
    password  = cfg["password"]
    schema    = cfg.get("schema", "public")
    # Dynamic worker calculation: W = min(sqrt(N), WLM_slots, config_max)
    # "parallel_workers" in config acts as a hard ceiling, not a fixed value.
    # Set it only if you want to cap workers below the calculated optimum.
    cfg_max_workers = cfg.get("parallel_workers")   # None = no cap
    top_limit = int(cfg.get("top_values_limit", DEFAULT_TOP_LIMIT))
    # allow overriding retry constants from config
    global DEFAULT_RETRIES, RETRY_WAIT
    DEFAULT_RETRIES = int(cfg.get("max_retries",        DEFAULT_RETRIES))
    RETRY_WAIT      = int(cfg.get("retry_wait_secs",    RETRY_WAIT))
    out_dir   = cfg.get("output_dir", ".")
    os.makedirs(out_dir, exist_ok=True)

    print(f"🔌  Connecting to {host}:{port}/{db} ...")
    conn   = _connect(host, port, db, user, password)
    cursor = new_cursor(conn)

    tables = cfg.get("tables") or get_all_tables(cursor, schema)
    print(f"📋  Tables ({len(tables)}): {', '.join(tables)}\n")

    # ── Dynamic worker selection ───────────────────────────────────────────
    wlm_slots = fetch_wlm_slots(cursor)
    workers   = calc_optimal_workers(len(tables), wlm_slots, cfg_max_workers)
    print(f"⚙️   Workers: {workers} (sqrt({len(tables)})={__import__('math').isqrt(len(tables))}, WLM slots={wlm_slots}, cap={cfg_max_workers or 'none'})")

    t0 = time.time()

    print("🗺️   Building ERD ...")
    erd_tables = build_erd(cursor, schema, tables)
    print(f"     done in {time.time()-t0:.1f}s\n")

    t1 = time.time()
    print("📈  Building EDA ...")
    conn_factory = lambda: _connect(host, port, db, user, password)
    eda_tables = build_eda(conn, conn_factory, schema, tables, erd_tables,
                           workers=workers, top_limit=top_limit)
    print(f"\n     done in {time.time()-t1:.1f}s")

    cursor.close()
    conn.close()

    metadata = {
        "source":       "Amazon Redshift",
        "host":         host,
        "database":     db,
        "schema":       schema,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "table_count":  len(tables),
    }

    for fname, payload, desc in [
        ("erd.json", {
            "metadata":    metadata,
            "description": "Entity-Relationship Diagram – schemas and FK relationships",
            "tables":      erd_tables,
        }, "ERD"),
        ("eda.json", {
            "metadata":    metadata,
            "description": "Exploratory Data Analysis – column-level statistics",
            "tables":      eda_tables,
        }, "EDA"),
    ]:
        path = os.path.join(out_dir, fname)
        tmp_path = path + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, default=str)
        os.replace(tmp_path, path)   # atomic on all major OS
        print(f"   ✅  {desc} → {path}")

    print(f"\n🎉  Total wall-clock time: {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
