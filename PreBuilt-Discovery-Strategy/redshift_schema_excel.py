"""
Redshift Schema → Target-Schema CSVs
====================================
Connects to Redshift and produces two CSV files:

  redshift_schema_columns.csv   – every table/column pair (Table | Column)
  redshift_schema_dimatrix.csv  – relationship matrix: fact tables (rows)
                                  vs dim tables (cols), cell = 1 where a
                                  foreign-key or inferred join exists

Usage:
    python redshift_schema_excel.py [--config config.json]

Config keys (same file as the ERD/EDA script):
    host, db, user, password  (required)
    port                      (default 5439)
    schema                    (default "public")
    tables                    list of names; omit/[] = all tables
    output_dir                where to write the CSVs (default: cwd)
"""

import argparse
import csv
import json
import os
import sys

try:
    import redshift_connector as _driver
    def _connect(cfg):
        conn = _driver.connect(
            host=cfg["host"],
            port=int(cfg.get("port", 5439)),
            database=cfg["db"],
            user=cfg["user"],
            password=cfg["password"],
        )
        conn.autocommit = True
        return conn
except ImportError:
    try:
        import psycopg2 as _driver
        def _connect(cfg):
            conn = _driver.connect(
                host=cfg["host"],
                port=int(cfg.get("port", 5439)),
                dbname=cfg["db"],
                user=cfg["user"],
                password=cfg["password"],
            )
            conn.autocommit = True
            return conn
    except ImportError:
        sys.exit("Install redshift-connector or psycopg2-binary.")

def run_query(cursor, sql, params=None):
    cursor.execute(sql, params or ())
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def load_config(path):
    with open(path, encoding="utf-8") as f:
        cfg = json.load(f)
    missing = [k for k in ("host", "db", "user", "password") if not cfg.get(k)]
    if missing:
        sys.exit(f"Missing config keys: {', '.join(missing)}")
    return cfg


def _is_duplicate_table(name):
    """
    Return True if the table looks like a versioned/duplicate copy.
    Filters patterns like:
        table_1, table_2          (numeric suffix)
        table_v2, table_v3        (_v + digit)
        table_old, table_bak      (common backup suffixes)
        table_backup, table_copy
        table_20240101            (date-stamp suffix)
        table_tmp, table_temp
        table_deleted             (_deleted suffix)
    """
    import re
    lower = name.lower()
    patterns = [
        r'_\d+$',              # ends in _1, _2, _123
        r'_v\d+$',             # ends in _v2, _v10
        r'_(old|bak|backup|copy|temp|tmp|archive|test|dev|staging)$',
        r'_\d{6,8}$',          # ends in date stamp like _20240101
        r'_deleted$',           # soft-delete shadow tables
    ]
    return any(re.search(p, lower) for p in patterns)


def get_all_tables(cursor, schema):
    """
    Fetch all base tables in the schema, filtering out versioned duplicates.
    Auto-discovers the correct schema name (case-insensitive match) if the
    exact name in config does not return any tables.
    """
    import re

    # ── Step 1: discover available schemas so we can warn/fix mismatches ──
    all_schemas = run_query(cursor, """
        SELECT DISTINCT table_schema
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'
          AND table_schema NOT IN ('information_schema','pg_catalog','pg_internal')
        ORDER BY table_schema
    """)
    available = [r["table_schema"] for r in all_schemas]
    print(f"  📂  Available schemas: {available}", flush=True)

    # Case-insensitive match if exact name not found
    resolved_schema = schema
    if schema not in available:
        matches = [s for s in available if s.lower() == schema.lower()]
        if matches:
            resolved_schema = matches[0]
            print(f"  ⚠️   Schema '{schema}' not found exactly — using '{resolved_schema}'", flush=True)
        elif available:
            resolved_schema = available[0]
            print(f"  ⚠️   Schema '{schema}' not found — falling back to '{resolved_schema}'", flush=True)

    # ── Step 2: fetch tables from resolved schema ──────────────────────────
    rows = run_query(cursor, """
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = %s AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """, (resolved_schema,))

    all_tables = [r["table_name"] for r in rows]
    print(f"  📋  Raw table count in '{resolved_schema}': {len(all_tables)}", flush=True)

    if not all_tables:
        print("  ❌  No tables found! Check your schema name and permissions.", flush=True)
        return []

    # ── Step 3: filter duplicates / versioned tables ───────────────────────
    seen_canonical = set()
    filtered = []

    for t in all_tables:
        if _is_duplicate_table(t):
            print(f"  ⏭️   Skipping versioned: {t}", flush=True)
            continue
        canonical = re.sub(r'_\d+$', '', t.lower())
        if canonical in seen_canonical:
            print(f"  ⏭️   Skipping duplicate: {t}", flush=True)
            continue
        seen_canonical.add(canonical)
        filtered.append(t)

    print(f"  ✅  {len(filtered)} tables kept ({len(all_tables) - len(filtered)} filtered out)", flush=True)
    return filtered


def fetch_columns(cursor, schema, tables):
    """Returns list of (table_name, column_name) ordered by table, ordinal.
    Tries exact schema match first, then case-insensitive fallback."""
    if not tables:
        return []
    placeholders = ", ".join(["%s"] * len(tables))

    # Try exact schema first
    rows = run_query(cursor, f"""
        SELECT table_name, column_name
        FROM   information_schema.columns
        WHERE  table_schema = %s
          AND  table_name   IN ({placeholders})
        ORDER  BY table_name, ordinal_position
    """, (schema, *tables))

    # If nothing returned, try LOWER() comparison
    if not rows:
        rows = run_query(cursor, f"""
            SELECT table_name, column_name
            FROM   information_schema.columns
            WHERE  LOWER(table_schema) = LOWER(%s)
              AND  table_name IN ({placeholders})
            ORDER  BY table_name, ordinal_position
        """, (schema, *tables))

    return [(r["table_name"], r["column_name"]) for r in rows]


def fetch_relationships(cursor, schema, tables):
    """
    Returns list of (fk_table, fk_column, pk_table, pk_column).
    Uses information_schema FK constraints.
    """
    placeholders = ", ".join(["%s"] * len(tables))
    rows = run_query(cursor, f"""
        SELECT
            kcu.table_name    AS fk_table,
            kcu.column_name   AS fk_column,
            ccu.table_name    AS pk_table,
            ccu.column_name   AS pk_column
        FROM information_schema.key_column_usage kcu
        JOIN information_schema.table_constraints tc
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema    = kcu.table_schema
        JOIN information_schema.constraint_column_usage ccu
          ON ccu.constraint_name = kcu.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND kcu.table_schema   = %s
          AND kcu.table_name     IN ({placeholders})
        ORDER BY kcu.table_name, kcu.column_name
    """, (schema, *tables))
    return [(r["fk_table"], r["fk_column"], r["pk_table"], r["pk_column"])
            for r in rows]


def _classify_tables(tables):
    """
    Classify tables into two groups for the matrix axes.

    Priority:
    1. If any table starts with 'fact' → use fact/dim split.
    2. If tables share a common prefix (e.g. ams_rem_accounting_) →
       treat the last segment as the logical name; use shared-column
       heuristic to find which tables act as "parent" (fewer FK cols)
       vs "child" (more FK cols referencing others).
    3. Fallback: split alphabetically into two halves.
    """
    lower = [t.lower() for t in tables]

    # Case 1: conventional fact/dim naming
    facts = [t for t in tables if t.lower().startswith("fact")]
    dims  = [t for t in tables if t.lower().startswith("dim")]
    if facts and dims:
        return sorted(facts), sorted(dims)

    # Case 2: common prefix pattern (e.g. ams_rem_<module>_<entity>)
    # Split each table name by "_" and find the longest common prefix
    parts_list = [t.split("_") for t in tables]
    # Find common prefix length
    prefix_len = 0
    if parts_list:
        min_len = min(len(p) for p in parts_list)
        for i in range(min_len):
            if len(set(p[i] for p in parts_list)) == 1:
                prefix_len = i + 1
            else:
                break

    # Group by the segment immediately after the common prefix (the "module")
    from collections import defaultdict
    module_groups = defaultdict(list)
    for t, parts in zip(tables, parts_list):
        module = parts[prefix_len] if prefix_len < len(parts) else "other"
        module_groups[module].append(t)

    # Tables with more columns that look like FKs (ending _id/_key) are
    # "fact-like"; tables with fewer are "dim-like"
    col_by_table = {}
    # We don't have columns here, so use table count per module as proxy:
    # modules with many tables are dimension-like (lookup tables),
    # modules with few tables and longer names are fact-like.
    # Best proxy without column data: use all tables as both axes
    # (produces a full square matrix showing cross-module joins).
    row_tables = sorted(tables)
    col_tables = sorted(tables)
    return row_tables, col_tables


def infer_matrix_joins(columns, relationships, tables):
    """
    Build join matrix.

    Strategy:
    1. FK constraints (most accurate).
    2. Shared column name heuristic: if table A has a column whose name
       exactly matches a column in table B (case-insensitive), they join.
       Common join columns: *_id, *_key, *_code columns shared across tables.
    3. For ams_rem_ style schemas with no FKs enforced: detect joins by
       finding columns in one table whose name matches <other_table_suffix>_key
       or <other_table_suffix>_id after stripping the common prefix.
    """
    # Build column lookup
    col_by_table = {}
    for tname, cname in columns:
        col_by_table.setdefault(tname, set()).add(cname.lower())

    # ── Step 1: FK-based joins ────────────────────────────────────────────
    fk_joins = set()
    for fk_table, _, pk_table, _ in relationships:
        if fk_table in col_by_table and pk_table in col_by_table:
            fk_joins.add((fk_table, pk_table))

    # ── Step 2: Shared column name joins ─────────────────────────────────
    # Two tables join if they share at least one column name that looks
    # like a key (ends in _id, _key, _code, or _num)
    KEY_SUFFIXES = ("_id", "_key", "_code", "_num")
    shared_joins = set()
    table_list = list(col_by_table.keys())
    for i, t1 in enumerate(table_list):
        keys1 = {c for c in col_by_table[t1]
                 if any(c.endswith(s) for s in KEY_SUFFIXES)}
        for t2 in table_list[i+1:]:
            keys2 = {c for c in col_by_table[t2]
                     if any(c.endswith(s) for s in KEY_SUFFIXES)}
            if keys1 & keys2:   # non-empty intersection
                shared_joins.add((t1, t2))
                shared_joins.add((t2, t1))

    # ── Step 3: Suffix heuristic for ams_rem_ style names ────────────────
    # Strip common prefix, then check if col name starts with another
    # table's suffix
    parts_list  = [t.split("_") for t in tables]
    prefix_len  = 0
    if parts_list:
        min_len = min(len(p) for p in parts_list)
        for i in range(min_len):
            if len(set(p[i] for p in parts_list)) == 1:
                prefix_len = i + 1
            else:
                break

    # logical suffix for each table (everything after common prefix)
    suffix_map = {}   # table → logical suffix (e.g. "accounting_orders")
    for t, parts in zip(tables, parts_list):
        suffix_map[t] = "_".join(parts[prefix_len:]).lower()

    suffix_joins = set()
    for t1 in tables:
        for t2 in tables:
            if t1 == t2:
                continue
            sfx = suffix_map[t2]
            for col in col_by_table.get(t1, set()):
                if col == f"{sfx}_id" or col == f"{sfx}_key" or col.startswith(sfx + "_"):
                    suffix_joins.add((t1, t2))
                    break

    all_joins = fk_joins | shared_joins | suffix_joins

    # ── Classify into row/col axes ────────────────────────────────────────
    # Tables that appear more as the SOURCE of joins = fact-like (rows)
    # Tables that appear more as the TARGET of joins = dim-like (cols)
    join_as_source = {}  # how many times t appears as t1 (has FK to others)
    join_as_target = {}  # how many times t appears as t2 (is referenced)
    for t1, t2 in all_joins:
        join_as_source[t1] = join_as_source.get(t1, 0) + 1
        join_as_target[t2] = join_as_target.get(t2, 0) + 1

    # Score: positive = more fact-like, negative = more dim-like
    scores = {}
    for t in tables:
        scores[t] = join_as_source.get(t, 0) - join_as_target.get(t, 0)

    median_score = sorted(scores.values())[len(scores)//2] if scores else 0

    # Prefer explicit fact/dim naming if present
    has_fact = any(t.lower().startswith("fact") for t in tables)
    has_dim  = any(t.lower().startswith("dim") for t in tables)

    if has_fact or has_dim:
        row_tables = sorted([t for t in tables if t.lower().startswith("fact")]
                            or [t for t in tables if scores.get(t,0) >= median_score])
        col_tables = sorted([t for t in tables if t.lower().startswith("dim")]
                            or [t for t in tables if scores.get(t,0) < median_score])
    else:
        # Use join score to split: higher score = more connections out = row
        row_tables = sorted([t for t in tables if scores.get(t, 0) >= median_score])
        col_tables = sorted(tables)   # all tables as columns (full matrix)

    # Ensure no empty axes
    if not row_tables:
        row_tables = sorted(tables)
    if not col_tables:
        col_tables = sorted(tables)

    return row_tables, col_tables, all_joins


def write_columns_csv(path, columns):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Table", "Column"])
        for tname, cname in columns:
            writer.writerow([tname, cname])


def write_dimatrix_csv(path, fact_tables, dim_tables, all_joins):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        header = ["Table"] + list(dim_tables)
        writer.writerow(header)
        for fact in fact_tables:
            row = [fact]
            for dim in dim_tables:
                row.append("1" if (fact, dim) in all_joins else "")
            writer.writerow(row)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Redshift Schema → target-schema CSVs")
    parser.add_argument("--config", default="config.json")
    args = parser.parse_args()

    cfg_path = args.config
    if not os.path.isabs(cfg_path):
        cfg_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                cfg_path)
    if not os.path.exists(cfg_path):
        sys.exit(f"Config not found: {cfg_path}")

    cfg     = load_config(cfg_path)
    schema  = cfg.get("schema", "public")
    out_dir = cfg.get("output_dir", ".")
    os.makedirs(out_dir, exist_ok=True)

    print(f"🔌  Connecting to {cfg['host']}:{cfg.get('port',5439)}/{cfg['db']} ...")
    print(f"📂  Schema: {schema}")
    conn   = _connect(cfg)
    cursor = conn.cursor()

    # Always fetch tables directly from schema — never from config
    tables = get_all_tables(cursor, schema)
    print(f"📋  Tables found: {len(tables)}", flush=True)
    if not tables:
        print("  ❌  No tables found — check schema name in config.json and user permissions.", flush=True)
        sys.exit(1)

    print("🔍  Fetching columns ...")
    columns = fetch_columns(cursor, schema, tables)
    print(f"    {len(columns)} column rows", flush=True)
    if not columns:
        print("  ❌  No columns returned — possible schema name mismatch or missing SELECT permission.", flush=True)
        sys.exit(1)

    print("🔗  Fetching relationships ...")
    relationships = fetch_relationships(cursor, schema, tables)
    print(f"    {len(relationships)} FK relationships found")

    print("🧮  Building join matrix ...")
    fact_tables, dim_tables, all_joins = infer_matrix_joins(
        columns, relationships, tables)
    print(f"    {len(fact_tables)} fact tables × {len(dim_tables)} dim tables")
    print(f"    {len(all_joins)} joins detected")

    cursor.close()
    conn.close()

    print("📊  Writing target-schema CSVs ...")
    cols_path = os.path.join(out_dir, "redshift_schema_columns.csv")
    dimx_path = os.path.join(out_dir, "redshift_schema_dimatrix.csv")

    write_columns_csv(cols_path, columns)
    write_dimatrix_csv(dimx_path, fact_tables, dim_tables, all_joins)

    print(f"\n✅  Saved → {cols_path}")
    print(f"✅  Saved → {dimx_path}")
    print(f"    Columns CSV   : {len(columns)} rows")
    print(f"    DiMatrix CSV  : {len(fact_tables)} facts × {len(dim_tables)} dims, {len(all_joins)} joins")


if __name__ == "__main__":
    main()