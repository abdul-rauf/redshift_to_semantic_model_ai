# Step 1 – Redshift Metadata Discovery (Read-Only)

**Type:** Automated  
**Prerequisite:** Step 0 completed  
**Variables:**
- `REDSHIFT_CLUSTER`
- `REDSHIFT_DATABASE`
- `REDSHIFT_SCHEMA`
- `REDSHIFT_TABLES`

---

## Objective

Use the Redshift MCP server to **read and lightly profile all columns for every table listed in `REDSHIFT_TABLES`** using a **single `SVV_ALL_COLUMNS` metadata query strategy (Option A)**.  
No modifications are made to Redshift in this step, and you **must not** fall back to per-table `list_columns` unless a future revision of this document explicitly reintroduces that option.

Follow global rules from `00-agent-prompt.md` (read-only, tool-only, minimal data, no guessing).

## Execution Modes

> **Note on buffer/output limits:** When `${REDSHIFT_TABLES}` is large, MCP/tool buffer limits may prevent returning column metadata for every table in a single run. In that case, run Step 1 **in multiple passes over subsets of `REDSHIFT_TABLES`** by splitting the `IN (...)` list in the metadata query and merging the results.

- **Option A – Single metadata query (default)**  
  Prefer a **single `execute_query` call** against `SVV_ALL_COLUMNS` that pulls column metadata for **all tables** in `${REDSHIFT_TABLES}` (or a few passes if needed), and then transform that result into the structured summary used by later steps.
- **Option B – Focused subset (fallback)**  
  If requested by the user, restrict profiling to a curated subset of tables by limiting the `IN (...)` list, and clearly document which ones were included/excluded.

All instructions below assume **Option A** unless explicitly overridden.

### Metadata query tracking

Instead of looping over tables with `list_columns`, this step issues **one read-only metadata query** over `SVV_ALL_COLUMNS` (optionally in a few passes if needed).  
You should:

- **Dynamically build the `IN (...)` list** from `REDSHIFT_TABLES` (no hard-coded table names).
- **Log progress per pass** (for example: `scanning N tables via SVV_ALL_COLUMNS execute_query`) so the user can see which batch is currently running.
- **Retry once on transient session errors** (for example: “Session is not available”); if the retry fails, record the failed batch and move on to the next one.

## Actions

Before calling any Redshift MCP tools in this step, remember that they **require explicit arguments**:
- `execute_query` requires: `cluster_identifier`, `database_name`, `sql`.
- `list_tables` requires explicit `cluster_identifier`, `table_database_name`, and `table_schema_name` arguments.

1. **Discover and validate tables**
   - **Do not** call `list_clusters`, `list_databases`, or `list_schemas` in this step; they are unnecessarily costly and the environment variables are already authoritative.
   - Assume `REDSHIFT_CLUSTER`, `REDSHIFT_DATABASE`, and `REDSHIFT_SCHEMA` are correct and available from the environment.
   - Use `list_tables` **only if you need to validate existence or type** for a specific table, and when you do, **pass arguments explicitly** from:
     - `cluster_identifier` = `REDSHIFT_CLUSTER`
     - `table_database_name` = `REDSHIFT_DATABASE`
     - `table_schema_name` = `REDSHIFT_SCHEMA`
   - Confirm that **every table** in `${REDSHIFT_TABLES}` exists in `REDSHIFT_SCHEMA` and is of type `TABLE`, using `list_tables` **only as needed** rather than as a full scan.

2. **Read column metadata for all tables (Option A)**
   - Use **one or a few `execute_query` calls** to select metadata from `SVV_ALL_COLUMNS` instead of per-table `list_columns`.
   - For each pass, call `execute_query` with:
     - `cluster_identifier` = `REDSHIFT_CLUSTER`
     - `database_name` = `REDSHIFT_DATABASE`
     - `sql` = a single read-only statement similar to:
       -  
       - `SELECT table_name, column_name, is_nullable, data_type, character_maximum_length, numeric_precision, numeric_scale, ordinal_position FROM SVV_ALL_COLUMNS WHERE schema_name = '<REDSHIFT_SCHEMA>' AND table_name IN (<tables from REDSHIFT_TABLES for this pass>) ORDER BY table_name, ordinal_position;`
   - From each result row, use:
     - `table_name`
     - `column_name`
     - `data_type`
     - `is_nullable`
     - `ordinal_position`
     - `character_maximum_length`
     - `numeric_precision`
     - `numeric_scale`

3. **Lightweight profiling (optional, bounded)**
   - Only if needed to distinguish keys or roles, run **small, targeted queries** with `execute_query`, passing:
     - `cluster_identifier` = `REDSHIFT_CLUSTER`
     - `database_name` = `REDSHIFT_DATABASE`
     - `sql` = profiling SQL text (read-only, with `LIMIT`, no `SELECT *`)
   - Example patterns:
     - `SELECT COUNT(*) AS row_count, COUNT(DISTINCT some_column) AS distinct_count FROM schema.table;`
     - `SELECT some_column FROM schema.table GROUP BY 1 ORDER BY 1 LIMIT 50;`
   - Do **not** scan whole tables or run unbounded profiling queries.

4. **Assemble structured column profiles**
   - For each table, build an in-memory structured summary like:
     - `[{ table: "schema.table_name", columns: [{ column_name, data_type, is_nullable, character_maximum_length, numeric_precision, numeric_scale, isPkCandidate, isFkCandidate }] }]`
   - Mark `isPkCandidate` / `isFkCandidate` based on:
     - Names (`id`, `<table>_id`, `<otherTable>_id`, `<table>_key`, `<otherTable>_key`)
     - (Optional) Uniqueness/cardinality checks from Step 3
     - Participation in obvious joins across tables (when known in later steps).

## Output

Return a **compact structured summary** of each table's column profiles (suitable for direct consumption by Step 2), not verbose prose.

- Write the full JSON object to `docs/output.md` under the top-level key `step1_redshift_metadata`.
- Later steps must read Step 1 results from `docs/output.md` instead of re-running metadata discovery.

### Agent-facing JSON structure (for all tables in `REDSHIFT_TABLES`)

- **Shape**
  - `tables`: `[{ table: string, columns: ColumnProfile[] }]`
  - `ColumnProfile`:  
    - `table`: fully qualified table name, for example: `"source.ams_rem_accounting_accounts"`
    - `column_name`
    - `data_type`
    - `is_nullable`
    - `character_maximum_length` (optional)
    - `numeric_precision` (optional)
    - `numeric_scale` (optional)
    - `isPkCandidate` (boolean)
    - `isFkCandidate` (boolean)

- **Example (truncated)**

```json
{
  "tables": [
    {
      "table": "source.ams_rem_accounting_accounts",
      "columns": [
        {
          "column_name": "ams_rem_accounting_accounts_key_sk",
          "data_type": "bigint",
          "is_nullable": "YES",
          "character_maximum_length": null,
          "numeric_precision": 64,
          "numeric_scale": 0,
          "isPkCandidate": true,
          "isFkCandidate": false
        }
        // ... more columns ...
      ]
    }
    // ... more tables ...
  ]
}
```

---

## Completion Criteria

- **Coverage**: All tables in `${REDSHIFT_TABLES}` have column profiles based on **actual Redshift metadata** (no guessed columns).
- **Metadata**: Column metadata is captured (name, type, nullability, and basic uniqueness/cardinality hints).
- **Structured output**: A single JSON-like `tables` object (as defined above) is available for Step 2 and, if desired, persisted or linked from this `.md` file.
- **User sign-off**: Short human-readable summary provided to user and approval received to proceed to Step 2.
