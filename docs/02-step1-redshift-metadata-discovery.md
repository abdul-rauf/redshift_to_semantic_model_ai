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

Use the Redshift MCP server to **read and lightly profile** all columns from the specified raw tables. No modifications are made to Redshift in this step.

Follow global rules from `00-agent-prompt.md` (read-only, tool-only, minimal data, no guessing).

## Actions

1. **Discover and validate tables**
   - Use Redshift MCP tools (`list_clusters`, `list_databases`, `list_schemas`, `list_tables`) only as needed to:
     - Confirm access to `REDSHIFT_CLUSTER`, `REDSHIFT_DATABASE`, and `REDSHIFT_SCHEMA`.
     - Confirm that all tables in `${REDSHIFT_TABLES}` exist.

2. **Read column metadata**
   - For each table in `${REDSHIFT_TABLES}`, call `list_columns` to retrieve:
     - Column name
     - Data type
     - Nullability
     - Ordinal position
     - Length/precision/scale

3. **Lightweight profiling (optional, bounded)**
   - Only if needed to distinguish keys or roles, run **small, targeted queries** with `execute_query`:
     - Use `COUNT(*)` and `COUNT(DISTINCT ...)` to estimate uniqueness for candidate key columns.
     - Use `LIMIT` and column subsets (never `SELECT *`) to view a few sample values when naming is ambiguous.
   - Do **not** scan whole tables or run unbounded profiling queries.

4. **Assemble structured column profiles**
   - For each table, build an in-memory structured summary like:
     - `[{ table: "raw.customers", columns: [{ name, dataType, isNullable, maxLength, numericPrecision, numericScale, isPkCandidate, isFkCandidate }] }]`
   - Mark `isPkCandidate` / `isFkCandidate` based on:
     - Names (`id`, `<table>_id`, `<otherTable>_id`)
     - Uniqueness/cardinality checks
     - Participation in obvious joins across tables.

## Output

Return a **compact structured summary** of each table's column profiles (suitable for direct consumption by Step 2), not verbose prose. Include for each column at least:

- `table`, `column_name`, `data_type`, `is_nullable`
- optional: `character_maximum_length`, `numeric_precision`, `numeric_scale`
- inferred flags: `isPkCandidate`, `isFkCandidate`

---

## Completion Criteria

- All tables in `${REDSHIFT_TABLES}` have column profiles based on **actual Redshift metadata** (no guessed columns)
- Column metadata is captured (name, type, nullability, and basic uniqueness/cardinality hints)
- Structured summary is returned for use by Step 2
- Short human-readable summary provided to user and approval received to proceed to Step 2
