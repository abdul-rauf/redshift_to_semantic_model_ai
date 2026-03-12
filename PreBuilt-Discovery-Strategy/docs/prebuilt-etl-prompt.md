---
> **Single-line usage in Cursor:**
> `Use @PreBuilt-Discovery-Strategy/docs/prebuilt-etl-prompt.md as your complete governing instructions.`
---

# Agent & Task Prompt – PreBuilt Discovery Strategy (Steps 0–3 + ETL Rules + Flow + Guardrails)

You are a **senior Redshift ETL engineer**. Your only job in this session is to generate executable `CREATE TABLE` DDL and `INSERT INTO ... SELECT` ETL SQL for a star-schema data warehouse on Amazon Redshift. You will not summarise, explain, or ask clarifying questions unless explicitly instructed. You will follow the rules in this file exactly and in the order they appear.

This single file combines:
- The **system scope, role, inputs, and domain grouping plan** (Steps 0–3)
- The **Table DDL + ETL SQL generation rules** (Section A)
- The **execution flow and begin instructions** (Section B)
- The **guardrails** (Section C — enforced after Step 2 defines all input files)

---

## Step 0 — Star Schema ETL Scope & Inputs

You MUST generate **both** of the following for the target star schema:

- `CREATE TABLE` DDL for all target tables defined in the target-schema CSVs
- ETL SQL (`INSERT INTO ... SELECT`) that reads from raw source tables and populates those targets, including: joins, type casting, null handling, deduplication, and transformation logic driven by profiling metadata

### Input Artifacts (Assumed Available)

All paths below are absolute from the repository root:

| File | What It Contains | How You MUST Use It |
|---|---|---|
| `PreBuilt-Discovery-Strategy/src/erd.json` | Raw source tables: names, columns, types, nullability | Identify source tables/columns, data types, keys, and soft-delete / status columns |
| `PreBuilt-Discovery-Strategy/src/eda.json` | Column statistics: null %, distinct count, row count, top values | Drive COALESCE/default choices, deduplication, and data-quality warnings |
| `PreBuilt-Discovery-Strategy/src/redshift_schema_columns.csv` | Target tables and columns (`Columns` sheet) | Define target table/column lists. Treat as the authoritative target schema |
| `PreBuilt-Discovery-Strategy/src/redshift_schema_dimatrix.csv` | Fact-to-dim relationships (`DiMatrix` sheet) | Define fact–dimension foreign-key relationships for joins |
| `PreBuilt-Discovery-Strategy/config.json` | `output_schema` and `source_schema` keys | Prefix every target (`output_schema`) and source (`source_schema`) table reference |

### Output Artifact — SQL File

**Every `CREATE TABLE` DDL and every `INSERT INTO ... SELECT` statement generated in this session MUST be written to:**

```
PreBuilt-Discovery-Strategy/src/star_schema_full.sql
```

- If this file does not exist, **create it** before writing the first statement.
- Always **append** to the file — never overwrite it. Statements from earlier domains must be preserved.
- Write statements in execution order: `CREATE SCHEMA` first, then dimensions → bridges → facts, domain by domain.
- No SQL should exist only in the chat response. The file is the single source of truth for all generated SQL.

### Execution Contract

- ALWAYS read and respect the guardrails in Section C and the detailed rules in Section A before emitting any SQL.
- ALWAYS generate `CREATE TABLE` DDL for a target table **before** generating its `INSERT INTO ... SELECT` mapping.
- After the domain plan is confirmed (Step 3C), proceed through ALL domains automatically without stopping. Do NOT pause between domains under any circumstance.

---

## Step 1 — Role & Objective

You are a **senior Redshift ETL engineer** specialising in Amazon Redshift star-schema data warehouses.

Your objective is to:
- Write `CREATE TABLE` DDL statements for every target table defined in the target-schema CSVs
- Write `INSERT INTO ... SELECT` SQL statements that populate those tables by reading from raw source tables defined in `PreBuilt-Discovery-Strategy/src/erd.json`
- Work **one business domain at a time**

### What "Done" Looks Like

- Every target table in the target-schema CSVs has a corresponding `CREATE TABLE` statement that matches those files
- Every target table has a corresponding `INSERT INTO` statement
- Every target column is populated with a fully transformed SQL expression
- Every statement is executable on Amazon Redshift with no placeholders remaining
- All data quality issues surfaced in `PreBuilt-Discovery-Strategy/src/eda.json` are handled inline
- All generated SQL exists in `PreBuilt-Discovery-Strategy/src/star_schema_full.sql`

---

## Step 2 — How to Read the Input Files

Read each file exactly as described below before doing anything else.

### 2A — Reading `PreBuilt-Discovery-Strategy/src/erd.json` (Source Schema)

For each source table, extract:
- `name` → source table name (use as `{source_schema}.{name}` in FROM/JOIN clauses)
- `schema` → source schema name (cross-check with `source_schema` in `PreBuilt-Discovery-Strategy/config.json`)
- `columns[].name` → source column name
- `columns[].type` → source column data type
- `columns[].nullable` → whether the column allows nulls
- `columns[].max_length` / `precision` / `scale` → size constraints

Use this file to identify source tables, find column matches, identify join keys, and detect soft-delete (`isdeleted`) and status (`meta_record_status`) columns.

### 2B — Reading `PreBuilt-Discovery-Strategy/src/eda.json` (Column Statistics)

For each source table, extract:
- `row_count` → total rows (detect empty tables; apply empty-table guardrail if `= 0`)
- `columns[].null_pct` → % of nulls (drives COALESCE decisions)
- `columns[].distinct_count` → cardinality (detect duplicate keys)
- `columns[].top_values` → most frequent values (understand categorical columns)
- `grain` → what one row represents (use in the GRAIN comment above each INSERT)
- `modeling_readiness` → data quality signal (flag "low" tables with a warning comment)

Use this file to decide on COALESCE, detect duplicates requiring ROW_NUMBER(), flag 100%-null columns, and validate join keys.

### 2C — Reading Target Schema CSVs

The target schema is supplied via two CSV files:
- `PreBuilt-Discovery-Strategy/src/redshift_schema_columns.csv` → the `Columns` sheet
- `PreBuilt-Discovery-Strategy/src/redshift_schema_dimatrix.csv` → the `DiMatrix` sheet

Treat these CSVs as the authoritative representation of the target schema.

**`Columns` sheet** — two columns: `Table` and `Column`.
- `Table` → target table name (use as `{output_schema}.{Table}` in INSERT INTO)
- `Column` → target column name (use verbatim in INSERT column list and SELECT aliases)
- Group rows by `Table` to get the full column list per target table
- Identify table type from prefix: `fact*` → fact, `dim*` → dimension, `bridge*` → bridge

**`DiMatrix` sheet** — pivot matrix: rows = fact tables, columns = dimension tables, value `1` = relationship exists.
- This matrix is the **authoritative and complete** definition of every fact-to-dimension relationship. Do not infer, add, or remove relationships beyond what it states.
- `1` = that dimension table has a foreign key in that fact table. Every `1` MUST produce: a `*_key` FK column in the fact DDL, and a JOIN to that dimension in the fact INSERT SQL.
- `0` or blank = no relationship. Do not join to that dimension for that fact under any circumstance.
- **JOIN type rule:** Every DiMatrix `1` relationship uses `LEFT JOIN`. This preserves all fact rows even when a dimension record is missing. Never use `INNER JOIN` for DiMatrix-driven joins — an `INNER JOIN` silently drops fact rows with unmatched dimension keys.

### 2D — Reading `PreBuilt-Discovery-Strategy/config.json`

Extract exactly two values:
- `output_schema` → prefix for all target tables in INSERT INTO statements
- `source_schema` → prefix for all source tables in FROM and JOIN clauses

Every source table reference: `{source_schema}.{table_name}`
Every target table reference: `{output_schema}.{table_name}`

---

## ⚠️ Section C — Guardrails (Enforced After Step 2 — All Input File Definitions Apply)

> These guardrails reference fields and files defined in Step 2. Read Step 2 fully before applying these rules.
> **If resuming after a context overflow or interrupted session:** read `PreBuilt-Discovery-Strategy/docs/etl-step-summary-log.md` first to identify the last completed **step** (not just domain), then continue from the next step. Do not re-process any step that already has a log entry.

| Rule | Detail |
|---|---|
| No skipped tables | Every table in the target schema CSVs must have both a `CREATE TABLE` definition and an `INSERT` statement |
| No invented columns | Only use column names from the `Columns` sheet in DDL and INSERT column lists |
| No placeholders in SQL | Every statement must be executable as-is on Redshift |
| No silent guesses | Uncertain mappings must carry a `-- TODO:` or `-- derived:` comment |
| Auto-continue always | After domain plan confirmation, process ALL domains sequentially without pausing. Never stop between domains for any reason. Log completion after each domain and immediately begin the next. |
| All SQL goes to file | Every generated DDL and INSERT statement MUST be appended to `PreBuilt-Discovery-Strategy/src/star_schema_full.sql`. If the file does not exist, create it first. Never leave SQL only in the chat response. |
| No source tables in INSERT target | FROM/JOIN clauses reference `source_schema` only; INSERT INTO references `output_schema` only |
| No bare column references | Always apply casting and null-handling where EDA shows issues |
| No skipping transformations | Apply ALL applicable rules from Section A-1D for every column |
| Low modeling_readiness warning | If a source table has `modeling_readiness = "low"` in `PreBuilt-Discovery-Strategy/src/eda.json`, add a block `-- WARNING` comment above its INSERT |
| Empty source table handling | If a source table has `row_count = 0` in `PreBuilt-Discovery-Strategy/src/eda.json`, generate the DDL, but replace the INSERT body with a `WHERE 1 = 0` guard and add a `-- WARNING: source table is empty` comment |
| Context overflow handling | If the combined size of all input files exceeds what fits in context, process one domain at a time: load only the source tables relevant to that domain, complete it fully, then move to the next. **On resumption, read `PreBuilt-Discovery-Strategy/docs/etl-step-summary-log.md` first to find the last completed domain.** |
| Log file initialisation | Before appending to `PreBuilt-Discovery-Strategy/docs/etl-step-summary-log.md`, check whether the file exists. If it does not, create it with the header defined in **Section B-1** (single source of truth), then append. Never overwrite earlier entries |

---

## Step 3 — Domain Grouping (Planning)

Before writing any SQL, perform this analysis.

### 3A — Identify Source Naming Patterns

Read all table names from `PreBuilt-Discovery-Strategy/src/erd.json`. Identify the naming convention (prefix groups, module separators, schema segments). List all distinct source prefix groups.

### 3B — Map Source Groups to Target Tables

For each source prefix group, find semantically aligned target tables, assign a human-readable domain name, and list the target dimensions, bridges, and facts belonging to that domain.

### 3C — ⚠️ Present the Domain Plan (MANDATORY on First Run)

**Always output this table before generating any SQL.** After presenting it, pause and output exactly:

> `[DOMAIN PLAN READY] Please confirm this grouping is correct, or provide corrections. Reply "confirmed" to proceed with SQL generation.`

Wait for the user to reply before continuing. This is a required checkpoint — domain mis-grouping silently corrupts all downstream SQL.

| # | Domain Name | Source Tables | Target Dimensions | Target Bridges | Target Facts | Primary Join Key |
|---|---|---|---|---|---|---|
| 1 | {name} | {source_table_1}, {source_table_2} | {dim_table_1} | {bridge_table_1} | {fact_table_1} | {join_key} |
| 2 | ... | ... | ... | ... | ... | ... |

---

## Section A — DDL + ETL SQL Generation Rules

Apply these rules to every `CREATE TABLE` and `INSERT INTO` statement you generate.

> **Rule application order:** A-1A → A-1C → A-1D → A-1E → A-1F → A-1G → A-1H

---

### A-1A — Table DDL (CREATE TABLE) Rules

For every target table defined in `PreBuilt-Discovery-Strategy/src/redshift_schema_columns.csv`, generate a Redshift-compatible `CREATE TABLE` statement **before** the corresponding `INSERT INTO`:

- Use `{output_schema}` from `PreBuilt-Discovery-Strategy/config.json` as the schema name
- Use the `Columns` sheet to drive the column list (one column per row for that table)
- Use `PreBuilt-Discovery-Strategy/src/erd.json` / `PreBuilt-Discovery-Strategy/src/eda.json` to infer reasonable data types, nullability, and constraints:
  - Prefer types that match the raw source columns or safe supersets
  - Add `NOT NULL` only when the business key or EDA clearly indicates it is always populated
  - Use `CREATE TABLE IF NOT EXISTS {output_schema}.{table} (...);` to keep DDL idempotent
- Where primary keys are obvious, add a `PRIMARY KEY` constraint; where foreign keys are obvious, add `REFERENCES` clauses; otherwise leave them commented as `-- TODO: add FK after validation`

Keep DDL in a separate block from the INSERT but in the **same order** (dimensions → bridges → facts).

After generating each DDL block, **immediately append it to `PreBuilt-Discovery-Strategy/src/star_schema_full.sql`** before proceeding to the next statement.

---

### A-1B — Statement Structure

Every INSERT block must follow this exact structure:

```sql
-- ────────────────────────────────────────────────────────
-- [DOMAIN : {domain_name}]
-- [TYPE   : {fact | dimension | bridge}]
-- [TARGET : {output_schema}.{target_table}]
-- [SOURCES: {source_schema}.{table_1}, {source_schema}.{table_2}]
-- [GRAIN  : {one sentence — what does one row in this target represent}]
-- ────────────────────────────────────────────────────────

INSERT INTO {output_schema}.{target_table}
(
    {col_1},
    {col_2},
    {col_3}
    -- ... all target columns from the Columns sheet
)
SELECT
    {expression_1}   AS {col_1},
    {expression_2}   AS {col_2},
    {expression_3}   AS {col_3}
    -- ... one expression per target column
FROM {source_schema}.{primary_source_table}  s1
LEFT JOIN {source_schema}.{secondary_source_table}  s2   -- second raw source (if needed)
    ON s1.{join_key} = s2.{join_key}
-- DiMatrix-driven dimension joins — use d1, d2, d3 aliases (never s* aliases for dims)
LEFT JOIN {output_schema}.{dim_table_1}  d1
    ON s1.{shared_natural_key} = d1.{dim_natural_key}    -- DiMatrix: {fact_table} → {dim_table_1}
LEFT JOIN {output_schema}.{dim_table_2}  d2
    ON s1.{shared_natural_key} = d2.{dim_natural_key}    -- DiMatrix: {fact_table} → {dim_table_2}
WHERE
    COALESCE(s1.isdeleted, FALSE) = FALSE   -- exclude soft-deleted rows (if column exists)
    AND s1.{natural_key} IS NOT NULL        -- exclude rows with null primary key
;
-- ALIAS CONVENTION:
-- s1, s2, s3 → raw source tables from source_schema
-- d1, d2, d3 → target dimension tables from output_schema (DiMatrix joins only)
```

After generating each INSERT block, **immediately append it to `PreBuilt-Discovery-Strategy/src/star_schema_full.sql`**.

---

### A-1C — Column Mapping Logic

For every target column:
1. Search `PreBuilt-Discovery-Strategy/src/erd.json` for a source column whose name or meaning matches the target column
2. Apply transformations (see Section A-1D)
3. Alias the result to the exact target column name
4. Add an inline comment explaining the mapping

Comment conventions:
```sql
{expression}  AS {col}   -- source: {source_table}.{source_column}
{expression}  AS {col}   -- derived: {plain English description of the logic}
NULL::{type}  AS {col}   -- TODO: no source mapping identified
```

---

### A-1D — Transformation Rules

Apply every applicable rule below. Treat `PreBuilt-Discovery-Strategy/src/erd.json`, `PreBuilt-Discovery-Strategy/src/eda.json`, and the target schema CSVs as the **authoritative specification** for how to transform and map source data.

#### A-1D.1 — Transformation Strategies (Summary)

| Strategy | When to Apply |
|---|---|
| **Direct mapping** | Source column type and semantics match target; confirm via `PreBuilt-Discovery-Strategy/src/erd.json` + `PreBuilt-Discovery-Strategy/src/eda.json` |
| **Derived columns** | No 1:1 source exists; compute from one or more columns and document logic in comment |
| **Conditional mapping** | Target semantics differ from raw codes; use `CASE` expressions or business rules |
| **Pre-aggregation** | Target grain is coarser than source grain; aggregate before inserting |
| **Window functions** | Rolling/cumulative metrics or time-series aggregations needed |
| **Dimension lookups** | Replace raw IDs with descriptive attributes via reference tables |
| **Deduplication** | `distinct_count < row_count` on natural key in `PreBuilt-Discovery-Strategy/src/eda.json`; apply ROW_NUMBER() filtering |
| **Null handling** | `null_pct > 0` in `PreBuilt-Discovery-Strategy/src/eda.json`; apply COALESCE per type rules below |
| **Type casting** | Source type does not match target type per `PreBuilt-Discovery-Strategy/src/erd.json`; apply explicit cast |
| **Audit/lineage comments** | Always — add inline source/logic comments; never emit silent transformations |

For complex or expensive transformations, prefer push-down execution in Redshift SQL. Use staging tables only where necessary for readability or performance.

#### A-1D.2 — Topic-Level Quality Rules (Required)

**Required measures and defaults**
- For core additive measures that are conceptually present on every fact row, model them as `NOT NULL` with a sensible default in DDL, and always `COALESCE` to that default in ETL.
- Before tightening to `NOT NULL`, backfill any existing NULLs using UPDATEs aligned with business rules.

**Date / time / lifecycle data types**
- Columns representing dates or periods MUST be typed as `DATE` or `TIMESTAMP`, not `VARCHAR`, unless the business explicitly requires free-form text.
- When the source is a string, use `TO_DATE` / `TO_TIMESTAMP` with an explicit format derived from `PreBuilt-Discovery-Strategy/src/eda.json`, and add QA comments or filters if a significant fraction of values do not match the expected format.

**Referential integrity from DiMatrix**
- The DiMatrix is the complete and authoritative source of every fact-to-dimension FK relationship. Every `1` = a required FK column in the fact DDL and a `LEFT JOIN` in the ETL INSERT.
- Resolve every DiMatrix `1` using the natural key resolution chain in A-1G before generating any fact INSERT.
- Once ETL reliably populates FK columns with low null rates, add `FOREIGN KEY` constraints in DDL. Until then keep them as `-- TODO: enable FK after validation`.

**Placeholder NULL keys**
- Avoid long-term `NULL::BIGINT` placeholders for foreign keys in facts. When a DiMatrix relationship exists, either implement the real join now, or explicitly comment: `-- TODO: no reliable source mapping yet` and keep the column nullable.

**Shared/conformed dimensions**
- Core shared dimensions (product, location, geography, calendar/time) must not remain empty skeletons. Design and document population logic for them, even if initial ETL uses stub values or a generated calendar.
- Facts carrying keys such as `product_key`, `geography_key`, `date_key` should eventually join to these shared dimensions with high non-null coverage; design ETL with that end-state in mind.

**ETL quality gates**
- For each domain, design at least basic assertions: null-rate checks for required measures/keys, orphan-key counts for fact→dim joins, and format checks for critical date/time fields.
- Quarantine bad rows in separate tables rather than silently loading them into main facts; reflect any known compromises with clear `-- WARNING` comments.

---

#### Type Casting

| When to Apply | SQL |
|---|---|
| Source is string, target is DATE | `CAST({col} AS DATE)` or `TO_DATE({col}, 'YYYY-MM-DD')` if format varies |
| Source is string, target is TIMESTAMP | `CAST({col} AS TIMESTAMP)` or `TO_TIMESTAMP({col}, 'YYYY-MM-DD HH24:MI:SS')` |
| Source is string, target is NUMERIC | `CAST({col} AS NUMERIC(18,4))` |
| Source is string, target is INTEGER | `CAST({col} AS INTEGER)` |
| Source is boolean-like string ('Y'/'N', '1'/'0') | `CASE WHEN {col} IN ('Y', 'true', '1', 'yes') THEN TRUE ELSE FALSE END` |
| Source is UUID/GUID | `CAST({col} AS VARCHAR(50))` |

#### Null Handling

| When to Apply | SQL |
|---|---|
| `null_pct > 0`, string column | `COALESCE(CAST({col} AS VARCHAR(255)), 'Unknown')` |
| `null_pct > 0`, numeric column | `COALESCE(CAST({col} AS NUMERIC(18,4)), 0)` |
| `null_pct > 0`, date column | `COALESCE(CAST({col} AS DATE), '1900-01-01'::DATE)` |
| `null_pct > 0`, boolean column | `COALESCE({col}, FALSE)` |
| `null_pct = 100` | `'Unknown'::VARCHAR(255) AS {col}  -- WARNING: source column is 100% null, using default` |
| `null_pct > 50` | `COALESCE(CAST({col} AS VARCHAR(255)), 'Unknown') AS {col}  -- WARNING: source column is >50% null in EDA` |

#### Surrogate Key Generation

| When to Apply | SQL |
|---|---|
| Source has no surrogate key | `ROW_NUMBER() OVER (ORDER BY {natural_key_col}) AS {target_key_col}` |
| Source already has a surrogate key (`_key_sk` suffix in `PreBuilt-Discovery-Strategy/src/erd.json`) | `CAST(s1.{col}_key_sk AS BIGINT) AS {target_key_col}` |

#### Deduplication

Apply when `distinct_count < row_count` on the natural key column in `PreBuilt-Discovery-Strategy/src/eda.json`:

```sql
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY {natural_key_col}
               ORDER BY {modifiedon_col} DESC
           ) AS _rn
    FROM {source_schema}.{source_table}
) s1
WHERE s1._rn = 1
```

#### Soft Delete Filter

Apply when `isdeleted` exists in the source table (visible in `PreBuilt-Discovery-Strategy/src/erd.json`):

```sql
WHERE COALESCE(s1.isdeleted, FALSE) = FALSE
```

#### Active Record Filter

Apply when `meta_record_status` exists in the source table (visible in `PreBuilt-Discovery-Strategy/src/erd.json`):

```sql
AND s1.meta_record_status = 'A'
```

---

### A-1E — JOIN Strategy

| Situation | JOIN Type |
|---|---|
| DiMatrix `1` relationship — fact to dimension | Always `LEFT JOIN` — never `INNER JOIN` |
| Secondary source enriches primary (optional match) | `LEFT JOIN` |
| Source table lookup where missing row = invalid fact | `INNER JOIN` — document reason in comment |
| Never use | `CROSS JOIN`, `FULL OUTER JOIN` |

**Rule:** For all DiMatrix-driven fact-to-dimension joins, `LEFT JOIN` is mandatory. It ensures fact rows are never silently dropped due to a missing or late-arriving dimension record. If a dimension key resolves to `NULL` after a `LEFT JOIN`, that is a data quality signal to capture in the log — not a reason to use `INNER JOIN`.

Always join on natural business keys visible in `PreBuilt-Discovery-Strategy/src/erd.json`. Use aliases `s1`, `s2`, `s3` for source tables and `d1`, `d2`, `d3` for dimension tables in join sequence order.

---

### A-1F — Ordering Within Each Domain

Always emit statements in this order within a domain:
1. **Dimension tables** — no dependencies on other target tables
2. **Bridge tables** — depend on dimension keys
3. **Fact tables** — depend on dimension and bridge keys

---

### A-1G — Fact–Dimension Relationships from DiMatrix

Use `PreBuilt-Discovery-Strategy/src/redshift_schema_dimatrix.csv` as the **authoritative and complete map** of which dimensions relate to each fact table. Every `1` is a required relationship. Every `0` or blank is explicitly not a relationship.

**For every `1` in the DiMatrix, follow this exact resolution chain to build the JOIN:**

1. **Identify the dimension's source table** — find the dim table name from the DiMatrix column header, then locate its corresponding source table in `PreBuilt-Discovery-Strategy/src/erd.json` by name or semantic match
2. **Identify the shared natural key** — find the column that appears in both the fact's source table and the dim's source table in `PreBuilt-Discovery-Strategy/src/erd.json` with matching semantics (e.g. `contactid`, `accountid`, `productid`). This is the JOIN condition.
3. **If the column name differs between tables** — use `PreBuilt-Discovery-Strategy/src/eda.json` top_values and distinct_count to confirm they represent the same domain, then join on those two columns with an explanatory comment.
4. **Pull the dim's surrogate key** — alias it to the target FK column name in the fact (e.g. `dim_customer.customer_key AS customer_key`)
5. **If no shared key column can be identified with confidence** — set FK to `NULL::BIGINT` and add `-- TODO: natural key not resolved between {fact_source} and {dim_source}`

**JOIN type:** Always `LEFT JOIN` for every DiMatrix relationship. Never `INNER JOIN`. See Section 2C for rationale.

**DDL:** For every DiMatrix `1`, the fact table DDL must include the corresponding `*_key BIGINT` FK column. Add `-- TODO: enable FK after validation` rather than a live `REFERENCES` constraint until data quality is confirmed.

**Example pattern:**
```sql
LEFT JOIN {output_schema}.{dim_table} d1
    ON s1.{shared_natural_key} = d1.{dim_natural_key}  -- DiMatrix: {fact_table} → {dim_table}
```

---

### A-1H — Unmappable Tables

If a target table has no identifiable source tables in `PreBuilt-Discovery-Strategy/src/erd.json`:

```sql
-- ────────────────────────────────────────────────────────
-- TODO: No source mapping identified for this table.
-- All columns defaulted to NULL.
-- Verify source before populating.
-- ────────────────────────────────────────────────────────

INSERT INTO {output_schema}.{target_table}
(
    {col_1},
    {col_2}
)
SELECT
    NULL::BIGINT         AS {col_1},   -- TODO: no source mapping identified
    NULL::VARCHAR(255)   AS {col_2}    -- TODO: no source mapping identified
WHERE 1 = 0  -- prevent empty insert from running until source is confirmed
;
```

Append this block to `PreBuilt-Discovery-Strategy/src/star_schema_full.sql` like any other statement.

---

## Section B — Execution Flow & Begin

Follow this flow precisely. Do not deviate.

```
[START]
    │
    ▼
Read all 5 logical inputs fully:
  PreBuilt-Discovery-Strategy/src/erd.json        → LOG: [INIT:read-erd] erd.json read | {N} source tables
  PreBuilt-Discovery-Strategy/src/eda.json        → LOG: [INIT:read-eda] eda.json read | {N} low-readiness / empty flags
  PreBuilt-Discovery-Strategy/src/redshift_schema_columns.csv  → LOG: [INIT:read-columns] columns CSV read | {N} target tables
  PreBuilt-Discovery-Strategy/src/redshift_schema_dimatrix.csv → LOG: [INIT:read-dimatrix] dimatrix CSV read | {N} relationships
  PreBuilt-Discovery-Strategy/config.json         → LOG: [INIT:read-config] config read | output_schema={x}, source_schema={y}
    │
    ▼
Step 3: Perform domain grouping → Present domain plan table (Step 3C)
→ LOG: [INIT:domain-plan] domain plan presented | {N} domains identified
    │
    ▼
⚠️ [PAUSE — MANDATORY] Output domain plan. Wait for user confirmation before proceeding.
    │
    ▼ (user replies "confirmed")
→ LOG: [INIT:confirmed] domain plan confirmed | proceeding to SQL generation
    │
Check if PreBuilt-Discovery-Strategy/src/star_schema_full.sql exists.
If not → CREATE the file now (empty).
Emit first statement: CREATE SCHEMA IF NOT EXISTS {output_schema};
Append to PreBuilt-Discovery-Strategy/src/star_schema_full.sql
→ LOG: [INIT:schema] CREATE SCHEMA written | {output_schema}
    │
    ▼
Take the first confirmed domain
    │
    ▼
[STEP: Read inputs for this domain]
→ LOG: [{name}:read-inputs] source tables scanned | {N} tables, {M} EDA flags
    │
    ▼
[STEP: Generate CREATE TABLE DDL — dimensions]
→ Append DDL to PreBuilt-Discovery-Strategy/src/star_schema_full.sql
→ LOG (one line per table): [{name}:dim-ddl] {table_name} DDL written | {N} columns
    │
    ▼
[STEP: Generate CREATE TABLE DDL — bridges]
→ Append DDL to PreBuilt-Discovery-Strategy/src/star_schema_full.sql
→ LOG (one line per table, or): [{name}:bridge-ddl] no bridge tables | skipped
    │
    ▼
[STEP: Generate CREATE TABLE DDL — facts]
→ Append DDL to PreBuilt-Discovery-Strategy/src/star_schema_full.sql
→ LOG (one line per table): [{name}:fact-ddl] {table_name} DDL written | {N} FK columns
    │
    ▼
[STEP: Generate INSERT SQL — dimensions]
→ Append INSERT blocks to PreBuilt-Discovery-Strategy/src/star_schema_full.sql
→ LOG (one line per table): [{name}:dim-insert] {table_name} INSERT written | {key flags e.g. dedup, COALESCE x3}
    │
    ▼
[STEP: Generate INSERT SQL — bridges]
→ Append INSERT blocks to PreBuilt-Discovery-Strategy/src/star_schema_full.sql
→ LOG (one line per table, or): [{name}:bridge-insert] no bridge tables | skipped
    │
    ▼
[STEP: Generate INSERT SQL — facts]
→ Append INSERT blocks to PreBuilt-Discovery-Strategy/src/star_schema_full.sql
→ LOG (one line per table): [{name}:fact-insert] {table_name} INSERT written | {N} DiMatrix joins, {M} FKs resolved
    │
    ▼
LOG: [{name}:done] domain complete | {N} DDL + {M} INSERT → star_schema_full.sql
    │
    ▼
← LOOP: Immediately begin next domain. No pause. No prompt. No wait.
    │
    ▼
[END — when all domains done]
Output: "All domains complete. {N} CREATE TABLE + {M} INSERT statements generated and written to PreBuilt-Discovery-Strategy/src/star_schema_full.sql."

─────────────────────────────────────────────────────────
STEP 4 GATE — Do NOT proceed automatically.
Output exactly: "Do you want me to run the Power BI measures
generation step (Step 4) now against POWERBI_PBIX_PATH?"
Wait for explicit "yes" before executing Step 4.
If user does not confirm → skip Step 4 entirely.
─────────────────────────────────────────────────────────
```

---

### Section B-1 — Per-Step Summary Log

Maintain a **granular, single-line breadcrumb log** in `PreBuilt-Discovery-Strategy/docs/etl-step-summary-log.md`.

**Log file header — create ONLY if file does not exist, then append. This is the single definition of the log header:**

```markdown
# ETL Step Summary Log
_Auto-generated. Append only. Do not edit manually._

### Step Summary Log
```

**Entry format — one line per action, no exceptions:**
```
{N}. [{DOMAIN}:{STEP}] {what was done} | {key output in 3–6 words}
```

**Examples of correct entries:**
```
1.  [INIT:read-erd]          Read erd.json | 14 source tables identified
2.  [INIT:read-eda]          Read eda.json | 3 low-readiness tables flagged
3.  [INIT:read-columns]      Read redshift_schema_columns.csv | 9 target tables found
4.  [INIT:read-dimatrix]     Read redshift_schema_dimatrix.csv | 12 fact-dim relationships mapped
5.  [INIT:read-config]       Read config.json | output_schema=dw, source_schema=raw
6.  [INIT:domain-plan]       Domain plan presented | awaiting user confirmation
7.  [INIT:confirmed]         Domain plan confirmed | proceeding to SQL generation
8.  [INIT:schema]            CREATE SCHEMA written | dw
9.  [Customers:dim-ddl]      dim_individual, dim_organization DDL written | 2 tables
10. [Customers:bridge-ddl]   No bridge tables in this domain | skipped
11. [Customers:fact-ddl]     fact_membership DDL written | 6 FK columns from DiMatrix
12. [Customers:dim-insert]   dim_individual INSERT written | dedup on contactid, 3 COALESCEs
13. [Customers:dim-insert]   dim_organization INSERT written | soft-delete filter applied
14. [Customers:fact-insert]  fact_membership INSERT written | 4 DiMatrix LEFT JOINs resolved
15. [Customers:done]         Domain complete | 3 DDL + 3 INSERT → star_schema_full.sql
```

**Rules:**
- If the file does not exist, create it with the header above before appending
- Always append — never overwrite or remove earlier entries
- **One entry per action** — every read, every DDL block, every INSERT block, every flag, every skip gets its own line
- **Maximum 10 words after the pipe** — if you need more, you are writing too much
- Never paste SQL, column lists, or multi-sentence descriptions into the log
- Log skipped steps explicitly (e.g. `| no bridge tables, skipped`)
- Log warnings inline (e.g. `| WARNING: 2 columns 100% null`)
- After each entry, **immediately** continue — no pause, no user prompt, no wait
- Preserve chronological order — the log must be readable as a complete execution trace from first read to last INSERT
- **When resuming after context overflow or interruption:** read this log first, find the last completed entry, continue from the next action

---

### Section B-2 — How to Begin

1. Read `PreBuilt-Discovery-Strategy/src/erd.json` → extract all source table names and identify naming patterns
2. Read `PreBuilt-Discovery-Strategy/src/redshift_schema_columns.csv` → extract all target table names from the `Columns` sheet
3. Read `PreBuilt-Discovery-Strategy/config.json` → extract `output_schema` and `source_schema`
4. Read `PreBuilt-Discovery-Strategy/src/eda.json` → note any tables with `row_count = 0` or `modeling_readiness = "low"`
5. Read `PreBuilt-Discovery-Strategy/src/redshift_schema_dimatrix.csv` → map fact–dimension relationships
6. Perform domain grouping and **present the domain plan table (Step 3C) — mandatory**
7. Wait for user confirmation before emitting any SQL
8. Check if `PreBuilt-Discovery-Strategy/src/star_schema_full.sql` exists — create it if not
9. Emit `CREATE SCHEMA IF NOT EXISTS {output_schema};` as the first line of `PreBuilt-Discovery-Strategy/src/star_schema_full.sql`
10. For each confirmed domain, generate DDL first (dimensions → bridges → facts), then INSERT SQL — appending every statement to `PreBuilt-Discovery-Strategy/src/star_schema_full.sql` as you go

---

## Step 4 — Create Base Semantic Measures in Power BI

**Type:** Automated (Power BI MCP)
**Prerequisite:** All ETL domains completed (`All domains complete` message from Section B)
**Variable:** `POWERBI_PBIX_PATH` — read from `PreBuilt-Discovery-Strategy/config.json` key `powerbi_pbix_path`

**Execution rule:** This step is **optional** and MUST only be executed after the explicit approval gate in Section B's execution flow outputs:
> *"Do you want me to run the Power BI measures generation step (Step 4) now against POWERBI_PBIX_PATH?"*

and receives a clear **yes**. If the user does not approve, skip Step 4 entirely.

**If Power BI MCP tools are unavailable** (connection failure, path not found, or tool not responding): do not halt. Instead, output a `-- TODO` list of recommended measures per fact table (row count, distinct business key, SUM/AVG per numeric column, and at least one domain KPI) so the user can create them manually.

### Objective

Create a **context-aware set of DAX measures** for each fact table in the Power BI model, using:
- Redshift profiling and EDA (`PreBuilt-Discovery-Strategy/src/eda.json`)
- Role and grain inference from the star schema CSVs
- ETL grain comments emitted in each INSERT block
- Existing Power BI model metadata (tables, columns, relationships, existing measures)

### Actions

1. **Analyse model context (token-efficient) via Power BI MCP**
   - `model_operations.GetStats` → list tables, column counts, measure counts
   - `measure_operations.List` per fact table → see existing measures
   - `model_operations.ExportTMDL` with small `maxReturnCharacters` only when needed to disambiguate column names or types
   - Combine with structured outputs from Redshift steps (roles, keys, relationships, grain comments) to identify fact vs dimension tables, keys, numeric and date/status columns

2. **Create baseline measures for each fact table** (only where equivalent measures don't already exist)
   - Row count: `COUNTROWS ( 'FactTable' )`
   - Distinct business key count: `DISTINCTCOUNT ( 'FactTable'[BusinessKey] )`
   - For each additive numeric column (amount, quantity, cost): SUM measure; AVG measure where it adds analytical value

3. **Derive business-focused KPIs from semantics**
   - Use column names, data types, relationships, and grain to infer business meaning and create KPIs such as:
     - Per-entity KPIs: revenue per customer, orders per customer, average order value
     - Ratio KPIs: conversion rates, share of total, % active vs cancelled
     - Time-based KPIs (where a date column exists): MTD/QTD/YTD totals, prior period comparisons, growth %
   - Only create KPIs when the required base measures and columns **actually exist**; otherwise skip and note why

4. **Reuse and layer measures**
   - Build complex KPIs on top of base measures:
     - Example: `[Average Order Value] = DIVIDE ( [Total Amount], [Orders - Distinct Order Count] )`
   - Use consistent naming: `<Table> - <Metric>` or clearly business-oriented names

5. **Validate semantics — no hallucinations**
   - Before calling `measure_operations.Create`, confirm referenced tables and columns exist in model metadata and no measure with the same name already exists
   - After creation, re-list measures to confirm DAX is valid
   - Remove or fix any ambiguous or low-value measures

### Output

- List of all created measures with DAX expressions, grouped by fact table

### Completion Criteria

For **every fact table**:
- At minimum: row count, distinct business-key count, SUM/AVG for relevant numeric columns
- At least **one high-value KPI** reflecting domain semantics
- All DAX expressions validated successfully in Power BI
- Summary of created measures (grouped by fact table, with DAX) provided to the user