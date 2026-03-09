## PreBuilt Discovery Strategy – Redshift → Star-Schema ETL

This folder contains:

- **Prompts** that drive a prebuilt ETL discovery and SQL‑generation flow (`docs/*.md`).
- **Helper scripts** that inspect your Redshift schema and generate machine‑readable metadata and target‑schema CSVs (openable in Excel / Power BI).

Use this strategy when you want an LLM‑driven, repeatable flow that:

- Reads Redshift metadata (`erd.json`, `eda.json`).
- Uses a vertical schema representation (`redshift_schema_columns.csv` + `redshift_schema_dimatrix.csv`, or an equivalent curated workbook such as `vertical_schema.xlsx`).
- Generates both `CREATE TABLE` DDL and `INSERT INTO ... SELECT` ETL for a star schema, driven by those inputs.

---

## 1. Prerequisites

- **Python**: 3.8+ recommended.
- **Packages**:
  - `redshift-connector` (preferred) **or** `psycopg2-binary`
  - `openpyxl` (for the Excel script)

Install with:

```bash
pip install redshift-connector openpyxl
# or, if you prefer psycopg2:
pip install psycopg2-binary openpyxl
```

You also need network access and credentials for the target Redshift cluster.

---

## 2. Shared Config (`config.json`)

Both scripts read a JSON config file with keys like:

- **`host`**: Redshift endpoint hostname.
- **`port`**: Port, default `5439`.
- **`db`**: Database name.
- **`user` / `password`**: Login credentials.
- **`schema`**: Target schema (for example `"model"`).
- **`tables`**: Optional list of table names; if empty or omitted, the scripts auto‑discover tables in the schema.
- **`output_dir`**: Directory where outputs are written (for example `src/`).
- **`top_values_limit`**, **`max_retries`**, **`retry_wait_secs`**: Advanced tuning for ERD/EDA profiling.

You can keep this file at the project root (for example `config.json`) or pass a custom path with `--config`.

---

## 3. `redshift_erd_eda.py` – ERD + EDA JSON

**Purpose**: Generate two JSON files describing your Redshift schema:

- `erd.json`: table/column metadata and foreign‑key style relationships.
- `eda.json`: column‑level statistics, distributions, and simple modeling hints.

**Basic usage** (from the repo root):

```bash
python redshift_erd_eda.py --config config.json
```

If `--config` is omitted, the script defaults to `config.json` in the same directory as the script.

**Key behaviors**:

- Profiles tables in parallel using a thread pool.
- Automatically calculates a safe worker count based on the number of tables and Redshift WLM slots (you can cap it via `parallel_workers` in the config).
- Collects:
  - Row counts, null counts, distinct counts.
  - Min/max/mean/stddev and percentiles for numeric columns.
  - Length stats for string columns.
  - Date ranges for date/time columns.
- Classifies each column roughly as **measure** or **dimension** and computes a modeling‑readiness score per table.
- Writes `erd.json` and `eda.json` atomically into `output_dir` (for example `src/erd.json`, `src/eda.json`).

---

## 4. `redshift_schema_excel.py` – Target-Schema CSVs / Vertical Schema (local testing)

**Purpose (for local use only)**: Connect to Redshift and generate **target‑schema CSVs** that act as a vertical schema and can be opened directly in Excel or Power BI. In the **final client workflow**, an equivalent curated workbook/CSV set (for example `vertical_schema.xlsx` or the two CSVs below) is expected to be **provided by the client**, and this script is mainly for development / testing when such artifacts are not available yet.

- `redshift_schema_columns.csv` (**Columns**): one row per `(Table, Column)` – the text version of the `Columns` sheet.
- `redshift_schema_dimatrix.csv` (**DiMatrix**): matrix of inferred joins (fact‑like tables as rows, dim‑like tables as columns; a `1` indicates a relationship).

**Basic usage (dev/testing)**:

```bash
python redshift_schema_excel.py --config config.json
```

If `--config` is omitted, it also defaults to `config.json` next to the script.

**Key behaviors**:

- Connects to the configured schema and **auto‑discovers tables**, filtering out obvious duplicates/temporary/versioned tables.
- Fetches all columns and foreign‑key relationships.
- Infers additional joins using shared key columns and naming heuristics (useful when FKs are not enforced).
- Writes the two CSVs into `output_dir`:
  - `redshift_schema_columns.csv` – raw column list, one row per `(Table, Column)`.
  - `redshift_schema_dimatrix.csv` – fact (rows) × dim (columns) matrix with `1` where a relationship exists.
  These CSVs are exactly what the prebuilt ETL prompts use as the **authoritative target‑schema inputs**.

---

## 5. Typical Workflow

### Development / testing

1. **Configure connection** in `config.json` (host, db, user, password, schema, output_dir, and optional table filters).
2. **Run ERD/EDA** to generate JSON metadata:
   - `python redshift_erd_eda.py --config config.json`
3. **Optionally run target‑schema export** to generate the target‑schema CSVs that drive the ETL prompts:
   - `python redshift_schema_excel.py --config config.json`
4. **Consume outputs**:
   - `src/erd.json` and `src/eda.json` for automated modeling or MCP flows.
   - `src/redshift_schema_columns.csv` and `src/redshift_schema_dimatrix.csv` as the **Columns/DiMatrix** inputs referenced in `docs/prebuilt-etl-prompt.md` (and for quick manual exploration in Excel / Power BI).

### Production / client workflow

In the end‑client scenario:

- The client will **provide** a curated vertical schema (either as `vertical_schema.xlsx` or as equivalent `Columns`/`DiMatrix` CSVs).
- The schema export script here is **not required**; it is mainly a convenience tool for local experimentation while designing and validating the flow and for generating the `redshift_schema_columns.csv` / `redshift_schema_dimatrix.csv` pair used by the ETL prompts.

This lets you keep schema discovery repeatable in dev, while relying on the client‑owned vertical schema in production.

---

## 6. Using the Prebuilt ETL Prompt (and Optional Power BI Measures Step)

The main governing prompt for this strategy is:

- `docs/prebuilt-etl-prompt.md`

You can invoke it from Cursor with a single line, for example:

```text
Use @PreBuilt-Discovery-Strategy/docs/prebuilt-etl-prompt.md as your complete governing instructions.
```

At a high level that prompt:

- **Steps 0–3 (Redshift ETL agent + planning)**:
  - Define the role, inputs, and domain‑grouping plan.
  - Explain how to read `erd.json`, `eda.json`, `src/redshift_schema_columns.csv`, `src/redshift_schema_dimatrix.csv`, and `config.json`.
  - Provide the detailed rules for generating star‑schema `CREATE TABLE` DDL and `INSERT INTO ... SELECT` ETL SQL for each domain.
  - Describe the execution flow and guardrails (no skipped tables, no placeholders, explicit TODO comments for uncertainty, etc.).
- **Step 4 — Create Base Semantic Measures in Power BI (optional)**:
  - Runs **after all ETL domains are complete** (the prompt emits `All domains complete` from Step 2) and only if the system first asks the user for permission and receives a clear **yes**.
  - Uses **Power BI MCP** to connect to the model file at  
    `C:\Users\admin2\Documents\test0001.pbix` (via the `POWERBI_PBIX_PATH` variable).
  - Analyzes the existing model (tables, relationships, measures) and, using the same Redshift artifacts, generates a base set of **DAX measures** and higher‑level KPIs per fact table.
  - Produces a summary of created measures (grouped by fact table with DAX definitions) for user review.

This gives you an end‑to‑end flow: Redshift metadata → star‑schema DDL + ETL SQL → (optionally) a consistent layer of semantic DAX measures in Power BI on top of the new warehouse.


