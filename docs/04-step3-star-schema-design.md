# Step 3 – Star-Schema Design in Redshift

**Type:** Automated (with optional Q/A)  
**Prerequisite:** Step 2 completed  
**Target Schema:** `${REDSHIFT_SCHEMA_OUTPUT}` (staging layer)  
**Note:** The Redshift MCP server is **read-only** — it cannot execute DDL. This step generates SQL queries for the user to run manually.

---

## Objective

Design fact and dimension tables for the Redshift `${REDSHIFT_SCHEMA_OUTPUT}` schema (staging layer) based on the **structured logical definitions from Step 2**, and **generate the SQL DDL/DML queries** for the user to execute.
Do **not** re-infer roles/keys from Redshift; rely on the Step 2 structured definitions.

## Actions

- **SQL generation rule**: Always double-quote schema names (use `"raw".table_name`, `"${REDSHIFT_SCHEMA_OUTPUT}".table_name`) to avoid reserved-word or namespace issues in Redshift.

1. **Generate `CREATE SCHEMA` DDL** for the staging schema
   - Emit a single schema-creation statement that the user can run once before any table DDLs:
     - `CREATE SCHEMA IF NOT EXISTS "${REDSHIFT_SCHEMA_OUTPUT}";`

2. **Generate `CREATE TABLE` DDL** for all staging tables
   - For each fact and dimension in the Step 2 output:
     - Use `${REDSHIFT_SCHEMA_OUTPUT}` as the schema (e.g., `"${REDSHIFT_SCHEMA_OUTPUT}".fact_orders`).
     - Define columns, data types, and nullable flags based on raw metadata from Steps 1–2.
     - Add surrogate keys where indicated.

3. **Generate `INSERT INTO ... SELECT` DML** to populate staging tables from `raw` schema
   - Use explicit schema- and table-qualification (e.g., `"raw".orders`, `"${REDSHIFT_SCHEMA_OUTPUT}".dim_customers`).
   - Avoid `SELECT *`; project only the required columns.
   - Preserve business keys and foreign-key relationships as defined in Step 2.

4. **Optional derived dimensions**
   - Where date/time columns exist and are marked as useful for analysis, generate an optional `dim_date` (or similar) and its population script.

5. **Design constraints**
   - Avoid snowflake schemas unless clearly required.
   - Avoid many-to-many relationships unless justified by Step 2 relationships.

6. **Q/A interactions**
   - Ask brief clarifying questions only when necessary (e.g., naming preferences or optional derived dimensions).

## Output

- Complete SQL scripts (DDL + DML) for creating and populating all staging tables in `${REDSHIFT_SCHEMA_OUTPUT}`
- Scripts presented to the user for manual execution against Redshift
- Each script clearly labeled with the target table name

---

## Completion Criteria

- DDL generated for all fact tables in `${REDSHIFT_SCHEMA_OUTPUT}` schema
- DDL generated for all dimension tables in `${REDSHIFT_SCHEMA_OUTPUT}` schema
- DML generated to populate each staging table from `raw` into `${REDSHIFT_SCHEMA_OUTPUT}`
- Surrogate keys introduced where needed
- Star-schema structure is clean (no snowflake, no many-to-many unless justified)
- All SQL scripts presented to user for execution
- Summary provided to user
- User confirms queries have been executed and approves proceeding to Step 4
