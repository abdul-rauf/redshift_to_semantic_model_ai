# Step 2 – Automatic Role Inference

**Type:** Automated (with optional Q/A)  
**Prerequisite:** Step 1 completed  
**Input:** Column profiles from Step 1

---

## Objective

Using the **structured column profiles from Step 1**, automatically infer keys, roles, and table definitions for the staging schema.
The inferred roles and table definitions are the structured input to Step 3 (which generates physical DDL/DML in the `stage` schema).

Do **not** re-profile Redshift unless Step 1 output is clearly incomplete; prefer working purely from the structured column metadata.

## Actions

1. **Determine table roles (fact vs dimension)**
   - Use column patterns and profiling hints from Step 1:
     - Facts: many rows, multiple foreign keys to other tables, numeric “amount”/“quantity” style columns, transaction-like timestamps.
     - Dimensions: relatively fewer rows, mostly descriptive attributes, a single primary key used as a target for foreign keys.

2. **Infer primary keys**
   - For each table, choose one or more columns as **primary key candidates** based on:
     - `isPkCandidate` flags from Step 1
     - High distinct count relative to row count
     - Naming conventions (`id`, `<table>_id`)
   - Avoid declaring composite keys unless strongly indicated by metadata or names.

3. **Infer foreign keys and relationships**
   - Use `isFkCandidate` flags and column names (`customer_id`, `product_id`, etc.) plus cross-table matches to:
     - Link fact tables to dimensions.
     - Identify direction: FK column in fact → PK in dimension.

4. **Draft logical staging table definitions**
   - For the `stage` schema, define **logical** (not yet physical SQL) table specs for:
     - All fact tables with:
       - Surrogate primary key (optional but recommended)
       - Business/natural key(s)
       - Foreign keys to dimensions
       - Measure columns (numeric) and key attributes (dates/statuses)
     - All dimension tables with:
       - Surrogate key (optional)
       - Business key(s)
       - Descriptive attributes

5. **Q/A interactions (optional)**
   - Ask only simple yes/no questions when metadata is ambiguous (e.g., “Is `status` important for analysis?”); if yes, then request minimal clarifications.

## Output

Return a **structured list of table definitions** for `stage`, for example:

- `[{ table: "stage.fact_orders", source: "raw.orders", role: "fact", pk: ["order_key"], businessKeys: ["order_id"], fks: [{ from: "customer_key", toTable: "stage.dim_customers", toColumn: "customer_key" }], measures: ["amount"], attributes: ["status", "created_at"] }]`

And similarly for each dimension. This output is the direct logical input to Step 3.

---

## Completion Criteria

- Every relevant table has an assigned role (fact or dimension) based **only on Step 1 metadata and simple business heuristics**
- Primary keys and foreign keys are identified without inventing non-existent columns
- Logical table definitions for staging are prepared in a structured format suitable for SQL generation in Step 3
- Short summary provided to user and approval received to proceed to Step 3
