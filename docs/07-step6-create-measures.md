# Step 6 – Create Base Semantic Measures in Power BI

**Type:** Automated  
**Prerequisite:** Step 5 completed  
**Variables:** `POWERBI_PBIX_PATH` = `C:\Users\admin2\Documents\test_3.pbix`

---

## Objective

Create a **context-aware set of DAX measures** for each fact table in the Power BI model, using information from previous steps (Redshift profiling, role inference, star-schema design, and relationships) to go beyond static measures and generate business-meaningful calculations.

## Actions

1. **Analyze model context (token-efficient)**
   - Use Power BI modeling MCP tools to understand the model **without exporting full TMDL unless needed**:
     - `model_operations.GetStats` to list tables, column counts, and measure counts.
     - `measure_operations.List` per fact table to see existing measures.
     - Optionally, `model_operations.ExportTMDL` with a small `maxReturnCharacters` only when necessary to disambiguate column names or data types.
   - Combine this with structured outputs from Steps 2–4 (roles, keys, relationships) to identify:
     - Fact vs dimension tables
     - Keys, foreign keys, numeric and date/status columns.

2. **Create baseline measures for each fact table**
   - For each fact table **that does not already have equivalent measures**:
     - **Row count** – `COUNTROWS ( 'FactTable' )`
     - **Distinct count of the main business key** – `DISTINCTCOUNT ( 'FactTable'[BusinessKey] )`
   - For each **additive numeric column** (amount, quantity, cost, etc.):
     - Create **SUM** measures.
     - Where it adds value (e.g., continuous amounts), also create **AVG** measures.

3. **Derive business-focused KPIs from semantics**
   - Use column names, data types, and relationships (from Steps 2–4 plus model metadata) to infer business meaning and create KPIs such as:
     - Per‑entity KPIs: revenue per customer, orders per customer, quantity per product, average order value.
     - Ratio KPIs: conversion rates, share of total, % of active vs cancelled, etc.
     - Time‑based KPIs (when a date column exists): MTD/QTD/YTD totals, prior period comparisons, growth percentages.
   - Only create KPIs when the required base measures and columns **actually exist**; otherwise, explicitly skip and note why.

4. **Reuse and layer measures**
   - Build complex KPIs on top of base measures instead of repeating logic:
     - Example: `[Average Order Value] = DIVIDE ( [Total Amount], [Orders - Distinct Order Count] )`
   - Use consistent naming patterns: `<Table> - <Metric>` or clearly business-oriented names.

5. **Validate semantics and avoid hallucinations**
   - Before calling `measure_operations.Create`, ensure:
     - Referenced tables and columns exist in the model metadata.
     - A measure with the same name does not already exist, or else update/skip instead of duplicating.
   - After creation, re-`Get` or `List` measures to confirm DAX is valid (no semantic errors).
   - Fix or remove any ambiguous or low‑value measures rather than keeping them.

## Output

- List of all created measures with their DAX expressions
- Measures organized by fact table

---

## Completion Criteria

- For **every fact table**:
  - At minimum: a row count, a distinct business‑key count, and SUM/AVG measures for relevant numeric columns
  - At least **one high‑value KPI** that reflects the domain semantics (e.g., average order value, revenue per customer, etc.)
- Measures are **derived from the actual model context** (not hard‑coded) and reference the correct tables/columns
- All DAX expressions validate successfully in Power BI
- Summary of created measures (grouped by fact table, with DAX) is provided to the user
- User approval received to proceed to Step 7
