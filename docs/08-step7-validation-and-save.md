# Step 7 – Validation and Save

**Type:** Automated  
**Prerequisite:** Step 6 completed  
**Variables:** `POWERBI_PBIX_PATH` = `C:\Users\admin2\Documents\test_3.pbix`

---

## Objective

Validate the complete semantic model for correctness and save the final PBIX file, using **lightweight model metadata** and avoiding unnecessary large exports.

## Validation Checks

1. **No ambiguous paths** – all relationship paths are deterministic (no multiple active routes between the same tables).
2. **No unused tables** – every visible fact/dimension table participates in at least one relationship or measure.
3. **No broken or inactive relationships** – all intended relationships are active and consistent with Steps 2–4.

## Actions

1. **Inspect model metadata**
   - Use `model_operations.GetStats` to:
     - List tables (names, hidden flags).
     - Get counts of relationships and measures.
   - If needed for deeper inspection, optionally call `model_operations.ExportTMDL` with a conservative `maxReturnCharacters`, but avoid this when `GetStats` is sufficient.

2. **Run validation checks**
   - Confirm relationships align with the Step 4 relationship map:
     - Same From/To tables and join columns.
   - Identify any **visible** tables that:
     - Have no relationships and no measures → flag as unused.
   - Check that all measures created in Step 6 are in a **Ready** state (no DAX errors).

3. **Report and (lightly) fix issues**
   - If issues are found, propose minimal, concrete fixes:
     - E.g., hide unused technical tables, correct relationship directions, or fix broken measures.
   - Do **not** invent new tables/relationships; only operate on existing metadata.

4. **Trigger data refresh (optional) and save**
   - Optionally run `model_operations.Refresh` with `refreshType = Full` to ensure data is current.
   - Instruct the user to save the PBIX at `${POWERBI_PBIX_PATH}` (MCP cannot directly save the file).

---

## Final Output Format

| Section | Details |
|---|---|
| **Fact tables** in Redshift `stage` schema | (list) |
| **Dimension tables** in Redshift `stage` schema | (list) |
| **Relationships** | From → To \| Columns \| Cardinality \| Filter Direction |
| **Measures** created in Power BI | (list) |
| **PBIX file** | Saved at `${POWERBI_PBIX_PATH}` |

---

## Completion Criteria

- All validation checks pass
- PBIX file saved successfully
- Final output summary provided to user
- User gives final approval
