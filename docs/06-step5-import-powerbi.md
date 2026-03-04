# Step 5 – Import into Power BI and Build Semantic Model (Manual Step)

**Type:** Manual / Automated hybrid  
**Prerequisite:** Step 4 completed  
**Variables:** `POWERBI_PBIX_PATH` = `C:\Users\admin2\Documents\test_3.pbix`

---

## Objective

Import the tables from the `${REDSHIFT_SCHEMA_OUTPUT}` schema in Redshift into Power BI and configure the basic semantic model structure.

## Actions

1. Open the PBIX file at `${POWERBI_PBIX_PATH}` in Power BI Desktop.
2. **Import tables** from the Redshift `${REDSHIFT_SCHEMA_OUTPUT}` schema (all fact and dimension tables from Step 3).
3. **Create relationships** in Power BI to match the Step 4 relationship map.
4. **Rename tables and columns** to business-friendly names.
5. **Hide technical/key columns** that report authors should not see.
6. **Set data types**, default summarization, and formatting.
7. Use brief Q/A only when needed to confirm naming or visibility choices.

## Output

- Power BI model with `${REDSHIFT_SCHEMA_OUTPUT}` tables imported
- Relationships configured to match Step 4
- Columns renamed and formatted
- Technical/key columns hidden

---

## Completion Criteria

- All tables from the `${REDSHIFT_SCHEMA_OUTPUT}` schema imported into Power BI
- Relationships match Step 4 design
- Tables and columns have business-friendly names
- Technical columns are hidden
- Data types and formatting are set
- Summary provided to user
- User approval received to proceed to Step 6
