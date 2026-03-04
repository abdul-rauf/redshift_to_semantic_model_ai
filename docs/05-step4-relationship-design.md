# Step 4 – Relationship Design in Redshift

**Type:** Automated (with optional Q/A)  
**Prerequisite:** Step 3 completed  
**Target Schema:** `stage`

---

## Objective

Define all relationships between the staging tables created in Step 3, specifying join columns and cardinality, using the **keys and roles inferred in Step 2** and the physical columns created in Step 3.

Do not invent relationships beyond what is implied by the Step 2 PK/FK definitions and actual `stage` table schemas.

## Actions

1. **Identify relationship candidates**
   - Start from the Step 2 structured output (PK/FK definitions).
   - Verify that all referenced columns exist in the `stage` schema tables defined in Step 3.

2. **Specify each relationship**
   - For each PK/FK pair:
     - Record:
       - From table (typically fact)
       - To table (typically dimension)
       - Join columns (`fromColumn = toColumn`)
       - Cardinality (one-to-many, one-to-one, etc.), inferred from Step 1/2 cardinality hints.
       - Filter direction (usually dimension → fact).

3. **Ensure model quality**
   - Confirm:
     - No ambiguous relationships (no multiple active paths between the same table pair).
     - No circular relationships.
   - If ambiguity would arise, **document it and stop** instead of guessing a resolution.

4. **Optional Q/A**
   - Ask simple yes/no questions only if a relationship is semantically unclear (e.g., multiple possible role-playing dimensions).

## Output

Return a concise relationship map table:

| From Table | To Table | Join Columns | Cardinality | Filter Direction |
|---|---|---|---|---|

---

## Completion Criteria

- All relationships defined between staging tables
- No ambiguous or circular paths
- Cardinality and join columns documented
- Summary provided to user
- User approval received to proceed to Step 5
