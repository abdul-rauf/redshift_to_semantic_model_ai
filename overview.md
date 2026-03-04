# MCP – Semi Automated Flow: Redshift → Power BI Semantic Model

This project uses Cursor AI with MCP servers to automatically build a star-schema semantic model from Redshift raw tables and import it into Power BI Desktop.

## MCP Servers

- [Power BI](https://github.com/microsoft/powerbi-modeling-mcp)
- [AWS Redshift](https://github.com/awslabs/mcp/tree/main/src/redshift-mcp-server)

## Workflow Guide (Files, Purpose, Dependencies)


| Order | File                                        | Purpose and how it fits in the flow                                                   |
| ----- | ------------------------------------------- | ------------------------------------------------------------------------------------- |
| N/A   | [00-agent-prompt.md](00-agent-prompt.md)   | Global agent role, rules, credentials, and variables — **must be loaded before any step** |
| 0     | [01-step0-create-pbix.md](01-step0-create-pbix.md) | Create/open the PBIX in Power BI Desktop (manual), using `POWERBI_PBIX_PATH` from 00 |
| 1     | [02-step1-redshift-metadata-discovery.md](02-step1-redshift-metadata-discovery.md) | Profile Redshift raw tables via MCP (read-only); outputs column profiles for Step 2  |
| 2     | [03-step2-automatic-role-inference.md](03-step2-automatic-role-inference.md) | Infer fact/dimension roles and PKs/FKs from Step 1 metadata; defines logical `stage` table shapes for Step 3 |
| 3     | [04-step3-star-schema-design.md](04-step3-star-schema-design.md) | Generate DDL/DML for `stage` fact/dim tables based on Step 2; SQL is run manually in Redshift |
| 4     | [05-step4-relationship-design.md](05-step4-relationship-design.md) | (Optional) Refine relationships/constraints using the `stage` schema created in Step 3 |
| 5     | [06-step5-import-powerbi.md](06-step5-import-powerbi.md) | Import `stage` tables into Power BI and build the basic semantic model (manual + MCP) |
| 6     | [07-step6-create-measures.md](07-step6-create-measures.md) | Define and refine base measures in Power BI on top of the star schema                |
| 7     | [08-step7-validation-and-save.md](08-step7-validation-and-save.md) | Validate model behavior in Power BI and save the final PBIX                          |


