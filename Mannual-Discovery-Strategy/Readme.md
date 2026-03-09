# MCP – Semi-Automated Flow: Redshift → Power BI Semantic Model

This project uses Cursor AI with MCP servers to automatically build a star-schema semantic model from Redshift raw tables and import it into Power BI Desktop.  
Each step writes machine-readable outputs into `docs/output.md` so later steps can reuse results instead of repeating heavy discovery.

## MCP Servers

- [Power BI](https://github.com/microsoft/powerbi-modeling-mcp)
- [AWS Redshift](https://github.com/awslabs/mcp/tree/main/src/redshift-mcp-server)

## Workflow Guide (Files, Purpose, Dependencies)

| Order | File                                        | Purpose and how it fits in the flow                                                                                                   |
| ----- | ------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| N/A   | [00-agent-prompt.md](00-agent-prompt.md)   | Global agent role, rules, credentials, variables, and naming conventions — **must be loaded before any step**                        |
| 0     | [01-step0-create-pbix.md](01-step0-create-pbix.md) | Create/open the PBIX in Power BI Desktop (manual), using `POWERBI_PBIX_PATH` from 00                                                 |
| 1     | [02-step1-redshift-metadata-discovery.md](02-step1-redshift-metadata-discovery.md) | Run a single `SVV_ALL_COLUMNS` metadata query via Redshift MCP; stores full column metadata reference in `step1_redshift_metadata`  |
| 2     | [03-step2-automatic-role-inference.md](03-step2-automatic-role-inference.md) | Use Step 1 metadata to infer facts/dimensions, PKs/FKs, and logical table definitions; writes them to `step2_role_inference`        |
| 3     | [04-step3-star-schema-design.md](04-step3-star-schema-design.md) | Generate Redshift DDL/DML for `${REDSHIFT_SCHEMA_OUTPUT}` fact/dim tables from Step 2 (read-only SQL generation); summary in `step3_star_schema` |
| 4     | [05-step4-relationship-design.md](05-step4-relationship-design.md) | Define clean star-schema relationships between `${REDSHIFT_SCHEMA_OUTPUT}` tables based on Step 2 PK/FK metadata; saved in `step4_relationships` |
| 5     | [06-step5-import-powerbi.md](06-step5-import-powerbi.md) | Import `${REDSHIFT_SCHEMA_OUTPUT}` tables into Power BI and build the basic semantic model (manual + MCP)                            |
| 6     | [07-step6-create-measures.md](07-step6-create-measures.md) | Define and refine base measures in Power BI on top of the star schema                                                                |
| 7     | [08-step7-validation-and-save.md](08-step7-validation-and-save.md) | Validate model behavior in Power BI and save the final PBIX                                                                          |

