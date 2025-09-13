## Databricks Labs DQX — Auto-generate Data Quality Rules

This repo provides a Databricks notebook that profiles a Unity Catalog table using Databricks Labs DQX and auto-generates candidate data quality rules. It then saves the rules to Workspace and applies them to split valid/invalid rows.

### Contents
- `notebooks/dqx_auto_rules.py`: Databricks notebook (Python) with widgets for target table and checks path
- `requirements.txt`: Python dependencies (for local reference; clusters install via `%pip` in notebook)

### Quick start (Databricks)
1. Import `notebooks/dqx_auto_rules.py` into your Databricks workspace.
2. Open the notebook and set the widgets:
   - `TARGET_TABLE = catalog.schema.table`
   - `CHECKS_WORKSPACE_PATH = /Shared/dqx/checks.yml` (or your preferred path)
3. Run all cells top-to-bottom.
4. Review `/Shared/dqx/checks.yml` and the invalid rows output.

### Outputs
- YAML checks in Workspace: path you specify
- Two Delta tables: `<table>__dq_valid` and `<table>__dq_invalid`

### Notes
- The notebook installs `databricks-labs-dqx` and `databricks-sdk` via `%pip`.
- Adjust profiling sample via the commented `options` example in the notebook if needed.

