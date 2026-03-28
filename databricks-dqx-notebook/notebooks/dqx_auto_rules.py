# Databricks notebook source

# COMMAND ----------
# MAGIC %pip install databricks-labs-dqx databricks-sdk

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

# Allow providing the table later via widget or default fallback
dbutils.widgets.text("TARGET_TABLE", "your_catalog.your_schema.your_table", "Target table (catalog.schema.table)")
TARGET_TABLE = dbutils.widgets.get("TARGET_TABLE")

# Optional: where to store generated checks in the Workspace
dbutils.widgets.text("CHECKS_WORKSPACE_PATH", "/Shared/dqx/checks.yml", "Checks workspace path")
CHECKS_WORKSPACE_PATH = dbutils.widgets.get("CHECKS_WORKSPACE_PATH")

ws = WorkspaceClient()

# COMMAND ----------
# Load table preview
df = spark.table(TARGET_TABLE)
display(df.limit(10))

# COMMAND ----------
# Profile the data (adjust sampling via options if needed)
profiler = DQProfiler(ws)

# Example for tuning:
# options = {"sample_fraction": 1.0, "max_records": 5000}
# summary_stats, profiles = profiler.profile(df, options=options)

summary_stats, profiles = profiler.profile(df)
print("Profiled columns:", len(profiles))

# COMMAND ----------
# Generate rule candidates from profiles
generator = DQGenerator(ws)
checks = generator.generate_dq_rules(profiles)
print("Generated checks:", len(checks))

# COMMAND ----------
# Save checks to Workspace (YAML)
dq_engine = DQEngine(ws)
dq_engine.save_checks_in_workspace_file(checks, workspace_path=CHECKS_WORKSPACE_PATH)
print(f"Saved checks to: {CHECKS_WORKSPACE_PATH}")

# COMMAND ----------
# Apply checks to your data
# Option A: Split into valid/invalid rows
valid_rows, invalid_rows = dq_engine.apply_checks_and_split(df, checks)
print(f"Valid rows: {valid_rows.count()}, Invalid rows: {invalid_rows.count()}")
display(invalid_rows.limit(50))

# COMMAND ----------
# (Optional) Persist results to tables
catalog, schema, base = TARGET_TABLE.split(".")
valid_table_name = f"{catalog}.{schema}.{base}__dq_valid"
invalid_table_name = f"{catalog}.{schema}.{base}__dq_invalid"

valid_rows.write.mode("overwrite").saveAsTable(valid_table_name)
invalid_rows.write.mode("overwrite").saveAsTable(invalid_table_name)

print(f"Wrote valid rows to:   {valid_table_name}")
print(f"Wrote invalid rows to: {invalid_table_name}")

# COMMAND ----------
# (Optional) Annotate without splitting (adds check result columns)
annotated_df = dq_engine.apply_checks(df, checks)
display(annotated_df.limit(50))

