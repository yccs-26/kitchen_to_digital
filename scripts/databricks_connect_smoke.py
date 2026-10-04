from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.getOrCreate()

print(f"Spark version: {spark.version}")

rows = spark.range(3).collect()
print(f"Rows: {[row.id for row in rows]}")

spark.sql("SHOW CATALOGS").show(truncate=False)