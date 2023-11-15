# Databricks notebook source
# Ensure you have the right library installed
# For Azure SQL Database, you would use the JDBC driver for SQL Server.

# Define your connection parameters
jdbcHostname = "anomalydetection.database.windows.net"
jdbcDatabase = "Invoices"
jdbcPort = 1433
jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase}"
connectionProperties = {
  "user" : "CloudSA2e12785d",
  "password" : "Supacha0s!", # https://medium.com/codex/get-started-with-azure-sql-in-databricks-9bfa8d590c64 should not actually do it this way.
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Specify the name of the table you want to load
dbTable = "dbo.Vendor"

# Load data from your Azure SQL Database table to a DataFrame
vendorDF = spark.read.jdbc(url=jdbcUrl, table=dbTable, properties=connectionProperties)

# Show the DataFrame to verify that your data has been loaded correctly
vendorDF.show()


# COMMAND ----------

vendorDF.write.parquet("/dbfs/mnt/invoice_images/vendorDF")