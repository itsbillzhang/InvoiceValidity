# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import *

def append_result_to_invoice_validations(name, result):
  comment_reasons = {
  2: "Validation failed as banking information not consistent with past invoices",
  4: "Validation failed as phone information not consistent with past invoices",
  3: "Validation failed as email information not consistent with past invoices",
  -1: "First appearance in history. Nothing to validate towards.",
  1: "Validation passed successfully"}

  if result in [-1, 1]:
    truth = True
  else:
    truth = False 

  data = [(name, truth, comment_reasons[result], datetime.now())]

  schema = StructType([
  StructField("Company", StringType(), True),
  StructField("ValidationSuccess", BooleanType(), True),
  StructField("Comments", StringType(), True),
  StructField("DateOfCheck", TimestampType(), True)])

  # Create a DataFrame with the sample data
  sampleDF = spark.createDataFrame(data, schema)

  # Write the DataFrame to the SQL table
  sampleDF.write \
      .jdbc(url=jdbcUrl, table="dbo.InvoiceValidationsAppends", mode="append", properties=connectionProperties)
