# Databricks notebook source
import pandas as pd
from pyspark.sql.functions import col, lower

# COMMAND ----------

df = pd.read_csv("/dbfs/mnt/invoice_images/extracted_invoice.csv")

# COMMAND ----------

df

# COMMAND ----------

vendorDF = spark.read.parquet("/dbfs/mnt/invoice_images/vendorDF")

# COMMAND ----------

vendorDF

# COMMAND ----------

def validate_vendor_information(pandas_row, vendorDF):
    # Convert the Pandas row to a dictionary
    row_dict = pandas_row.to_dict()
    # Retrieve the corresponding vendor information from vendorDF
    # Note that we are assuming the name is a string and stripping leading/trailing whitespaces
    vendor_info = vendorDF.filter(lower(col("Name")).alias("Name") == row_dict['name'].lower().strip())
    
    # Check if the vendor exists in vendorDF
    if vendor_info.count() == 0:
        return -1
    
    # Collect the first matching row to a dictionary
    vendor_info_dict = vendor_info.limit(1).collect()[0].asDict()
    
    # Check BankAccount, Email, and PhoneNumber in order
    if str(vendor_info_dict.get('BankAccount', '')).strip() != str(row_dict.get('bankAccount', '')).strip():
        print("Banking not same")
        return (row_dict["name"], 2)
    if str(vendor_info_dict.get('Email', '')).strip().lower() != str(row_dict.get('email', '')).strip().lower():
        print("Email not same")
        return (row_dict["name"], 3)
    if str(vendor_info_dict.get('PhoneNumber', '')).strip() != str(row_dict.get('phonenumber', '')).strip():
        print("Number not same")
        return (row_dict["name"], 4)
    
    # If all checks pass, return 1
    return (row_dict["name"], 1)

# Example usage:
result = validate_vendor_information(df.iloc[0], vendorDF)
print(result)

# COMMAND ----------


# Write to a file
with open("/dbfs/mnt/invoice_images/validation_results_name_result.txt", "w") as file:
    file.write(f"{result[0]},{result[1]}")