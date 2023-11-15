# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC This notebook shows you how to create and query a table or DataFrame loaded from data stored in Azure Blob storage.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 1: Set the data location and type
# MAGIC
# MAGIC There are two ways to access Azure Blob storage: account keys and shared access signatures (SAS).
# MAGIC
# MAGIC To get started, we need to set the location and type of the file.

# COMMAND ----------

storage_account_name = "invoices1"
storage_container_name = "invoicecontainer"
storage_account_access_key = "YOUR_ACCESS_KEY"

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://invoicecontainer@invoices1.blob.core.windows.net/",
  mount_point = "/mnt/invoice_images",
  extra_configs = {
    "fs.azure.account.key.invoices1.blob.core.windows.net":"cc6Ym3mWc1rs5ShbZFYWuqO9oAtCvGtSx4asuXt+4bCjbWa8WSdpwusH872IiJxs+9T7qvZ7NCMC+AStK4lwHA=="
   }
)

# COMMAND ----------

dbutils.fs.ls("/mnt/invoice_images")

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md Now Calling the Computer Vision Library
# MAGIC

# COMMAND ----------

from azure.cognitiveservices.vision.computervision import ComputerVisionClient
from azure.cognitiveservices.vision.computervision.models import OperationStatusCodes
from azure.cognitiveservices.vision.computervision.models import VisualFeatureTypes
from msrest.authentication import CognitiveServicesCredentials

from array import array
import os
from PIL import Image
import sys
import time

endpoint = "https://invoicecompute.cognitiveservices.azure.com/"
key = "62029ad014004686986ae1d375772327"

computervision_client = ComputerVisionClient(endpoint, CognitiveServicesCredentials(key))

# Get the list of image file paths
image_paths = dbutils.fs.ls("/mnt/invoice_images")

extracted_texts = []
for image_info in image_paths:
    print(image_info)
    image_path = "/" + image_info.path
    image_path = image_path.replace(':', '') # DBFS path should not be dbfs:/ but dbfs/
    # Open the image file
    text = ""
    with open(image_path, "rb") as image_file:
        # Call API with image and raw response (allows you to get the operation location)
        read_response = computervision_client.read_in_stream(image_file, raw=True)
        read_operation_location = read_response.headers["Operation-Location"]
        
        # Get the operation id from the operation location
        operation_id = read_operation_location.split("/")[-1]
        
        while True:
            read_result = computervision_client.get_read_result(operation_id)
            if read_result.status not in ['notStarted', 'running']:
                break
            time.sleep(1)
        
        
        # Print results from the operation
        if read_result.status == OperationStatusCodes.succeeded:
            print(read_result.analyze_result.read_results)
            for text_result in read_result.analyze_result.read_results:
                for line in text_result.lines:
                    #print(line.text)
                    text += line.text + "\n"
    
    extracted_texts += [text]
        

# COMMAND ----------

text = extracted_texts[0]
print(extracted_texts[0])

# COMMAND ----------

print(text)

# COMMAND ----------

import re
import pandas as pd

def extract_invoice_features(text):
    # Define regex patterns for each feature
    patterns = {
        'name': r'^([^\n\r]+)',  # 
        'phonenumber': r'(\d{3}-\d{3}-\d{4})',
        'email': r'(\S+@\S+)',
        'bankAccount': r'IBAN (\d+)'
    }
    
    # Extract features using regex
    features = {}
    for feature, pattern in patterns.items():
        match = re.search(pattern, text, re.MULTILINE)
        features[feature] = match.group(1) if match else None

    return pd.DataFrame([features])



# COMMAND ----------

# Example usage:
df = extract_invoice_features(text)
print(df)

# COMMAND ----------

df.iloc[0]

# COMMAND ----------

remote_table = (spark.read
  .format("sqlserver")
  .option("host", "anomalydetection.database.windows.net")
  .option("user", "h662zhan@uwaterloo.ca")
  .option("password", "Supacha0s!")
  .option("database", "Invoices")
  .option("dbtable", "dbo.Vendor") # (if schemaName not provided, default to "dbo")
  .load()
)

# COMMAND ----------

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

from pyspark.sql.functions import col, lower

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
        return 2
    if str(vendor_info_dict.get('Email', '')).strip().lower() != str(row_dict.get('email', '')).strip().lower():
        print("Email not same")
        return 3
    if str(vendor_info_dict.get('PhoneNumber', '')).strip() != str(row_dict.get('phonenumber', '')).strip():
        print("Number not same")
        return 4
    
    # If all checks pass, return 1
    return 1

# Example usage:
result = validate_vendor_information(df.iloc[0], vendorDF)
print(result)

# COMMAND ----------

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



    

# COMMAND ----------

append_result_to_invoice_validations("TechSolvers", 2)

# COMMAND ----------

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def send_email_if_condition_met(name, condition):
    comment_reasons = {
      2: "Validation failed as banking information not consistent with past invoices",
      4: "Validation failed as phone information not consistent with past invoices",
      3: "Validation failed as email information not consistent with past invoices"}
    
    if condition in [2,3,4]:
        # Set up the SMTP server
        s = smtplib.SMTP(host='smtp.gmail.com', port=587)
        s.starttls()
        s.login('bunatbilly@gmail.com', 'vbqm bjrj uzgg hgud')

        # Create the message
        msg = MIMEMultipart()
        message = f"Validation for {name} failed on {datetime.now()} with reason: {comment_reasons[condition]}"

        # Setup the parameters of the message
        msg['From'] = 'bunatbilly@gmail.com'
        msg['To'] = 'bunatbilly@gmail.com'
        msg['Subject'] = "Failed Invoice Validation"
        
        # Add in the message body
        msg.attach(MIMEText(message, 'plain'))
        
        # Send the message via the server set up earlier.
        s.send_message(msg)
        
        # Terminate the SMTP session and close the connection
        s.quit()


# COMMAND ----------

# Usage
send_email_if_condition_met("Best Buy", 3)

# COMMAND ----------

for i in range(3):
  # Example usage:
  result = validate_vendor_information(df.iloc[i], vendorDF)
  print(result)

# COMMAND ----------

import random
from datetime import datetime, timedelta

# Define company names and their false rate ratios
companies = [
    ('TechSolvers', 0.3),  # 3/10 false rate
    ('FurniSphere', 0.3),  
    ('SafeGuard Pro', 0.2),
    ('EcoCleaners', 0.2),
    ('BotanicHub', 0.2),
    ('BrewedAwake', 0.6),
    ('Ink&Print', 0.7),
    ('WebNest', 0.0),
    ('NetSecureIT', 0.1),  # 1/10 false rate
    ('CanvasCrafters', 0.1)  # 1/10 false rate
]

# Reasons for failed validation
failure_reasons = [
    "Validation failed as banking information not consistent with past invoices",
    "Validation failed as phone information not consistent with past invoices",
    "Validation failed as email information not consistent with past invoices"
]

# Generate a random date between Oct 1, 2023, and Oct 31, 2023
def random_date():
    start_date = datetime(2023, 10, 1)
    end_date = datetime(2023, 10, 31)
    delta = end_date - start_date
    random_days = random.randrange(delta.days)
    return (start_date + timedelta(days=random_days)).strftime('%Y-%m-%d %H:%M:%S')

# Function to generate random cost based on validation success
def random_cost(validation_success):
    if validation_success:
        return round(random.uniform(500, 3500), 2)
    else:
        return round(random.uniform(1000, 5200), 2)

# Generate SQL insert statements
sql_statements = []
for _ in range(100):
    company, false_rate = random.choice(companies)
    validation_success = random.random() >= false_rate
    date_of_check = random_date()
    total_cost = random_cost(validation_success)
    if validation_success:
        comments = 'Validation passed successfully'
    else:
        comments = random.choice(failure_reasons)
    sql_statements.append(
        f"INSERT INTO InvoiceValidations (Company, ValidationSuccess, Comments, DateOfCheck, TotalCost) VALUES ('{company}', {1 if validation_success else 0}, '{comments}', '{date_of_check}', {total_cost});"
    )

# Output the statements to a text file
with open('insert_statements.sql', 'w') as file:
    for statement in sql_statements:
        file.write(statement + '\n')

print(f"Generated {len(sql_statements)} insert statements into 'insert_statements.sql'")


# COMMAND ----------

df