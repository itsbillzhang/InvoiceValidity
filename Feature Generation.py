# Databricks notebook source
with open("/dbfs/mnt/invoice_images/invoice_extracted_text.txt", "r") as file:
    extracted_texts = file.read()

# COMMAND ----------

extracted_texts

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
df = extract_invoice_features(extracted_texts)
print(df)

# COMMAND ----------

df.to_csv("/dbfs/mnt/invoice_images/extracted_invoice.csv", index=False)