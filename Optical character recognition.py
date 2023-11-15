# Databricks notebook source
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

extracted_texts[0]

# COMMAND ----------

# saving list to DBFS

with open("/dbfs/mnt/invoice_images/invoice_extracted_text.txt", "w") as file:
    for item in extracted_texts:
        file.write(extracted_texts[0])