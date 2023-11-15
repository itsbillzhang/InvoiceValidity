# Databricks notebook source


with open("/dbfs/mnt/invoice_images/validation_results_name_result.txt", "r") as file:
    data = file.read().split(',')
    read_output = (data[0], int(data[1]))

# COMMAND ----------

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime

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

send_email_if_condition_met(read_output[0], read_output[1])