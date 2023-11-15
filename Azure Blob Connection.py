# Databricks notebook source
dbutils.fs.mount(
  source = "wasbs://invoicecontainer@invoices1.blob.core.windows.net/",
  mount_point = "/mnt/invoice_images",
  extra_configs = {
    "fs.azure.account.key.invoices1.blob.core.windows.net":"cc6Ym3mWc1rs5ShbZFYWuqO9oAtCvGtSx4asuXt+4bCjbWa8WSdpwusH872IiJxs+9T7qvZ7NCMC+AStK4lwHA=="
   }
)

dbutils.fs.ls("/mnt/invoice_images")