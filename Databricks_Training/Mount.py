# Databricks notebook source
dbutils.fs.mount(
  source = "wasbs://sfmcontainer@sfmdatabricks.blob.core.windows.net",
  mount_point = "/mnt/sfmdatabricks/sfmcontainer",
  extra_configs = {"fs.azure.account.key.sfmdatabricks.blob.core.windows.net":"accesskey"})
