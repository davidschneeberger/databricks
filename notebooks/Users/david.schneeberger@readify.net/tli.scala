// Databricks notebook source
dbutils.fs.mount(
  source = "wasbs://tli@dsazdemosatli.blob.core.windows.net/",
  mountPoint = "/mnt/tli/",
  extraConfigs = Map("fs.azure.account.key.dsazdemosatli.blob.core.windows.net" -> "h0Kr4qtS98/mzi0DEmvi1mccYOEws4frBaz3EnEJWaR4FneB+uk/90AD7kaviiPr9a8J0Gj+24E67cWDdaVeDQ=="))

// COMMAND ----------

// MAGIC %sql 
// MAGIC USE LocationInsights;
// MAGIC DROP TABLE IF EXISTS UseCase1Data; 
// MAGIC CREATE TABLE UseCase1Data 
// MAGIC  USING csv
// MAGIC OPTIONS (path "/mnt/tli/uc1/", header "true");

// COMMAND ----------

// MAGIC %python
// MAGIC dataFrame = "/mnt/tli/uc1/14_201709_anon.csv"
// MAGIC spark.read.format("csv").option("header","true")\
// MAGIC   .option("inferSchema", "true").load(dataFrame)\
// MAGIC   .write.saveAsTable("LocationInsights.UseCase1Data")

// COMMAND ----------

// MAGIC %sql 
// MAGIC USE LocationInsights;
// MAGIC SELECT date, purpose_of_trip, SUM(anon_count) AS Count
// MAGIC FROM UseCase1Data
// MAGIC WHERE anon_count <> "<10"
// MAGIC GROUP BY date, purpose_of_trip