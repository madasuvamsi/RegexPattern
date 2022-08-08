# Databricks notebook source
print("Hello")

# COMMAND ----------

# MAGIC %sql
# MAGIC use database Dev

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Employee

# COMMAND ----------

#convert table to a dataframe
df = sqlContext.table("Employee")

# COMMAND ----------

df.show()

# COMMAND ----------

#create a temporry view from the data frame
df.createOrReplaceTempView("testEmpView")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from testEmpview

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from testempview

# COMMAND ----------

# MAGIC %sql
# MAGIC select Name,Email, REGEXP_REPLACE(REGEXP_REPLACE(Email,'DevID','ProdID'),'Test.com','Prod.com') AS NewEmail FROM testempview

# COMMAND ----------

#Convert the result back to a dataframe
Finaldf = spark.sql("select Name, REGEXP_REPLACE(Email,'DevID','ProdID') AS NewEmail FROM testempview")

# COMMAND ----------

Finaldf.show()

# COMMAND ----------

# steps
# create a temperory view on top of Dataframe
# Use %sql magic command to convert the cell to SQL format
# Use sql regex replace function to replace the string with the the requried pattern
# Convert the result back to a dataframe

