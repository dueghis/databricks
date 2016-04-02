// Databricks notebook source exported at Sat, 2 Apr 2016 22:34:14 UTC
// MAGIC %md
// MAGIC # **Introduction to Spark SQL**
// MAGIC This notebook shows an example of how to access your S3 data and register a Spark SQL table to access.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### To see how this example works, you can clone the notebook and use the `Run All` button above.

// COMMAND ----------

// MAGIC %md
// MAGIC ### **Create a DBFS Mount**
// MAGIC 
// MAGIC DBFS is the default filesystem in Databricks. Creating a mount in DBFS provides an easy (and authenticated) way to work with your own S3 buckets.

// COMMAND ----------

// MAGIC %md
// MAGIC ### **Get Your S3 Keys Set-up**
// MAGIC 
// MAGIC In the *Keys* cell called below, there are some parameters set for AWS credentials that are used later in this notebook. You can adopt the same practice to keep keys in a different notebook, so it is easy to update/revoke them in a single place. However, it is just as easy to set the key variables directly in this notebook as well.  
// MAGIC 
// MAGIC **IMPORTANT** - You should leave the cell below collapsed so nobody sees your keys when passing by.

// COMMAND ----------

// Set-up Your Keys Below, you can delete them after you mount --------------------------------
import java.net.URLEncoder
val default_aws_key = "AKIAJNCDPH33SE6XVZHQ"
val default_aws_secret = "ikLXWVvCT2+89kTah7VEiHuvN/yvtl/DIgpTti2P".replace("/", "%2F")
val encoded_default_aws_secret =  URLEncoder.encode(default_aws_key, "UTF-8")

// COMMAND ----------

// MAGIC %md
// MAGIC ### ** Mount the Bucket in to DBFS** 
// MAGIC The command below uses the keys that were instantiated from the command above, and mounts an S3 bucket in DBFS. Note the s3 URI concatenates the keys using string interpolation.

// COMMAND ----------

dbutils.fs.help() //Leave this cell collapsed when not reading the man page

// COMMAND ----------

dbutils.fs.help("mount") //Leave this cell collapsed when not reading the man page

// COMMAND ----------

try { 
  dbutils.fs.unmount("/mnt/vd") //Unmount if previously run 
} catch { case _ : Throwable => true}

// COMMAND ----------

dbutils.fs.mount(s"s3n://$default_aws_key:$encoded_default_aws_secret@dse-team1-2014", "/mnt/vd")

// COMMAND ----------

// MAGIC %md
// MAGIC ### **Inspect the Data in S3**
// MAGIC Runs some basic commands to take a look at the .

// COMMAND ----------

display(dbutils.fs.ls("/mnt/vd"))

// COMMAND ----------

val diamonds_data = sc.textFile("/mnt/databricks-public-datasets/dimonds/diamonds.csv")

// COMMAND ----------

diamonds_data.count()

// COMMAND ----------

diamonds_data.take(5).foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC ### **Create the Table Metadata Using HiveQL**
// MAGIC * Optionally, Hive SerDe's can also be used, such as the built in RegexSerDe or upload a library with a third party SerDe.

// COMMAND ----------

// MAGIC %sql 
// MAGIC DROP TABLE IF EXISTS diamonds_data 

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE EXTERNAL TABLE diamonds_data (
// MAGIC   id int,
// MAGIC   carat double,
// MAGIC   cut String,
// MAGIC   color String,
// MAGIC   clarity string,
// MAGIC   depth double,
// MAGIC   tble int,
// MAGIC   price int,
// MAGIC   x double,
// MAGIC   y double,
// MAGIC   z double
// MAGIC )
// MAGIC ROW FORMAT DELIMITED
// MAGIC FIELDS TERMINATED BY ','
// MAGIC LOCATION "dbfs:/mnt/databricks-public-datasets/dimonds/diamonds.csv"
// MAGIC TBLPROPERTIES ("skip.header.line.count"="1");

// COMMAND ----------

// MAGIC %md
// MAGIC ### **Test Your Table by running a SQL select statement**

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from diamonds_data