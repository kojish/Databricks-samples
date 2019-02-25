/*
 *  Note: The following code works with Databricks Runtime Version 5.2. 
 *        The error will be thrown with 5.1, and saying "error: object cosmosdb is not a member of package com.microsoft.azure".
 */

// Databricks notebook source
import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark._
import com.microsoft.azure.cosmosdb.spark.config.Config

// Configure connection to your collection
val readConfig = Map(
  "Endpoint" -> "https://xxxxxxxx.documents.azure.com:443/",
  "Masterkey" -> "taMHODLLfp2b9mcWLQ0rGJgxwkUUd79Q4LW9X9nSdv5l0i4oekVl35pgoY1bpvLElQWed93iLgVMIDY6jjewQ==",
  "Database" -> "iotdata",
  "PreferredRegions" -> "Japan East;Japan West;",
  "Collection" -> "iot"
)
val config = Config(readConfig)

// Connect via azure-cosmosdb-spark to create Spark DataFrame
// 
val df = spark.sqlContext.read.cosmosDB(config)
// The following code works as well for creating the DataFrame
//val df = spark.read.cosmosDB(config)

// COMMAND ----------

df.count()

// COMMAND ----------

df.show()
