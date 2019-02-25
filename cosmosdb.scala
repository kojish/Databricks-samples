// Databricks notebook source
// Import Necessary Libraries
import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark._
import com.microsoft.azure.cosmosdb.spark.config.Config

// Configure connection to your collection
val readConfig = Map(
  "Endpoint" -> "https://kojis.documents.azure.com:443/",
  "Masterkey" -> "tLaMHODLLfp2b9mcWLQ0rGJgxwkUUd79Q4LW9X9nSdv5l0i4oekVl35pgoY1bpvLElQWed93iLgVMIDY6jjewQ==",
  "Database" -> "iotdata",
  "PreferredRegions" -> "Japan East;Japan West;",
  "Collection" -> "iot"
)
val config = Config(readConfig)

// Connect via azure-cosmosdb-spark to create Spark DataFrame
val df = spark.sqlContext.read.cosmosDB(config)

// COMMAND ----------

df.count()

// COMMAND ----------

df.show()
