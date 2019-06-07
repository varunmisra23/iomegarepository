// Databricks notebook source
val secret = dbutils.secrets.get(scope="sql-credentials",key="sqlpassword")

// COMMAND ----------

import com.microsoft.azure.sqldb.spark.config.Config
import com.microsoft.azure.sqldb.spark.connect._

// COMMAND ----------



val config = Config(Map(
  "url"            -> "iomegasqldbserverv.database.windows.net",
  "databaseName"   -> "iomegasqldatabasev",
  "dbTable"        -> "dbo.Customers",
  "user"           -> "varun",
  "password"       -> secret,
  "connectTimeout" -> "5", 
  "queryTimeout"   -> "5"  
))

val collection = spark.read.sqlDB(config)
collection.printSchema
collection.createOrReplaceTempView("customers")

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT * FROM customers

// COMMAND ----------

val config = Config(Map(
  "url"            -> "iomegasqldbserverv.database.windows.net",
  "databaseName"   -> "iomegasqldatabasev",
  "dbTable"        -> "dbo.Locations",
  "user"           -> "varun",
  "password"       -> secret,
  "connectTimeout" -> "5", 
  "queryTimeout"   -> "5"  
))

val collection1 = spark.read.sqlDB(config)
collection1.printSchema
collection1.createOrReplaceTempView("locations")

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT * FROM Locations

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT C.CustomerId, CONCAT(FName, " ", MName, " ", LName) AS FullName,
// MAGIC   C.CreditLimit, C.ActiveStatus, L.City, L.State, L.Country
// MAGIC FROM Customers C
// MAGIC INNER JOIN Locations L ON C.LocationId = L.LocationId
// MAGIC WHERE L.State IN ("AP","TN","KA")

// COMMAND ----------

val dlsSecret = dbutils.secrets.get(scope = "trainingscope", key = "adbclientaccesskey")
val configs = Map(
  "fs.azure.account.auth.type" -> "OAuth",
  "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id" -> "fb11cda6-0683-4520-9ed6-02e76700e727",
  "fs.azure.account.oauth2.client.secret" -> dlsSecret,
  "fs.azure.account.oauth2.client.endpoint" -> "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token")

dbutils.fs.mount(
  source = "abfss://data@iomegastorageaccountv.dfs.core.windows.net/",
  mountPoint = "/mnt/dlsdata", extraConfigs = configs)

// COMMAND ----------

// MAGIC     %fs
// MAGIC 
// MAGIC ls /mnt/dlsdata/salesfiles/

// COMMAND ----------

import org.apache.spark.sql._
import org.apache.spark.sql.types._

val fileNames = "/mnt/dlsdata/salesfiles/*.csv"
val schema = StructType(
Array(
StructField("SaleId",IntegerType,true),
StructField("SaleDate",IntegerType,true),
StructField("CustomerId",DoubleType,true),
StructField("EmployeeId",DoubleType,true),
StructField("StoreId",DoubleType,true),
StructField("ProductId",DoubleType,true),
StructField("NoOfUnits",DoubleType,true),
StructField("SaleAmount",DoubleType,true),
StructField("SalesReasonId",DoubleType,true),
StructField("ProductCost",DoubleType,true)
)
)

val data = spark.read.option("inferSchema",false).option("header","true").option("sep",",").schema(schema).csv(fileNames)

data.printSchema
data.createOrReplaceTempView("factsales")

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE TEMP View ProcessedResults AS
// MAGIC   SELECT C.CustomerId, CONCAT(FName, " ", MName, " ", LName) AS FullName,
// MAGIC   C.CreditLimit, C.ActiveStatus, L.City, L.State, L.Country,
// MAGIC   S.SaleAmount, S.NoOfUnits
// MAGIC FROM Customers C
// MAGIC INNER JOIN Locations L ON C.LocationId = L.LocationId
// MAGIC INNER JOIN factsales S on S.CustomerId = C.CustomerId
// MAGIC WHERE L.State in ( "AP", "TN" )
// MAGIC     

// COMMAND ----------

val results = spark.sql("SELECT * FROM ProcessedResults")

results.printSchema
results.write.mode("append").parquet("/mnt/dlsdata/parque/sales")



// COMMAND ----------

val processedSales = spark.read.format("parquet").option("header", "true").option("inferschema", "true").load("/mnt/dlsdata/parque/sales")
processedSales.printSchema
processedSales.show(100, false)

// COMMAND ----------

// DBTITLE 1,DW and Polybase configuration for bulk updates
val blobStorage = "iomegastorageblobv.blob.core.windows.net"
	val blobContainer = "tempdata"
	val blobAccessKey =  "WfKLrkkuCR6YUevYXISnQjQ50vY6kENZmT0gwFiasfCU+gngMJ6D9ivAzz8uVFsThCbz6ckpfMeiPhvo6LqVvw=="
	val tempDir = "wasbs://" + blobContainer + "@" + blobStorage +"/tempDirs"

// COMMAND ----------

val acntInfo = "fs.azure.account.key."+ blobStorage
sc.hadoopConfiguration.set(acntInfo, blobAccessKey)

// COMMAND ----------

val dwDatabase = "iomegadatawarehousev"
	val dwServer = "iomegasqldbserverv.database.windows.net"
	val dwUser = "varun"
	val dwPass = secret
	val dwJdbcPort =  "1433"
	val sqlDwUrlSmall = "jdbc:sqlserver://" + dwServer + ":" + dwJdbcPort + ";database=" + dwDatabase + ";user=" + dwUser+";password=" + dwPass

	spark.conf.set(
		"spark.sql.parquet.writeLegacyFormat",
		"true")

// COMMAND ----------

results.write
    .format("com.databricks.spark.sqldw")
    .option("url", sqlDwUrlSmall) 
    .option("dbtable", "ProcessedResults")
    .option("forward_spark_azure_storage_credentials","True")
    .option("tempdir", tempDir)
    .mode("overwrite")
    .save()