import java.util.Properties
import org.apache.spark.sql._
import org.apache.spark.sql.types._

// Note:
// JDBC driver for SQL Server is already installed in HDI Spark cluster.
// This application is tested with Spark 2.1.0, and should work with other version 2.x or above.
// This app also works with SQL DWH if you just change the url.
// csv read, SparkSession are features supported from 2.x.
// Set appropriate username and password for target database.
// 
object BlobToSqlDwh {
  def main (arg: Array[String]): Unit = {
   
    // SQL DBもしくはSQL DWHへのJDBC文字列
    val url = "jdbc:sqlserver://kshimizu.database.windows.net:1433;database=onedb;user=kshimizu@kshimizu;password={password4db};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
    // CSVのデータセット。データの読み出し元のBlobストレージ
    val dataset = "wasb://nyctaxi@datasetks.blob.core.windows.net/2013/trip_data/trip_data_*.csv" 
    // Spark 2.x以上でSparkSessionがサポートされています。 
    val spark = SparkSession.builder().appName("BlobToSqlDwh").getOrCreate()
 
    // Load csv files and create a DataFrame
    val df = spark.read.options(Map("header" -> "true", "inferSchema" -> "true", "ignoreLeadingWhiteSpace" -> "true")).csv(dataset)
    df.registerTempTable("nyctaxi")

    // Spark SQLでデータの抽出等を行っています。
    val retDf = spark.sql("SELECT * FROM nyctaxi WHERE passenger_count > 0 AND payment_type in ('CSH', 'CRD') AND fare_amount >= 1 AND trip_distance > 0 trip_time_in_secs > 0");
    
    // Write data to SQL Data Warehouse or SQL DB
    // ユーザ名・パスワードは適当に変更してください。
    val prop = new java.util.Properties()
    prop.put("user", "kshimizu")
    prop.put("password", "password4database")
    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver").newInstance() 
    retDf.write.mode("append").jdbc(url, "dbo.trip_data", prop)
  }
}
