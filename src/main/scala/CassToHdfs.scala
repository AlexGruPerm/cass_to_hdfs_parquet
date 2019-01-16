import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.LoggerFactory

//import com.datastax.spark.connector.cql.CassandraConnectorConf
//import com.datastax.spark.connector.rdd.ReadConf
//import com.datastax.spark.connector._

object otocLogg extends Serializable {
  @transient lazy val log = LoggerFactory.getLogger(getClass.getName)
}



object CassToHdfs extends App {
  otocLogg.log.info("BEGIN [CassToHdfs]")

  case class T_DAT_META(ticker_id: Int, ddate :java.sql.Date)

  val spark = SparkSession.builder()
    .appName("CassToHdfs")
    .config("spark.cassandra.connection.host","193.124.112.90")
    .config("spark.jars", "/root/casstohdfs_v1.jar")
    .getOrCreate()

  import com.datastax.spark.connector.cql.CassandraConnectorConf
  import org.apache.spark.sql.cassandra._
  spark.setCassandraConf("Test Cluster", CassandraConnectorConf.ConnectionHostParam.option("193.124.112.90"))

  def getTicksFromCassByTickerDate(TickerID :Int,Ddate :java.sql.Date) = {
    import org.apache.spark.sql.functions._
    spark.read.format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "ticks", "keyspace" -> "mts_src"))
      .option("fetchSize", "10000")
      .load()
      .where(col("ticker_id") === TickerID)
      .where(col("ddate") === Ddate)
      .select(col("ticker_id"), col("ddate"), col("db_tsunx"), col("ask"), col("bid"))
      .sort(asc("db_tsunx"))
  }

  def getMetadata = {
    import org.apache.spark.sql.functions._
    spark.read.format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "ticks", "keyspace" -> "mts_src"))
      .option("fetchSize", "100")
      .load()
      .select(col("ticker_id"),col("ddate"))
      .distinct
      .sort(asc("ticker_id"))
      .collect
      .toSeq
  }

  val t1_common = System.currentTimeMillis

  val dsMetadata = getMetadata

  otocLogg.log.info("dsMetadata Count :" + dsMetadata.size + " rows.")

  for (thisTickerDate <- dsMetadata.map(row => T_DAT_META(row.getInt(0),row.getDate(1))))  {
    println("--------------------------------------------------------------------------------------")
    println("BEGIN TICKER_ID="+thisTickerDate.ticker_id + " DDATE=" + thisTickerDate.ddate)
    val dsTicks = getTicksFromCassByTickerDate(thisTickerDate.ticker_id,thisTickerDate.ddate)
    dsTicks.cache()
    val cntRows = dsTicks.count()
    dsTicks.write.mode(SaveMode.Overwrite).parquet("hdfs://hdpnn:9000/user/tickers/ticks.parquet/ticker_id="+thisTickerDate.ticker_id+"/ddate="+thisTickerDate.ddate)
    println("INSERTED = "+ cntRows +" ROWS.")
    println("--------------------------------------------------------------------------------------")
  }

  val t2_common = System.currentTimeMillis
  otocLogg.log.info("================== SUMMARY ========================================")
  otocLogg.log.info(" DURATION :"+ ((t2_common - t1_common)/1000.toDouble) + " sec.")
  otocLogg.log.info("================== END [OraToCass] ================================")
}
