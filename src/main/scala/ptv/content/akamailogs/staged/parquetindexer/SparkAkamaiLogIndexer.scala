package ptv.content.akamailogs.staged.parquetindexer

import org.slf4j.LoggerFactory
import org.apache.spark.sql.{ SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import ptv.content.akamailogs.staged.parquetindexer.parameters.ParamHdfsDate

/**
  * Created by joao.cerqueira on 24/01/2017.
  *
  **/
object SparkAkamaiLogIndexer extends App {

  val log = LoggerFactory.getLogger(this.getClass.getName)

  override def main(args: Array[String]) {

    val inputArgs = ParamHdfsDate(args) match {
      case Some(x) => x
      case _ => throw new IllegalArgumentException("Wrong input parameters")
    }

    // Cluster master types
    // val cluster = "local" // -> Laptop
    // val cluster = "client" // -> package run client
    // val cluster = "cluster" // -> package run Cluster

    val spark = SparkSession
      .builder()
      .appName("DAZN-OTT - Spark - Log Akamai Transformer")
      .config("spark.hadoop.validateOutputSpecs", "false")
      .config("spark.sql.parquet.compression.codec","gzip")
      .config("spark.sql.hive.convertMetastoreParquet","false")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    /*
    val sparkConfig = new SparkConf().setAppName("Spark - Log Akamai Transformer ")
      .set("spark.hadoop.validateOutputSpecs", "false")
    //.setMaster(cluster)

    val sparkContext = new SparkContext(sparkConfig)
    val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)

    sqlContext.setConf("spark.sql.parquet.compression.codec","gzip")
    sqlContext.setConf("spark.sql.hive.convertMetastoreParquet","false")
    */

    // Yesterday
    val yesterday = DateTime.now().minusDays(1)

    val eventLogDate:Long = if (inputArgs.processdate.length < 10)
      utils.stringTODateLong(yesterday.toDate().toString,"yyyyMMdd") else utils.stringTODateLong(inputArgs.processdate,"yyyy-MM-dd")

    println("eventLogDate=" + eventLogDate)

    val eventFormatDate1 = DateTimeFormat.forPattern("yyyyMMdd")
    val eventFormatDate2 = DateTimeFormat.forPattern("yyyy-MM-dd")

    val eventIdDate = utils.formateDateTime(eventLogDate, "yyyyMMdd", "Europe/London")
    val eventDate = utils.formateDateTime(eventLogDate, "yyyy-MM-dd", "Europe/London")

    println(s"eventIdDate=${eventIdDate} eventDate=${eventDate}")

    val dataRaw = if(inputArgs.inputpath.length > 0) inputArgs.inputpath else s"/data/raw/akamai/dazn_logs/dt=${eventDate}/id=*/hh=*/*"

    val dataStaged = if(inputArgs.outputpath.length > 0) inputArgs.outputpath else "/data/staged/akamai/spark/dazn_logs/"

    executionRawStagedJob(spark,5,eventDate,dataRaw,dataStaged)

    spark.stop()
  }

  def executionRawStagedJob(spark: SparkSession, inputRepartition: Int, eventDate: String, dataRaw:String, dataStaged:String): Unit = {

    // Number partitions in Output
    val numRepartition = inputRepartition

    println(s"numRepartition=${inputRepartition}  eventDate=${eventDate}")

    import spark.implicits._

    val myToInt = udf(utils.nullValueToMinus)
    val myStringTake2 = udf(utils.stringTakeN(_: String, 2))
    val myStringTake10 = udf(utils.stringTakeN(_: String, 10))
    val myConvertDateLong = udf(utils.stringTODateLong(_: String, "YYYY-MM-DD"))

    // Counter Switched ON/OFF for Speed - Start
    val datadf1 = spark.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", "\t")
      .option("inferSchema", "true")
      .load(dataRaw)
      .persist(newLevel = StorageLevel.MEMORY_AND_DISK)

    datadf1.printSchema()

    val totalData1 = datadf1.count()
    println(s" DAILY - TOTAL RECORDS : ${totalData1}")

    datadf1.unpersist()
    // Counter Switched ON/OFF for Speed - End

    val dailyfilter = spark.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", "\t")
      .option("inferSchema", "true")
      .load(dataRaw)
      .filter("date IS NOT NULL ")
      .withColumn("dt", myStringTake10(col("date")))
      .withColumn("hh", myStringTake2(col("time")))
      .filter("dt NOT IN ('-1') AND hh NOT IN ('-1') ")
      .persist(newLevel = StorageLevel.MEMORY_AND_DISK)

    dailyfilter.printSchema()

    val totalData2 = dailyfilter.count()
    println(s" DAILY FILTER date IS NOT NULL - TOTAL RECORDS : ${totalData2}")

    // save Daily results in paquet format
    val dailySave = dailyfilter
      //.repartition(numRepartition) // too much memory overhead
      .write.mode("append").partitionBy("dt", "hh").parquet(s"${dataStaged}")

  }

}
