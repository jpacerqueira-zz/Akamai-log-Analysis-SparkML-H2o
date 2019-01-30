package ptv.content.akamailogs.staged.parquetindexer

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.lit
import org.apache.spark.storage.StorageLevel
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import ptv.content.akamailogs.staged.parquetindexer.parameters.ParamHdfsDate
import ptv.content.akamailogs.staged.parquetindexer.DataFrameUtils._

object SparkStageLogsArchiveProd extends App{

  override def main(args: Array[String]): Unit = {

    val inputArgs = ParamHdfsDate(args) match {
      case Some(x) => x
      case _ => throw new IllegalArgumentException("Wrong input parameters")
    }

    val spark = SparkSession
      .builder()
      .appName("DAZN-OTT - Stage logs-archive-production")
      .config("spark.hadoop.validateOutputSpecs", "false")
      .config("spark.sql.hive.convertMetastoreParquet", "false")
      .config("spark.sql.parquet.compression.codec","gzip")
      .getOrCreate()

    // Yesterday
    val yesterday = DateTime.now().minusDays(1)

    val eventLogDate:Long = if (inputArgs.processdate.length < 8)
      utils.stringTODateLong(yesterday.toDate().toString,"yyyyMMdd") else utils.stringTODateLong(inputArgs.processdate,"yyyyMMdd")

    println("eventLogDate=" + eventLogDate)

    val eventFormatDate1 = DateTimeFormat.forPattern("yyyyMMdd")
    val eventFormatDate2 = DateTimeFormat.forPattern("yyyy-MM-dd")

    val eventIdDate = utils.formateDateTime(eventLogDate, "yyyy-MM-dd", "Europe/London")
    val eventDate = utils.formateDateTime(eventLogDate, "yyyyMMdd", "Europe/London")

    println(s"eventIdDate=${eventIdDate} eventDate=${eventDate}")

    val rawDataPath =
      if(inputArgs.inputpath.length > 0) inputArgs.inputpath + s"dt=${eventDate}/*.gz"
      else s"/data/raw/ott_dazn/logs-archive-production/gzip/dt=${eventDate}/*.gz"

    val stagedDataPath =
      if(inputArgs.outputpath.length > 0) inputArgs.outputpath
      else "/data/staged/ott_dazn/logs-archive-production/parquet"

    val coalescefactorPath: Int  = utils.mytoInt(inputArgs.coalescefactor) match {
      case Some(x) => x
      case None => 144
    }

    println(s"coalescefactorPath=${coalescefactorPath}")

    executionRawStagedJob(spark,eventDate,rawDataPath,stagedDataPath,coalescefactorPath)

    spark.stop()

  }

  def executionRawStagedJob(spark: SparkSession, eventDate: String, rawDataPath: String, stagedDataPath: String,coalescefactor: Int) : Unit = {

    import DataFrameUtils._

    // 16Aug2018 - Repartition 1000 to account for 1.1GB file sizes
    val df1: DataFrame =spark.read.json(rawDataPath).repartition(1000)
    df1.printSchema()

    val df2=df1
      // start - workarround 20June2018
      // .withColumnRenamed("__logzio_id","logzio_id")
      // |-- __logz_account_id: string (nullable = true)
      // |-- __logz_received_timestamp: string (nullable = true)
      // |-- __logzio_X-B3-Sampled: string (nullable = true)
      // |-- __logzio_X-B3-SpanId: string (nullable = true)
      // |-- __logzio_X-B3-TraceId: string (nullable = true)
      .filter(df1.col("@metadata").isNotNull)
      .filter(df1.col("@timestamp").isNotNull)
      .filter(df1.col("message").isNotNull)
      // Duplicate Timestamp only has only drop method capable to fix issue
      // .withColumnRenamed("@timestamp","timestamp")
      .drop("@timestamp")
      .drop("__logz_account_id").drop("__logz_received_timestamp")
      .drop("__logzio_X-B3-Sampled").drop("__logzio_X-B3-SpanId").drop("__logzio_X-B3-TraceId")
      .withColumnRenamed("__logzio_id","logzio_id")
      // end - workarround 20June2018
      .withColumnRenamed("@metadata","metadata")
      .withColumn("token", lit(null).cast(StringType))
      .withColumn("dt",lit(s"${eventDate}").cast(StringType))
      // workarround for metadata.version introduced in 19April2018
      // Workarround for Volume with coalesce 144 instead of 72
      // Made CoalesceFactor Dynami depending on Data volume evaluation //17Aug2018
      .dropNestedColumn("metadata.version").coalesce(coalescefactor).persist(newLevel = StorageLevel.MEMORY_AND_DISK)
    df2.printSchema()

    df2.write.mode("append").partitionBy("dt").parquet(stagedDataPath)

  }
}
