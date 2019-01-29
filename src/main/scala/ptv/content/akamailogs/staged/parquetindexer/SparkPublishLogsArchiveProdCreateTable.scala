package ptv.content.akamailogs.staged.parquetindexer

import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime
import ptv.content.akamailogs.staged.parquetindexer.parameters.ParamHdfsDate

object SparkPublishLogsArchiveProdCreateTable extends App {

  override def main(args: Array[String]): Unit = {
    val inputArgs = ParamHdfsDate(args) match {
      case Some(x) => x
      case _ => throw new IllegalArgumentException("Wrong input parameters")
    }

    val sparkBuilder = SparkSession
      .builder()
      .appName("DAZN-OTT - Publish logs-archive-production")
      .config("spark.hadoop.validateOutputSpecs", "false")
      .config("spark.sql.hive.convertMetastoreParquet", "false")
      .config("spark.sql.parquet.compression.codec","gzip")
      .enableHiveSupport()

    val spark = inputArgs.hivemetastore.
      map(uri => sparkBuilder.config("hive.metastore.uris", uri))
      .getOrElse(sparkBuilder)
      .getOrCreate()

    val stagedDataPath = inputArgs.inputpath

    val yesterday = DateTime.now().minusDays(1)

    val eventLogDate: Long = if (inputArgs.processdate.length < 8)
      utils.stringTODateLong(yesterday.toDate().toString, "yyyyMMdd") else utils.stringTODateLong(inputArgs.processdate, "yyyyMMdd")

    println("eventLogDate=" + eventLogDate)

    val eventIdDate = utils.formateDateTime(eventLogDate, "yyyy-MM-dd", "Europe/London")
    val eventDate = utils.formateDateTime(eventLogDate, "yyyyMMdd", "Europe/London")

    println(s"eventIdDate=${eventIdDate} eventDate=${eventDate}")

    executeLogsArchivePublish(spark, eventDate, stagedDataPath)

    spark.stop()
  }

  private def executeLogsArchivePublish(spark: SparkSession, eventdate: String, stagedDataPath: String) = {

    val staged = s"${stagedDataPath}"

    val sql_publish = s"CREATE TABLE published_ott_dazn.massive_elb_logs USING PARQUET PARTITIONED BY (dt) " +
      s"AS SELECT substring(message,0,23) as app_timestamp, substring(message,25,4) as app_level, substring(message,30,6) as app_region, metadata ,logzio_id,beat,input_type,it,logzio_codec,message,offset,source,tags,token,type,dt " +
      s"FROM massive_elb_logs_temp WHERE dt=${eventdate}"

    spark.sql("USE published_ott_dazn")
    val df1 = spark.read.format("parquet").load(staged)
    df1.printSchema()
    df1.createOrReplaceTempView("massive_elb_logs_temp")
    spark.sql(sql_publish)
  }

}
