package ptv.content.akamailogs.staged.parquetindexer.parameters

import org.apache.spark.sql.catalyst.expressions.Coalesce

/**
  * Created by joao.cerqueira on 19/03/2017.
  */
case class ParamHdfsDate(processdate:String = null, inputpath:String = null, outputpath:String = null, coalescefactor:String=null, hivemetastore: Option[String] = None)

object ParamHdfsDate {

  def apply(args: Array[String]): Option[ParamHdfsDate] = {

    val parser = new scopt.OptionParser[ParamHdfsDate]("SparkAkamaiLogIndexer") {

      opt[String]("processdate")
        .action((x, c) => c.copy(processdate = x))
        .required()
        .text("YYYY-MM-DD date Input HDFS directory")

      opt[String]("inputpath")
        .action((x, c) => c.copy(inputpath = x))
        .required()
        .text("Input HDFS directory")

      opt[String]("outputpath")
        .action((x, c) => c.copy(outputpath = x))
        .required()
        .text("Output HDFS directory")

      opt[String]("coalescefactor")
        .action((x,c) => c.copy(coalescefactor = x))
        .optional()
        .text("Optional coalesce factor")

      opt[String]("hivemetastore")
        .action((x, c) => c.copy(hivemetastore = Option(x)))
        .optional()
        .text("Optional hive metastore uri")

    }

    parser.parse(args, ParamHdfsDate())
  }
}
