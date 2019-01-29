package ptv.content.akamailogs.staged.parquetindexer

import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.joda.time.{DateTime, DateTimeZone}

/**
  * Created by joao.cerqueira on 24/01/2017.
  */
class utils {}
object utils {

  val tsTODateStr: (Long => String) = (time: Long) => {
    val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd")
    sdf.format(new java.util.Date(time))
  }


  val nullValueToUnknown: (String => String) = (dataItem: String) => {
    val dataOut = if (dataItem == null || dataItem.isEmpty ||
      dataItem.length < 1 || dataItem.toLowerCase.contains("null")) "(unknown)" else dataItem
    dataOut
  }

  val nullValueToMinus: (String => Int) = (dataItem: String) => {
    val dataOut = try {
      dataItem.toInt
    } catch {
      case e: Exception => -1
    }
    dataOut
  }

  val stringTakeN: ( (String,Int) => String) = (dataItem:String, nDigits:Int) => {
    val dataOut = try {
      dataItem.toString.take(nDigits)
    } catch {
      case e: Exception => "-1"
    }
    dataOut
  }


  /** *
    * convert long to formatted datetime string based on timezone
    */
  val formateDateTime: (Long, String, String) => String =
    (epocTime: Long, dtFormat: String, timezone: String) => {
      val fmt = if (dtFormat != null) dtFormat else "yyyy-MM-dd HH:mm:ss"

      val dtFormatter: DateTimeFormatter = {
        if (timezone != null) DateTimeFormat.forPattern(dtFormat).withZone(DateTimeZone.forID(timezone))
        else DateTimeFormat.forPattern(fmt)
      }

      new DateTime(epocTime).toString(dtFormatter)
    }



  /** *
    * convert long to formatted datetime string as yyyy-MM-dd HH:mm:ss in the time zone of Europe/London
    */
  val defaultFormateDateTime: (Long) => String = {
    (epocTime: Long) => {
      formateDateTime(epocTime, "yyyy-MM-dd HH:mm:ss", "Europe/London")
    }
  }


  /**
    * Extract date from date time
    */
  val extractDateStrFromLogDateTime: (String, String, String, String) => String =
    (formattedDT: String, fromFormat: String, timezone: String, toFormat: String) => {
      val fromFmt = DateTimeFormat.forPattern(fromFormat).withZone(DateTimeZone.forID(timezone))
      val toFmt = DateTimeFormat.forPattern(toFormat).withZone(DateTimeZone.forID(timezone))
      fromFmt.parseDateTime(formattedDT).toString(toFmt)
    }


  val extractDateStrFromDefaultLogDateTime: (String) => String =
    (formattedDT: String) => extractDateStrFromLogDateTime(formattedDT, "yyyy-MM-dd HH:mm:ss", "Europe/London", "yyyy-MM-dd")


  val stringTODateStr: (String => String) = (timestamp: String) => {

    try{
      val inputDate=timestamp.split(" ")
      val incomingFormat = new java.text.SimpleDateFormat("dd-MM-yyyy")
      val outgoingFormat = new java.text.SimpleDateFormat("yyyy-MM-dd")
      outgoingFormat.format(incomingFormat.parse(inputDate(0)))
    } catch  {
      case e : Exception => "2000-01-01"
    }
  }

  val stringTODateLong: ((String, String) => Long) = (timestamp: String, format: String) => {

    try{
      val inputDate=timestamp.split(" ")
      val incomingFormat = new java.text.SimpleDateFormat(format)
      val outgoingFormat = new java.text.SimpleDateFormat("yyyyMMdd")
      val outdate = outgoingFormat.format(incomingFormat.parse(inputDate(0)))
      DateTimeFormat.forPattern("yyyyMMdd").parseDateTime(outdate).getMillis()
    } catch  {
      case e : Exception => {
        val inputDate=timestamp.split("2000-01-01 00:00:00")
        val incomingFormat = new java.text.SimpleDateFormat("yyyy-MM-dd")
        val outgoingFormat = new java.text.SimpleDateFormat("yyyyMMdd")
        val outdate = outgoingFormat.format(incomingFormat.parse(inputDate(0)))
        DateTimeFormat.forPattern("yyyyMMdd").parseDateTime(outdate).getMillis()
      }
    }
  }

  val stringToDateTime: ((String, String, String) => DateTime) = (dtString: String, fmt: String, timezone: String) => {
    val fromFmt = DateTimeFormat.forPattern(fmt).withZone(DateTimeZone.forID(timezone))
    fromFmt.parseDateTime(dtString)
  }


  val stringDateToStringDate: (String => String) = (inputdate: String) => {

    val incomingFormat = new java.text.SimpleDateFormat("yyyyMMdd")
    val outgoingFormat = new java.text.SimpleDateFormat("yyyy-MM-dd")

    val outdate = outgoingFormat.format(incomingFormat.parse(inputdate))
    outdate
  }

  val mytoInt: (String => Option[Int]) = (inputInt:String) => {
    try {
      Some(inputInt.toInt)
    } catch {
      case e: Exception => None
    }
  }

}
