package com.igniteplus.data.pipeline.service

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.igniteplus.data.pipeline.constants.ApplicationConstants.{INPUT_LOCATION_CLICKSTREAM, READ_EXCEPTION_FILE}
import com.igniteplus.data.pipeline.exception.FileReadException

object FileReaderService {
  def readFile(path:String
              ,fileFormat:String)
              (implicit spark:SparkSession): DataFrame = {

    val dfReadData: DataFrame =
      try {
        spark.read.option("header","true").format(fileFormat).load(path)
      }
      catch {
        case e: Exception =>
          FileReadException("unable to read files in the given location " + s"$INPUT_LOCATION_CLICKSTREAM")
        //  FileWriterService.writeFile(e.toString,READ_EXCEPTION_FILE)
          spark.emptyDataFrame

      }

    val dfDataCount: Long = dfReadData.count()

    if(dfDataCount == 0) {

      throw FileReadException("No files read from the file reader " + s"$INPUT_LOCATION_CLICKSTREAM")

    }
    else
      {
        println(dfReadData)
      }


    dfReadData
  }
}
