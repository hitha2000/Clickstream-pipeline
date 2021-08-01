package com.igniteplus.data.pipeline.service

import com.igniteplus.data.pipeline.constants.ApplicationConstants. INPUT_WRITE_DATA
import com.igniteplus.data.pipeline.exception.FileWriteException
import org.apache.spark.sql.{DataFrame, SparkSession}

object FileWriterService {

  def writeFile(df:DataFrame,
                fileFormat:String,
                fileSaveMode:String,
                path:String)(implicit spark:SparkSession): Unit = {


      try {
        df.write.format(fileFormat).mode(fileSaveMode).save(path)

      }
      catch {
        case e: Exception =>
          FileWriteException("unable to write files in the given location " + s"$INPUT_WRITE_DATA")

      }



//    if(dfWriteData) {
//
//      throw FileWriteException("No files read from the file reader " + s"$INPUT_WRITE_DATA")
//    }
//    else
//    {
//      println(dfWriteData)
//    }

 //   dfWriteData

  }
}
