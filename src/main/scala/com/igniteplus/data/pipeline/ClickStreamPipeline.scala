package com.igniteplus.data.pipeline

import com.igniteplus.data.pipeline.constants.ApplicationConstants.{JSON_FORMAT, READ_EXCEPTION_FILE, SAVE_FILE_MODE}
import com.igniteplus.data.pipeline.exception.{FileReadException, FileWriteException}
import com.igniteplus.data.pipeline.service.{FileWriterService, PipelineService}
import com.sun.org.slf4j.internal
import com.sun.org.slf4j.internal.LoggerFactory
import org.apache.spark.internal._

object ClickStreamPipeline extends Logging {
  def main(args: Array[String]): Unit = {

    val logger :internal.Logger = LoggerFactory.getLogger(this.getClass)
    val exception = try {
       PipelineService.executePipeline()
    }

    catch {
      case e: FileReadException =>
        //logger.error("File read exception")
        logError("File read exception ", e)
        FileWriterService.writeFile(e.toString,READ_EXCEPTION_FILE)

      case e: FileWriteException =>

        logError("File write exception ", e)




      case e: Exception =>
       // logger.error("Unknown exception")
      logError("Unknown exception ", e)
    }
   // FileWriterService.writeFile(exception.toString,READ_EXCEPTION_FILE)
  }
}
