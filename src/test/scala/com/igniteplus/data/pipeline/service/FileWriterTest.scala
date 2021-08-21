package com.igniteplus.data.pipeline.service
import com.igniteplus.data.pipeline.service.FileReaderService.readFile
import com.igniteplus.data.pipeline.service.FileWriterService.writeFile
import com.igniteplus.data.pipeline.helper._
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec

class FileWriterTest extends AnyFlatSpec with Helpers {

  val testDf : DataFrame = readFile(writeTestCaseInputPath,fileFormat)(sparkSession)
  val testDfCount:Long = testDf.count()


  "writeFile() method" should "write data to the given location" in {

    if(testDfCount!=0)
      {
        writeFile(testDf,fileFormat,writeTestCaseOutputPath)
        val readSampleOutputDf:DataFrame = readFile(writeTestCaseOutputPath,fileFormat)(sparkSession)
        val checkOutputFile = readSampleOutputDf.count()
        assertResult(testDfCount)(checkOutputFile)
      }
  }

}

