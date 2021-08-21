package com.igniteplus.data.pipeline.service
import com.igniteplus.data.pipeline.service.FileReaderService.readFile
import com.igniteplus.data.pipeline.service.FileWriterService.writeFile
import org.apache.spark.sql.{DataFrame, SparkSession}

import org.scalatest.flatspec.AnyFlatSpec

class FileWriterTest extends AnyFlatSpec {
  @transient var spark: SparkSession = _

  spark = SparkSession.builder().appName("Tests").master("local").getOrCreate()


  val testDf : DataFrame = readFile("data/input/testDf/testData.csv","csv")(spark)
  val testDfCount = testDf.count()


  "writeFile() method" should "write data to the given location" in {

    if(testDfCount!=0)
      {
        writeFile(testDf,"csv","data/output/testOutput/testDataOutput.csv")
        val readSampleOutputDf:DataFrame = readFile("data/output/testOutput/testDataOutput.csv","csv")(spark)
        val checkOutputFile = readSampleOutputDf.count()
        assertResult(testDfCount)(checkOutputFile)
      }


  }

}

