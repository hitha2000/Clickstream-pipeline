package com.igniteplus.data.pipeline.service

import com.igniteplus.data.pipeline.constants.TestConstants.{countShouldBe, fileFormat, readLocation, readWrongLocation}
import com.igniteplus.data.pipeline.exception.FileReadException
import com.igniteplus.data.pipeline.service.FileReaderService.readFile
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

class FileReaderTest extends AnyFlatSpec with BeforeAndAfterAll{

  @transient var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().appName("Tests").master("local").getOrCreate()
  }

  "readFile() method" should "read data from the given location" in {
    val sampleDF:DataFrame = readFile(readLocation,fileFormat)(spark)
    val rcount = sampleDF.count()
    assertResult(countShouldBe)(rcount)
  }

  "readFile() method" should "throw exception in case it's not able to read data" in {
    assertThrows[FileReadException] {
      val sampleDF:DataFrame = readFile(readWrongLocation, fileFormat)(spark)
    }
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

}

