package com.igniteplus.data.pipeline.cleanse

import com.igniteplus.data.pipeline.cleaner.Cleanser.removeDuplicates
import com.igniteplus.data.pipeline.helper.Helpers
import com.igniteplus.data.pipeline.service.FileReaderService.readFile
import com.igniteplus.data.pipeline.service.FileWriterService.writeFile
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

class CleanserTest extends AnyFlatSpec with BeforeAndAfterAll with Helpers{


  @transient var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().appName("Tests").master("local").getOrCreate()
  }

  "removeDuplicates() method" should "remove the duplicates from the inputDF" in {
    val sampleDF:DataFrame = readFile(deduplicationLocation, fileFormat)(spark)
    val deDuplicatedDF:DataFrame = removeDuplicates(sampleDF,CLICKSTREAM_UNIQUE_COLUMNS,Some("event_timestamp"))
    val rcount = deDuplicatedDF.count()
    val expectedCount = 2
    assertResult(expectedCount)(rcount)
  }


  override def afterAll(): Unit = {
    spark.stop()
  }


}