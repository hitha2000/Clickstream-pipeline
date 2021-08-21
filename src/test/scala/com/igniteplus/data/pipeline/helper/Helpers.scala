package com.igniteplus.data.pipeline.helper

import com.igniteplus.data.pipeline.constants.ApplicationConstants
import com.igniteplus.data.pipeline.constants.ApplicationConstants.{APP_NAME, MASTER}
import com.igniteplus.data.pipeline.util.ApplicationUtil
import org.apache.spark.sql.SparkSession

trait Helpers {

  implicit val sparkSession :SparkSession = ApplicationUtil.createSparkSession(APP_NAME , MASTER)


  val writeTestCaseInputPath = "data/input/testDf/testData.csv"
  val writeTestCaseOutputPath = "data/output/testOutput/testDataOutput.csv"

  val deduplicationLocation="data/input/testDf/deduplicationLocation.csv"
  val fileFormat="csv"
  val writeOutputPathForDeduplication="data/output/testOutput/writeOutputPathForDeduplication.csv"
  val CLICKSTREAM_UNIQUE_COLUMNS:Seq[String] = Seq(ApplicationConstants.SESSION_ID , ApplicationConstants.ITEM_ID)

  val readWrongLocation = "data/input/testDf/test.csv"
  val readLocation = "data/input/testDf/testData.csv"
  val countShouldBe=3
}
