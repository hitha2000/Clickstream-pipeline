package com.igniteplus.data.pipeline.constants

import com.igniteplus.data.pipeline.service.FileReaderService.readFile
import org.apache.spark.sql.DataFrame

object TestConstants {
 val deduplicationLocation="data/input/testDf/deduplicationLocation.csv"
 val fileFormat="csv"
 val writeOutputPathForDeduplication="data/output/testOutput/writeOutputPathForDeduplication.csv"
  val CLICKSTREAM_UNIQUE_COLUMNS:Seq[String] = Seq(ApplicationConstants.SESSION_ID , ApplicationConstants.ITEM_ID)

 val readWrongLocation = "data/input/testDf/test.csv"
 val readLocation = "data/input/testDf/testData.csv"
 val countShouldBe=3
}
