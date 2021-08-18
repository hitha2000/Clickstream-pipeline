package com.igniteplus.data.pipeline.service

import com.igniteplus.data.pipeline.constants.ApplicationConstants._
import com.igniteplus.data.pipeline.util.ApplicationUtil
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.igniteplus.data.pipeline.cleaner.Cleanser

object PipelineService {

  def executePipeline():Unit = {

    //Creating a SparkSession
    implicit val sparkSession :SparkSession = ApplicationUtil.createSparkSession(APP_NAME , MASTER)


    //Reading the dataset
    val dfCLickStream:DataFrame = FileReaderService.readFile(INPUT_LOCATION_CLICKSTREAM , CSV_FORMAT)
    val dfItem:DataFrame = FileReaderService.readFile(INPUT_LOCATION_ITEM , CSV_FORMAT)


    // Filter null columns and write to an output file
    val dfClickStreamNullKey:DataFrame = Cleanser.checkNullKeyColumns(dfCLickStream,
                                                                      CLICKSTREAM_UNIQUE_COLUMNS,CLICKSTREAM_NULL_ROWS_DATASET
                                                                     )
    val dfItemNullKey:DataFrame = Cleanser.checkNullKeyColumns(dfItem ,
                                                                ITEM_UNIQUE_COLUMNS, ITEM_NULL_ROWS_DATASET
                                                               )


    //Validate datatype
    val dfClickStreamDatatype:DataFrame = Cleanser.changeDataType(dfCLickStream,
                                                                  CLICKSTREAM_VALID_DATATYPE_COLUMNS ,
                                                                  CLICKSTREAM_VALID_DATATYPE )
    val dfItemDatatype:DataFrame = Cleanser.changeDataType(dfItem,
                                                            ITEM_VALID_DATATYPE_COLUMNS ,
                                                            ITEM_VALID_DATATYPE)

    //Remove Null key columns
    val dfFilterClickStream:DataFrame = Cleanser.DataNotNull(dfClickStreamDatatype,CLICKSTREAM_UNIQUE_COLUMNS).distinct()
    val dfFilterItem:DataFrame = Cleanser.DataNotNull(dfItemDatatype,ITEM_UNIQUE_COLUMNS).distinct()

    //Remove duplicates
    val dfDropClickStreamDup:DataFrame = Cleanser.removeDuplicates(dfFilterClickStream,
                                                          CLICKSTREAM_UNIQUE_COLUMNS,
                                                          Some(EVENT_TIMESTAMP_OPTION)
                                                          )

    val dfDropItemDup:DataFrame = Cleanser.removeDuplicates(dfFilterItem ,ITEM_UNIQUE_COLUMNS,None)

    //Convert to correct casing
    val dfClickStreamLowerCase:DataFrame = Cleanser.toLowerCase(dfDropClickStreamDup,CLICKSTREAM_LOWERCASE_COLUMNS )
    val dfItemLowerCase:DataFrame = Cleanser.toLowerCase(dfDropItemDup, ITEM_LOWERCASE_COLUMNS)

    // Trim data
    val dfTrimClickStreamData:DataFrame = Cleanser.trimData(dfClickStreamLowerCase)
    val dfTrimItemData:DataFrame = Cleanser.trimData(dfItemLowerCase)
  }

}
