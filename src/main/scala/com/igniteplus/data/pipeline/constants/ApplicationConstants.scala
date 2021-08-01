package com.igniteplus.data.pipeline.constants

object ApplicationConstants {
  val MASTER:String="local"
  val APP_NAME:String="Clickstream Pipeline"

  val INPUT_LOCATION_CLICKSTREAM:String="data/input/clickstream/clickstream_log.csv"
  val INPUT_LOCATION_ITEM:String="data/input/item/item_data.csv"

  val INPUT_WRITE_DATA:String = "data/output/merged-data/writeNullKeyCol/abc"

  val CSV_FORMAT:String = "csv"

  val SAVE_FILE_MODE:String = "overwrite"

  val WRITE_PATH:String = "data/output/merged-data/writeNullKeyCol.csv"

  val TIMESTAMP_TYPE:String = "timestamp"
  val FLOAT_TYPE:String = "float"


  val SESSION_ID:String = "session_id"
  val EVENT_TIMESTAMP:String = "event_timestamp"
  val REDIRECTION_SOURCE:String = "redirection_source"
  val DEVICE_TYPE:String = "device_type"

  val ITEM_ID:String = "item_id"
  val ITEM_PRICE:String = "item_price"
  val DEPARTMENT_NAME:String = "department_name"

  val CLICKSTREAM_VALID_DATATYPE_COLUMNS:Seq[String] = Seq(ApplicationConstants.EVENT_TIMESTAMP)
  val CLICKSTREAM_VALID_DATATYPE:Seq[String] = Seq(ApplicationConstants.TIMESTAMP_TYPE)
  val CLICKSTREAM_UNIQUE_COLUMNS:Seq[String] = Seq(ApplicationConstants.SESSION_ID , ApplicationConstants.ITEM_ID)
  val CLICKSTREAM_LOWERCASE_COLUMNS:Seq[String] = Seq(ApplicationConstants.REDIRECTION_SOURCE , ApplicationConstants.DEVICE_TYPE )

  val ITEM_VALID_DATATYPE_COLUMNS:Seq[String] = Seq(ApplicationConstants.ITEM_PRICE)
  val ITEM_VALID_DATATYPE:Seq[String] = Seq(ApplicationConstants.FLOAT_TYPE)
  val ITEM_UNIQUE_COLUMNS:Seq[String] = Seq(ApplicationConstants.ITEM_ID)
  val ITEM_LOWERCASE_COLUMNS:Seq[String] = Seq(ApplicationConstants.DEPARTMENT_NAME)

  val ROW_NUMBER:String = "row_number"
  val ROW_CONDITION:String = "row_number == 1"




}
