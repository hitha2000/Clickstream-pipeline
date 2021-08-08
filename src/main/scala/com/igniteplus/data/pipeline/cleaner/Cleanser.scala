package com.igniteplus.data.pipeline.cleaner

import com.igniteplus.data.pipeline.constants.ApplicationConstants.JSON_FORMAT
import com.igniteplus.data.pipeline.service.FileWriterService
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, length, lower, row_number, substring, trim, unix_timestamp, when}



object Cleanser {

  // Convert into correct datatype
  def changeDataType(df:DataFrame ,
                     colList: Seq[String],
                     dataType : Seq[String]):DataFrame = {
    var dfToDataType = df
    for( i <- colList.indices) {
      if(dataType(i) == "timestamp")
        {
          dfToDataType= dfToDataType.withColumn(colList(i),unix_timestamp(col(colList(i)),"MM/dd/yyyy H:mm").cast("double").cast(dataType(i)))
          dfToDataType.printSchema()

        }
      else
        {
          dfToDataType = dfToDataType.withColumn(colList(i), col(colList(i)).cast(dataType(i)))
        }

    }
    dfToDataType.printSchema()
    dfToDataType
  }


  // Filter Null Columns
  def checkNullKeyColumns(df:DataFrame,
                          columnList: Seq[String],
                          fileFormat:String,
                          fileSaveMode:String,
                          path:String):DataFrame = {

    val colNames:Seq[Column] = columnList.map(ex => col(ex))
    val condition:Column = colNames.map(ex => ex.isNull).reduce(_ || _)
    val dfCheckNullKey:DataFrame = df.withColumn("nullFlag" , when(condition,value = "true").otherwise(value = "false"))

    if(dfCheckNullKey.count() == 0)
      {
        FileWriterService.writeDf(dfCheckNullKey,fileFormat,fileSaveMode,path)
      }
    else
      {
        println(dfCheckNullKey)
      }
  dfCheckNullKey
  }

  // Filter Not Null key columns
  def DataNotNull(df:DataFrame,
                  columnList: Seq[String]):DataFrame = {
    val dfNotNull:DataFrame= df.na.drop(columnList)

    dfNotNull.show()
    dfNotNull
  }


  //Remove Duplicates

  def removeDuplicates (df:DataFrame ,
                        keyColumns : Seq[String],
                        orderByCol: String
                      ) : DataFrame  = {

    if( orderByCol != null) {
      val windowSpec = Window.partitionBy(keyColumns.map(col):_* ).orderBy(desc(orderByCol.toString))
      val dfDropDuplicate: DataFrame = df.withColumn(colName ="row_number", row_number().over(windowSpec))
        .filter(conditionExpr = "row_number == 1" ).drop("row_number")
      println("Distinct count of session_id and visitor_id  and event_timestamp and item id: "+ dfDropDuplicate.count())
      dfDropDuplicate
    }
    else {
      val dfDropDupItem = df.dropDuplicates(keyColumns)
      dfDropDupItem.show()
      dfDropDupItem
    }
  }



  // Convert columns to lowercase
  def toLowerCase(df:DataFrame,
                  columnList:Seq[String]):DataFrame = {
   var dfLowerCase:DataFrame = df
    for(i <- columnList) {
      dfLowerCase = dfLowerCase.withColumn(df(i).toString(), lower(col(df(i).toString())))
    }
    dfLowerCase.show()
    dfLowerCase
  }


  // Trim data in the columns

  def trimData(df:DataFrame):DataFrame = {

    var dfTrim = df
    for ( i <- df.columns) {
      dfTrim = dfTrim.withColumn(df(i).toString(),trim(col(df(i).toString())))
    }
    dfTrim.show(false)
    dfTrim
  }
}





// *********************************************************************************************************************



/*  // Convert a column to timestamp datatype
  def toTimestampType(df:DataFrame,
                      colName:String):DataFrame={
    val dfTimestampType= df.withColumn(colName,unix_timestamp(col(colName),"MM/dd/yyyy H:mm").cast("double").cast("timestamp"))
    dfTimestampType.printSchema()
    dfTimestampType
  }


 // Convert Seq(columns) to FloatType
  def toFloatType(df:DataFrame,
                  colList:Seq[String]):DataFrame = {
    val dfFloatType:DataFrame = colList.foldLeft(df) { (memoDF, colName) =>
      memoDF.withColumn(
        colName,
        col(colName).cast(FloatType)
      )
    }
    dfFloatType.printSchema()
    dfFloatType
  }

// Filter Not Null key columns
def DataNotNull(df:DataFrame,
  columnList: Seq[String]):DataFrame = {
  val dfNotNull:DataFrame= df.na.drop(columnList)

  dfNotNull.show()
  dfNotNull
  }

   //  Remove duplicate Seq(columns) using window partition
  def removeClickStreamDuplicates(df:DataFrame,
                                  partitioncolumns: Seq[String] ,
                                  colName1:String,
                                  colName2:String,
                                  rowCondition:String):DataFrame = {

    val windowSpec = Window.partitionBy(partitioncolumns.map(col):_* ).orderBy(desc(colName1))
    val duplicate: DataFrame = df.withColumn(colName =colName2, row_number().over(windowSpec))
      .filter(conditionExpr = rowCondition).drop(colName2)
    println("Distinct count of session_id and visitor_id  and event_timestamp and item id: "+duplicate.count())

    duplicate.show()
    duplicate


  }


  // Remove duplicates from a single column using dropDuplicates()
  def removeItemDuplicates(df:DataFrame,
                           columnList:Seq[String]):DataFrame = {
    val dfDropDupItem = df.dropDuplicates(columnList)
    dfDropDupItem.show()
    dfDropDupItem
  }

  //  Convert Seq(columns) to lowercase
  def toLowerCase(df:DataFrame,
                  columnList:Seq[String]):DataFrame = {
    val dfLowerCase:DataFrame = columnList.foldLeft(df) { (memoDF, colName) =>
      memoDF.withColumn(
        colName,
        lower(col(colName))
      )
    }
    dfLowerCase.show()
    dfLowerCase
  }

  // Trim data in the columns

  def trimData(df:DataFrame):DataFrame = {

    var dfTrim = df
    for ( i <- df.columns) {
      dfTrim = dfTrim.withColumn(df(i).toString(),trim(col(df(i).toString())))
    }
    dfTrim.show(false)
    dfTrim
  }
}
 */