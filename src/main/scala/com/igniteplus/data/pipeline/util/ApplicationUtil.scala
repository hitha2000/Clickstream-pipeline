package com.igniteplus.data.pipeline.util

import org.apache.spark.sql.SparkSession

object ApplicationUtil {
  def createSparkSession(appName:String, masterName:String): SparkSession =
  {
    implicit val spark:SparkSession=SparkSession.builder.master(masterName).appName(appName).getOrCreate()
    spark
  }
}
