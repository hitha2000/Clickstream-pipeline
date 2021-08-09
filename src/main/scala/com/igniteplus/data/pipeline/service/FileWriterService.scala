package com.igniteplus.data.pipeline.service


import org.apache.spark.sql.{DataFrame, SaveMode}




object FileWriterService {

  def writeFile(df:DataFrame,
                writeFormat: String,
               path:String
                ): Unit = {
//    df.write
//      .option("header",true)
//      .format("com.databricks.spark.csv")
//      .save(path)
//    df.write.format(writeFormat) .option("header", "true") .option("inferSchema", "true") .option("delimiter", "|")
  //    .save(path)

    //df.write.csv(path)
//    df.write
//      .format("com.databricks.spark.csv")
//      .option("header", "true")
//      .option("delimiter",",")
//      .save(path)
println("path : " + path)
    df.coalesce(2)
      .write
      .option("header", "true")
      .mode(SaveMode.Overwrite).csv(path)

//    df. write.mode(SaveMode.Overwrite).csv("data/output/pipeline_failures/clickstream_null_values")
  }

}






