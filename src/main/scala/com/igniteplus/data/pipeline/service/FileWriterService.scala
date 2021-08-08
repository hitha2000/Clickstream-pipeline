package com.igniteplus.data.pipeline.service

import com.igniteplus.data.pipeline.constants.ApplicationConstants.INPUT_WRITE_DATA
import com.igniteplus.data.pipeline.exception.FileWriteException
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.nio.file.{Files, Paths, StandardOpenOption}
import java.nio.charset.StandardCharsets
import java.io.{File, FileWriter, PrintWriter}
import scala.io.Source

object FileWriterService {

  def writeFile(df:String,
              //  fileFormat:String,
               // fileSaveMode:String,
                path:String): Unit = {


      try {


        val fileWrite = new FileWriter(path, true) ;
        fileWrite.write( df)
        fileWrite.append("\n")
        fileWrite.close()

     // METHOD-2
    //        val writer = new PrintWriter(new File(path))
    //
    //        writer.write(df)
    //        writer.append("\n")
    //        writer.close()

      // ITS ONLY USED TO WRITE DF
      // df.write.format(fileFormat).mode(fileSaveMode).save(path)

      // WORKS BUT NEW EXCEPTIONS CANT BE WRITTEN IN A NEW LINE
      // val writeString:String = System.lineSeparator + df
      // Files.write(Paths.get(path), df.getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND)
      //Files.write(Paths.get(path), writeString, StandardOpenOption.APPEND)



      }
      catch {
        case e: Exception =>
          FileWriteException("unable to write files in the given location " + s"$INPUT_WRITE_DATA")

      }



//    if(dfWriteData) {
//
//      throw FileWriteException("No files read from the file reader " + s"$INPUT_WRITE_DATA")
//    }
//    else
//    {
//      println(dfWriteData)
//    }
//
//

  }
}
