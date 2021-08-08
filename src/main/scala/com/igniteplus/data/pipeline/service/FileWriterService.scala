package com.igniteplus.data.pipeline.service

import com.igniteplus.data.pipeline.exception.FileWriteException
import org.apache.spark.sql.DataFrame

import java.io.FileWriter
import java.text.SimpleDateFormat
import java.util.Calendar


object FileWriterService {

  def writeDf(df:DataFrame,
              fileFormat:String,
              fileSaveMode:String,
              path:String):Unit = {

    val dfWriteFile = try {

      df.write.format(fileFormat).mode(fileSaveMode).save(path)

    }
    catch {
      case e: Exception =>
        FileWriteException("unable to write files in the given location " + path )
    }

    if(dfWriteFile == null) {

      throw FileWriteException("No files written to the output file  " + path )
    }
    else
    {
      println(dfWriteFile)
    }


  }

  def writeException(exceptions:String,
                path:String): Unit = {


    val format = new SimpleDateFormat("dd/MM/yyyy hh:mm aa")
    val currDate:String =format.format(Calendar.getInstance().getTime())
    val writeException:String = currDate+":  " + exceptions

     val writeFileException:Unit =  try {

        val fileWrite = new FileWriter(path, true) ;
        fileWrite.write(writeException)
        fileWrite.append("\n")
        fileWrite.close()

     }

      catch {
        case e: Exception =>
          FileWriteException("unable to write file exceptions in the given location " + path)

      }

    if(writeFileException == null) {

      throw FileWriteException("No file exceptions written to the output file  " + path)
    }
    else
    {
      println(writeException)
    }

    // METHOD-2
    //        val writer = new PrintWriter(new File(path))
    //
    //        writer.write(df)
    //        writer.append("\n")
    //        writer.close()



    // WORKS BUT NEW EXCEPTIONS CANT BE WRITTEN IN A NEW LINE
    // val writeString:String = System.lineSeparator + df
    // Files.write(Paths.get(path), df.getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND)
    //Files.write(Paths.get(path), writeString, StandardOpenOption.APPEND)

  }
}






