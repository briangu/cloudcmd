package cloudcmd.common.util

import java.io.{ByteArrayInputStream, File, InputStream}
import cloudcmd.common.FileUtil

object StreamUtil {
  def spoolStream(is: InputStream): (String, File) = {
    val tmpFile = File.createTempFile("cld", "tmp")
    val hash = try {
      FileUtil.writeFileAndComputeHash(is, tmpFile)
    } catch {
      case e: Exception => {
        FileUtil.delete(tmpFile)
        throw new RuntimeException(e)
      }
    }
    (hash, tmpFile)
  }

  def spoolStream(is: InputStream, destPath: File): (String, File) = {
    val tmpFile = File.createTempFile("cld", "tmp", destPath)
    val hash = try {
      FileUtil.writeFileAndComputeHash(is, tmpFile)
    } catch {
      case e: Exception => {
        FileUtil.delete(tmpFile)
        throw new RuntimeException(e)
      }
    }
    (hash, tmpFile)
  }

  def spoolStreamToBytes(is: InputStream): Array[Byte] = {
    Stream.continually(is.read).takeWhile(-1 !=).map(_.toByte).toArray
  }

  def spoolStreamToString(is: InputStream): String = {
    scala.io.Source.fromInputStream(is).mkString
  }

  def stringToInputStream(str: String): InputStream = {
    new ByteArrayInputStream(str.getBytes("UTF-8"))
  }
}
