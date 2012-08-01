package cloudcmd.common.util

import java.io.{File, InputStream}
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
}
