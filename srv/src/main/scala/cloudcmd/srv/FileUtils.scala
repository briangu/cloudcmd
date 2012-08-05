package cloudcmd.srv

import java.net.{JarURLConnection, URL}
import java.util.jar.JarFile
import io.viper.core.server.Util
import java.io._
import java.nio.channels.FileChannel
import java.nio.charset.Charset
import org.jboss.netty.buffer.ChannelBuffer
import javax.imageio.ImageIO

object FileUtils {
  def readFile(path: String): String = readFile(new File(path))

  def readFile(file: File): String = {
    val stream = new FileInputStream(file)
    try {
      val fc = stream.getChannel()
      val bb = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size())
      Charset.forName("UTF-8").decode(bb).toString()
    }
    finally {
      stream.close()
    }
  }

  def readResourceFile(clazz: Class[_], path: String): String = {
    val url: URL = clazz.getResource(path)

    if (url == null) {
      return null
    }

    if (url.toString.startsWith("jar:")) {
      var bytes: Array[Byte] = null
      clazz.synchronized {
        val conn: JarURLConnection = url.openConnection.asInstanceOf[JarURLConnection]
        val jarFile: JarFile = conn.getJarFile
        val input: InputStream = jarFile.getInputStream(conn.getJarEntry)
        bytes = Util.copyStream(input)
        jarFile.close
      }
      new String(bytes, "UTF-8")
    } else {
      readFile(url.getFile)
    }
  }

  def storeAsFile(buffer: ChannelBuffer, size: Int) {
    val file = File.createTempFile("", "")
    val outputStream = new FileOutputStream(file)
    val localfileChannel = outputStream.getChannel()
    val byteBuffer = buffer.toByteBuffer()
    var written = 0
    while (written < size) {
      written += localfileChannel.write(byteBuffer)
    }
    buffer.readerIndex(buffer.readerIndex() + written)
    localfileChannel.force(false)
    localfileChannel.close()
  }
}
