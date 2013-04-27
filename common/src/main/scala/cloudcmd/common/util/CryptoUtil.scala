package cloudcmd.common.util

import cloudcmd.common.FileUtil
import java.io._
import java.math.BigInteger
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.nio.channels.ReadableByteChannel
import java.nio.channels.WritableByteChannel
import java.security.MessageDigest
import java.security.NoSuchAlgorithmException

object CryptoUtil {

  private val buffer = new ThreadLocal[ByteBuffer] {
    override def initialValue = ByteBuffer.allocate(1024 * 1024)
  }

  def digestToString(digest: Array[Byte]): String = {
    if (digest == null) return null
    val bigInt: BigInteger = new BigInteger(1, digest)
    var hash: String = bigInt.toString(16)
    if (hash.length == 63) hash = "0" + hash
    hash
  }

  def computeHashAsString(targetFile: File): String = {
    digestToString(computeHash(targetFile))
  }

  def computeHash(targetFile: File): Array[Byte] = {
    var fis: FileInputStream = null
    try {
      fis = new FileInputStream(targetFile)
      computeDigest(Channels.newChannel(fis), "SHA-256")
    }
    catch {
      case e: NoSuchAlgorithmException => {
        throw new RuntimeException("SHA-256 is not supported")
      }
    }
    finally {
      FileUtil.SafeClose(fis)
    }
  }

  def computeMD5Hash(targetFile: File): Array[Byte] = {
    var fis: FileInputStream = null
    try {
      fis = new FileInputStream(targetFile)
      computeDigest(Channels.newChannel(fis), "MD5")
    }
    catch {
      case e: NoSuchAlgorithmException => {
        throw new RuntimeException("SHA-256 is not supported")
      }
    }
    finally {
      FileUtil.SafeClose(fis)
    }
  }

  def writeAndComputeHash(srcData: InputStream, destFile: File): Array[Byte] = {
    var fos: FileOutputStream = null
    try {
      fos = new FileOutputStream(destFile)
      writeAndComputeHash(srcData, fos)
    }
    finally {
      FileUtil.SafeClose(fos)
    }
  }

  def writeAndComputeHash(is: InputStream, os: OutputStream): Array[Byte] = {
    try {
      writeAndComputeHash(Channels.newChannel(is), Channels.newChannel(os), "SHA-256")
    }
    catch {
      case e: NoSuchAlgorithmException => {
        throw new RuntimeException("SHA-256 is not supported")
      }
    }
  }

  def writeAndComputeHash(in: ReadableByteChannel, out: WritableByteChannel, digestId: String): Array[Byte] = {
    val md = MessageDigest.getInstance(digestId)
    val buff = buffer.get
    while (in.read(buff) != -1) {
      buff.flip
      md.update(buff)
      buff.flip
      out.write(buff)
      buff.clear
    }
    md.digest
  }

  def computeHashAsString(is: InputStream): String = {
    digestToString(computeHash(is))
  }

  def computeHash(is: InputStream): Array[Byte] = {
    try {
      computeDigest(Channels.newChannel(is), "SHA-256")
    }
    catch {
      case e: NoSuchAlgorithmException => {
        throw new RuntimeException("SHA-256 is not supported")
      }
    }
  }

  def computeMD5HashAsString(is: InputStream) : String = {
    digestToString(computeMD5Hash(Channels.newChannel(is)))
  }

  def computeMD5Hash(channel: ReadableByteChannel): Array[Byte] = {
    try {
      computeDigest(channel, "MD5")
    }
    catch {
      case e: NoSuchAlgorithmException => {
        throw new RuntimeException("MD5 is not supported")
      }
    }
  }

  private def computeDigest(channel: ReadableByteChannel, digestId: String): Array[Byte] = {
    val md = MessageDigest.getInstance(digestId)
    val buff = buffer.get
    while (channel.read(buff) != -1) {
      buff.flip
      md.update(buff)
      buff.clear
    }
    md.digest
  }
}