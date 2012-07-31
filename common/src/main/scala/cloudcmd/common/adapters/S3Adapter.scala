package cloudcmd.common.adapters

import cloudcmd.common.CryptoUtil
import cloudcmd.common.FileChannelBuffer
import cloudcmd.common.UriUtil
import org.apache.commons.io.IOUtils
import org.jboss.netty.buffer.ChannelBuffer
import org.jets3t.service.impl.rest.httpclient.RestS3Service
import org.jets3t.service.model.S3Object
import org.jets3t.service.security.AWSCredentials
import java.io.ByteArrayInputStream
import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import java.net.URI
import java.nio.channels.Channels

//     "s3://<aws id>@<bucket>?tier=2&tags=s3&secret=<aws secret>"

object S3Adapter {
  def parseAwsInfo(adapterUri: URI): (String, String, String) = {
    val parts = adapterUri.getAuthority.split("@")
    if (parts.length != 2) throw new IllegalArgumentException("authority format: awsKey@bucketname")
    val queryParams = UriUtil.parseQueryString(adapterUri)
    if (!queryParams.containsKey("secret")) throw new IllegalArgumentException("missing aws secret")
    (parts(0), queryParams.get("secret"), parts(1))
  }
}

class S3Adapter extends Adapter with MD5Storable {

  private var _bucketName: String = null
  private var _s3Service: RestS3Service = null

  override
  def init(configDir: String, tier: Int, adapterType: String, tags: Set[String], uri: URI) {
    super.init(configDir, tier, adapterType, tags, uri)
    val (awsKey, awsSecret, awsBucketName) = S3Adapter.parseAwsInfo(uri)
    val creds = new AWSCredentials(awsKey, awsSecret)
    _s3Service = new RestS3Service(creds)
    _bucketName = awsBucketName
    if () {
      bootstrap
    }
  }

  private def bootstrap {
    val file: File = new File(getDbFileName + ".h2.db")
    if (!file.exists) {
      bootstrapS3
    }
  }

  private def bootstrapS3 {
    _s3Service.getOrCreateBucket(_bucketName)
  }

  override
  def refreshCache {}

  override
  def contains(hash: String): Boolean = _s3Service.isObjectInBucket(_bucketName, hash)

  override
  def shutdown {}

  override
  def remove(hash: String): Boolean = {
    _s3Service.deleteObject(_bucketName, hash)
    true
  }

  override
  def verify(hash: String): Boolean = {
    true
  }

  override
  def store(data: InputStream, hash: String) {
    if (data.isInstanceOf[ByteArrayInputStream]) {
      val buffer: ByteArrayInputStream = data.asInstanceOf[ByteArrayInputStream]
      val md5Hash = CryptoUtil.computeMD5Hash(Channels.newChannel(buffer))
      buffer.reset
      store(buffer, hash, md5Hash, buffer.available)
    }
    else if (data.isInstanceOf[FileInputStream]) {
      val buffer: FileInputStream = data.asInstanceOf[FileInputStream]
      val md5Hash = CryptoUtil.computeMD5Hash(buffer.getChannel)
      buffer.reset
      store(buffer, hash, md5Hash, buffer.available)
    }
    else {
      val buffer = IOUtils.toByteArray(data)
      val md5Hash = CryptoUtil.computeMD5Hash(Channels.newChannel(new ByteArrayInputStream(buffer)))
      store(new ByteArrayInputStream(buffer), hash, md5Hash, buffer.length)
    }
  }

  override
  def store(data: InputStream, hash: String, md5Digest: Array[Byte], length: Long) {
    if (contains(hash)) return
    val s3Object: S3Object = new S3Object(hash)
    s3Object.setDataInputStream(data)
    s3Object.setContentLength(length)
    s3Object.setMd5Hash(md5Digest)
    s3Object.setBucketName(_bucketName)
    _s3Service.putObject(_bucketName, s3Object)
  }

  override
  def load(hash: String): InputStream = {
    if (!contains(hash)) throw new DataNotFoundException(hash)
    _s3Service.getObject(_bucketName, hash).getDataInputStream
  }

  override
  def loadChannel(hash: String): ChannelBuffer = {
    if (!contains(hash)) throw new DataNotFoundException(hash)
    val s3Object: S3Object = _s3Service.getObject(_bucketName, hash)
    val length = s3Object.getContentLength.toInt
    new FileChannelBuffer(s3Object.getDataInputStream, length)
  }

  override
  def describe: Set[String] = {
    Set() ++ _s3Service.listObjects(_bucketName).flatMap{ s3Object =>
      Set(s3Object.getKey)
    }
  }
}