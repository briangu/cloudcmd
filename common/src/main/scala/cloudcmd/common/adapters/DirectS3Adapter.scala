package cloudcmd.common.adapters

import cloudcmd.common.util.{CryptoUtil, StreamUtil}
import cloudcmd.common.{FileUtil, UriUtil}
import org.jets3t.service.impl.rest.httpclient.RestS3Service
import org.jets3t.service.model.S3Object
import org.jets3t.service.security.AWSCredentials
import java.io.{FileInputStream, InputStream}
import java.net.URI
import java.nio.channels.Channels
import org.jets3t.service.io.RepeatableInputStream

//     "s3://<aws id>@<bucket>?tier=2&tags=s3&secret=<aws secret>"

class DirectS3Adapter extends Adapter {

  private var _bucketName: String = null
  private var _s3Service: RestS3Service = null
  private var _useReducedRedundancy: Boolean = true

  override def init(configDir: String, tier: Int, adapterType: String, tags: Set[String], uri: URI) {
    super.init(configDir, tier, adapterType, tags, uri)
    val (awsKey, awsSecret, awsBucketName, useRRS) = parseAwsInfo(uri)
    val creds = new AWSCredentials(awsKey, awsSecret)
    _s3Service = new RestS3Service(creds)
    _bucketName = awsBucketName
    _useReducedRedundancy = useRRS

    if (!_s3Service.isBucketAccessible(_bucketName)) {
      _s3Service.getOrCreateBucket(_bucketName)
      _isOnline = _s3Service.isBucketAccessible(_bucketName)
    }
  }

  def shutdown {}

  def parseAwsInfo(adapterUri: URI): (String, String, String, Boolean) = {
    val parts = adapterUri.getAuthority.split("@")
    if (parts.length != 2) throw new IllegalArgumentException("authority format: awsKey@bucketname")
    val queryParams = UriUtil.parseQueryString(adapterUri)
    if (!queryParams.containsKey("secret")) throw new IllegalArgumentException("missing aws secret")
    val useRRS = Option(queryParams.get("useRRS")) match {
      case Some(param) => param.toBoolean
      case None => true // default
    }
    (parts(0), queryParams.get("secret"), parts(1), useRRS)
  }

  def refreshCache {}

  def containsAll(hashes: Set[String]): Map[String, Boolean] = {
    Map() ++ hashes.par.flatMap{hash =>
      Map(hash -> _s3Service.isObjectInBucket(_bucketName, hash))
    }
  }

  def removeAll(hashes: Set[String]): Map[String, Boolean] = {
    Map() ++ hashes.par.flatMap{hash =>
      _s3Service.deleteObject(_bucketName, hash)
      Map(hash -> true)
    }
  }

  def ensureAll(hashes: Set[String], blockLevelCheck: Boolean) : Map[String, Boolean] = {
    Map() ++ hashes.par.flatMap(x => Map(x -> true))
  }

  def store(hash: String, is: InputStream) {
    if (is.markSupported()) {
      is.mark(0)
      val md5Hash = CryptoUtil.computeMD5Hash(Channels.newChannel(is))
      is.reset()
      store(hash, is, md5Hash, is.available)
    } else {
      val (hash, tmpFile) = StreamUtil.spoolStream(is)
      if (hash != getHashFromDataFile(hash)) throw new RuntimeException("retrieved data hash %s not equal to expected %s".format(hash, hash))
      val fis = new FileInputStream(tmpFile)
      val buffer = new FileInputStream(tmpFile)
      try {
        val md5Hash = CryptoUtil.computeMD5Hash(fis.getChannel)
        store(hash, buffer, md5Hash, buffer.available)
      } finally {
        fis.close()
        buffer.close()
        FileUtil.delete(tmpFile)
      }
    }
  }

  private def getHashFromDataFile(hash: String): String = {
    val idx = hash.lastIndexOf(".")
    if (idx >= 0) hash.substring(0, idx) else hash
  }

  private def store(hash: String, data: InputStream, md5Digest: Array[Byte], length: Long) {
    val s3Object: S3Object = new S3Object(hash)
    s3Object.setDataInputStream(new RepeatableInputStream(data, length.toInt))
    s3Object.setContentLength(length)
    s3Object.setMd5Hash(md5Digest)
    s3Object.setBucketName(_bucketName)
    if (_useReducedRedundancy) s3Object.setStorageClass(S3Object.STORAGE_CLASS_REDUCED_REDUNDANCY)
    _s3Service.putObject(_bucketName, s3Object)
  }

  def load(hash: String): (InputStream, Int) = {
    val obj = _s3Service.getObject(_bucketName, hash)
    (obj.getDataInputStream, obj.getContentLength.toInt)
  }

  def describe: Set[String] = {
    Set() ++ _s3Service.listObjects(_bucketName).par.map(s3Object => s3Object.getKey)
  }
}