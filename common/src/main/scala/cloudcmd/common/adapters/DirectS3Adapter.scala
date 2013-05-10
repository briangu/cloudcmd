package cloudcmd.common.adapters

import cloudcmd.common.util.{JsonUtil, CryptoUtil, StreamUtil}
import cloudcmd.common.{FileMetaData, BlockContext, FileUtil, UriUtil}
import org.jets3t.service.impl.rest.httpclient.RestS3Service
import org.jets3t.service.model.S3Object
import org.jets3t.service.security.AWSCredentials
import java.io.{FileInputStream, InputStream}
import java.net.URI
import java.nio.channels.Channels
import collection.mutable
import org.jets3t.service.io.RepeatableInputStream

//     "s3://<aws id>@<bucket>?tier=2&tags=s3&secret=<aws secret>"

class DirectS3Adapter extends DirectAdapter {

  private var _bucketName: String = null
  private var _s3Service: RestS3Service = null
  private var _useReducedRedundancy: Boolean = true

  override def init(configDir: String, tier: Int, adapterType: String, tags: Set[String], uri: URI) {
    super.init(configDir, tier, adapterType, tags, uri)
    val (awsKey, awsSecret, awsBucketName, useRRS) = parseAwsInfo(URI)
    val creds = new AWSCredentials(awsKey, awsSecret)
    _s3Service = new RestS3Service(creds)
    _bucketName = awsBucketName
    _useReducedRedundancy = useRRS

    if (!_s3Service.isBucketAccessible(_bucketName)) {
      _s3Service.getOrCreateBucket(_bucketName)
    }

    _isOnline = _s3Service.isBucketAccessible(_bucketName)
  }

  def shutdown() {}

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

  def containsAll(ctxs: Set[BlockContext]): Map[BlockContext, Boolean] = {
    Map() ++ ctxs.par.flatMap{ctx =>
      Map(ctx -> _s3Service.isObjectInBucket(_bucketName, ctx.hash))
    }
  }

  def removeAll(ctxs: Set[BlockContext]): Map[BlockContext, Boolean] = {
    Map() ++ ctxs.par.flatMap{ctx =>
      _s3Service.deleteObject(_bucketName, ctx.hash)
      Map(ctx -> true)
    }
  }

  def ensureAll(ctxs: Set[BlockContext], blockLevelCheck: Boolean) : Map[BlockContext, Boolean] = {
    Map() ++ ctxs.par.flatMap(ctxs => Map(ctxs -> true))
  }

  def store(ctx: BlockContext, is: InputStream) {
    if (is.markSupported()) {
      is.mark(0)
      val md5Hash = CryptoUtil.computeMD5Hash(Channels.newChannel(is))
      is.reset()
      store(ctx, is, md5Hash, is.available)
    } else {
      val (hash, tmpFile) = StreamUtil.spoolStream(is)
      if (hash != getHashFromDataFile(ctx.hash)) throw new RuntimeException("retrieved data hash %s not equal to expected %s".format(hash, ctx.hash))
      val fis = new FileInputStream(tmpFile)
      val buffer = new FileInputStream(tmpFile)
      try {
        val md5Hash = CryptoUtil.computeMD5Hash(fis.getChannel)
        store(ctx, buffer, md5Hash, buffer.available)
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

  private def store(ctx: BlockContext, data: InputStream, md5Digest: Array[Byte], length: Long) {
    val s3Object: S3Object = new S3Object(ctx.hash)
    s3Object.setDataInputStream(new RepeatableInputStream(data, length.toInt))
    s3Object.setContentLength(length)
    s3Object.setMd5Hash(md5Digest)
    s3Object.setBucketName(_bucketName)
    if (_useReducedRedundancy) s3Object.setStorageClass(S3Object.STORAGE_CLASS_REDUCED_REDUNDANCY)
    _s3Service.putObject(_bucketName, s3Object)
  }

  def load(ctx: BlockContext): (InputStream, Int) = {
    val obj = _s3Service.getObject(_bucketName, ctx.hash)
    (obj.getDataInputStream, obj.getContentLength.toInt)
  }

  def describe(): Set[String] = {
    Set() ++ _s3Service.listObjects(_bucketName).par.map(s3Object => s3Object.getKey)
  }
}