package cloudcmd.common.adapters

import cloudcmd.common.util.{JsonUtil, CryptoUtil, StreamUtil}
import cloudcmd.common.{FileMetaData, BlockContext, FileUtil, UriUtil}
import org.jets3t.service.impl.rest.httpclient.RestS3Service
import org.jets3t.service.model.S3Object
import org.jets3t.service.security.AWSCredentials
import java.io.{ByteArrayInputStream, FileInputStream, InputStream}
import java.net.URI
import java.nio.channels.Channels
import collection.mutable

//     "s3://<aws id>@<bucket>?tier=2&tags=s3&secret=<aws secret>"

class DirectS3Adapter extends Adapter {

  private var _bucketName: String = null
  private var _s3Service: RestS3Service = null

  override def init(configDir: String, tier: Int, adapterType: String, tags: Set[String], uri: URI) {
    super.init(configDir, tier, adapterType, tags, uri)
    val (awsKey, awsSecret, awsBucketName) = parseAwsInfo(uri)
    val creds = new AWSCredentials(awsKey, awsSecret)
    _s3Service = new RestS3Service(creds)
    _bucketName = awsBucketName

    if (!_s3Service.isBucketAccessible(_bucketName)) {
      _s3Service.getOrCreateBucket(_bucketName)
      _isOnline = _s3Service.isBucketAccessible(_bucketName)
    }
  }

  def shutdown {}

  def parseAwsInfo(adapterUri: URI): (String, String, String) = {
    val parts = adapterUri.getAuthority.split("@")
    if (parts.length != 2) throw new IllegalArgumentException("authority format: awsKey@bucketname")
    val queryParams = UriUtil.parseQueryString(adapterUri)
    if (!queryParams.containsKey("secret")) throw new IllegalArgumentException("missing aws secret")
    (parts(0), queryParams.get("secret"), parts(1))
  }

  def refreshCache {}

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
    if (is.isInstanceOf[ByteArrayInputStream]) {
      val buffer = is.asInstanceOf[ByteArrayInputStream]
      try {
        buffer.mark(0)
        val md5Hash = CryptoUtil.computeMD5Hash(Channels.newChannel(buffer))
        buffer.reset
        store(ctx, buffer, md5Hash, buffer.available)
      } finally {
        buffer.close()
      }
    }
    else if (is.isInstanceOf[FileInputStream]) {
      val buffer = is.asInstanceOf[FileInputStream]
      try {
        buffer.mark(0)
        val md5Hash = CryptoUtil.computeMD5Hash(buffer.getChannel)
        buffer.reset
        store(ctx, buffer, md5Hash, buffer.available)
      } finally {
        buffer.close()
      }
    }
    else {
      val (hash, tmpFile) = StreamUtil.spoolStream(is)
      if (hash != ctx.hash) throw new RuntimeException("retrieved data hash %s not equal to expected %s".format(hash, ctx.hash))
      val fis = new FileInputStream(tmpFile)
      try {
        store(ctx, fis)
      } finally {
        fis.close()
        FileUtil.delete(tmpFile)
      }
    }
  }

  private def store(ctx: BlockContext, data: InputStream, md5Digest: Array[Byte], length: Long) {
    if (contains(ctx)) return
    val s3Object: S3Object = new S3Object(ctx.hash)
    s3Object.setDataInputStream(data)
    s3Object.setContentLength(length)
    s3Object.setMd5Hash(md5Digest)
    s3Object.setBucketName(_bucketName)
    _s3Service.putObject(_bucketName, s3Object)
  }

  def load(ctx: BlockContext): (InputStream, Int) = {
    if (!contains(ctx)) throw new DataNotFoundException(ctx.hash)
    val obj = _s3Service.getObject(_bucketName, ctx.hash)
    (obj.getDataInputStream, obj.getContentLength.toInt)
  }

  def describe: Set[BlockContext] = {
    val hashes = describeHashes

    val ctxs = new mutable.HashSet[BlockContext] with mutable.SynchronizedSet[BlockContext]
    hashes.par.filter(h => h.endsWith(".meta")).par.foreach{ hash =>
      val fis = _s3Service.getObject(_bucketName, hash).getDataInputStream
      try {
        val fmd = FileMetaData.create(hash, JsonUtil.loadJson(fis))
        ctxs.add(fmd.createBlockContext)
        val blockHashes = fmd.getBlockHashes
        (0 until blockHashes.length()).foreach{i =>
          val blockHash = blockHashes.getString(i)
          if (hashes.contains(blockHash)) {
            ctxs.add(fmd.createBlockContext(blockHash))
          }
        }
      } finally {
        FileUtil.SafeClose(fis)
      }
    }
    ctxs.toSet
  }


  def describeHashes: Set[String] = {
    Set() ++ _s3Service.listObjects(_bucketName).par.map(s3Object => s3Object.getKey)
  }
}