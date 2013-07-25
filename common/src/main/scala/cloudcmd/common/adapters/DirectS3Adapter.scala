package cloudcmd.common.adapters

import cloudcmd.common.util.{CryptoUtil, StreamUtil}
import cloudcmd.common.{BlockContext, FileUtil, UriUtil}
import org.jets3t.service.impl.rest.httpclient.RestS3Service
import org.jets3t.service.model.S3Object
import org.jets3t.service.security.AWSCredentials
import java.io.{FileInputStream, InputStream}
import java.net.URI
import java.nio.channels.Channels
import org.jets3t.service.io.RepeatableInputStream

object DirectS3Adapter {
  def parseAwsInfo(adapterUri: URI): (String, String, String, String, Boolean) = {
    val parts = adapterUri.getAuthority.split("@")
    if (parts.length != 2) {
      throw new IllegalArgumentException("authority format: awsKey@bucketname")
    }

    val queryParams = UriUtil.parseQueryString(adapterUri)
    if (!queryParams.containsKey("secret")) {
      throw new IllegalArgumentException("missing aws secret")
    }

    val awsKey = parts(0)
    val awsSecret = queryParams.get("secret")
    val bucketName = parts(1)
    val objectPrefix = if (adapterUri.getPath.length > 0) {
      val partial = if (adapterUri.getPath.endsWith("/")) {
        adapterUri.getPath
      } else {
        "%s/".format(adapterUri.getPath)
      }
      if (partial.startsWith("/")) {
        partial.substring(1)
      } else {
        partial
      }
    } else {
      ""
    }

    val useRRS = Option(queryParams.get("useRRS")).getOrElse("true").toBoolean

    (awsKey, awsSecret, bucketName, objectPrefix, useRRS)
  }
}

//     "s3://<aws id>@<bucket>/<bucket path>?tier=2&tags=s3&secret=<aws secret>&useRRS=<bool>"
//        secret - AWS secret
//        (optional) tier - adapter tier
//        (optional) useRRS - use reduced redundancy storage: default is true
//        (optional) tags - tags that the adapter accepts or rejects

class DirectS3Adapter extends DirectAdapter {

  private var _s3Service: RestS3Service = null

  private var _bucketName: String = null
  private var _objectPrefix: String = null
  private var _useReducedRedundancy: Boolean = true

  override def init(configDir: String, tier: Int, adapterType: String, tags: Set[String], uri: URI) {
    super.init(configDir, tier, adapterType, tags, uri)

    val (awsKey, awsSecret, awsBucketName, objectPrefix, useRRS) = DirectS3Adapter.parseAwsInfo(URI)
    val creds = new AWSCredentials(awsKey, awsSecret)
    _s3Service = new RestS3Service(creds)
    _bucketName = awsBucketName
    _objectPrefix = objectPrefix
    _useReducedRedundancy = useRRS

    _isOnline = try {
      if (!_s3Service.isBucketAccessible(_bucketName)) {
        _s3Service.getOrCreateBucket(_bucketName)
        _s3Service.isBucketAccessible(_bucketName)
      } else {
        true
      }
    } catch {
      case e: Exception => {
        false
      }
    }
  }

  def shutdown() {}

  private def getObjectNameFromBlockContext(ctx: BlockContext): String = {
    ctx.ownerId match {
      case Some(id) => "%s%s/%s".format(_objectPrefix, id, ctx.hash)
      case None => "%s%s".format(_objectPrefix, ctx.hash)
    }
  }

  def containsAll(ctxs: Set[BlockContext]): Map[BlockContext, Boolean] = {
    Map() ++ ctxs.par.flatMap{ctx =>
      Map(ctx -> _s3Service.isObjectInBucket(_bucketName, getObjectNameFromBlockContext(ctx)))
    }
  }

  def removeAll(ctxs: Set[BlockContext]): Map[BlockContext, Boolean] = {
    Map() ++ ctxs.par.flatMap{ctx =>
      _s3Service.deleteObject(_bucketName, getObjectNameFromBlockContext(ctx))
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
      if (hash != getHashFromDataFile(ctx.hash)) {
        throw new RuntimeException("retrieved data hash %s not equal to expected %s".format(hash, ctx.hash))
      }
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
    val s3Object: S3Object = new S3Object(getObjectNameFromBlockContext(ctx))
    s3Object.setDataInputStream(new RepeatableInputStream(data, length.toInt))
    s3Object.setContentLength(length)
    s3Object.setMd5Hash(md5Digest)
    s3Object.setBucketName(_bucketName)
    if (_useReducedRedundancy) s3Object.setStorageClass(S3Object.STORAGE_CLASS_REDUCED_REDUNDANCY)
    _s3Service.putObject(_bucketName, s3Object)
  }

  def load(ctx: BlockContext): (InputStream, Int) = {
    val obj = _s3Service.getObject(_bucketName, getObjectNameFromBlockContext(ctx))
    (obj.getDataInputStream, obj.getContentLength.toInt)
  }

  /** *
    * Describe the contents of the s3 bucket.
    * If ownerId is specified then return the description of the objects for that owner (without a prefix),
    * otherwise return all objects for all owners prefixed by the ownerId
    * @param ownerId Owner to describe objects for. May be None.
    * @return
    */
  def describe(ownerId: Option[String] = None): Set[String] = {
    ownerId match {
      case Some(id) => {
        val prefix = "%s%s/".format(_objectPrefix, id)
        val objList = _s3Service.listObjects(_bucketName, prefix, "/", Int.MaxValue)
        Set() ++ objList.flatMap{ obj =>
          if (obj.getKey.length > prefix.length) {
            Set(obj.getKey.substring(prefix.length))
          } else {
            // sometimes S3 returns the prefix object itself
            None
          }
        }
      }
      case None => {
        val listObjects = if (_objectPrefix == "") {
          _s3Service.listObjects(_bucketName)
        } else {
          _s3Service.listObjects(_bucketName, _objectPrefix, "/")
        }
        Set() ++ listObjects.flatMap(obj => Set(obj.getKey))
      }
    }
  }
}