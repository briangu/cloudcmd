package cloudcmd.common.config

import java.net.URI
import cloudcmd.common.adapters.{IndexedAdapter, DirectAdapter}
import cloudcmd.common.util.CryptoUtil
import java.nio.channels.Channels
import java.io.{File, ByteArrayInputStream}
import cloudcmd.common.UriUtil

object AdapterFactory {

  def getDefaultIndexedAdapterHandlers: Map[String, String] = {
    Map("file" -> "cloudcmd.common.adapters.FileAdapter",
        "s3" -> "cloudcmd.common.adapters.S3Adapter",
        "http" -> "cloudcmd.common.adapters.HttpAdapter")
  }

  def getDefaultDirectAdapterHandlers: Map[String, String] = {
    Map("file" -> "cloudcmd.common.adapters.DirectFileAdapter",
        "s3" -> "cloudcmd.common.adapters.DirectS3Adapter",
        "http" -> "cloudcmd.common.adapters.DirectHttpAdapter")
  }

  def createIndexedAdapter(configRoot: String, adapterHandlers: Map[String, String], adapterUri: URI, defaultTier: Int = 1): IndexedAdapter = {
    val directAdapter = createDirectAdapter(configRoot, adapterHandlers, adapterUri, defaultTier)
    if (directAdapter.isInstanceOf[IndexedAdapter]) {
      directAdapter.asInstanceOf[IndexedAdapter]
    } else {
      throw new IllegalArgumentException("adapter %s is not an indexed adapter".format(adapterUri.toASCIIString))
    }
  }

  def createDirectAdapter(adapterUri: URI): DirectAdapter = {
    createDirectAdapter("", getDefaultDirectAdapterHandlers, adapterUri)
  }

  def createDirectAdapter(configRoot: String, adapterHandlers: Map[String, String], adapterUri: URI, defaultTier: Int = 1): DirectAdapter = {
    var adapter: DirectAdapter = null
    val scheme = adapterUri.getScheme
    if (!adapterHandlers.contains(scheme)) {
      throw new IllegalArgumentException(String.format("scheme %s in adapter URI %s is not supported!", scheme, adapterUri))
    }
    val handlerType = adapterHandlers.get(scheme).get
    val tier = getTierFromUri(adapterUri, defaultTier)
    val tags = getTagsFromUri(adapterUri)
    val clazz = classOf[JsonConfigStorage].getClassLoader.loadClass(handlerType)
    try {
      adapter = clazz.newInstance.asInstanceOf[DirectAdapter]
      val adapterSignature = getAdapterSignature(adapterUri)
      val adapterIdHash = CryptoUtil.digestToString(CryptoUtil.computeMD5Hash(Channels.newChannel(new ByteArrayInputStream(adapterSignature.getBytes("UTF-8")))))
      adapter.init(configRoot + File.separator + "adapterCaches" + File.separator + adapterIdHash, tier, handlerType, tags.toSet, adapterUri)
    }
    catch {
      case e: Exception => {
        throw new RuntimeException(String.format("failed to initialize adapter %s for adapter %s", handlerType, adapterUri), e)
      }
    }
    adapter
  }

  private def getAdapterSignature(uri: URI): String = {
    val path = uri.getPath
    if (path.length == 0) {
      uri.getAuthority
    } else {
      path
    }
  }

  private def getTierFromUri(adapterUri: URI, defaultTier: Int = 1): Int = {
    val queryParams = UriUtil.parseQueryString(adapterUri)
    if ((queryParams.containsKey("tier"))) queryParams.get("tier").toInt else defaultTier
  }

  private def getTagsFromUri(adapterUri: URI): Set[String] = {
    val queryParams = UriUtil.parseQueryString(adapterUri)
    if (queryParams.containsKey("tags")) {
      val parts = queryParams.get("tags").split(",").filter(_.length > 0)
      parts.flatMap(Set(_)).toSet
    } else {
      Set()
    }
  }
}
