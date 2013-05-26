package cloudcmd.common.adapters

import java.net.URI
import cloudcmd.common.{IndexedContentAddressableStorage, BlockContext, ContentAddressableStorage}
import java.io.File

trait DirectAdapter extends ContentAddressableStorage {

  def ConfigDir: String = _configDir
  def Type: String = _type
  def URI: URI = _uri
  def Tier: Int = _tier
  def AcceptsTags: Set[String] = _acceptsTags

  // TODO: this is dynamic
  def IsOnLine: Boolean = _isOnline
  // TODO: this is dynamic
  def IsFull: Boolean = false

  protected var _configDir: String = null
  protected var _type: String = null
  protected var _uri: URI = null
  protected var _tier: Int = 0
  protected var _acceptsTags: Set[String] = null
  protected var _keepTags: Set[String] = Set()
  protected var _ignoreTags: Set[String] = Set()
  protected var _isOnline = false

  def init(configDir: String, tier: Int, adapterType: String, acceptsTags: Set[String], uri: URI) {
    _configDir = if (configDir.endsWith(File.separator)) configDir.substring(0, configDir.length - 1) else configDir
    _tier = tier
    _type = adapterType
    _acceptsTags = acceptsTags
    _keepTags = _acceptsTags.filter(!_.startsWith("-"))
    _ignoreTags = _acceptsTags.filter(_.startsWith("-")).map(_.substring(1))
    _uri = uri
  }

  def shutdown()

  def accepts(ctx: BlockContext): Boolean = {
    if (_ignoreTags.intersect(ctx.routingTags).size > 0) return false
    if (_keepTags.size == 0) return true
    _keepTags.intersect(ctx.routingTags).size > 0
  }

  def getSignature: String = {
    val path = URI.getPath
    val hostOrPath = if (path.length == 0) {
      if (URI.getPort > 0) {
        "%s:%d".format(URI.getHost, URI.getPort)
      } else {
        URI.getHost
      }
    } else {
      path
    }
    "%s://%s".format(URI.getScheme, hostOrPath)
  }

  override def hashCode: Int = {
    URI.hashCode()
  }

  override def equals(obj: Any): Boolean = {
    obj.isInstanceOf[DirectAdapter] && obj.asInstanceOf[DirectAdapter].URI.equals(URI)
  }
}

trait IndexedAdapter extends DirectAdapter with IndexedContentAddressableStorage {}

