package cloudcmd.common.adapters

import java.net.URI
import cloudcmd.common.{BlockContext, ContentAddressableStorage}

trait Adapter extends ContentAddressableStorage {

  def ConfigDir: String = _configDir
  def Type: String = _type
  def URI: URI = _uri
  def Tier: Int = _tier
  def AcceptsTags: Set[String] = _acceptsTags
  def IsOnLine: Boolean = _isOnline
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
    _configDir = configDir
    _tier = tier
    _type = adapterType
    _acceptsTags = acceptsTags
    _uri = uri
  }

  def shutdown()

  def accepts(ctx: BlockContext): Boolean = {
    if (_ignoreTags.intersect(ctx.routingTags).size > 0) return false
    if (_keepTags.size == 0) return true
    _keepTags.intersect(ctx.routingTags).size > 0
  }

  override def hashCode: Int = {
    URI.hashCode()
  }

  override def equals(obj: Any): Boolean = {
    obj.isInstanceOf[Adapter] && obj.asInstanceOf[Adapter].URI.equals(URI)
  }
}