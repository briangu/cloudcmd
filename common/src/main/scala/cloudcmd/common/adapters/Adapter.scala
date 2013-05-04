package cloudcmd.common.adapters

import java.net.URI
import cloudcmd.common.{BlockContext, ContentAddressableStorage}

trait Adapter extends ContentAddressableStorage {

  var ConfigDir: String = null
  var Type: String = null
  var URI: URI = null
  var Tier: Int = 0

  protected var _isOnline = false

  def IsOnLine(): Boolean = _isOnline
  def IsFull(): Boolean = false

  def init(configDir: String, tier: Int, adapterType: String, acceptsTags: Set[String], uri: URI) {
    ConfigDir = configDir
    Tier = tier
    Type = adapterType
    URI = uri

    AcceptsTags = acceptsTags
    KeepTags = acceptsTags.filterNot(_.startsWith("-"))
    IgnoreTags = (acceptsTags -- KeepTags).map(_.substring(1))
  }

  def shutdown()

  var AcceptsTags: Set[String] = null
  protected var KeepTags: Set[String] = Set()
  protected var IgnoreTags: Set[String] = Set()

  def accepts(ctx: BlockContext): Boolean = {
    if (IgnoreTags.intersect(ctx.routingTags).size > 0) return false
    if (KeepTags.size == 0) return true
    KeepTags.intersect(ctx.routingTags).size > 0
  }

  override def hashCode: Int = {
    URI.hashCode()
  }

  override def equals(obj: Any): Boolean = {
    obj.isInstanceOf[Adapter] && obj.asInstanceOf[Adapter].URI.equals(URI)
  }
}