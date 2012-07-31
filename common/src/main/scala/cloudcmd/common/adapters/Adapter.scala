package cloudcmd.common.adapters

import org.jboss.netty.buffer.ChannelBuffer
import java.io.InputStream
import java.net.URI

trait Adapter {

  var ConfigDir: String = null
  var Type: String = null
  var Tags: Set[String] = null
  var URI: URI = null
  var Tier: Int = 0
  protected var _isOnline: Boolean = false

  def IsOnLine(): Boolean = true
  def IsFull(): Boolean = true

  def init(configDir: String, tier: Int, adapterType: String, tags: java.util.Set[String], uri: URI) {
    import scala.collection.JavaConversions._
    ConfigDir = configDir
    Tier = tier
    Type = adapterType
    Tags = tags.toSet
    URI = uri
  }

  def shutdown()

  def accepts(tags: Set[String]): Boolean = {
    if (Tags == null || Tags.size == 0) true
    for (tag <- tags) {
      if (Tags.contains(tag)) true
    }
    false
  }

  def remove(hash: String): Boolean

  def verify(hash: String): Boolean

  def refreshCache()

  def contains(hash: String): Boolean

  def store(data: InputStream, hash: String)

  def load(hash: String): InputStream

  def loadChannel(hash: String): ChannelBuffer

  def describe(): Set[String]
}