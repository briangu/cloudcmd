package cloudcmd.common.adapters

import org.jboss.netty.buffer.ChannelBuffer
import java.io.InputStream
import java.net.URI

trait Adapter {

  var ConfigDir: String = null
  var Type: String = null
  var AcceptsTags: Set[String] = null
  var URI: URI = null
  var Tier: Int = 0

  protected var _isOnline = false

  def IsOnLine(): Boolean = _isOnline
  def IsFull(): Boolean = false

  def init(configDir: String, tier: Int, adapterType: String, acceptsTags: Set[String], uri: URI) {
    ConfigDir = configDir
    Tier = tier
    Type = adapterType
    AcceptsTags = acceptsTags
    URI = uri
  }

  def shutdown()

  def accepts(tags: Set[String]): Boolean = {
    if (AcceptsTags == null || AcceptsTags.size == 0 || tags.size == 0) return true
    AcceptsTags.intersect(tags).size > 0
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