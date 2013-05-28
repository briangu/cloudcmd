package cloudcmd.common

import org.json.JSONObject
import util.JsonUtil

object BlockContext {
  def fromJson(json: String) : BlockContext = {
    val obj = new JSONObject(json)
    new BlockContext(obj.getString("hash"), JsonUtil.createSet(obj.getJSONArray("tags")), None)
  }
  def fromJson(obj: JSONObject, ownerId: Option[String] = None) : BlockContext = {
    new BlockContext(obj.getString("hash"), JsonUtil.createSet(obj.getJSONArray("tags")), ownerId)
  }
  def create(hash: String, tags: String, delim: String, ownerId: Option[String] = None) : BlockContext = {
    new BlockContext(hash, tags.split(delim).filter(_.length > 0).toSet, ownerId)
  }
}

class BlockContext(val hash: String, val routingTags: Set[String] = Set(), val ownerId: Option[String] = None) {
  def getId: String = hash
  def isMeta: Boolean = hash.endsWith(".meta")

  def hashEquals(test: String) : Boolean = {
    (if (isMeta) test.equals(hash.substring(0, hash.length - ".meta".length)) else test.equals(hash))
  }

  override def hashCode() : Int = 31 * getId.hashCode * routingTags.hashCode
  override def equals(other: Any) : Boolean = {
    if (!other.isInstanceOf[BlockContext]) return false
    val octx = other.asInstanceOf[BlockContext]
    getId.equals(octx.hash) && routingTags.equals(octx.routingTags)
  }

  def toJson : JSONObject = {
    val obj = new JSONObject()
    obj.put("hash", hash)
    obj.put("tags", JsonUtil.toJsonArray(routingTags))
    obj
  }

  override def toString() : String = hash
}
