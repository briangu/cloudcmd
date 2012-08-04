package cloudcmd.common

import org.json.JSONObject
import util.JsonUtil

object BlockContext {
  def fromJson(json: String) : BlockContext = {
    val obj = new JSONObject(json)
    new BlockContext(obj.getString("hash"), JsonUtil.createSet(obj.getJSONArray("tags")))
  }
}

class BlockContext(val hash: String, val routingTags: Set[String] = Set()) {
  def getId() : String = hash

  override def hashCode() : Int = 31 * getId().hashCode * routingTags.hashCode
  override def equals(other: Any) : Boolean = {
    if (!other.isInstanceOf[BlockContext]) return false
    val octx = other.asInstanceOf[BlockContext]
    getId().equals(octx.hash) && routingTags.equals(octx.routingTags)
  }

  def toJson : JSONObject = {
    val obj = new JSONObject()
    obj.put("hash", hash)
    obj.put("tags", JsonUtil.toJsonArray(routingTags))
    obj
  }

  override def toString() : String = hash
}
