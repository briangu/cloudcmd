package cloudcmd.common.util

import cloudcmd.common.FileUtil
import org.json.JSONArray
import org.json.JSONException
import org.json.JSONObject
import java.io._

object JsonUtil {

  def toStringMap(obj: JSONObject) : Map[String, String] = {
    import scala.collection.JavaConversions._
    Map() ++ obj.keys().flatMap{key => Map(key.asInstanceOf[String] -> obj.getString(key.asInstanceOf[String]))}
  }

  def mergeLeft(dest: JSONObject, src: JSONObject) {
    val keys = src.keys
    while (keys.hasNext) {
      val key = keys.next.asInstanceOf[String]
      dest.put(key, src.get(key))
    }
  }

  def createJsonObject(args: AnyRef*): JSONObject = {
    if (args.length % 2 != 0) throw new IllegalArgumentException("missing last value: args require key/value pairs")
    val obj = new JSONObject
    (0 until args.length by 2).foreach(i => obj.put(args(i).toString, args(i + 1)))
    obj
  }

  /***
   * For convenience, THIS METHOD CLOSES THE inputstream
   */
  def loadJsonArray(load: InputStream): JSONArray = {
    var dis : DataInputStream = null
    var isr : InputStreamReader = null
    var br : BufferedReader = null
    try {
      dis = new DataInputStream(load)
      isr = new InputStreamReader(dis)
      br = new BufferedReader(isr)
      new JSONArray(br.readLine)
    }
    finally {
      FileUtil.SafeClose(br)
      FileUtil.SafeClose(isr)
      FileUtil.SafeClose(dis)
      FileUtil.SafeClose(load)
    }
  }

  def loadJson(is: InputStream): JSONObject = {
    var dis : DataInputStream = null
    var isr : InputStreamReader = null
    var br : BufferedReader = null
    try {
      dis = new DataInputStream(is)
      isr = new InputStreamReader(dis)
      br = new BufferedReader(isr)
      new JSONObject(br.readLine)
    }
    finally {
      FileUtil.SafeClose(br)
      FileUtil.SafeClose(isr)
      FileUtil.SafeClose(dis)
      FileUtil.SafeClose(is)
    }
  }

  def createSet(array: JSONArray): Set[String] = {
    (0 until array.length).flatMap{i =>
      val objVal = array.getString(i)
      if (objVal.length > 0) Set(objVal) else Nil
    }.toSet
  }

  def createSet(rowTags: String, delimiter: String): Set[String] = {
    rowTags.split(delimiter).toSet
  }

  def prettyToString(jsonObject: JSONObject): String = {
    try {
      jsonObject.toString(2)
    }
    catch {
      case e: JSONException => jsonObject.toString
    }
  }
}