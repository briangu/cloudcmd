package cloudcmd.common.util

import cloudcmd.common.ResourceUtil
import org.json.JSONException
import org.json.JSONObject
import java.io.IOException

object FileTypeUtil {

  def instance: FileTypeUtil = {
    if (_instance == null) {
      classOf[FileTypeUtil] synchronized {
        if (_instance == null) {
          val ftu: FileTypeUtil = new FileTypeUtil
          try {
            ftu._typeMap = ResourceUtil.loadJson("mimeTypes.json")
            val skipMap: JSONObject = ResourceUtil.loadJson("skipMap.json")
            ftu._skipDir = JsonUtil.createSet(skipMap.getJSONArray("dirs"))
            ftu._skipExt = JsonUtil.createSet(skipMap.getJSONArray("exts"))
          }
          catch {
            case e: IOException => {
              e.printStackTrace
            }
            case e: JSONException => {
              e.printStackTrace
            }
          }
          _instance = ftu
        }
      }
    }
    _instance
  }

  private var _instance: FileTypeUtil = null
}

class FileTypeUtil {

  private var _typeMap: JSONObject = null
  private var _skipDir: Set[String] = null
  private var _skipExt: Set[String] = null

  def skipDir(dir: String): Boolean = {
    _skipDir.contains(dir)
  }

  def skipExt(ext: String): Boolean = {
    _skipExt.contains(ext)
  }

  def getTypeFromName(filename: String): String = {
    val idx: Int = filename.lastIndexOf(".")
    if ((idx > -1)) getTypeFromExtension(filename.substring(idx + 1)) else ""
  }

  def getTypeFromExtension(ext: String): String = {
    if (ext == null) return "applicaton/octet-stream"
    val lcext = ext.toLowerCase
    if (_typeMap.has(lcext)) _typeMap.getString(lcext) else "applicaton/octet-stream"
  }
}