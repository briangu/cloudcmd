package cloudcmd.common.engine

import java.io.File
import org.json.JSONObject
import cloudcmd.common.FileMetaData

trait FileProcessor extends EventSource {
  def add(file: File, fileName: String, tags: Set[String], properties: JSONObject = new JSONObject, mimeType: String = null) : FileMetaData
}
