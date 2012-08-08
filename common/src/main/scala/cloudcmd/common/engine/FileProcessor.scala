package cloudcmd.common.engine

import java.io.File
import org.json.JSONObject
import cloudcmd.common.FileMetaData

trait FileProcessor extends EventSource {
  def add(file: File, tags: Set[String], mimeType: String = null, properties: JSONObject = new JSONObject) : FileMetaData
  def addAll(file: Set[File], tags: Set[String], properties: JSONObject = new JSONObject)  : Set[FileMetaData]
}
