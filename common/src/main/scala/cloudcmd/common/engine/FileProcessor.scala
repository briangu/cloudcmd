package cloudcmd.common.engine

import java.io.File
import org.json.JSONObject

trait FileProcessor extends EventSource {
  def add(file: File, tags: Set[String], properties: JSONObject = new JSONObject)
  def addAll(file: Set[File], tags: Set[String], properties: JSONObject = new JSONObject)
}
