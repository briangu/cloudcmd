package cloudcmd.common.engine

import cloudcmd.common.util.FileMetaData
import org.json.JSONArray
import org.json.JSONObject

trait IndexStorage extends EventSource  {
  def init(configRoot: String)

  def purge

  def flush

  def shutdown

  def reindex()

  def find(filter: JSONObject): JSONArray

  def add(meta: FileMetaData)

  def addAll(fmds: List[FileMetaData])

  def remove(meta: FileMetaData)

  def pruneHistory(fmds: List[FileMetaData])

  def sync(selections: JSONArray)

  def fetch(selections: JSONArray)

  def verify(selections: JSONArray, deleteOnInvalid: Boolean)

  def remove(selections: JSONArray)

  def addTags(selections: JSONArray, tags: Set[String]): JSONArray
}