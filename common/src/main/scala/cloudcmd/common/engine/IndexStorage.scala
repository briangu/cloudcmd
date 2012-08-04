package cloudcmd.common.engine

import cloudcmd.common.FileMetaData
import org.json.JSONArray
import org.json.JSONObject

trait IndexStorage extends EventSource  {
  def init(configRoot: String)

  def purge

  def flush

  def shutdown

  def reindex()

  def find(filter: JSONObject): JSONArray

  def add(fmd: FileMetaData)

  def addAll(fmds: List[FileMetaData])

  def remove(fmd: FileMetaData)

  def pruneHistory(fmds: List[FileMetaData])

  def get(fmds: JSONArray)

  def ensure(fmds: JSONArray, blockLevelCheck: Boolean)

  def remove(fmds: JSONArray)

  def addTags(fmds: JSONArray, tags: Set[String]): JSONArray
}