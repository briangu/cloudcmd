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

  def find(filter: JSONObject): Seq[FileMetaData]

  def add(fmd: FileMetaData)

  def addAll(fmds: Seq[FileMetaData])

  def remove(fmd: FileMetaData)

  def pruneHistory(fmds: Seq[FileMetaData])

  def get(fmds: Seq[FileMetaData])

  def ensure(fmds: Seq[FileMetaData], blockLevelCheck: Boolean)

  def remove(fmds: Seq[FileMetaData])

  def addTags(fmds: Seq[FileMetaData], tags: Set[String]): Seq[FileMetaData]
}