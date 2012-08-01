package cloudcmd.common.index

import cloudcmd.common.util.FileMetaData
import org.json.JSONArray
import org.json.JSONObject
import java.io.File
import cloudcmd.common.engine.CloudEngine

trait IndexStorage {
  def init(configRoot: String, cloudEngine: CloudEngine)

  def purge

  def flush

  def shutdown

  def registerListener(listener: IndexStorageListener)

  def find(filter: JSONObject): JSONArray

  def add(meta: FileMetaData)

  def remove(meta: FileMetaData)

  def addAll(fmds: List[FileMetaData])

  def pruneHistory(fmds: List[FileMetaData])

  def reindex()

  def add(file: File, tags: Set[String])

  def batchAdd(file: Set[File], tags: Set[String])

  def sync(selections: JSONArray)

  def fetch(selections: JSONArray)

  def verify(selections: JSONArray, deleteOnInvalid: Boolean)

  def remove(selections: JSONArray)

  def addTags(selections: JSONArray, tags: Set[String]): JSONArray
}