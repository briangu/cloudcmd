package cloudcmd.common.index

import cloudcmd.common.FileMetaData
import org.json.JSONArray
import org.json.JSONObject
import java.util.List
import java.io.File

trait IndexStorage {
  def init(configRoot: String)

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

  def add(file: File, tags: java.util.Set[String])

  def batchAdd(file: java.util.Set[File], tags: java.util.Set[String])

  def sync(selections: JSONArray)

  def fetch(selections: JSONArray)

  def verify(selections: JSONArray, deleteOnInvalid: Boolean)

  def remove(selections: JSONArray)

  def addTags(selections: JSONArray, tags: java.util.Set[String]): JSONArray
}