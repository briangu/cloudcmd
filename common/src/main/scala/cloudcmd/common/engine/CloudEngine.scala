package cloudcmd.common.engine

import cloudcmd.common.adapters.Adapter
import cloudcmd.common.config.ConfigStorage
import cloudcmd.common.index.IndexStorage
import org.json.JSONArray
import java.io.File

trait CloudEngine {
  def init(configService: ConfigStorage, indexStorage: IndexStorage)

  def run()

  def shutdown()

  def reindex()

  def add(file: File, tags: java.util.Set[String], adapter: Adapter)

  def batchAdd(file: java.util.Set[File], tags: java.util.Set[String], adapter: Adapter)

  def sync(minTier: Int, maxTier: Int, selections: JSONArray)

  def fetch(minTier: Int, maxTier: Int, selections: JSONArray)

  def verify(minTier: Int, maxTier: Int, selections: JSONArray, deleteOnInvalid: Boolean)

  def remove(minTier: Int, maxTier: Int, selections: JSONArray)

  def addTags(selections: JSONArray, tags: java.util.Set[String]): JSONArray

  def registerListener(listener: CloudEngineListener)
}