package cloudcmd.common.engine

import cloudcmd.common.adapters.Adapter
import cloudcmd.common.config.ConfigStorage
import cloudcmd.common.index.IndexStorage
import org.json.JSONArray
import java.io.{InputStream, File}

trait CloudEngine {
  def init(configService: ConfigStorage, indexStorage: IndexStorage)

  def filterAdapters(minTier: Int, maxTier: Int)

  def registerListener(listener: CloudEngineListener)

  def run()

  def shutdown()

  def sync(hash : String)
  def sync(hashes : Set[String])

  def verify(hashes: Set[String], deleteOnInvalid: Boolean)
  def verify(hash: String, deleteOnInvalid: Boolean)

  def load(hash: String) : InputStream

  def store(hash: String, is: InputStream)

  def remove(hash: String)
  def removeAll(hashes: Set[String])

  def getHashProviders(): Map[String, List[Adapter]]
  def getHashProviders(hash: String) : List[Adapter]
  def getMetaHashSet() : Set[String]
  def refreshAdapterCaches()
}