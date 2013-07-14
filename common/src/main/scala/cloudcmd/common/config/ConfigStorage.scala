package cloudcmd.common.config

import cloudcmd.common.adapters.{DirectAdapter, IndexedAdapter}
import cloudcmd.common.engine.ReplicationStrategy
import java.net.URI

trait ConfigStorage {
  def init(configRoot: String)

  def shutdown()

  def getConfigRoot: String

  def getProperty(key: String): String

  def getPropertyAsInt(key: String): Int

  def setAdapterTierRange(minTier: Int, maxTier: Int)

  def getPrimaryDirectAdapters: List[DirectAdapter]

  def getFilteredDirectAdapters: List[DirectAdapter]

  def getPrimaryIndexedAdapters: List[IndexedAdapter]

  def getFilteredIndexedAdapters: List[IndexedAdapter]

  def addAdapter(adapterUri: URI)

  def removeAdapter(adapterUri: URI): Boolean

  def isDebugEnabled: Boolean

  def createDefaultConfig(path: String)

  def writeConfig()

  def findIndexedAdapterByBestMatch(id: String): Option[IndexedAdapter] = {
    var maxMatchLength = 0
    var maxMatchAdapter: Option[IndexedAdapter] = None
    for (adapter <- getPrimaryIndexedAdapters) {
      val adapterUri = adapter.URI.toASCIIString
      if (id.length <= adapterUri.length) {
        var idx = 0
        while (idx < id.length && adapterUri.charAt(idx) == id.charAt(idx)) {
          idx = idx + 1
        }
        if (idx > maxMatchLength) {
          maxMatchLength = idx
          maxMatchAdapter = Some(adapter)
        }
      }
    }
    maxMatchAdapter
  }

  def getIndexedAdapter(adapterURI: URI): Option[IndexedAdapter]
  def getDirectAdapter(adapterURI: URI): Option[DirectAdapter]

  def getReplicationStrategy: ReplicationStrategy
}