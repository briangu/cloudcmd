package cloudcmd.common.config

import cloudcmd.common.adapters.Adapter
import cloudcmd.common.engine.ReplicationStrategy
import java.net.URI

trait ConfigStorage {
  def init(configRoot: String)

  def shutdown()

  def getConfigRoot: String

  def getProperty(key: String): String

  def getPropertyAsInt(key: String): Int

  def setAdapterTierRange(minTier: Int, maxTier: Int)

  def getAllAdapters: List[Adapter]

  def getFilteredAdapters: List[Adapter]

  def addAdapter(adapterUri: URI)

  def removeAdapter(adapterUri: URI): Boolean

  def isDebugEnabled: Boolean

  def createDefaultConfig(path: String)

  def writeConfig()

  def findAdapterByBestMatch(id: String): Option[Adapter] = {
    var maxMatchLength = 0
    var maxMatchAdapter: Option[Adapter] = None
    for (adapter <- getAllAdapters) {
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

  def getAdapter(adapterURI: URI): Adapter

  def getReplicationStrategy: ReplicationStrategy
}