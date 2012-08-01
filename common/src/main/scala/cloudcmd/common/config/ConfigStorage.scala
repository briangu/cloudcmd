package cloudcmd.common.config

import cloudcmd.common.adapters.Adapter
import cloudcmd.common.engine.ReplicationStrategy
import java.net.URI
import java.util.List

trait ConfigStorage {
  def init(configRoot: String)

  def shutdown

  def getConfigRoot: String

  def getProperty(key: String): String

  def getPropertyAsInt(key: String): Int

  def getAdapters: List[Adapter]

  def addAdapter(adapterUri: URI)

  def removeAdapter(adapterUri: URI): Boolean

  def isDebugEnabled: Boolean

  def createDefaultConfig(path: String)

  def writeConfig

  def getAdapter(adapterURI: URI): Adapter

  def getReplicationStrategy: ReplicationStrategy
}