package cloudcmd.cld

import cloudcmd.common.config.JsonConfigStorage
import cloudcmd.common.adapters.ReplicationStrategyAdapter
import cloudcmd.common.IndexedContentAddressableStorage

object CloudServices {

  def ConfigService = _configService
  def BlockStorage: IndexedContentAddressableStorage = _blockStorage

  private val _configService = new JsonConfigStorage
  private var _blockStorage: IndexedContentAddressableStorage = null
  private var _configRoot: String = null

  def setConfigRoot(configRoot: String) {
    _configRoot = configRoot

    CloudServices.ConfigService.init(_configRoot)
  }

  def initWithTierRange(minTier: Int, maxTier: Int) {
    _configService.setAdapterTierRange(minTier, maxTier)
    _blockStorage = new ReplicationStrategyAdapter(_configService.getFilteredAdapters, _configService.getReplicationStrategy)
  }

  def shutdown() {
    ConfigService.shutdown()
  }
}
