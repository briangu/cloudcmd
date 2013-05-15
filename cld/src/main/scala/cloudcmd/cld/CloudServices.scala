package cloudcmd.cld

import cloudcmd.common.engine._
import cloudcmd.common.config.JsonConfigStorage
import cloudcmd.common.adapters.ReplicationStrategyAdapter
import cloudcmd.common.IndexedContentAddressableStorage
import cloudcmd.common.util.FileWalker

object CloudServices {

  def ConfigService = _configService
  def BlockStorage: IndexedContentAddressableStorage = _blockStorage

  private val _configService = new JsonConfigStorage
  private var _blockStorage: IndexedContentAddressableStorage = null
  private var _configRoot: String = null
  private var _listener: EngineEventListener = null

  def setConfigRoot(configRoot: String) {
    _configRoot = configRoot

    CloudServices.ConfigService.init(_configRoot)
  }

  def setListener(listener: EngineEventListener) {
    _listener = listener
  }

  def initWithTierRange(minTier: Int, maxTier: Int) {
    _configService.setAdapterTierRange(minTier, maxTier)
    _blockStorage = new ReplicationStrategyAdapter(_configService.getFilteredAdapters, _configService.getReplicationStrategy)

    CloudServices.ConfigService.getReplicationStrategy.registerListener(_listener)
  }

  def shutdown() {
    ConfigService.shutdown()
  }

  def onMessage(msg: String) {
    if (_listener != null) {
      _listener.onMessage(msg)
    }
  }
}
