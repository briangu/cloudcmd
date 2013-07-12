package cloudcmd.srv

import cloudcmd.common.config.JsonConfigStorage
import cloudcmd.common.adapters.{DirectAdapter, ReplicationStrategyAdapter}
import cloudcmd.common.{ContentAddressableStorage, IndexedContentAddressableStorage}
import cloudcmd.common.srv.ThumbnailService

object CloudServices {

  def ConfigService = _configService
  def BlockStorage: IndexedContentAddressableStorage = _blockStorage
  def ThumbCAS: ContentAddressableStorage = _thumbCAS

  private val _configService = new JsonConfigStorage
  private var _blockStorage: IndexedContentAddressableStorage = null
  private var _thumbCAS: DirectAdapter = null
  private var _configRoot: String = null

  def setConfigRoot(configRoot: String) {
    _configRoot = configRoot

    CloudServices.ConfigService.init(_configRoot)
  }

  def initWithTierRange(minTier: Int, maxTier: Int) {
    _configService.setAdapterTierRange(minTier, maxTier)
    _blockStorage = new ReplicationStrategyAdapter(_configService.getFilteredAdapters, _configService.getReplicationStrategy)

    ConfigService.getAuxilaryAdapter("thumb") match {
      case Some(adapter) => _thumbCAS = adapter
      case None => throw new RuntimeException("missing thumbCAS adapter")
    }

    ThumbnailService.start()
  }

  def shutdown() {
    ThumbnailService.shutdown()
    ConfigService.shutdown()
  }
}
