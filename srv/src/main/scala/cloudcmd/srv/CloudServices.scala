package cloudcmd.srv

import cloudcmd.common.engine._
import cloudcmd.common.config.JsonConfigStorage
import cloudcmd.common.adapters.ReplicationStrategyAdapter
import cloudcmd.common.ContentAddressableStorage

object CloudServices {

  def ConfigService = _configService
  def IndexStorage = _indexStorage
  def FileProcessor = _fileProcessor

  private val _configService = new JsonConfigStorage
  private var _fileProcessor: FileProcessor = null
  private var _indexStorage: IndexStorage = null
  private var _blockStorage: ContentAddressableStorage = null

  val BlockStorage: ContentAddressableStorage = {
    if (_blockStorage == null) {
      _blockStorage = new ReplicationStrategyAdapter(ConfigService.getFilteredAdapters, ConfigService.getReplicationStrategy)
      _indexStorage = new H2IndexStorage(_blockStorage)
      _fileProcessor = new DefaultFileProcessor(ConfigService, _blockStorage, _indexStorage, 640, 480)
    }
    _blockStorage
  }

  def init(configRoot: String) {
    CloudServices.ConfigService.init(configRoot)
    CloudServices.IndexStorage.init(configRoot)
  }

  def registerListener(listener: EngineEventListener) {
    CloudServices.ConfigService.getReplicationStrategy.registerListener(listener)
    CloudServices.IndexStorage.registerListener(listener)
    CloudServices.FileProcessor.registerListener(listener)
  }

  def shutdown() {
    IndexStorage.shutdown()
    ConfigService.shutdown()
  }
}
