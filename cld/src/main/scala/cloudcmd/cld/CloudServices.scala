package cloudcmd.cld

import cloudcmd.common.engine._
import cloudcmd.common.config.JsonConfigStorage
import com.thebuzzmedia.imgscalr.AsyncScalr
import cloudcmd.common.adapters.ReplicationStrategyAdapter
import cloudcmd.common.ContentAddressableStorage

object CloudServices {

  def ConfigService = _configService
  def IndexStorage = _indexStorage
  def FileProcessor = _fileProcessor
  def BlockStorage: ContentAddressableStorage = _blockStorage

  private val _configService = new JsonConfigStorage
  private var _fileProcessor: FileProcessor = null
  private var _indexStorage: IndexStorage = null
  private var _blockStorage: ContentAddressableStorage = null
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
    _indexStorage = new H2IndexStorage(_blockStorage)
    _fileProcessor = new DefaultFileProcessor(ConfigService, _blockStorage, _indexStorage, 640, 480)

    CloudServices.ConfigService.getReplicationStrategy.registerListener(_listener)

    CloudServices.IndexStorage.registerListener(_listener)
    CloudServices.IndexStorage.init(_configRoot)

    CloudServices.FileProcessor.registerListener(_listener)
  }

  def shutdown() {
    if (_indexStorage != null) IndexStorage.shutdown()
    ConfigService.shutdown()

    if (AsyncScalr.getService != null) {
      AsyncScalr.getService.shutdownNow
    }
  }
}
