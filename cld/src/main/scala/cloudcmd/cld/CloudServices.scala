package cloudcmd.cld

import cloudcmd.common.engine._
import cloudcmd.common.config.JsonConfigStorage
import com.thebuzzmedia.imgscalr.AsyncScalr
import cloudcmd.common.adapters.ReplicationStrategyAdapter
import cloudcmd.common.IndexedContentAddressableStorage

object CloudServices {

  def ConfigService = _configService
  def FileProcessor = _fileProcessor
  def BlockStorage: IndexedContentAddressableStorage = _blockStorage

  private val _configService = new JsonConfigStorage
  private var _fileProcessor: FileProcessor = null
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
    _fileProcessor = new DefaultFileProcessor(_blockStorage)

    CloudServices.ConfigService.getReplicationStrategy.registerListener(_listener)

    CloudServices.FileProcessor.registerListener(_listener)
  }

  def shutdown() {
    ConfigService.shutdown()

    if (AsyncScalr.getService != null) {
      AsyncScalr.getService.shutdownNow
    }
  }
}
