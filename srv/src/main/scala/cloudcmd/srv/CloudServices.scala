package cloudcmd.srv

import cloudcmd.common.engine.{DefaultFileProcessor, H2IndexStorage, ParallelCloudEngine}
import cloudcmd.common.config.JsonConfigStorage

object CloudServices {
  val ConfigService = new JsonConfigStorage
  val CloudEngine = new ParallelCloudEngine(ConfigService)
  val IndexStorage = new H2IndexStorage(CloudEngine)
  val FileProcessor = new DefaultFileProcessor(ConfigService, CloudEngine, IndexStorage, 640, 480)

  def init(configRoot: String) {
    CloudServices.ConfigService.init(configRoot)
    CloudServices.CloudEngine.init
    CloudServices.IndexStorage.init(configRoot)

    println("refreshing adapter caches")
    CloudEngine.refreshCache()

    println("initializing adapters with describe()")
    CloudEngine.describe()

    println("reindexing index storage")
    IndexStorage.reindex()

    println("ready!")
  }

  def shutdown() {
    IndexStorage.shutdown
    CloudEngine.shutdown
    ConfigService.shutdown
  }
}
