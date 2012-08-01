package cloudcmd.cld

import cloudcmd.common.engine.{DefaultFileProcessor, H2IndexStorage, ParallelCloudEngine}
import cloudcmd.common.config.JsonConfigStorage

object CloudServices {
  val ConfigService = new JsonConfigStorage
  val CloudEngine = new ParallelCloudEngine(ConfigService)
  val IndexStorage = new H2IndexStorage(CloudEngine)
  val FileProcessor = new DefaultFileProcessor(ConfigService, CloudEngine, IndexStorage)

  def shutdown() {
    IndexStorage.shutdown
    CloudEngine.shutdown
    ConfigService.shutdown
  }
}
