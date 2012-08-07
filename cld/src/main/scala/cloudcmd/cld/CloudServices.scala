package cloudcmd.cld

import cloudcmd.common.engine.{DefaultFileProcessor, H2IndexStorage, ParallelCloudEngine}
import cloudcmd.common.config.JsonConfigStorage
import com.thebuzzmedia.imgscalr.AsyncScalr

object CloudServices {
  val ConfigService = new JsonConfigStorage
  val CloudEngine = new ParallelCloudEngine(ConfigService)
  val IndexStorage = new H2IndexStorage(CloudEngine)
  val FileProcessor = new DefaultFileProcessor(ConfigService, CloudEngine, IndexStorage, 640, 480)

  def init(configRoot: String) {
    CloudServices.ConfigService.init(configRoot)
    CloudServices.CloudEngine.init
    CloudServices.IndexStorage.init(configRoot)
  }

  def shutdown() {
    IndexStorage.shutdown
    CloudEngine.shutdown
    ConfigService.shutdown

    if (AsyncScalr.getService != null) {
      AsyncScalr.getService.shutdownNow
    }
  }
}
