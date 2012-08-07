package cloudcmd.srv

import cloudcmd.common.engine.ParallelCloudEngine
import cloudcmd.common.config.JsonConfigStorage

object CloudServices {
  val ConfigService = new JsonConfigStorage
  val CloudEngine = new ParallelCloudEngine(ConfigService)

  def init(configRoot: String) {
    CloudServices.ConfigService.init(configRoot)
    CloudServices.CloudEngine.init
  }

  def shutdown() {
    CloudEngine.shutdown
    ConfigService.shutdown
  }
}
