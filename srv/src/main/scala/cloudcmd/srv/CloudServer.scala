package cloudcmd.srv

import io.viper.common.{NestServer, ViperServer}
import cloudcmd.common.{ContentAddressableStorage, FileUtil}
import java.io._
import cloudcmd.common.srv.{SimpleAuthSessionService, CloudAdapter, HmacRouteConfig}

object CloudServer {
  def main(args: Array[String]) {
    var configRoot: String = FileUtil.findConfigDir(FileUtil.getCurrentWorkingDirectory, ".cld")
    if (configRoot == null) {
      configRoot = System.getenv("HOME") + File.separator + ".cld"
      new File(configRoot).mkdir
    }

    CloudServices.init(configRoot)

    try {
      val apiConfig = new HmacRouteConfig(SimpleAuthSessionService.instance)
      NestServer.run(8080, new CloudServer(CloudServices.CloudEngine, apiConfig))
    } finally {
      CloudServices.shutdown
    }
  }
}

class CloudServer(cas: ContentAddressableStorage, apiConfig: HmacRouteConfig) extends ViperServer("res:///cloudserver") {

  val _apiHandler = new CloudAdapter(cas, apiConfig)

  override
  def addRoutes {
    _apiHandler.addRoutes(this)
  }
}
