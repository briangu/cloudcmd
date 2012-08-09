package cloudcmd.srv

import io.viper.common.{NestServer, ViperServer}
import cloudcmd.common.{ContentAddressableStorage, FileUtil}
import java.io._
import cloudcmd.common.srv.{SimpleOAuthSessionService, OAuthSessionService, CloudAdapter, OAuthRouteConfig}
import java.net.InetAddress
import cloudcmd.common.engine.IndexStorage

object CloudServer {
  def getIpAddress: String = {
    InetAddress.getLocalHost.getHostAddress
  }

  def main(args: Array[String]) {
    val ipAddress = getIpAddress
    val port = 8080
    println("booting at http://%s:%d".format(ipAddress, port))

    var configRoot: String = FileUtil.findConfigDir(FileUtil.getCurrentWorkingDirectory, ".cld")
    if (configRoot == null) {
      configRoot = System.getenv("HOME") + File.separator + ".cld"
      new File(configRoot).mkdir
    }

    CloudServices.init(configRoot)

    try {
      val baseHostPort = "http://%s:%d".format(ipAddress, port)
      val apiConfig = new OAuthRouteConfig(baseHostPort, SimpleOAuthSessionService.instance)
      NestServer.run(8080, new CloudServer(CloudServices.CloudEngine, CloudServices.IndexStorage, apiConfig))
    } finally {
      CloudServices.shutdown
    }
  }
}

class CloudServer(cas: ContentAddressableStorage, indexStorage: IndexStorage, apiConfig: OAuthRouteConfig) extends ViperServer("res:///cloudserver") {

  val _apiHandler = new CloudAdapter(cas, indexStorage, apiConfig)

  override
  def addRoutes {
    _apiHandler.addRoutes(this)
  }
}
