package cloudcmd.srv

import io.viper.common.{NestServer, ViperServer}
import cloudcmd.common.{IndexedContentAddressableStorage, ContentAddressableStorage, FileUtil}
import java.io._
import cloudcmd.common.srv.{SimpleOAuthSessionService, OAuthSessionService, CloudAdapter, OAuthRouteConfig}
import java.net.InetAddress
import cloudcmd.common.engine.IndexStorage

object CloudServer {
  def getIpAddress: String = {
    InetAddress.getLocalHost.getHostAddress
  }

  def main(args: Array[String]) {
    val ipAddress = args(0)
    val port = args(1).toInt
    println("booting at http://%s:%d".format(ipAddress, port))

    var configRoot: String = FileUtil.findConfigDir(FileUtil.getCurrentWorkingDirectory, ".cld")
    if (configRoot == null) {
      configRoot = System.getenv("HOME") + File.separator + ".cld"
      new File(configRoot).mkdir
    }

    println("config root = %s".format(configRoot))

    CloudServices.setConfigRoot(configRoot)

    try {
      val baseHostPort = "http://%s:%d".format(ipAddress, port)
      val apiConfig = new OAuthRouteConfig(baseHostPort, SimpleOAuthSessionService.instance)
      NestServer.run(8080, new CloudServer(CloudServices.BlockStorage, CloudServices.IndexStorage, apiConfig))
    } finally {
      CloudServices.shutdown()
    }
  }
}

class CloudServer(cas: IndexedContentAddressableStorage, indexStorage: IndexStorage, apiConfig: OAuthRouteConfig) extends ViperServer("res:///cloudserver") {

  val _apiHandler = new CloudAdapter(cas, indexStorage, apiConfig)

  override
  def addRoutes {
    _apiHandler.addRoutes(this)
  }
}
