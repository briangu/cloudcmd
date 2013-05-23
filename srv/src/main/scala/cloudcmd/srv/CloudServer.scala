package cloudcmd.srv

import io.viper.common.{NestServer, ViperServer}
import cloudcmd.common.{IndexedContentAddressableStorage, FileUtil}
import java.io._
import cloudcmd.common.srv.{SimpleOAuthSessionService, CloudAdapter, OAuthRouteConfig}
import java.net.InetAddress

object CloudServer {
  def getIpAddress: String = {
    InetAddress.getLocalHost.getHostAddress
  }

  def main(args: Array[String]) {
    val ipAddress = if (args.length > 0) args(0) else "127.0.0.1"
    val port = if (args.length > 1) args(1).toInt else 8080
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
      CloudServices.initWithTierRange(0, Int.MaxValue)
      NestServer.run(port, new CloudServer(CloudServices.BlockStorage, apiConfig))
    } finally {
      CloudServices.shutdown()
    }
  }
}

class CloudServer(cas: IndexedContentAddressableStorage, apiConfig: OAuthRouteConfig) extends ViperServer("res:///cloudserver") {

  val _apiHandler = new CloudAdapter(cas, apiConfig)

  override
  def addRoutes {
    _apiHandler.addRoutes(this)
  }
}
