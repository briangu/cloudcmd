package cloudcmd.srv

import io.viper.common.{NestServer, ViperServer}
import cloudcmd.common.{IndexedContentAddressableStorage, FileUtil}
import java.io._
import cloudcmd.common.srv.{FileServices, SimpleOAuthSessionService, CloudAdapter, OAuthRouteConfig}
import java.net.InetAddress
import org.apache.log4j.Logger

object CloudServer {

  private val log = Logger.getLogger(classOf[CloudServer])

  def report(msg: String) {
    log.info(msg)
    println(msg)
  }

  def getIpAddress: String = {
    InetAddress.getLocalHost.getHostAddress
  }

  def main(args: Array[String]) {
    try {
      val ipAddress = if (args.length > 0) args(0) else "127.0.0.1"
      val port = if (args.length > 1) args(1).toInt else 8080
      report("booting at http://%s:%d".format(ipAddress, port))

      var configRoot: String = FileUtil.findConfigDir(FileUtil.getCurrentWorkingDirectory, ".cld")
      if (configRoot == null) {
        configRoot = System.getenv("HOME") + File.separator + ".cld"
        new File(configRoot).mkdir
      }

      report("config root = %s".format(configRoot))

      CloudServices.setConfigRoot(configRoot)

      val baseHostPort = "http://%s:%d".format(ipAddress, port)
      val apiConfig = new OAuthRouteConfig(baseHostPort, SimpleOAuthSessionService.instance)
      CloudServices.initWithTierRange(0, Int.MaxValue)

      report("using all available adapters:")
      CloudServices.ConfigService.getFilteredAdapters foreach { adapter =>
        report("\t%s".format(adapter.getSignature))
      }

      NestServer.run(port, new CloudServer(CloudServices.BlockStorage, apiConfig))
    } finally {
      CloudServices.shutdown()
    }
  }
}

class CloudServer(cas: IndexedContentAddressableStorage, apiConfig: OAuthRouteConfig) extends ViperServer("res:///cloudserver") {

  val _apiHandler = new CloudAdapter(cas, apiConfig)
  val _fileServices = new FileServices(cas, apiConfig)

  override
  def addRoutes {
    _apiHandler.addRoutes(this)
    _fileServices.addRoutes(this)
  }
}
