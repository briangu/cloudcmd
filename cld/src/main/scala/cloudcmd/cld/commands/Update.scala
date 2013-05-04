package cloudcmd.cld.commands

import jpbetz.cli.Command
import jpbetz.cli.CommandContext
import jpbetz.cli.Opt
import jpbetz.cli.SubCommand
import java.net.URI
import cloudcmd.cld.CloudServices

@SubCommand(name = "update", description = "update cached adapter information.")
class Update extends Command {

  @Opt(opt = "n", longOpt = "minTier", description = "min tier to verify to", required = false) private var _minTier: Number = 0
  @Opt(opt = "m", longOpt = "maxTier", description = "max tier to verify to", required = false) private var _maxTier: Number = Integer.MAX_VALUE
  @Opt(opt = "u", longOpt = "uri", description = "adapter URI", required = false) private var _uri: String = null

  def exec(commandLine: CommandContext) {
    if (_uri == null) {
      System.err.println("updating all adapters")
      CloudServices.CloudEngine.filterAdapters(_minTier.intValue, _maxTier.intValue)
      CloudServices.CloudEngine.refreshCache()
    }
    else {
      val adapterURI: URI = new URI(_uri)
      for (adapter <- CloudServices.ConfigService.getAdapters) {
        if ((adapter.URI.toString == _uri) || ((adapterURI.getPath == adapter.URI.getPath))) {
          System.err.println("updating adapter: " + adapter.URI.toString)
          adapter.refreshCache()
        }
      }
    }
    System.err.println("rebuilding index available adapters")
    CloudServices.IndexStorage.reindex()
  }
}