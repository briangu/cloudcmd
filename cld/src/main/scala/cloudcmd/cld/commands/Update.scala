package cloudcmd.cld.commands

import cloudcmd.cld.CloudEngineService
import cloudcmd.cld.ConfigStorageService
import cloudcmd.cld.IndexStorageService
import jpbetz.cli.Command
import jpbetz.cli.CommandContext
import jpbetz.cli.Opt
import jpbetz.cli.SubCommand
import java.net.URI

@SubCommand(name = "update", description = "update cached adapter information.") class Update extends Command {
  @Opt(opt = "n", longOpt = "minTier", description = "min tier to verify to", required = false) private var _minTier: Number = 0
  @Opt(opt = "m", longOpt = "maxTier", description = "max tier to verify to", required = false) private var _maxTier: Number = Integer.MAX_VALUE
  @Opt(opt = "a", longOpt = "adapter", description = "adapter URI to refresh", required = false) private var _uri: String = null

  def exec(commandLine: CommandContext) {
    if (_uri == null) {
      System.err.println("updating all adapters")
      CloudEngineService.instance.filterAdapters(_minTier.intValue, _maxTier.intValue)
      CloudEngineService.instance.refreshAdapterCaches
    }
    else {
      val adapterURI: URI = new URI(_uri)
      for (adapter <- ConfigStorageService.instance.getAdapters) {
        if ((adapter.URI.toString == _uri) || ((adapterURI.getPath == adapter.URI.getPath))) {
          System.err.println("updating adapter: " + adapter.URI.toString)
          adapter.refreshCache
        }
      }
    }
    System.err.println("rebuilding index available adapters")
    IndexStorageService.instance.reindex
  }
}