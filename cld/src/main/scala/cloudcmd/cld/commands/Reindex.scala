package cloudcmd.cld.commands

import cloudcmd.cld.CloudServices
import jpbetz.cli.{Opt, Command, CommandContext, SubCommand}

@SubCommand(name = "reindex", description = "Rebuild the index from available adapters.") class Reindex extends Command {
  @Opt(opt = "u", longOpt = "uri", description = "adapter URI", required = false) private var _uri: String = null
  @Opt(opt = "n", longOpt = "minTier", description = "min tier to verify to", required = false) private var _minTier: Number = 0
  @Opt(opt = "m", longOpt = "maxTier", description = "max tier to verify to", required = false) private var _maxTier: Number = Integer.MAX_VALUE

  def exec(commandLine: CommandContext) {
    val matchedAdapter = Option(_uri) match {
      case Some(uri) => {
        CloudServices.ConfigService.findAdapterByBestMatch(_uri) match {
          case Some(adapter) => {
            System.err.println("reindexing adapter: %s".format(adapter.URI.toASCIIString))
            adapter
          }
          case None => {
            System.err.println("adapter %s not found.".format(_uri))
            null
          }
        }
      }
      case None => {
        CloudServices.initWithTierRange(_minTier.intValue, _maxTier.intValue)
        System.err.println("reindexing all adapters.")
        CloudServices.BlockStorage
      }
    }

    Option(matchedAdapter) match {
      case Some(adapter) => {
        adapter.reindex()
      }
      case None => {
        System.err.println("nothing to do.")
      }
    }
  }
}