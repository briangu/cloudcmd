package cloudcmd.cld.commands

import cloudcmd.cld.{Util, CloudServices}
import jpbetz.cli.{Opt, Command, CommandContext, SubCommand}
import org.json.JSONObject
import cloudcmd.common.FileMetaData
import cloudcmd.common.util.JsonUtil

@SubCommand(name = "ls", description = "Perform a directory listing of archived files.") class Listing extends Command {

  @Opt(opt = "u", longOpt = "uri", description = "adapter URI", required = false) private var _uri: String = null
  @Opt(opt = "n", longOpt = "minTier", description = "min tier to verify to", required = false) private var _minTier: Number = 0
  @Opt(opt = "m", longOpt = "maxTier", description = "max tier to verify to", required = false) private var _maxTier: Number = Integer.MAX_VALUE

  def exec(commandLine: CommandContext) {
    val matchedAdapter = Option(_uri) match {
      case Some(uri) => {
        CloudServices.ConfigService.findAdapterByBestMatch(_uri) match {
          case Some(adapter) => {
            System.err.println("listing from adapter: %s".format(adapter.URI.toASCIIString))
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
        System.err.println("listing from all adapters.")
        CloudServices.BlockStorage
      }
    }

    Option(matchedAdapter) match {
      case Some(adapter) => {
        adapter.find(new JSONObject).foreach(selection => System.out.println(selection.getPath))
      }
    }
  }
}