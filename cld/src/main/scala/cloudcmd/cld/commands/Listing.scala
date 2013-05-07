package cloudcmd.cld.commands

import cloudcmd.cld.CloudServices
import jpbetz.cli.{Opt, Command, CommandContext, SubCommand}
import org.json.JSONObject

@SubCommand(name = "ls", description = "Perform a directory listing of archived files.") class Listing extends Command {

  @Opt(opt = "u", longOpt = "uri", description = "adapter URI", required = false) private var _uri: String = null
  @Opt(opt = "n", longOpt = "minTier", description = "min tier to verify to", required = false) private var _minTier: Number = 0
  @Opt(opt = "m", longOpt = "maxTier", description = "max tier to verify to", required = false) private var _maxTier: Number = Integer.MAX_VALUE

  def exec(commandLine: CommandContext) {
    CloudServices.ConfigService.findAdapterByBestMatch(_uri) match {
      case Some(adapter) => {
        CloudServices.initWithTierRange(_minTier.intValue, _maxTier.intValue)
        CloudServices.IndexStorage.find(new JSONObject).foreach(selection => System.out.println(selection.getPath))
      }
      case None => {
        CloudServices.initWithTierRange(_minTier.intValue, _maxTier.intValue)
        CloudServices.BlockStorage.find(new JSONObject).foreach(selection => println(selection.getPath))
      }
    }
  }
}