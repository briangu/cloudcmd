package cloudcmd.cld.commands

import cloudcmd.cld.CloudEngineService
import cloudcmd.cld.IndexStorageService
import cloudcmd.common.JsonUtil
import jpbetz.cli.Command
import jpbetz.cli.CommandContext
import jpbetz.cli.Opt
import jpbetz.cli.SubCommand
import org.json.JSONArray

@SubCommand(name = "remove", description = "Remove files from storage.")
class Remove extends Command {

  @Opt(opt = "n", longOpt = "minTier", description = "min tier to verify to", required = false) private var _minTier: Number = 0
  @Opt(opt = "m", longOpt = "maxTier", description = "max tier to verify to", required = false) private var _maxTier: Number = Integer.MAX_VALUE

  def exec(commandLine: CommandContext) {
    val selections: JSONArray = JsonUtil.loadJsonArray(System.in)
    CloudEngineService.instance.filterAdapters(_minTier.intValue, _maxTier.intValue)
    IndexStorageService.instance.remove(selections)
  }
}