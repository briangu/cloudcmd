package cloudcmd.cld.commands

import cloudcmd.common.util.JsonUtil
import jpbetz.cli.Command
import jpbetz.cli.CommandContext
import jpbetz.cli.Opt
import jpbetz.cli.SubCommand
import org.json.JSONArray
import cloudcmd.cld.CloudServices
import cloudcmd.common.FileMetaData

@SubCommand(name = "remove", description = "Remove files from storage.")
class Remove extends Command {

  @Opt(opt = "n", longOpt = "minTier", description = "min tier to verify to", required = false) private var _minTier: Number = 0
  @Opt(opt = "m", longOpt = "maxTier", description = "max tier to verify to", required = false) private var _maxTier: Number = Integer.MAX_VALUE

  def exec(commandLine: CommandContext) {
    CloudServices.CloudEngine.filterAdapters(_minTier.intValue, _maxTier.intValue)
    CloudServices.IndexStorage.remove(FileMetaData.fromJsonArray(JsonUtil.loadJsonArray(System.in)))
  }
}