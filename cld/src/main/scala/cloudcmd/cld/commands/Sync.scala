package cloudcmd.cld.commands

import cloudcmd.cld.CloudEngineService
import cloudcmd.cld.IndexStorageService
import cloudcmd.common.JsonUtil
import jpbetz.cli.Command
import jpbetz.cli.CommandContext
import jpbetz.cli.Opt
import jpbetz.cli.SubCommand
import org.json.JSONArray
import org.json.JSONObject

@SubCommand(name = "sync", description = "Sync the local cache to storage endpoints.")
class Sync extends Command {

  @Opt(opt = "n", longOpt = "minTier", description = "min tier to verify to", required = false) private var _minTier: Number = 0
  @Opt(opt = "m", longOpt = "maxTier", description = "max tier to verify to", required = false) private var _maxTier: Number = Integer.MAX_VALUE
  @Opt(opt = "a", longOpt = "all", description = "sync all", required = false) private var _syncAll: Boolean = true

  def exec(commandLine: CommandContext) {
    val selections = if (_syncAll) IndexStorageService.instance.find(new JSONObject) else JsonUtil.loadJsonArray(System.in)
    System.err.println("syncing %d files".format(selections.length))
    CloudEngineService.instance.filterAdapters(_minTier.intValue, _maxTier.intValue)
    IndexStorageService.instance.sync(selections)
  }
}