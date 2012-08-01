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

@SubCommand(name = "verify", description = "Verify storage contents.") class Verify extends Command {

  @Opt(opt = "n", longOpt = "minTier", description = "min tier to verify to", required = false) private var _minTier: Number = 0
  @Opt(opt = "m", longOpt = "maxTier", description = "max tier to verify to", required = false) private var _maxTier: Number = Integer.MAX_VALUE
  @Opt(opt = "a", longOpt = "all", description = "verify all", required = false) private var _verifyAll: Boolean = true
  @Opt(opt = "d", longOpt = "delete", description = "delete invalid blocks", required = false) private var _deleteOnInvalid: Boolean = true

  def exec(commandLine: CommandContext) {
    val selections = if (_verifyAll) IndexStorageService.instance.find(new JSONObject) else JsonUtil.loadJsonArray(System.in)
    CloudEngineService.instance.filterAdapters(_minTier.intValue, _maxTier.intValue)
    IndexStorageService.instance.verify(selections, _deleteOnInvalid)
  }
}