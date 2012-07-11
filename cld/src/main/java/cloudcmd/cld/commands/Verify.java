package cloudcmd.cld.commands;


import cloudcmd.common.JsonUtil;
import cloudcmd.common.config.ConfigStorageService;
import cloudcmd.common.engine.CloudEngineService;
import cloudcmd.common.index.IndexStorageService;
import jpbetz.cli.Command;
import jpbetz.cli.CommandContext;
import jpbetz.cli.Opt;
import jpbetz.cli.SubCommand;
import org.json.JSONArray;
import org.json.JSONObject;

@SubCommand(name = "verify", description = "Verify storage contents.")
public class Verify implements Command
{
  @Opt(opt = "n", longOpt = "minTier", description = "min tier to verify to", required = false)
  Number _minTier = 0;

  @Opt(opt = "m", longOpt = "maxTier", description = "max tier to verify to", required = false)
  Number _maxTier = Integer.MAX_VALUE;

  @Opt(opt = "a", longOpt = "all", description = "verify all", required = false)
  boolean _verifyAll = true;

  @Opt(opt = "d", longOpt = "delete", description = "delete invalid blocks", required = false)
  boolean _deleteOnInvalid = true;

  @Override
  public void exec(CommandContext commandLine) throws Exception
  {
    JSONArray selections = _verifyAll ? IndexStorageService.instance().find(new JSONObject()) : JsonUtil.loadJsonArray(System.in);
    CloudEngineService.instance().verify(_minTier.intValue(), _maxTier.intValue(), selections, _deleteOnInvalid);
  }
}
