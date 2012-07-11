package cloudcmd.cld.commands;


import cloudcmd.common.JsonUtil;
import cloudcmd.common.config.ConfigStorageService;
import cloudcmd.common.engine.CloudEngineService;
import jpbetz.cli.Command;
import jpbetz.cli.CommandContext;
import jpbetz.cli.Opt;
import jpbetz.cli.SubCommand;
import org.json.JSONArray;

@SubCommand(name = "remove", description = "Remove files from storage.")
public class Remove implements Command
{
  @Opt(opt = "n", longOpt = "minTier", description = "min tier to verify to", required = false)
  Number _minTier = 0;

  @Opt(opt = "m", longOpt = "maxTier", description = "max tier to verify to", required = false)
  Number _maxTier = Integer.MAX_VALUE;

  @Override
  public void exec(CommandContext commandLine) throws Exception
  {
    JSONArray selections = JsonUtil.loadJsonArray(System.in);
    CloudEngineService.instance().remove(_minTier.intValue(), _maxTier.intValue(), selections);
  }
}
