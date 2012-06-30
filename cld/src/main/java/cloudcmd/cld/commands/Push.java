package cloudcmd.cld.commands;


import cloudcmd.common.JsonUtil;
import cloudcmd.common.config.ConfigStorageService;
import cloudcmd.common.engine.CloudEngineService;
import jpbetz.cli.Command;
import jpbetz.cli.CommandContext;
import jpbetz.cli.Opt;
import jpbetz.cli.SubCommand;
import org.json.JSONArray;

@SubCommand(name = "push", description = "Push the local cache to storage endpoints.")
public class Push implements Command
{
  @Opt(opt = "t", longOpt = "tier", description = "max tier to push to", required = false)
  Number _maxTier = Integer.MAX_VALUE;

  @Opt(opt = "a", longOpt = "all", description = "push all", required = false)
  boolean _pushAll = true;

  @Override
  public void exec(CommandContext commandLine) throws Exception
  {
    CloudEngineService.instance().init(ConfigStorageService.instance().getReplicationStrategy(), "push.ops");

    if (_pushAll)
    {
      CloudEngineService.instance().push(_maxTier.intValue());
    }
    else
    {
      JSONArray selections = JsonUtil.loadJsonArray(System.in);
      CloudEngineService.instance().push(_maxTier.intValue(), selections);
    }
  }
}
