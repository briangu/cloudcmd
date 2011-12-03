package cloudcmd.cld.commands;


import cloudcmd.common.JsonUtil;
import cloudcmd.common.engine.CloudEngineService;
import jpbetz.cli.*;
import org.json.JSONArray;

@SubCommand(name = "push", description = "Push the local cache to storage endpoints.")
public class Push implements Command
{
  @Opt(opt = "t", longOpt = "tier", description = "max tier to push to", required = false)
  Number _maxTier = Integer.MAX_VALUE;

  @Opt(opt = "a", longOpt = "all", description = "push all", required = false)
  Boolean _pushAll = true;

  @Override
  public void exec(CommandContext commandLine) throws Exception
  {
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
