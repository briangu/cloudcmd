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

@SubCommand(name = "push", description = "Push the local cache to storage endpoints.")
public class Push implements Command
{
  @Opt(opt = "n", longOpt = "minTier", description = "min tier to verify to", required = false)
  Number _minTier = 0;

  @Opt(opt = "m", longOpt = "maxTier", description = "max tier to verify to", required = false)
  Number _maxTier = Integer.MAX_VALUE;

  @Opt(opt = "a", longOpt = "all", description = "push all", required = false)
  boolean _pushAll = true;

  @Override
  public void exec(CommandContext commandLine) throws Exception
  {
    JSONArray selections = _pushAll ? IndexStorageService.instance().find(new JSONObject()) : JsonUtil.loadJsonArray(System.in);
    System.err.println(String.format("pushing %d files", selections.length()));
    CloudEngineService.instance().push(_minTier.intValue(), _maxTier.intValue(), selections);
  }
}
