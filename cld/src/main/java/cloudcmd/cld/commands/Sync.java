package cloudcmd.cld.commands;


import cloudcmd.cld.CloudEngineService;
import cloudcmd.common.JsonUtil;
import cloudcmd.cld.IndexStorageService;
import jpbetz.cli.Command;
import jpbetz.cli.CommandContext;
import jpbetz.cli.Opt;
import jpbetz.cli.SubCommand;
import org.json.JSONArray;
import org.json.JSONObject;

@SubCommand(name = "push", description = "Sync the local cache to storage endpoints.")
public class Sync implements Command
{
  @Opt(opt = "n", longOpt = "minTier", description = "min tier to verify to", required = false)
  Number _minTier = 0;

  @Opt(opt = "m", longOpt = "maxTier", description = "max tier to verify to", required = false)
  Number _maxTier = Integer.MAX_VALUE;

  @Opt(opt = "a", longOpt = "all", description = "sync all", required = false)
  boolean _syncAll = true;

  @Override
  public void exec(CommandContext commandLine) throws Exception
  {
    JSONArray selections = _syncAll ? IndexStorageService.instance().find(new JSONObject()) : JsonUtil.loadJsonArray(System.in);
    System.err.println(String.format("syncing %d files", selections.length()));
    CloudEngineService.instance().sync(_minTier.intValue(), _maxTier.intValue(), selections);
  }
}
