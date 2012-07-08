package cloudcmd.cld.commands;


import cloudcmd.common.JsonUtil;
import cloudcmd.common.config.ConfigStorageService;
import cloudcmd.common.engine.CloudEngineService;
import jpbetz.cli.Command;
import jpbetz.cli.CommandContext;
import jpbetz.cli.Opt;
import jpbetz.cli.SubCommand;
import org.json.JSONArray;

@SubCommand(name = "pull", description = "Pull the meta data from storage endpoints.")
public class Pull implements Command
{
  @Opt(opt = "n", longOpt = "minTier", description = "min tier to verify to", required = false)
  Number _minTier = 0;

  @Opt(opt = "m", longOpt = "maxTier", description = "max tier to verify to", required = false)
  Number _maxTier = Integer.MAX_VALUE;

  @Opt(opt = "b", longOpt = "blocks", description = "retrieve blocks as well as meta data", required = false)
  boolean _retrieveBlocks = false;

  @Opt(opt = "a", longOpt = "all", description = "push all", required = false)
  boolean _pullAll = true;

  @Override
  public void exec(CommandContext commandLine) throws Exception
  {
    CloudEngineService.instance().init(ConfigStorageService.instance().getReplicationStrategy(), "pull.ops");

    // TODO: purgeHistory/reindex after successful pull     false

    if (_pullAll)
    {
      CloudEngineService.instance().pull(_minTier.intValue(), _maxTier.intValue(), _retrieveBlocks);
    }
    else
    {
      JSONArray selections = JsonUtil.loadJsonArray(System.in);
      CloudEngineService.instance().pull(_minTier.intValue(), _maxTier.intValue(), _retrieveBlocks, selections);
    }
  }
}
