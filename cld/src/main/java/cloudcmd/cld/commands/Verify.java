package cloudcmd.cld.commands;


import cloudcmd.common.JsonUtil;
import cloudcmd.common.engine.CloudEngineService;
import jpbetz.cli.Command;
import jpbetz.cli.CommandContext;
import jpbetz.cli.Opt;
import jpbetz.cli.SubCommand;
import org.json.JSONArray;

@SubCommand(name = "verify", description = "Verify storage contents.")
public class Verify implements Command
{
  @Opt(opt = "t", longOpt = "tier", description = "max tier to verify to", required = false)
  Number _maxTier = Integer.MAX_VALUE;

  @Opt(opt = "a", longOpt = "all", description = "verify all", required = false)
  boolean _verifyAll = true;

  @Opt(opt = "d", longOpt = "delete", description = "delete invalid blocks", required = false)
  boolean _deleteOnInvalid = true;

  @Override
  public void exec(CommandContext commandLine) throws Exception
  {
    if (_verifyAll)
    {
      CloudEngineService.instance().verify(_maxTier.intValue(), _deleteOnInvalid);
    }
    else
    {
      JSONArray selections = JsonUtil.loadJsonArray(System.in);
      CloudEngineService.instance().verify(_maxTier.intValue(), selections, _deleteOnInvalid);
    }
  }
}
