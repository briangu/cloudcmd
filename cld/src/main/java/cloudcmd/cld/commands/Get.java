package cloudcmd.cld.commands;


import cloudcmd.common.JsonUtil;
import cloudcmd.common.engine.CloudEngineService;
import jpbetz.cli.Command;
import jpbetz.cli.CommandContext;
import jpbetz.cli.CommandError;
import jpbetz.cli.SubCommand;
import org.json.JSONArray;

@SubCommand(name = "get", description = "Export files from the cloud.")
public class Get implements Command
{
  @Override
  public void exec(CommandContext commandLine) throws Exception
  {
    JSONArray selections = JsonUtil.loadJsonArray(System.in);
    CloudEngineService.instance().fetch(selections);
  }
}
