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
  @Override
  public void exec(CommandContext commandLine) throws Exception
  {
    CloudEngineService.instance().init(ConfigStorageService.instance().getReplicationStrategy(), "remove.ops");

    JSONArray selections = JsonUtil.loadJsonArray(System.in);
    CloudEngineService.instance().remove(selections);
  }
}
