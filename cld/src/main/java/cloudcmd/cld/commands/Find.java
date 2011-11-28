package cloudcmd.cld.commands;


import cloudcmd.common.index.IndexStorageService;
import jpbetz.cli.Command;
import jpbetz.cli.CommandContext;
import jpbetz.cli.CommandError;
import jpbetz.cli.SubCommand;
import org.json.JSONArray;
import org.json.JSONObject;

@SubCommand(name = "find", description = "Query the index of archived files.")
public class Find implements Command
{
  @Override
  public void exec(CommandContext commandLine) throws Exception
  {
    JSONObject filter = new JSONObject();
    JSONArray result = IndexStorageService.instance().find(filter);
    System.out.println(result.toString(2));
  }
}
