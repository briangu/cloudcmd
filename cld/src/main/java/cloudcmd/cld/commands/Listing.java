package cloudcmd.cld.commands;


import cloudcmd.common.index.IndexStorageService;
import jpbetz.cli.Command;
import jpbetz.cli.CommandContext;
import jpbetz.cli.SubCommand;
import org.json.JSONArray;
import org.json.JSONObject;

@SubCommand(name = "ls", description = "Perform a directory listing of archived files.")
public class Listing implements Command
{
  @Override
  public void exec(CommandContext commandLine) throws Exception
  {
    JSONObject filter = new JSONObject();

    JSONArray result = IndexStorageService.instance().find(filter);

    for (int i = 0; i < result.length(); i++)
    {
      JSONObject entry = result.getJSONObject(i);
      System.out.println(entry.getString("path"));
    }
  }
}