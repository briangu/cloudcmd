package cloudcmd.cld.commands;


import cloudcmd.cld.IndexStorageService;
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

    JSONArray selections = IndexStorageService.instance().find(filter);

    for (int i = 0; i < selections.length(); i++)
    {
      JSONObject entry = selections.getJSONObject(i).getJSONObject("data");
      System.out.println(entry.getString("path"));
    }
  }
}
