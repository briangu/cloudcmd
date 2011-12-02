package cloudcmd.cld.commands;


import cloudcmd.common.index.IndexStorageService;
import java.util.Arrays;
import javax.swing.text.html.Option;
import jpbetz.cli.Command;
import jpbetz.cli.CommandContext;
import jpbetz.cli.CommandError;
import jpbetz.cli.Opt;
import jpbetz.cli.SubCommand;
import org.json.JSONArray;
import org.json.JSONObject;

@SubCommand(name = "find", description = "Query the index of archived files.")
public class Find implements Command
{
  @Opt(opt = "t", longOpt = "tag", description = "tags to find by", required = false)
  String _tags;

  @Override
  public void exec(CommandContext commandLine) throws Exception
  {
    JSONObject filter = new JSONObject();

    if (_tags != null) filter.put("tags", _tags.replace(",", " "));

    JSONArray result = IndexStorageService.instance().find(filter);

    System.out.println(result.toString(2));
  }
}
