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

  @Opt(opt = "p", longOpt = "path", description = "path to find by", required = false)
  String _path;

  @Opt(opt = "f", longOpt = "name", description = "filename to find by", required = false)
  String _filename;

  @Opt(opt = "e", longOpt = "ext", description = "file extension to find by", required = false)
  String _fileext;

  @Override
  public void exec(CommandContext commandLine) throws Exception
  {
    JSONObject filter = new JSONObject();

    if (_tags != null) filter.put("tags", _tags.replace(",", " "));
    if (_path != null) filter.put("path", _path);
    if (_filename != null) filter.put("filename", _filename);
    if (_fileext != null) filter.put("fileext", _fileext);

    JSONArray result = IndexStorageService.instance().find(filter);

    System.out.println(result.toString());
  }
}
