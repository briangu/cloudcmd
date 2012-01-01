package cloudcmd.cld.commands;


import cloudcmd.common.MetaUtil;
import cloudcmd.common.StringUtil;
import cloudcmd.common.index.IndexStorageService;
import java.util.List;
import java.util.Set;

import jpbetz.cli.*;
import org.json.JSONArray;
import org.json.JSONObject;

@SubCommand(name = "find", description = "Query the index of archived files.")
public class Find implements Command
{
  @Arg(name = "tags", optional = true, isVararg = true)
  List<String> _tags;

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

    if (_tags != null)
    {
      Set<String> tags = MetaUtil.prepareTags(_tags);
      if (tags.size() > 0) filter.put("tags", StringUtil.join(tags, " "));
    }

    if (_path != null) filter.put("path", _path);
    if (_filename != null) filter.put("filename", _filename);
    if (_fileext != null) filter.put("fileext", _fileext);

    JSONArray selections = IndexStorageService.instance().find(filter);

    System.out.println(selections.toString());
  }
}
