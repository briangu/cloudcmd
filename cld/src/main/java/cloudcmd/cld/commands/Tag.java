package cloudcmd.cld.commands;


import cloudcmd.common.JsonUtil;
import cloudcmd.common.index.IndexStorageService;
import jpbetz.cli.*;
import org.json.JSONArray;

import java.util.HashSet;
import java.util.List;

@SubCommand(name = "tag", description = "Add or remove tags to/from archived files.")
public class Tag implements Command
{
  @Arg(name = "tags", optional = false, isVararg = true)
  public List<String> _tags;

  @Opt(opt = "r", longOpt = "remove", description = "remove tags", required = false)
  public boolean _remove;

  @Override
  public void exec(CommandContext commandLine) throws Exception
  {
    JSONArray selections = JsonUtil.loadJsonArray(System.in);

    if (_remove)
    {
      IndexStorageService.instance().removeTags(selections, new HashSet<String>(_tags));
    } else
    {
      IndexStorageService.instance().addTags(selections, new HashSet<String>(_tags));
    }
  }
}
