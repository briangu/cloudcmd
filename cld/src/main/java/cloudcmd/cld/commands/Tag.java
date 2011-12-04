package cloudcmd.cld.commands;


import cloudcmd.common.JsonUtil;
import cloudcmd.common.index.IndexStorageService;
import jpbetz.cli.*;
import org.json.JSONArray;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
    Set<String> preparedTags = prepareTags(_tags);

    JSONArray selections = JsonUtil.loadJsonArray(System.in);

    if (_remove)
    {
      IndexStorageService.instance().removeTags(selections, preparedTags);
    } else
    {
      IndexStorageService.instance().addTags(selections, preparedTags);
    }
  }

  private Set<String> prepareTags(List<String> incomingTags)
  {
    Set<String> tags = new HashSet<String>();

    for (String tag : incomingTags)
    {
      tag = tag.trim();
      if (tag.length() == 0) continue;
      String[] parts = tag.split(",");
      for (String part : parts)
      {
        part = part.trim();
        if (part.length() == 0) continue;
        tags.add(part);
      }
    }

    return tags;
  }
}
