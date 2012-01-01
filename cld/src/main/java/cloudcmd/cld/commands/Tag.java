package cloudcmd.cld.commands;


import cloudcmd.common.JsonUtil;
import cloudcmd.common.MetaUtil;
import cloudcmd.common.engine.CloudEngineService;
import cloudcmd.common.index.IndexStorageService;
import jpbetz.cli.*;
import org.json.JSONArray;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.json.JSONObject;


@SubCommand(name = "tag", description = "Add or remove tags to/from archived files.")
public class Tag implements Command
{
  @Arg(name = "tags", optional = false, isVararg = true)
  public List<String> _tags;

  @Opt(opt = "r", longOpt = "remove", description = "remove tags", required = false)
  public boolean _remove;

  @Opt(opt = "i", longOpt = "input", description = "input file", required = false)
  String _inputFilePath = null;

  @Override
  public void exec(CommandContext commandLine) throws Exception
  {
    InputStream is = (_inputFilePath != null) ? new FileInputStream(new File(_inputFilePath)) : System.in;

    try
    {
      Set<String> preparedTags = MetaUtil.prepareTags(_tags);

      JSONArray selections = JsonUtil.loadJsonArray(is);

      if (_remove)
      {
//        IndexStorageService.instance().removeTags(selections, preparedTags);
        // TODO: apply - tags
      }

      JSONArray newMeta = CloudEngineService.instance().addTags(selections, preparedTags);
      System.out.println(newMeta.toString());
    }
    finally
    {
      if (is != System.in) is.close();
    }
  }
}
