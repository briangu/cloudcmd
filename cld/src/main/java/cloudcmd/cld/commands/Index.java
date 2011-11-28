package cloudcmd.cld.commands;


import cloudcmd.common.*;
import cloudcmd.common.FileTypeUtil;
import cloudcmd.common.FileWalker;
import cloudcmd.common.FileHandler;
import cloudcmd.common.engine.CloudEngineService;
import cloudcmd.common.index.IndexStorageService;
import jpbetz.cli.*;
import jpbetz.cli.Command;
import jpbetz.cli.CommandContext;
import ops.*;
import org.json.JSONObject;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

@SubCommand(name = "index", description = "Index files")
public class Index implements Command
{

  @Arg(name = "path", optional = false)
  public String _path = null;

  @Arg(name = "tags", optional = true, isVararg = true)
  public List<String> _tags = null;

  @Override
  public void exec(CommandContext commandLine) throws Exception
  {
    if (_path == null)
    {
      _path = FileUtil.getCurrentWorkingDirectory();
    }

    FileWalker.enumerateFolders(_path, new FileHandler()
    {
      @Override
      public void process(File file)
      {
        CloudEngineService.instance().add(file, new HashSet<String>(_tags));
      }
    });

    CloudEngineService.instance().run();
  }
}
