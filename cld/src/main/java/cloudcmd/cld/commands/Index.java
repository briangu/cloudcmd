package cloudcmd.cld.commands;


import cloudcmd.cld.CloudEngineService;
import cloudcmd.common.FileTypeUtil;
import cloudcmd.common.FileUtil;
import cloudcmd.common.FileWalker;
import cloudcmd.common.adapters.Adapter;
import cloudcmd.cld.ConfigStorageService;
import jpbetz.cli.*;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

    // TODO: allow selectable adapters
    Adapter adapter = null;

    for (Adapter a : ConfigStorageService.instance().getAdapters())
    {
      if (a.IsOnLine() && !a.IsFull())
      {
        adapter = a;
        break;
      }
    }

    if (adapter == null)
    {
      throw new IllegalArgumentException("there are no adapters to flush to");
    }

    final Set<File> fileSet = new HashSet<File>();

    FileWalker.enumerateFolders(_path, new FileWalker.FileHandler()
    {
      @Override
      public boolean skipDir(File file)
      {
        boolean skip = FileTypeUtil.instance().skipDir(file.getName());
        if (skip)
        {
          System.err.println(String.format("Skipping dir: " + file.getAbsolutePath()));
        }
        return skip;
      }

      @Override
      public void process(File file)
      {
        fileSet.add(file);
      }
    });

    CloudEngineService.instance().batchAdd(fileSet, new HashSet<String>(_tags), adapter);
  }
}
