package cloudcmd.cld.commands;


import cloudcmd.common.FileHandler;
import cloudcmd.common.FileTypeUtil;
import cloudcmd.common.FileUtil;
import cloudcmd.common.FileWalker;
import cloudcmd.common.adapters.Adapter;
import cloudcmd.common.config.ConfigStorageService;
import cloudcmd.common.engine.CloudEngineService;
import jpbetz.cli.*;

import java.io.File;
import java.util.HashSet;
import java.util.List;

@SubCommand(name = "index", description = "Index files")
public class Index implements Command
{
  @Arg(name = "path", optional = false)
  public String _path = null;

  @Arg(name = "tags", optional = true, isVararg = true)
  public List<String> _tags = null;

  @Opt(opt = "s", longOpt = "skipcache", description = "skip the cache and index directly to an adapter", required = false)
  boolean _skipCache = false;

  @Override
  public void exec(CommandContext commandLine) throws Exception
  {
    CloudEngineService.instance().init("index.ops");

    if (_path == null)
    {
      _path = FileUtil.getCurrentWorkingDirectory();
    }

    if (_skipCache)
    {
      List<Adapter> adapters = ConfigStorageService.instance().getAdapters();

      if (adapters.size() == 0)
      {
        throw new IllegalArgumentException("there are no adapters to flush to");
      }

      // TODO: make selectable
      CloudEngineService.instance().flushToAdapter(adapters.get(0));
    }

    FileWalker.enumerateFolders(_path, new FileHandler()
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
        CloudEngineService.instance().add(file, new HashSet<String>(_tags));
      }
    });
  }
}
