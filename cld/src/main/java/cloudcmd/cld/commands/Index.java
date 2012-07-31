package cloudcmd.cld.commands;


import cloudcmd.cld.CloudEngineService;
import cloudcmd.common.FileTypeUtil;
import cloudcmd.common.FileUtil;
import cloudcmd.common.engine.FileWalker;
import cloudcmd.common.adapters.Adapter;
import cloudcmd.cld.ConfigStorageService;
import jpbetz.cli.*;

import java.io.File;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@SubCommand(name = "index", description = "Index files")
public class Index implements Command {
  @Arg(name = "path", optional = false)
  public String _path = null;

  @Arg(name = "tags", optional = true, isVararg = true)
  public List<String> _tags = null;

  @Override
  public void exec(CommandContext commandLine) throws Exception {
    if (_path == null) {
      _path = FileUtil.getCurrentWorkingDirectory();
    }

    final Set<File> fileSet = Collections.newSetFromMap(new ConcurrentHashMap<File, Boolean>());

    final FileTypeUtil fileTypeUtil = FileTypeUtil.instance();

    FileWalker.enumerateFolders(_path, new FileWalker.FileHandler() {
      @Override
      public boolean skipDir(File file) {
        boolean skip = fileTypeUtil.skipDir(file.getName());
        if (skip) {
          System.err.println(String.format("Skipping dir: " + file.getAbsolutePath()));
        }
        return skip;
      }

      @Override
      public void process(File file) {
        String fileName = file.getName();
        Integer extIndex = fileName.lastIndexOf(".");
        String ext = (extIndex > 0) ? fileName.substring(extIndex + 1) : null;
        if (!fileTypeUtil.skipExt(ext)) {
          fileSet.add(file);
        }
      }
    });

    CloudEngineService.instance().batchAdd(fileSet, new HashSet<String>(_tags));
  }
}
