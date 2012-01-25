package cloudcmd.cld.commands;


import cloudcmd.common.FileUtil;
import cloudcmd.common.config.ConfigStorageService;
import jpbetz.cli.Command;
import jpbetz.cli.CommandContext;
import jpbetz.cli.SubCommand;

import java.io.File;

@SubCommand(name = "init", description = "Create a CloudCmd project with the current directory as the root.")
public class Init implements Command
{
  @Override
  public void exec(CommandContext commandLine) throws Exception
  {
    String path = FileUtil.getCurrentWorkingDirectory() + File.separatorChar + ".cld";
    new File(path).mkdirs();

    ConfigStorageService.instance().createDefaultConfig(path);

    System.err.println("Project successfully created.");
    System.err.println("Edit config in: " + path);
  }
}
