package cloudcmd.cld.commands;


import cloudcmd.common.FileUtil;
import jpbetz.cli.Command;
import jpbetz.cli.CommandContext;
import jpbetz.cli.CommandError;
import jpbetz.cli.SubCommand;

import java.io.File;

@SubCommand(name = "init", description = "Create a CloudCmd project with the current directory as the root.")
public class Init implements Command
{
  @Override
  public void exec(CommandContext commandLine) throws Exception
  {
    new File(FileUtil.getCurrentWorkingDirectory() + File.separatorChar + ".cld").mkdir();
    System.err.println("Project successfully created.");
  }
}
