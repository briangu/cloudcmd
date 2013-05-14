package cloudcmd.cld.commands

import cloudcmd.cld.CloudServices
import cloudcmd.common.FileUtil
import jpbetz.cli.Command
import jpbetz.cli.CommandContext
import jpbetz.cli.SubCommand
import java.io.File

@SubCommand(name = "init", description = "Create a CloudCmd project with the current directory as the root.")
class Init extends Command {
  def exec(commandLine: CommandContext) {
    val path = FileUtil.getCurrentWorkingDirectory + File.separatorChar + ".cld"
    new File(path).mkdirs
    CloudServices.ConfigService.createDefaultConfig(path)
    System.err.println("Project successfully created.")
    System.err.println("Edit config in: " + path)
  }
}