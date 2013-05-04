package cloudcmd.cld.commands

import cloudcmd.cld.CloudServices
import jpbetz.cli.Command
import jpbetz.cli.CommandContext
import jpbetz.cli.SubCommand
import org.json.JSONObject

@SubCommand(name = "ls", description = "Perform a directory listing of archived files.") class Listing extends Command {
  def exec(commandLine: CommandContext) {
    CloudServices.IndexStorage.find(new JSONObject).foreach(selection => System.out.println(selection.getPath))
  }
}