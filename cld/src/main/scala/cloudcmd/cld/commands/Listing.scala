package cloudcmd.cld.commands

import cloudcmd.cld.CloudServices
import jpbetz.cli.{Opt, Command, CommandContext, SubCommand}
import org.json.JSONObject

@SubCommand(name = "ls", description = "Perform a directory listing of archived files.") class Listing extends Command {
  @Opt(opt = "u", longOpt = "uri", description = "adapter URI", required = false) private var _uri: String = null

  def exec(commandLine: CommandContext) {
    CloudServices.IndexStorage.find(new JSONObject).foreach(selection => System.out.println(selection.getPath))
  }
}