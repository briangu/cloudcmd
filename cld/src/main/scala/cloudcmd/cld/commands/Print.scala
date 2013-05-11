package cloudcmd.cld.commands

import cloudcmd.common.util.JsonUtil
import jpbetz.cli.Command
import jpbetz.cli.CommandContext
import jpbetz.cli.SubCommand

@SubCommand(name = "print", description = "Pretty print JSON from other commands.")
class Print extends CloudCommand {
  def doCommand() {
    System.out.println(JsonUtil.loadJsonArray(System.in).toString(2))
  }
}