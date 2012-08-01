package cloudcmd.cld.commands

import cloudcmd.cld.IndexStorageService
import jpbetz.cli.Command
import jpbetz.cli.CommandContext
import jpbetz.cli.SubCommand
import org.json.JSONArray
import org.json.JSONObject

@SubCommand(name = "ls", description = "Perform a directory listing of archived files.") class Listing extends Command {
  def exec(commandLine: CommandContext) {
    val filter = new JSONObject
    val selections = IndexStorageService.instance.find(filter)
    (0 until selections.length()).foreach{ i =>
      val entry = selections.getJSONObject(i).getJSONObject("data")
      System.out.println(entry.getString("path"))
    }
  }
}