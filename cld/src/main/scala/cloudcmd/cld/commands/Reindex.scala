package cloudcmd.cld.commands

import cloudcmd.cld.CloudServices
import jpbetz.cli.Command
import jpbetz.cli.CommandContext
import jpbetz.cli.SubCommand

@SubCommand(name = "reindex", description = "Rebuild the index from available adapters.") class Reindex extends Command {
  def exec(commandLine: CommandContext) {
    System.err.println("rebuilding file search index...")
    CloudServices.IndexStorage.reindex
  }
}