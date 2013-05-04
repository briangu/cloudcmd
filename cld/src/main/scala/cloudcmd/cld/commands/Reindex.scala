package cloudcmd.cld.commands

import cloudcmd.cld.CloudServices
import jpbetz.cli.{Opt, Command, CommandContext, SubCommand}

@SubCommand(name = "reindex", description = "Rebuild the index from available adapters.") class Reindex extends Command {
  @Opt(opt = "u", longOpt = "uri", description = "adapter URI", required = false) private var _uri: String = null

  def exec(commandLine: CommandContext) {
    System.err.println("rebuilding file search index...")
    CloudServices.IndexStorage.reindex()
  }
}