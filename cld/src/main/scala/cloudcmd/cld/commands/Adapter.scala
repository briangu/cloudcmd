package cloudcmd.cld.commands

import jpbetz.cli.Command
import jpbetz.cli.CommandContext
import jpbetz.cli.Opt
import jpbetz.cli.SubCommand
import java.net.URI
import cloudcmd.cld.CloudServices
import cloudcmd.cld.Notifications._

@SubCommand(name = "adapter", description = "Manager storage adapters.") 
class Adapter extends Command {
  @Opt(opt = "r", longOpt = "remove", description = "remove adapter", required = false) private var _remove: String = null
  @Opt(opt = "l", longOpt = "list", description = "list adapters", required = false) private var _list: Boolean = false
  @Opt(opt = "a", longOpt = "add", description = "add adapter", required = false) private var _add: String = null
  @Opt(opt = "d", longOpt = "dump", description = "dump adapter URI", required = false) private var _dumpUri: String = null

  def exec(commandLine: CommandContext) {
    if (_remove != null) {
      CloudServices.ConfigService.findAdapterByBestMatch(_remove) match {
        case Some(adapter) => {
          CloudServices.ConfigService.removeAdapter(adapter.URI)
          CloudServices.ConfigService.writeConfig()
          list()
        }
        case None => {
          msg("adapter %s not found.".format(_remove))
        }
      }
    } else if (_add != null) {
      CloudServices.ConfigService.addAdapter(new URI(_add))
      CloudServices.ConfigService.writeConfig()
      list()
    } else if (_dumpUri != null) {
      CloudServices.ConfigService.findAdapterByBestMatch(_dumpUri) match {
        case Some(adapter) => {
          adapter.describe().foreach { hash =>
            System.out.println("%s".format(hash))
          }
        }
        case None => {
          msg("adapter %s not found.".format(_dumpUri))
        }
      }
    } else if (_list) {
      list()
    }
  }

  def list() {
    msg("Adapters:")
    msg("\n")
    for (adapter <- CloudServices.ConfigService.getAllAdapters) {
      msg("Adapter: " + adapter.Type)
      msg("  URI: " + adapter.URI.toString)
      msg("  ConfigDir: " + adapter.ConfigDir)
      msg("  IsOnline: " + adapter.IsOnLine)
      msg("  IsFull: " + adapter.IsFull)
      msg("\n")
    }
  }
}