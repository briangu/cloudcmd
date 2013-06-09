package cloudcmd.cld.commands

import jpbetz.cli.Command
import jpbetz.cli.CommandContext
import jpbetz.cli.Opt
import jpbetz.cli.SubCommand
import java.net.URI
import cloudcmd.cld.CloudServices

@SubCommand(name = "adapter", description = "Manager storage adapters.") 
class Adapter extends Command {

  @Opt(opt = "r", longOpt = "remove", description = "remove adapter", required = false) private var _remove: String = null
  @Opt(opt = "l", longOpt = "list", description = "list adapters", required = false) private var _list: Boolean = false
  @Opt(opt = "a", longOpt = "add", description = "add adapter", required = false) private var _add: String = null
  @Opt(opt = "d", longOpt = "describe", description = "describe adapter URI", required = false) private var _dumpUri: String = null

  def exec(commandLine: CommandContext) {
    if (_remove != null) {
      CloudServices.ConfigService.findAdapterByBestMatch(_remove) match {
        case Some(adapter) => {
          CloudServices.ConfigService.removeAdapter(adapter.URI)
          CloudServices.ConfigService.writeConfig()
          list()
        }
        case None => {
          System.err.println("adapter %s not found.".format(_remove))
        }
      }
    } else if (_add != null) {
      CloudServices.ConfigService.addAdapter(new URI(_add))
      CloudServices.ConfigService.writeConfig()
      list()
    } else if (_dumpUri != null) {
      val adapter = CloudServices.ConfigService.findAdapterByBestMatch(_dumpUri) match {
        case Some(adapter) => {
          System.err.println("using adapter: %s".format(adapter.getSignature))
          adapter
        }
        case None => {
          System.err.println("adapter %s not found.".format(_dumpUri))
          null
        }
      }

      Option(adapter) match {
        case Some(adapter) => {
          val description = adapter.describe()
          if (description.size > 0) {
            description.foreach { hash =>
              System.out.println("%s".format(hash))
            }
          } else {
            System.out.println("no blocks found.")
          }
        }
        case None => {
          System.err.println("adapter %s not found.".format(_dumpUri))
        }
      }
    } else if (_list) {
      list()
    }
  }

  def list() {
    System.err.println("Adapters:")
    System.err.println()
    for (adapter <- CloudServices.ConfigService.getAllAdapters) {
      System.err.println("Adapter: " + adapter.Type)
      System.err.println("  URI: " + adapter.URI.toString)
      System.err.println("  Signature: " + adapter.getSignature)
      System.err.println("  ConfigDir: " + adapter.ConfigDir)
      System.err.println("  IsOnline: " + adapter.IsOnLine)
      System.err.println("  IsFull: " + adapter.IsFull)
      System.err.println()
    }
  }
}