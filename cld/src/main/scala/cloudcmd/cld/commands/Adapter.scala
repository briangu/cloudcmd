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
          System.err.println("could not find adapter to remove: " + _remove)
        }
      }
    } else if (_add != null) {
      CloudServices.ConfigService.addAdapter(new URI(_add))
      CloudServices.ConfigService.writeConfig()
      list()
    } else if (_dumpUri != null) {
      val uri: URI = new URI(_dumpUri)
      val adapter = CloudServices.ConfigService.getAdapter(uri)
      adapter.describe().foreach { blockContext =>
        println("%s (%s)".format(blockContext.hash, blockContext.routingTags.mkString(",")))
      }
    } else if (_list) {
      list()
    }
  }

  def list() {
    System.out.println("Adapters:")
    System.out.println()
    for (adapter <- CloudServices.ConfigService.getAllAdapters) {
      System.out.println("Adapter: " + adapter.Type)
      System.out.println("  URI: " + adapter.URI.toString)
      System.out.println("  ConfigDir: " + adapter.ConfigDir)
      System.out.println("  IsOnline: " + adapter.IsOnLine)
      System.out.println("  IsFull: " + adapter.IsFull)
      System.out.println()
    }
  }
}