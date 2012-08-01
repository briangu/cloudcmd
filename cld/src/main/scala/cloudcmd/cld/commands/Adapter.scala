package cloudcmd.cld.commands

import jpbetz.cli.Command
import jpbetz.cli.CommandContext
import jpbetz.cli.Opt
import jpbetz.cli.SubCommand
import java.net.URI
import cloudcmd.cld.CloudServices

@SubCommand(name = "adapter", description = "Manager storage adapters.") 
class Adapter extends Command {
  @Opt(opt = "r", longOpt = "remove", description = "remove adapter", required = false) private var _remove: Boolean = false
  @Opt(opt = "l", longOpt = "list", description = "list adapters", required = false) private var _list: Boolean = false
  @Opt(opt = "a", longOpt = "add", description = "add adapter", required = false) private var _add: Boolean = false
  @Opt(opt = "u", longOpt = "uri", description = "adapter URI", required = false) private var _uri: String = null

  def exec(commandLine: CommandContext) {
    if (_remove) {
      if (_uri == null) {
        System.err.println("adapter URI not specified.")
        return
      }
      val uri: URI = new URI(_uri)
      val found: Boolean = CloudServices.ConfigService.removeAdapter(uri)
      if (!found) {
        System.err.println("could not find adapter to remove: " + uri)
        return
      }
      CloudServices.ConfigService.writeConfig
    }
    else if (_add) {
      if (_uri == null) {
        System.err.println("adapter URI not specified.")
        return
      }
      val uri: URI = new URI(_uri)
      CloudServices.ConfigService.addAdapter(uri)
      CloudServices.ConfigService.writeConfig
    }
    else if (_list) {
      System.out.println("Adapters:")
      System.out.println
      for (adapter <- CloudServices.ConfigService.getAdapters) {
        System.out.println("Adapter: " + adapter.Type)
        System.out.println("  URI: " + adapter.URI.toString)
        System.out.println("  ConfigDir: " + adapter.ConfigDir)
        System.out.println("  IsOnline: " + adapter.IsOnLine)
        System.out.println("  IsFull: " + adapter.IsFull)
        System.out.println
      }
    }
  }
}