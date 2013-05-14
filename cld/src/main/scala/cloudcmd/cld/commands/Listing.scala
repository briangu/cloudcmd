package cloudcmd.cld.commands

import jpbetz.cli.{Command, CommandContext, Opt, SubCommand}
import cloudcmd.common.IndexedContentAddressableStorage
import cloudcmd.cld.AdapterUtil

@SubCommand(name = "ls", description = "Perform a directory listing of archived files.")
class Listing extends Command {

  @Opt(opt = "n", longOpt = "minTier", description = "min tier to verify to", required = false) var _minTier: Number = 0
  @Opt(opt = "m", longOpt = "maxTier", description = "max tier to verify to", required = false) var _maxTier: Number = Integer.MAX_VALUE
  @Opt(opt = "u", longOpt = "uri", description = "adapter URI", required = false) var _uri: String = null

  def exec(p1: CommandContext) {
    AdapterUtil.exec(p1, _uri, _minTier, _maxTier, doCommand)
  }

  def doCommand(adapter: IndexedContentAddressableStorage) {
    adapter.find() foreach { fmd => System.out.println(fmd.getPath) }
  }
}