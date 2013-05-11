package cloudcmd.cld.commands

import jpbetz.cli.{CommandContext, Opt, Command}
import cloudcmd.cld.AdapterUtil._
import cloudcmd.common.IndexedContentAddressableStorage
import cloudcmd.cld.Notifications._
import cloudcmd.common.engine.NotificationCenter._
import scala.Some

abstract class AdapterCommand extends Command {

  @Opt(opt = "n", longOpt = "minTier", description = "min tier to verify to", required = false) private var _minTier: Number = 0
  @Opt(opt = "m", longOpt = "maxTier", description = "max tier to verify to", required = false) private var _maxTier: Number = Integer.MAX_VALUE
  @Opt(opt = "u", longOpt = "uri", description = "adapter URI", required = false) private var _uri: String = null

  def nothingToDo() {
    msg("nothing to do.")
  }

  def exec(p1: CommandContext) {

    withAdapter(Some(_uri), _minTier, _maxTier, (adapter: IndexedContentAddressableStorage) => {
      execWithAdapter(adapter)
    })

    defaultCenter.removeObserver(this, None, None)
  }

  def execWithAdapter(adapter: IndexedContentAddressableStorage)

}