package cloudcmd.cld.commands

import cloudcmd.common.{IndexedContentAddressableStorage, FileMetaData}
import cloudcmd.common.util.JsonUtil
import jpbetz.cli.Opt
import jpbetz.cli.SubCommand
import cloudcmd.cld.Notifications._
import cloudcmd.cld.AdapterUtil._

@SubCommand(name = "ensure", description = "Validate storage and ensure files are properly replicated.")
class Ensure extends AdapterCommand {

  @Opt(opt = "a", longOpt = "all", description = "sync all", required = false) private var _syncAll: Boolean = true
  @Opt(opt = "b", longOpt = "chkblks", description = "do a block-level check", required = false) private var _blockLevelCheck: Boolean = false

  def execWithAdapter(adapter: IndexedContentAddressableStorage) {
    val fmds = if (_syncAll) {
      describeAsFileMetaData(adapter)
    } else {
      FileMetaData.fromJsonArray(JsonUtil.loadJsonArray(System.in))
    }

    msg("ensuring %d files.".format(fmds.size))
    if (fmds.size > 0) {
      fmds foreach { fmd =>
        println(fmd.getPath)
        adapter.ensureAll(fmd.createAllBlockContexts)
      }
//          adapter.ensureAll(FileMetaData.toBlockContexts(fmds),blockLevelCheck = _blockLevelCheck)
    } else {
      nothingToDo()
    }
  }
}