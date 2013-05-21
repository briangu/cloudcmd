package cloudcmd.cld.commands

import cloudcmd.common.util.JsonUtil
import jpbetz.cli.{Command, CommandContext, Opt, SubCommand}
import cloudcmd.cld.{AdapterUtil, CloudServices}
import cloudcmd.common.{IndexedContentAddressableStorage, FileMetaData}
import cloudcmd.common.adapters.DirectAdapter

@SubCommand(name = "remove", description = "Remove files from storage.")
class Remove extends Command {

  @Opt(opt = "b", longOpt = "blocks", description = "remove associated blocks", required = false) private var _removeBlockHashes: Boolean = false

  @Opt(opt = "n", longOpt = "minTier", description = "min tier to verify to", required = false) var _minTier: Number = 0
  @Opt(opt = "m", longOpt = "maxTier", description = "max tier to verify to", required = false) var _maxTier: Number = Integer.MAX_VALUE
  @Opt(opt = "u", longOpt = "uri", description = "adapter URI", required = false) var _uri: String = null

  def exec(p1: CommandContext) {
    AdapterUtil.exec(p1, _uri, _minTier, _maxTier, doCommand)
  }

  def doCommand(adapter: IndexedContentAddressableStorage) {
    val jsonFileMetaDataArray = JsonUtil.loadJsonArray(System.in)

    if (adapter == CloudServices.BlockStorage) {
      if (_removeBlockHashes) {
        System.err.println("removing meta and file data for %d files from all adapters.".format(jsonFileMetaDataArray.length()))
      } else {
        System.err.println("removing meta data for %d files from adapters.".format(jsonFileMetaDataArray.length()))
      }
    } else {
      if (_removeBlockHashes) {
        System.err.println("removing meta and file data for %d files from adapter: %s".format(jsonFileMetaDataArray.length(), adapter.asInstanceOf[DirectAdapter].URI.toASCIIString))
      } else {
        System.err.println("removing meta data for %d files from adapter: %s".format(jsonFileMetaDataArray.length(), adapter.asInstanceOf[DirectAdapter].URI.toASCIIString))
      }
    }

    val blockContexts = FileMetaData.toBlockContextsFromJsonArray(jsonFileMetaDataArray, includeBlockHashes = _removeBlockHashes)
    adapter.removeAll(blockContexts)
  }
}