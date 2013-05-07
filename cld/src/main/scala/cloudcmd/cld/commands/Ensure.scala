package cloudcmd.cld.commands

import cloudcmd.common.{ContentAddressableStorage, BlockContext, FileMetaData}
import cloudcmd.common.util.JsonUtil
import jpbetz.cli.Command
import jpbetz.cli.CommandContext
import jpbetz.cli.Opt
import jpbetz.cli.SubCommand
import cloudcmd.cld.CloudServices

@SubCommand(name = "ensure", description = "Validate storage and ensure files are properly replicated.")
class Ensure extends Command {

  @Opt(opt = "n", longOpt = "minTier", description = "min tier to verify to", required = false) private var _minTier: Number = 0
  @Opt(opt = "m", longOpt = "maxTier", description = "max tier to verify to", required = false) private var _maxTier: Number = Integer.MAX_VALUE
  @Opt(opt = "a", longOpt = "all", description = "sync all", required = false) private var _syncAll: Boolean = true
  @Opt(opt = "b", longOpt = "chkblks", description = "do a block-level check", required = false) private var _blockLevelCheck: Boolean = false
  @Opt(opt = "u", longOpt = "uri", description = "adapter URI", required = false) private var _uri: String = null

  def exec(commandLine: CommandContext) {
    CloudServices.ConfigService.findAdapterByBestMatch(_uri) match {
      case Some(adapter) => {
        System.err.println("reindexing adapter: %s".format(adapter.URI.toASCIIString))
        val selections = describeToFileBlockContexts(adapter)
        System.err.println("syncing %d files".format(selections.size))
        adapter.ensureAll(fileMetaDataToBlockContexts(selections), blockLevelCheck = _blockLevelCheck)
      }
      case None => {
        CloudServices.initWithTierRange(_minTier.intValue, _maxTier.intValue)

        val selections = if (_syncAll) {
          describeToFileBlockContexts(CloudServices.BlockStorage)
        } else {
         FileMetaData.fromJsonArray(JsonUtil.loadJsonArray(System.in))
        }

        System.err.println("syncing %d files".format(selections.size))
        CloudServices.BlockStorage.ensureAll(fileMetaDataToBlockContexts(selections), _blockLevelCheck)
      }
    }
  }

  def describeToFileBlockContexts(cas: ContentAddressableStorage): Seq[FileMetaData] = {
    cas.describe().filter(_.endsWith(".meta")).par.flatMap { hash =>
      try {
        List(FileMetaData.create(hash, JsonUtil.loadJson(cas.load(new BlockContext(hash))._1)))
      } catch {
        case e: Exception => {
          println("Failed to load: %s".format(hash))
          // TODO: REPORT via notification center
          Nil
        }
      }
    }.toList
  }

  def fileMetaDataToBlockContexts(fmds: Seq[FileMetaData]): Set[BlockContext] = {
    Set() ++ fmds.flatMap(_.createAllBlockContexts)
  }
}