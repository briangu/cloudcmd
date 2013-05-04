package cloudcmd.cld.commands

import cloudcmd.common.FileMetaData
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

  def exec(commandLine: CommandContext) {

    val selections = if (_syncAll) {
      val metaHashes = CloudServices.CloudEngine.describe().filter(_.isMeta())
      metaHashes.par.map{ ctx => FileMetaData.create(ctx.hash, JsonUtil.loadJson(CloudServices.CloudEngine.load(ctx)._1)) }.toList
    } else {
      FileMetaData.fromJsonArray(JsonUtil.loadJsonArray(System.in))
    }

    System.err.println("syncing %d files".format(selections.length))
    CloudServices.CloudEngine.filterAdapters(_minTier.intValue, _maxTier.intValue)
    CloudServices.IndexStorage.ensure(selections, _blockLevelCheck)
    System.err.println("rebuilding file search index...")
    CloudServices.IndexStorage.reindex()
  }
}