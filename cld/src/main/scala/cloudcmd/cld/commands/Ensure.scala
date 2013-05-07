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
  @Opt(opt = "u", longOpt = "uri", description = "adapter URI", required = false) private var _uri: String = null

  def exec(commandLine: CommandContext) {
    CloudServices.ConfigService.findAdapterByBestMatch(_uri) match {
      case Some(adapter) => {
        System.err.println("reindexing %s".format(adapter.URI.toASCIIString))
        val metaHashes = CloudServices.BlockStorage.describe().filter(_.isMeta())
        metaHashes.par.map{ ctx => FileMetaData.create(ctx.hash, JsonUtil.loadJson(CloudServices.BlockStorage.load(ctx)._1)) }.toList
        adapter.ensure()
      }
      case None => {
        CloudServices.initWithTierRange(_minTier.intValue, _maxTier.intValue)

        val selections = if (_syncAll) {
          val metaHashes = CloudServices.BlockStorage.describe().filter(_.isMeta())
          metaHashes.par.map{ ctx => FileMetaData.create(ctx.hash, JsonUtil.loadJson(CloudServices.BlockStorage.load(ctx)._1)) }.toList
        } else {
          FileMetaData.fromJsonArray(JsonUtil.loadJsonArray(System.in))
        }

        System.err.println("syncing %d files".format(selections.length))
        CloudServices.BlockStorage.ensureAll(selections, _blockLevelCheck)
      }
    }
  }
}