package cloudcmd.cld.commands

import cloudcmd.common.util.JsonUtil
import jpbetz.cli.Command
import jpbetz.cli.CommandContext
import jpbetz.cli.Opt
import jpbetz.cli.SubCommand
import org.json.JSONArray
import cloudcmd.cld.CloudServices
import cloudcmd.common.FileMetaData

@SubCommand(name = "remove", description = "Remove files from storage.")
class Remove extends Command {

  @Opt(opt = "u", longOpt = "uri", description = "adapter URI", required = false) private var _uri: String = null
  @Opt(opt = "n", longOpt = "minTier", description = "min tier to verify to", required = false) private var _minTier: Number = 0
  @Opt(opt = "m", longOpt = "maxTier", description = "max tier to verify to", required = false) private var _maxTier: Number = Integer.MAX_VALUE

  def exec(commandLine: CommandContext) {
    Option(_uri) match {
      case Some(uri) => {
        CloudServices.ConfigService.findAdapterByBestMatch(_uri) match {
          case Some(adapter) => {
            System.err.println("reindexing %s".format(adapter.URI.toASCIIString))
            val metaHashes = CloudServices.BlockStorage.describe().filter(_.isMeta())
            metaHashes.par.map{ ctx => FileMetaData.create(ctx.hash, JsonUtil.loadJson(CloudServices.BlockStorage.load(ctx)._1)) }.toList
            System.err.println("removing %d files".format(selections.length))
            CloudServices.IndexStorage.remove(FileMetaData.fromJsonArray(JsonUtil.loadJsonArray(System.in)))
          }
          case None => {
            println("adapter %s not found.".format(_uri))
          }
        }
      }
      case None => {
        CloudServices.initWithTierRange(_minTier.intValue, _maxTier.intValue)

        val selections = if (_syncAll) {
          val metaHashes = CloudServices.BlockStorage.describe().filter(_.isMeta())
          metaHashes.par.map{ ctx => FileMetaData.create(ctx.hash, JsonUtil.loadJson(CloudServices.BlockStorage.load(ctx)._1)) }.toList
        } else {
          FileMetaData.fromJsonArray(JsonUtil.loadJsonArray(System.in))
        }

        System.err.println("removing %d files".format(selections.length))
        CloudServices.BlockStorage.remove(FileMetaData.fromJsonArray(JsonUtil.loadJsonArray(System.in)))
      }
    }
 }
}