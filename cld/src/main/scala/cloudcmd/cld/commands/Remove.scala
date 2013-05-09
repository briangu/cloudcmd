package cloudcmd.cld.commands

import cloudcmd.common.util.JsonUtil
import jpbetz.cli.Command
import jpbetz.cli.CommandContext
import jpbetz.cli.Opt
import jpbetz.cli.SubCommand
import org.json.JSONArray
import cloudcmd.cld.{Util, CloudServices}
import cloudcmd.common.{BlockContext, FileMetaData}

@SubCommand(name = "remove", description = "Remove files from storage.")
class Remove extends Command {

  @Opt(opt = "u", longOpt = "uri", description = "adapter URI", required = false) private var _uri: String = null
  @Opt(opt = "n", longOpt = "minTier", description = "min tier to verify to", required = false) private var _minTier: Number = 0
  @Opt(opt = "m", longOpt = "maxTier", description = "max tier to verify to", required = false) private var _maxTier: Number = Integer.MAX_VALUE
  @Opt(opt = "b", longOpt = "blocks", description = "remove associated blocks", required = false) private var _removeBlockHashes: Boolean = false

  def exec(commandLine: CommandContext) {
    val jsonFileMetaDataArray = JsonUtil.loadJsonArray(System.in)

    val matchedAdapter = Option(_uri) match {
      case Some(uri) => {
        CloudServices.ConfigService.findAdapterByBestMatch(_uri) match {
          case Some(adapter) => {
            if (_removeBlockHashes) {
              System.err.println("removing meta and file data for %d files from adapter: %s".format(jsonFileMetaDataArray.length()), adapter.URI.toASCIIString)
            } else {
              System.err.println("removing meta data for %d files from adapter: %s".format(jsonFileMetaDataArray.length()), adapter.URI.toASCIIString)
            }
            adapter
          }
          case None => {
            System.err.println("adapter %s not found.".format(_uri))
            null
          }
        }
      }
      case None => {
        CloudServices.initWithTierRange(_minTier.intValue, _maxTier.intValue)
        if (_removeBlockHashes) {
          System.err.println("removing meta and file data for %d files from all adapters.".format(jsonFileMetaDataArray.length()))
        } else {
          System.err.println("removing meta data for %d files from adapters.".format(jsonFileMetaDataArray.length()))
        }
        CloudServices.BlockStorage
      }
    }

    Option(matchedAdapter) match {
      case Some(adapter) => {
        val blockContexts = FileMetaData.toBlockContextsFromJsonArray(jsonFileMetaDataArray, includeBlockHashes = _removeBlockHashes)
        adapter.removeAll(blockContexts)
      }
    }
  }
}