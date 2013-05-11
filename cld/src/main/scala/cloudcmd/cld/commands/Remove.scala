package cloudcmd.cld.commands

import cloudcmd.common.util.JsonUtil
import jpbetz.cli.Opt
import jpbetz.cli.SubCommand
import cloudcmd.cld.CloudServices
import cloudcmd.common.FileMetaData
import cloudcmd.cld.Notifications._

@SubCommand(name = "remove", description = "Remove files from storage.")
class Remove extends CloudCommand {

  @Opt(opt = "u", longOpt = "uri", description = "adapter URI", required = false) private var _uri: String = null
  @Opt(opt = "n", longOpt = "minTier", description = "min tier to verify to", required = false) private var _minTier: Number = 0
  @Opt(opt = "m", longOpt = "maxTier", description = "max tier to verify to", required = false) private var _maxTier: Number = Integer.MAX_VALUE
  @Opt(opt = "b", longOpt = "blocks", description = "remove associated blocks", required = false) private var _removeBlockHashes: Boolean = false

  def doCommand() {
    val jsonFileMetaDataArray = JsonUtil.loadJsonArray(System.in)

    val matchedAdapter = Option(_uri) match {
      case Some(uri) => {
        CloudServices.ConfigService.findAdapterByBestMatch(_uri) match {
          case Some(adapter) => {
            if (_removeBlockHashes) {
              msg("removing meta and file data for %d files from adapter: %s".format(jsonFileMetaDataArray.length(), adapter.URI.toASCIIString))
            } else {
              msg("removing meta data for %d files from adapter: %s".format(jsonFileMetaDataArray.length(), adapter.URI.toASCIIString))
            }
            Some(adapter)
          }
          case None => {
            msg("adapter %s not found.".format(_uri))
            None
          }
        }
      }
      case None => {
        CloudServices.initWithTierRange(_minTier.intValue, _maxTier.intValue)
        if (_removeBlockHashes) {
          msg("removing meta and file data for %d files from all adapters.".format(jsonFileMetaDataArray.length()))
        } else {
          msg("removing meta data for %d files from adapters.".format(jsonFileMetaDataArray.length()))
        }
        Some(CloudServices.BlockStorage)
      }
    }

    matchedAdapter match {
      case Some(adapter) => {
        val blockContexts = FileMetaData.toBlockContextsFromJsonArray(jsonFileMetaDataArray, includeBlockHashes = _removeBlockHashes)
        adapter.removeAll(blockContexts)
      }
      case None => {
        nothingToDo()
      }
    }
  }
}