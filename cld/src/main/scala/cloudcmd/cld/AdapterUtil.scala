package cloudcmd.cld

import cloudcmd.common.{IndexedContentAddressableStorage, BlockContext, FileMetaData, ContentAddressableStorage}
import cloudcmd.common.util.JsonUtil
import jpbetz.cli.CommandContext

object AdapterUtil {

  def exec(p1: CommandContext, uri: String = null, minTier: Number = 0, maxTier: Number = Integer.MAX_VALUE, doCommand: (IndexedContentAddressableStorage) => Unit) {
    val matchedAdapter = Option(uri) match {
      case Some(uri) => {
        CloudServices.ConfigService.findAdapterByBestMatch(uri) match {
          case Some(adapter) => {
            System.err.println("using adapter: %s".format(adapter.URI.toASCIIString))
            adapter
          }
          case None => {
            System.err.println("adapter %s not found.".format(uri))
            null
          }
        }
      }
      case None => {
        CloudServices.initWithTierRange(minTier.intValue, maxTier.intValue)
        if (minTier != 0 || maxTier != Integer.MAX_VALUE) {
          System.err.println("using all adapters in tier range: (%d, %d)".format(CloudServices.ConfigService.getMinAdapterTier, CloudServices.ConfigService.getMaxAdapterTier))
        } else {
          System.err.println("using all adapters.")
        }
        CloudServices.BlockStorage
      }
    }

    Option(matchedAdapter) match {
      case Some(adapter) => {
        doCommand(adapter)
      }
      case None => {
        System.err.println("nothing to do.")
      }
    }
  }
}
