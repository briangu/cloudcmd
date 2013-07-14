package cloudcmd.cld

import cloudcmd.common.IndexedContentAddressableStorage
import jpbetz.cli.CommandContext

object AdapterUtil {

  def exec(p1: CommandContext, uri: String = null, minTier: Number = 0, maxTier: Number = Integer.MAX_VALUE, doCommand: (IndexedContentAddressableStorage) => Unit) {
    val matchedAdapter = Option(uri) match {
      case Some(uri) => {
        CloudServices.ConfigService.findIndexedAdapterByBestMatch(uri) match {
          case Some(adapter) => {
            System.err.println("using adapter: %s".format(adapter.getSignature))
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
          System.err.println("using all adapters in tier range (%d, %d):".format(CloudServices.ConfigService.getMinAdapterTier, CloudServices.ConfigService.getMaxAdapterTier))
        } else {
          System.err.println("using all available adapters:")
        }
        CloudServices.ConfigService.getFilteredIndexedAdapters foreach { adapter =>
          System.err.println("\t%s".format(adapter.getSignature))
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
