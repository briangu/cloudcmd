package cloudcmd.cld

import cloudcmd.common.{IndexedContentAddressableStorage, BlockContext, FileMetaData, ContentAddressableStorage}
import cloudcmd.common.util.JsonUtil
import Notifications._

object AdapterUtil {

  def describeAsFileMetaData(cas: ContentAddressableStorage): Set[FileMetaData] = {
    Set() ++ cas.describe().filter(_.endsWith(".meta")).par.flatMap { hash =>
      try {
        Set(FileMetaData.create(hash, JsonUtil.loadJson(cas.load(new BlockContext(hash))._1)))
      } catch {
        case e: Exception => {
          msg("Failed to load: %s".format(hash))
          Nil
        }
      }
    }
  }

  def withAdapter(uriOption: Option[String], minTier: Number, maxTier: Number, fn: (IndexedContentAddressableStorage) => Unit) {
    val matchedAdapter = uriOption match {
      case Some(uri) => {
        CloudServices.ConfigService.findAdapterByBestMatch(uri) match {
          case Some(adapter) => {
            msg("using adapter: %s".format(adapter.URI.toASCIIString))
            Some(adapter)
          }
          case None => {
            msg("adapter %s not found.".format(uri))
            None
          }
        }
      }
      case None => {
        CloudServices.initWithTierRange(minTier.intValue, maxTier.intValue)
        if (maxTier != Int.MaxValue) {
          msg("using all adapters in tier range: (%d, %d)".format(minTier, maxTier))
        } else {
          msg("using all available adapters.")
        }
        Some(CloudServices.BlockStorage)
      }
    }

    matchedAdapter match {
      case Some(adapter) => fn(adapter)
      case None => msg("nothing to do.")
    }
  }
}
