package cloudcmd.cld

import cloudcmd.common.{BlockContext, FileMetaData, ContentAddressableStorage}
import cloudcmd.common.util.JsonUtil

object Util {
  def describeToFileBlockContexts(cas: ContentAddressableStorage): Seq[FileMetaData] = {
    cas.describe().filter(_.endsWith(".meta")).par.flatMap { hash =>
      try {
        List(FileMetaData.create(hash, JsonUtil.loadJson(cas.load(new BlockContext(hash))._1)))
      } catch {
        case e: Exception => {
          System.err.println("Failed to load: %s".format(hash))
          // TODO: REPORT via notification center
          Nil
        }
      }
    }.toList
  }
}
