package cloudcmd.cld.commands

import jpbetz.cli.{Opt, SubCommand}
import cloudcmd.common.IndexedContentAddressableStorage
import cloudcmd.cld.AdapterUtil._
import cloudcmd.cld.Notifications._


@SubCommand(name = "ls", description = "Perform a directory listing of archived files.")
class Listing extends AdapterCommand {

  def execWithAdapter(adapter: IndexedContentAddressableStorage) {
    describeAsFileMetaData(adapter).foreach(selection => msg(selection.getPath))
  }
}