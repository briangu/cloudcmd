package cloudcmd.cld.commands

import jpbetz.cli.{Opt, SubCommand}
import cloudcmd.common.IndexedContentAddressableStorage

@SubCommand(name = "reindex", description = "Rebuild the index from available adapters.")
class Reindex extends AdapterCommand {
  def execWithAdapter(adapter: IndexedContentAddressableStorage) {
    adapter.reindex()
  }
}