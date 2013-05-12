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
    val matchedAdapter = Option(_uri) match {
      case Some(uri) => {
        CloudServices.ConfigService.findAdapterByBestMatch(_uri) match {
          case Some(adapter) => {
            System.err.println("ensuring adapter: %s".format(adapter.URI.toASCIIString))
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
        System.err.println("ensuring all adapters.")
        CloudServices.BlockStorage
      }
    }

    Option(matchedAdapter) match {
      case Some(adapter) => {
        System.err.println("scanning adapters.")
        val fmds = if (_syncAll) {
          adapter.find()
        } else {
          FileMetaData.fromJsonArray(JsonUtil.loadJsonArray(System.in))
        }

        System.err.println("ensuring %d files.".format(fmds.size))

        if (fmds.size > 0) {
          fmds foreach { fmd =>
            System.err.println("ensuring: %s".format(fmd.getPath))

            val metaBlock = fmd.createBlockContext
            System.err.println("\tmeta: %s".format(metaBlock.getId()))
            adapter.ensure(metaBlock, blockLevelCheck = _blockLevelCheck)

            val blockHashes = fmd.createBlockHashBlockContexts
            blockHashes.foreach{ blockHash =>
              System.err.println("\tblockhash: %s".format(blockHash.getId()))
              adapter.ensure(blockHash, blockLevelCheck = _blockLevelCheck)
            }
          }

          System.err.println("Flushing metadata...")
          adapter.flushIndex()
        } else {
          System.err.println("nothing to do.")
        }
      }
      case None => {
        System.err.println("nothing to do.")
      }
    }
  }
}