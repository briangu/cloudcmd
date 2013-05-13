package cloudcmd.cld.commands

import cloudcmd.common.FileMetaData
import cloudcmd.common.util.JsonUtil
import jpbetz.cli.Command
import jpbetz.cli.CommandContext
import jpbetz.cli.Opt
import jpbetz.cli.SubCommand
import cloudcmd.cld.CloudServices
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

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

        val blockMap = new mutable.HashMap[String, ListBuffer[FileMetaData]]
        fmds foreach { fmd =>
          fmd.getBlockHashes foreach { blockHash =>
            blockMap.get(blockHash) match {
              case Some(list) => list.append(fmd)
              case None => {
                val newList = new ListBuffer[FileMetaData]
                newList.append(fmd)
                blockMap.put(blockHash, newList)
              }
            }
          }
        }

        System.err.println("ensuring %d unique underlying blocks.".format(blockMap.size))

        var failedCount = 0

        if (blockMap.size > 0) {
          blockMap.par.foreach{
            case (blockHash: String, fmds: ListBuffer[FileMetaData]) => {
              fmds foreach { fmd =>
                val metaBlock = fmd.createBlockContext
                val metaOK = adapter.ensure(metaBlock, blockLevelCheck = _blockLevelCheck)

                var blockHashesOK = true
                val blockHashEnsureResults = fmd.createBlockHashBlockContexts flatMap { blockHash =>
                  val blockHashOK = adapter.ensure(blockHash, blockLevelCheck = _blockLevelCheck)
                  if (!blockHashOK) {
                    blockHashesOK = false
                  }
                  List(blockHashOK)
                }

                if (!metaOK || !blockHashesOK) {
                  val sb = new StringBuilder

                  metaOK match {
                    case true => sb.append("\tOK meta: %s\n".format(metaBlock.getId()))
                    case false => sb.append("\tFAILED meta: %s\n".format(metaBlock.getId()))
                  }

                  (0 until blockHashEnsureResults.size) foreach { idx =>
                    blockHashEnsureResults(idx) match {
                      case true => sb.append("\tOK blockhash: %s\n".format(fmd.getBlockHashes(idx)))
                      case false => sb.append("\tFAILED blockhash: %s\n".format(fmd.getBlockHashes(idx)))
                    }
                  }

                  sb.append("FAILED ensuring: %s\n".format(fmd.getPath))
                  System.err.println(sb.toString())
                  failedCount = failedCount + 1
                }
              }
            }
          }

          if (failedCount > 0) {
            System.err.println("failed %d/%d".format(failedCount, fmds.size))
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