package cloudcmd.cld.commands

import cloudcmd.common.{ContentAddressableStorage, IndexedContentAddressableStorage, FileMetaData, FileUtil}
import cloudcmd.common.util.{CryptoUtil, JsonUtil}
import jpbetz.cli.Command
import jpbetz.cli.CommandContext
import jpbetz.cli.Opt
import jpbetz.cli.SubCommand
import org.json.JSONObject
import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import cloudcmd.cld.CloudServices
import scala.collection.mutable

@SubCommand(name = "get", description = "Fetch files from the cloud and store locally.")
class Get extends Command {

  private val MAX_FETCH_RETRIES = 3

  @Opt(opt = "a", longOpt = "all", description = "get all files", required = false) private var _getAll: Boolean = false
  @Opt(opt = "f", longOpt = "filenames", description = "only use the filenames from archived files when storing locally", required = false) private var _removePaths: Boolean = false
  @Opt(opt = "d", longOpt = "destdir", description = "destination directory where the files will be stored", required = false) private var _outdir: String = null
  @Opt(opt = "p", longOpt = "prefix", description = "path to prefix the archived file paths with when they are stored locally.", required = false) private var _prefix: String = null
  @Opt(opt = "i", longOpt = "input", description = "input file", required = false) private var _inputFilePath: String = null
  @Opt(opt = "n", longOpt = "minTier", description = "min tier to verify to", required = false) private var _minTier: Number = 0
  @Opt(opt = "m", longOpt = "maxTier", description = "max tier to verify to", required = false) private var _maxTier: Number = Integer.MAX_VALUE
  @Opt(opt = "q", longOpt = "unique", description = "only retrieve unique files", required = false) private var _uniqueOnly: Boolean = false
  @Opt(opt = "u", longOpt = "uri", description = "adapter URI", required = false) private var _uri: String = null

  def exec(commandLine: CommandContext) {

    val matchedAdapter: IndexedContentAddressableStorage = Option(_uri) match {
      case Some(uri) => {
        CloudServices.ConfigService.findAdapterByBestMatch(_uri) match {
          case Some(adapter) => {
            System.err.println("getting from adapter: %s".format(adapter.URI.toASCIIString))
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
        System.err.println("getting from all adapters.")
        CloudServices.BlockStorage
      }
    }

    Option(matchedAdapter) match {
      case Some(adapter) => {
        var selections: Iterable[FileMetaData] = if (_getAll) {
          adapter.find(new JSONObject)
        } else {
          var is: InputStream = null
          try {
            is = if ((_inputFilePath != null)) new FileInputStream(new File(_inputFilePath)) else System.in
            FileMetaData.fromJsonArray(JsonUtil.loadJsonArray(System.in))
          } finally {
            if (is ne System.in) is.close()
          }
        }

        if (_removePaths) removePaths(selections)
        if (_prefix != null) prefixPaths(_prefix, selections)
        if (_outdir == null) _outdir = FileUtil.getCurrentWorkingDirectory
        if (_uniqueOnly) selections = removeDuplicates(selections)
        prefixPaths(_outdir, selections)

        if (selections.size > 0) {
          get(adapter, selections)
        } else {
          System.err.println("nothing to do.")
        }
      }
      case None => {
        System.err.println("nothing to do.")
      }
    }
  }

  private def removeDuplicates(selections: Iterable[FileMetaData]): Iterable[FileMetaData] = {
    val dedupMap = new mutable.HashMap[Iterable[String], FileMetaData]()
    selections.foreach{ selection =>
      dedupMap.put(selection.getBlockHashes, selection)
    }
    val result = dedupMap.values
    if (result.size < selections.size) {
      System.err.println("removed %d duplicates".format(selections.size - result.size))
    }
    result
  }

  private def removePaths(selections: Iterable[FileMetaData]) {
    selections.foreach{ selection =>
      selection.getRawData.put("path", new File(selection.getRawData.getString("path")).getName)
    }
  }

  private def prefixPaths(prefix: String, selections: Iterable[FileMetaData]) {
    selections.foreach{ selection =>
      var path: String = selection.getRawData.getString("path")
      path = prefix + (if (path.startsWith(File.separator)) path else File.separator + path)
      selection.getRawData.put("path", path)
    }
  }

  def get(cas: ContentAddressableStorage, selections: Iterable[FileMetaData]) {
    selections.par.foreach { fmd =>
      try {
        fetch(cas, fmd)
      } catch {
        case e: Exception => {
          System.err.println(e.getMessage)
        }
      }
    }
  }

  def onMessage(msg: String) {
    CloudServices.onMessage(msg)
  }

  // TODO: only read the file size bytes back (if the file is one block)
  // TODO: support writing to an offset of the existing file to allow for sub-blocks
  def fetch(cas: ContentAddressableStorage, fmd: FileMetaData) {
    if (fmd.getBlockHashes.size == 0) throw new IllegalArgumentException("no block hashes found!")
    if (fmd.getBlockHashes.find(h => !cas.contains(fmd.createBlockContext(h))) == None) {
      if (fmd.getBlockHashes.size == 1) {
        attemptSingleBlockFetch(cas, fmd.getBlockHashes(0), fmd)
      } else {
        throw new RuntimeException("multiple block hashes not yet supported!")
      }
    } else {
      onMessage(String.format("some blocks of %s not currently available!", fmd.getBlockHashes.mkString(",")))
    }
  }

  def attemptSingleBlockFetch(cas: ContentAddressableStorage, blockHash: String, fmd: FileMetaData) : Boolean = {
    var success = false
    var retries = MAX_FETCH_RETRIES

    while (!success && retries > 0) {
      var remoteData: InputStream = null
      try {
        remoteData = cas.load(fmd.createBlockContext(blockHash))._1
        val destFile = new File(fmd.getPath)
        destFile.getParentFile.mkdirs
        val remoteDataHash = CryptoUtil.digestToString(CryptoUtil.writeAndComputeHash(remoteData, destFile))
        if (remoteDataHash.equals(blockHash)) {
          onMessage("retrieved: %s".format(fmd.getPath))
          success = true
        } else {
          onMessage("%s is corrupted after download using block %s".format(fmd.getFilename, blockHash))
          destFile.delete
        }
      } catch {
        case e: Exception => {
          onMessage("%s failed to read block %s".format(fmd.getFilename, blockHash))
//          log.error(blockHash, e)
        }
      } finally {
        FileUtil.SafeClose(remoteData)
      }

      if (!success) {
        retries -= 1
        cas.ensure(fmd.createBlockContext(blockHash), blockLevelCheck = true)
        if (!cas.contains(fmd.createBlockContext(blockHash))) {
          onMessage("giving up on %s, block %s not currently available!".format(fmd.getFilename, blockHash))
          retries = 0
        }
      }
    }
    success
  }
}