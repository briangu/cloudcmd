package cloudcmd.cld.commands

import cloudcmd.common.{FileMetaData, FileUtil}
import cloudcmd.common.util.JsonUtil
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

  @Opt(opt = "a", longOpt = "all", description = "get all files", required = false) private var _pullAll: Boolean = false
  @Opt(opt = "f", longOpt = "filenames", description = "only use the filenames from archived files when storing locally", required = false) private var _removePaths: Boolean = false
  @Opt(opt = "d", longOpt = "destdir", description = "destination directory where the files will be stored", required = false) private var _outdir: String = null
  @Opt(opt = "p", longOpt = "prefix", description = "path to prefix the archived file paths with when they are stored locally.", required = false) private var _prefix: String = null
  @Opt(opt = "y", longOpt = "dryrun", description = "perform a dryrun and just show what would be fetched.", required = false) private var _dryrun: Boolean = false
  @Opt(opt = "i", longOpt = "input", description = "input file", required = false) private var _inputFilePath: String = null
  @Opt(opt = "n", longOpt = "minTier", description = "min tier to verify to", required = false) private var _minTier: Number = 0
  @Opt(opt = "m", longOpt = "maxTier", description = "max tier to verify to", required = false) private var _maxTier: Number = Integer.MAX_VALUE
  @Opt(opt = "q", longOpt = "unique", description = "only retrieve unique files", required = false) private var _uniqueOnly: Boolean = false
  @Opt(opt = "u", longOpt = "uri", description = "adapter URI", required = false) private var _uri: String = null

  def exec(commandLine: CommandContext) {

    CloudServices.initWithTierRange(_minTier.intValue, _maxTier.intValue)

    var selections: Seq[FileMetaData] = null
    if (_pullAll) {
      selections = CloudServices.BlockStorage.find(new JSONObject)
    } else {
      var is: InputStream = null
      try {
        is = if ((_inputFilePath != null)) new FileInputStream(new File(_inputFilePath)) else System.in
        selections = FileMetaData.fromJsonArray(JsonUtil.loadJsonArray(System.in))
      } finally {
        if (is ne System.in) is.close()
      }
    }
    if (_removePaths) removePaths(selections)
    if (_prefix != null) prefixPaths(_prefix, selections)
    if (_outdir == null) _outdir = FileUtil.getCurrentWorkingDirectory
    if (_uniqueOnly) selections = removeDuplicates(selections)
    prefixPaths(_outdir, selections)
    if (!_dryrun) {
      CloudServices.IndexStorage.get(selections)
    } else {
      System.out.println(FileMetaData.toJsonArray(selections).toString)
    }
  }

  private def removeDuplicates(selections: Seq[FileMetaData]): Seq[FileMetaData] = {
    val dedupMap = new mutable.HashMap[Seq[String], FileMetaData]()
    selections.foreach{ selection =>
      dedupMap.put(selection.getBlockHashes, selection)
    }
    val result = dedupMap.values.toSeq
    if (result.size < selections.length) {
      println("removed %d duplicates".format(selections.length - result.size))
    }
    result
  }

  private def removePaths(selections: Seq[FileMetaData]) {
    selections.foreach{ selection =>
      selection.getRawData.put("path", new File(selection.getRawData.getString("path")).getName)
    }
  }

  private def prefixPaths(prefix: String, selections: Seq[FileMetaData]) {
    selections.foreach{ selection =>
      var path: String = selection.getRawData.getString("path")
      path = prefix + (if (path.startsWith(File.separator)) path else File.separator + path)
      selection.getRawData.put("path", path)
    }
  }
}