package cloudcmd.cld.commands

import cloudcmd.cld.CloudEngineService
import cloudcmd.cld.IndexStorageService
import cloudcmd.common.FileUtil
import cloudcmd.common.util.JsonUtil
import jpbetz.cli.Command
import jpbetz.cli.CommandContext
import jpbetz.cli.Opt
import jpbetz.cli.SubCommand
import org.json.JSONArray
import org.json.JSONObject
import java.io.File
import java.io.FileInputStream
import java.io.InputStream

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

  def exec(commandLine: CommandContext) {
    var selections: JSONArray = null
    if (_pullAll) {
      selections = IndexStorageService.instance.find(new JSONObject)
    }
    else {
      var is: InputStream = null
      try {
        is = if ((_inputFilePath != null)) new FileInputStream(new File(_inputFilePath)) else System.in
        selections = JsonUtil.loadJsonArray(is)
      }
      finally {
        if (is ne System.in) is.close
      }
    }
    if (_removePaths) removePaths(selections)
    if (_prefix != null) prefixPaths(_prefix, selections)
    if (_outdir == null) _outdir = FileUtil.getCurrentWorkingDirectory
    prefixPaths(_outdir, selections)
    if (!_dryrun) {
      CloudEngineService.instance.filterAdapters(_minTier.intValue, _maxTier.intValue)
      IndexStorageService.instance.fetch(selections)
    }
    else {
      System.out.print(selections.toString)
    }
  }

  private def removePaths(selections: JSONArray) {
    (0 until selections.length()).foreach{ i =>
      val data: JSONObject = selections.getJSONObject(i).getJSONObject("data")
      val path: String = data.getString("path")
      data.put("path", new File(path).getName)
    }
  }

  private def prefixPaths(prefix: String, selections: JSONArray) {
    (0 until selections.length()).foreach{ i =>
      val data: JSONObject = selections.getJSONObject(i).getJSONObject("data")
      var path: String = data.getString("path")
      path = prefix + (if (path.startsWith(File.separator)) path else File.separator + path)
      data.put("path", path)
    }
  }
}