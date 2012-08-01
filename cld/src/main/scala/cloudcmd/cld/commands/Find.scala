package cloudcmd.cld.commands

import cloudcmd.common.util.MetaUtil
import cloudcmd.common.StringUtil
import jpbetz.cli._
import org.json.JSONObject
import cloudcmd.cld.CloudServices

@SubCommand(name = "find", description = "Query the index of archived files.")
class Find extends Command {

  @Arg(name = "tags", optional = true, isVararg = true) private var _tags: java.util.List[String] = null
  @Opt(opt = "p", longOpt = "path", description = "path to find by", required = false) private var _path: String = null
  @Opt(opt = "f", longOpt = "name", description = "filename to find by", required = false) private var _filename: String = null
  @Opt(opt = "h", longOpt = "hash", description = "hash to find by", required = false) private var _hash: String = null
  @Opt(opt = "e", longOpt = "ext", description = "file extension to find by", required = false) private var _fileext: String = null
  @Opt(opt = "c", longOpt = "count", description = "limit response count", required = false) private var _count: Number = 0
  @Opt(opt = "o", longOpt = "offset", description = "pagination start offset", required = false) private var _offset: Number = 0

  def exec(commandLine: CommandContext) {
    val filter = new JSONObject
    if (_tags != null) {
      import scala.collection.JavaConversions._
      val tags = MetaUtil.prepareTags(_tags.toList)
      if (tags.size > 0) filter.put("tags", StringUtil.join(tags, " "))
    }
    if (_path != null) filter.put("path", _path)
    if (_filename != null) filter.put("filename", _filename)
    if (_fileext != null) filter.put("fileext", _fileext)
    if (_hash != null) filter.put("hash", _hash)
    if (_count.intValue > 0) filter.put("count", _count.intValue)
    if (_offset.intValue > 0) filter.put("offset", _offset.intValue)
    val selections = CloudServices.IndexStorage.find(filter)
    System.out.println(selections.toString)
  }
}