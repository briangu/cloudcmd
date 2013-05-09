package cloudcmd.cld.commands

import cloudcmd.common.{FileMetaData, StringUtil}
import jpbetz.cli._
import org.json.JSONObject
import cloudcmd.cld.CloudServices

@SubCommand(name = "find", description = "Query the index of archived files.")
class Find extends Command {

  @Arg(name = "tags", optional = true, isVararg = true) private var _tags: java.util.List[String] = null
  @Opt(opt = "n", longOpt = "minTier", description = "min tier to verify to", required = false) private var _minTier: Number = 0
  @Opt(opt = "m", longOpt = "maxTier", description = "max tier to verify to", required = false) private var _maxTier: Number = Integer.MAX_VALUE
  @Opt(opt = "p", longOpt = "path", description = "path to find by", required = false) private var _path: String = null
  @Opt(opt = "f", longOpt = "name", description = "filename to find by", required = false) private var _filename: String = null
  @Opt(opt = "h", longOpt = "hash", description = "hash to find by", required = false) private var _hash: String = null
  @Opt(opt = "e", longOpt = "ext", description = "file extension to find by", required = false) private var _fileext: String = null
  @Opt(opt = "c", longOpt = "count", description = "limit response count", required = false) private var _count: Number = 0
  @Opt(opt = "o", longOpt = "offset", description = "pagination start offset", required = false) private var _offset: Number = 0
  @Opt(opt = "u", longOpt = "uri", description = "adapter URI", required = false) private var _uri: String = null

  def exec(commandLine: CommandContext) {
    val matchedAdapter = Option(_uri) match {
      case Some(uri) => {
        CloudServices.ConfigService.findAdapterByBestMatch(_uri) match {
          case Some(adapter) => {
            System.err.println("searching adapter: %s".format(adapter.URI.toASCIIString))
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
        System.err.println("searching all adapters.")
        CloudServices.BlockStorage
      }
    }

    Option(matchedAdapter) match {
      case Some(adapter) => {
        val filter = new JSONObject
        if (_tags != null) {
          import scala.collection.JavaConversions._
          val tags = FileMetaData.prepareTags(_tags.toList)
          if (tags.size > 0) filter.put("tags", StringUtil.join(tags, " "))
        }
        if (_path != null) filter.put("path", _path)
        if (_filename != null) filter.put("filename", _filename)
        if (_fileext != null) filter.put("fileext", _fileext)
        if (_hash != null) filter.put("hash", _hash)
        if (_count.intValue > 0) filter.put("count", _count.intValue)
        if (_offset.intValue > 0) filter.put("offset", _offset.intValue)

        val selections = adapter.find(filter)
        System.out.println(FileMetaData.toJsonArray(selections).toString)
      }
    }
 }
}