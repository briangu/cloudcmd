package cloudcmd.cld.commands

import cloudcmd.common.util.JsonUtil
import cloudcmd.common.{IndexedContentAddressableStorage, FileMetaData, FileUtil}
import jpbetz.cli._
import java.io.{ByteArrayInputStream, File, FileInputStream}
import cloudcmd.cld.CloudServices
import org.json.JSONArray

@SubCommand(name = "tag", description = "Add or remove tags to/from archived files.")
class Tag extends Command {

  @Arg(name = "tags", optional = false, isVararg = true) var _tags: java.util.List[String] = null
  @Opt(opt = "r", longOpt = "remove", description = "remove tags", required = false) var _remove: Boolean = false
  @Opt(opt = "i", longOpt = "input", description = "input file", required = false) private var _inputFilePath: String = null
  @Opt(opt = "n", longOpt = "minTier", description = "min tier to verify to", required = false) private var _minTier: Number = 0
  @Opt(opt = "m", longOpt = "maxTier", description = "max tier to verify to", required = false) private var _maxTier: Number = Integer.MAX_VALUE
  @Opt(opt = "u", longOpt = "uri", description = "adapter URI", required = false) private var _uri: String = null

  def exec(commandLine: CommandContext) {
    val matchedAdapter = Option(_uri) match {
      case Some(uri) => {
        CloudServices.ConfigService.findAdapterByBestMatch(_uri) match {
          case Some(adapter) => {
            System.err.println("adding tags to adapter: %s".format(uri))
            adapter
          }
          case None => {
            System.err.println("adapter %s not found.".format(_uri))
            null
          }
        }
      }
      case None => {
        System.err.println("adding tags to all adapters.")
        CloudServices.BlockStorage
      }
    }

    Option(matchedAdapter) match {
      case Some(adapter) => {
        import scala.collection.JavaConversions._

        val is = if ((_inputFilePath != null)) new FileInputStream(new File(_inputFilePath)) else System.in
        try {
          val selections = FileMetaData.fromJsonArray(JsonUtil.loadJsonArray(is))

          var preparedTags = FileMetaData.prepareTags(_tags.toList)
          if (_remove) {
            preparedTags = preparedTags.map("-" + _)
          }

          addTags(adapter, selections, preparedTags.toSet)
        } finally {
          if (is ne System.in) FileUtil.SafeClose(is)
        }
      }
      case None => {
        System.err.println("nothing to do.")
      }
    }
  }

  def addTags(cas: IndexedContentAddressableStorage, selections: Seq[FileMetaData], tags: Set[String]) {
    selections.par.foreach {
      selection =>
        val newTags = FileMetaData.applyTags(selection.getTags, tags)
        if (newTags.equals(selection.getTags)) {
          Nil
        } else {
          val selectionJson = selection.toJson
          val data = selectionJson.getJSONObject("data")
          data.put("tags", new JSONArray(newTags))

          val derivedMeta = FileMetaData.deriveMeta(selection.getHash, data)
          cas.store(derivedMeta.createBlockContext, new ByteArrayInputStream(derivedMeta.getDataAsString.getBytes("UTF-8")))
        }
    }
    cas.flushIndex()
  }
}