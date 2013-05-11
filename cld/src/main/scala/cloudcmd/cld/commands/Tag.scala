package cloudcmd.cld.commands

import cloudcmd.common.util.JsonUtil
import cloudcmd.common.{IndexedContentAddressableStorage, FileMetaData, FileUtil}
import jpbetz.cli._
import java.io.{ByteArrayInputStream, File, FileInputStream}
import org.json.JSONArray

@SubCommand(name = "tag", description = "Add or remove tags to/from archived files.")
class Tag extends AdapterCommand {

  @Arg(name = "tags", optional = false, isVararg = true) var _tags: java.util.List[String] = null
  @Opt(opt = "r", longOpt = "remove", description = "remove tags", required = false) var _remove: Boolean = false
  @Opt(opt = "i", longOpt = "input", description = "input file", required = false) private var _inputFilePath: String = null

  def execWithAdapter(adapter: IndexedContentAddressableStorage) {
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