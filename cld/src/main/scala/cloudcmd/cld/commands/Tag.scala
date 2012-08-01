package cloudcmd.cld.commands

import cloudcmd.cld.IndexStorageService
import cloudcmd.common.util.{MetaUtil, JsonUtil}
import cloudcmd.common.{FileUtil}
import jpbetz.cli._
import java.io.File
import java.io.FileInputStream

@SubCommand(name = "tag", description = "Add or remove tags to/from archived files.")
class Tag extends Command {

  @Arg(name = "tags", optional = false, isVararg = true) var _tags: java.util.List[String] = null
  @Opt(opt = "r", longOpt = "remove", description = "remove tags", required = false) var _remove: Boolean = false
  @Opt(opt = "i", longOpt = "input", description = "input file", required = false) private var _inputFilePath: String = null

  def exec(commandLine: CommandContext) {
    import scala.collection.JavaConversions._

    val is = if ((_inputFilePath != null)) new FileInputStream(new File(_inputFilePath)) else System.in
    try {
      val selections = JsonUtil.loadJsonArray(is)

      var preparedTags = MetaUtil.prepareTags(_tags.toList)
      if (_remove) {
        preparedTags = preparedTags.map("-" + _)
      }

      val newMeta = IndexStorageService.instance.addTags(selections, preparedTags.toSet)
      System.out.println(newMeta.toString)
    }
    finally {
      if (is ne System.in) FileUtil.SafeClose(is)
    }
  }
}